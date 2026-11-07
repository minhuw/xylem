//! User-space TCP transport over ntcp (Linux only, `ntcp` feature)
//!
//! `NtcpConnectionGroup` runs every connection of a group on one
//! caller-driven ntcp [`Network`], generic over its packet backend (`P`). The
//! group drives the network from its own calls: `add_connection`, `send`,
//! `poll` and `close_all` advance the stack, and `poll` blocks on the
//! backend's file descriptor when the stack has nothing left to do.
//!
//! `NtcpTunFactory` builds groups over an existing Linux TUN device. The
//! `Network` is not `Send`: it is built by `create_group` on the calling
//! (worker) thread and stays there.
//!
//! Limits of this first version: TCP only, user-space timestamps only (no
//! TX timestamps, no backend RX timestamps).

use crate::{
    ConnectionGroup, Error, RecvOutcome, Result, SendReceipt, Timestamp, TransportFactory,
};
use ntcp_io::PacketIo;
use ntcp_native::{
    BlockReason, CloseReason, ConnId, DriveReport, Event, Network, NetworkConfig, ReadOutcome,
    State, TimestampEvent, TxTimestampEvent, TxTimestampOutcome, TxTimestampSource,
    TxTimestampStage, WorkBudget, WriteOptions,
};
use std::collections::HashMap;
use std::hash::Hash;
use std::io::{self, Read};
use std::net::{IpAddr, SocketAddr};
use std::os::fd::{AsRawFd, RawFd};
use std::time::Duration;

/// Work done per category (received packets, transmitted packets, timers,
/// reclaimed buffers) in one `Network::drive` call
///
/// ntcp's own examples use 64 per category; revisit if ntcp recommends
/// other values. A drive that hits a limit reports `more_work`, so the group
/// drives again before it blocks.
const WORK_BUDGET: WorkBudget = WorkBudget::new(64, 64, 64, 64);

/// Work done by the drive that puts a request on the wire at the end of
/// `send`: transmit only
///
/// Following ntcp's guidance, received packets and timers wait for the next
/// `poll` instead of costing every request a full drive.
const FLUSH_BUDGET: WorkBudget = WorkBudget::new(0, WORK_BUDGET.transmit, 0, 0);

/// Number of events taken from the network per `drain_events` call
const EVENT_BATCH: usize = 64;

/// Number of TX timestamp records taken per `drain_timestamps` call
const TIMESTAMP_BATCH: usize = 16;

/// How long `add_connection` waits for the handshake, like the kernel TCP
/// group's effective connect timeout (50 polls of 100 ms)
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long `send` keeps retrying while ntcp's send buffer is full
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// How long `close_all` keeps driving so that FINs and their
/// retransmissions reach the peers
pub const DEFAULT_CLOSE_LINGER: Duration = Duration::from_millis(200);

/// Connection limit of a TUN factory's stacks unless set with
/// [`NtcpTunFactory::with_max_connections`]
///
/// Kept small on purpose: at the pinned ntcp revision each `drive` scans
/// every connection slot, about 0.45 us per slot in a release build (ntcp's
/// default of 1024 costs ~460 us per drive).
pub const DEFAULT_MAX_CONNECTIONS: usize = 64;

/// Timebase of the ticks passed to ntcp: nanoseconds from [`crate::time_ns`]
const UNITS_PER_SECOND: u64 = 1_000_000_000;

fn now_ticks() -> u64 {
    crate::time_ns()
}

fn ticks(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

// =============================================================================
// Slot table
// =============================================================================

/// What the group knows about one connection between calls
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct SlotState {
    /// The handshake completed (`Connected` event); only `add_connection`
    /// reads it
    connected: bool,
    /// Data may be waiting: set by `Readable` events, cleared by a receive
    /// that found nothing (or that did not fill the caller's buffer)
    readable: bool,
    /// The peer's FIN has been read
    eof: bool,
    /// The connection ended (`Closed` event or network stopped); a receive
    /// reports what is left and then the error
    closed: bool,
}

impl SlotState {
    fn ready(&self) -> bool {
        self.readable || self.eof || self.closed
    }
}

struct Slot<H> {
    /// `None` once the connection was closed locally or failed to connect
    handle: Option<H>,
    state: SlotState,
}

/// Maps xylem connection ids to ntcp handles (`H`) and keeps each
/// connection's readiness
///
/// Ids are indices that start at 0 and are never reused, like the kernel
/// groups' ids, even after a connection closes or fails to connect.
struct SlotTable<H> {
    slots: Vec<Slot<H>>,
    by_handle: HashMap<H, usize>,
    live: usize,
}

impl<H: Copy + Eq + Hash> SlotTable<H> {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            by_handle: HashMap::new(),
            live: 0,
        }
    }

    /// Reserve the next id for a connection that is not open yet
    fn reserve(&mut self) -> usize {
        self.slots.push(Slot {
            handle: None,
            state: SlotState::default(),
        });
        self.slots.len() - 1
    }

    /// Attach the open connection `handle` to the reserved id
    fn open(&mut self, id: usize, handle: H) {
        let slot = &mut self.slots[id];
        debug_assert!(slot.handle.is_none());
        slot.handle = Some(handle);
        self.by_handle.insert(handle, id);
        self.live += 1;
    }

    /// The handle of a live connection
    fn handle(&self, id: usize) -> Result<H> {
        match self.slots.get(id) {
            Some(Slot { handle: Some(handle), .. }) => Ok(*handle),
            Some(_) => Err(Error::Connection("Connection closed".to_string())),
            None => Err(Error::Connection("Connection not found".to_string())),
        }
    }

    fn state_mut(&mut self, id: usize) -> &mut SlotState {
        &mut self.slots[id].state
    }

    /// Forget a connection; returns its handle if it was live
    fn remove(&mut self, id: usize) -> Option<H> {
        let handle = self.slots.get_mut(id)?.handle.take()?;
        self.by_handle.remove(&handle);
        self.live -= 1;
        Some(handle)
    }

    fn on_connected(&mut self, handle: H) {
        if let Some(&id) = self.by_handle.get(&handle) {
            self.slots[id].state.connected = true;
        }
    }

    fn on_readable(&mut self, handle: H) {
        if let Some(&id) = self.by_handle.get(&handle) {
            self.slots[id].state.readable = true;
        }
    }

    /// The connection ended; bytes may still be buffered before its end
    fn on_closed(&mut self, handle: H) {
        if let Some(&id) = self.by_handle.get(&handle) {
            let state = &mut self.slots[id].state;
            state.readable = true;
            state.closed = true;
        }
    }

    /// The whole network stopped: every live connection has ended
    fn on_stopped(&mut self) {
        for slot in self.slots.iter_mut().filter(|s| s.handle.is_some()) {
            slot.state.readable = true;
            slot.state.closed = true;
        }
    }

    fn any_ready(&self) -> bool {
        self.slots.iter().any(|s| s.handle.is_some() && s.state.ready())
    }

    /// Clear `ready` and fill it with the ids of live connections to report
    fn collect_ready(&self, ready: &mut Vec<usize>) {
        ready.clear();
        ready.extend(
            self.slots
                .iter()
                .enumerate()
                .filter(|(_, s)| s.handle.is_some() && s.state.ready())
                .map(|(id, _)| id),
        );
    }

    fn live_handles(&self) -> impl Iterator<Item = (usize, H)> + '_ {
        self.slots.iter().enumerate().filter_map(|(id, s)| s.handle.map(|h| (id, h)))
    }

    fn len(&self) -> usize {
        self.live
    }
}

// =============================================================================
// Connection group
// =============================================================================

fn network_error(context: &str, e: ntcp_native::Error) -> Error {
    match e {
        ntcp_native::Error::Backend(e) => {
            Error::Io(io::Error::new(e.kind(), format!("{context}: ntcp packet backend: {e}")))
        }
        ntcp_native::Error::ConnectionClosed(reason) => {
            Error::Connection(format!("{context}: connection closed ({})", reason_text(reason)))
        }
        e => Error::Connection(format!("{context}: {e}")),
    }
}

fn reason_text(reason: Option<CloseReason>) -> String {
    reason.map_or_else(|| "no reason".to_string(), |r| format!("{r:?}"))
}

/// Read 32 bytes from the kernel CSPRNG for ntcp's TCP secret
fn random_secret() -> Result<[u8; 32]> {
    let mut secret = [0; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut secret)?;
    Ok(secret)
}

/// A group of connections on one ntcp network
///
/// Generic over the packet backend; the backend's file descriptor must
/// become readable when packets arrive (and writable when a blocked
/// transmit can proceed), so that `poll` can sleep on it.
pub struct NtcpConnectionGroup<P: PacketIo + AsRawFd> {
    network: Network<P>,
    slots: SlotTable<ConnId>,
    /// Connections closed locally that may still have FINs to (re)transmit
    closing: Vec<ConnId>,
    /// Reused buffer for `drain_events`
    events: Vec<Event>,
    /// The last tick passed to ntcp, which rejects time going backwards
    last_tick: u64,
    /// Source of ticks; tests replace it
    clock: fn() -> u64,
    connect_timeout: Duration,
    send_timeout: Duration,
    close_linger: Duration,
}

impl<P: PacketIo + AsRawFd> NtcpConnectionGroup<P> {
    /// Build the network on the calling thread
    ///
    /// Sets the config's timebase to nanoseconds, the unit of the ticks this
    /// group passes, and generates the TCP secret from `/dev/urandom`.
    pub fn new(mut config: NetworkConfig, io: P) -> Result<Self> {
        config.tcp.connection.timebase.units_per_second = UNITS_PER_SECOND;
        let now = now_ticks();
        let network = Network::new(config, io, random_secret()?, now)
            .map_err(|e| network_error("ntcp network setup", e))?;
        Ok(Self {
            network,
            slots: SlotTable::new(),
            closing: Vec::new(),
            events: vec![Event::NetworkStopped; EVENT_BATCH],
            last_tick: now,
            clock: now_ticks,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            send_timeout: DEFAULT_SEND_TIMEOUT,
            close_linger: DEFAULT_CLOSE_LINGER,
        })
    }

    /// Set how long `add_connection` waits for the handshake
    pub fn set_connect_timeout(&mut self, timeout: Duration) {
        self.connect_timeout = timeout;
    }

    /// Set how long `send` retries while the send buffer is full
    pub fn set_send_timeout(&mut self, timeout: Duration) {
        self.send_timeout = timeout;
    }

    /// Set how long `close_all` drives the network after closing
    pub fn set_close_linger(&mut self, linger: Duration) {
        self.close_linger = linger;
    }

    /// The underlying ntcp network
    pub fn network(&self) -> &Network<P> {
        &self.network
    }

    /// The tick to pass to ntcp: the clock, but never below the last tick
    /// passed, since ntcp fails a drive whose time went backwards (the TSC
    /// clock of `time_ns` may step back when the thread changes CPU)
    fn now(&mut self) -> u64 {
        self.last_tick = self.last_tick.max((self.clock)());
        self.last_tick
    }

    /// The tick `timeout` from now
    fn deadline_after(&mut self, timeout: Duration) -> u64 {
        self.now().saturating_add(ticks(timeout))
    }

    /// Drive the network once with `budget`
    fn drive_with(&mut self, budget: WorkBudget) -> Result<DriveReport> {
        let now = self.now();
        self.network.drive(now, budget).map_err(|e| network_error("ntcp drive", e))
    }

    /// Drive the network once: receive, transmit, timers and reclaim
    fn drive(&mut self) -> Result<DriveReport> {
        self.drive_with(WORK_BUDGET)
    }

    /// Drain every pending event into the slot table; returns whether the
    /// network still reports more
    fn drain(&mut self) -> bool {
        // Each round hands out at most one latch per connection, so a few
        // rounds cover every connection; the bound only guards a livelock
        for _ in 0..1024 {
            let report = self.network.drain_events(&mut self.events);
            for event in &self.events[..report.written] {
                match *event {
                    Event::Connected(id) => self.slots.on_connected(id),
                    Event::Readable(id) => self.slots.on_readable(id),
                    Event::Closed { connection, .. } => self.slots.on_closed(connection),
                    Event::NetworkStopped => self.slots.on_stopped(),
                    // Writable: send drives until its bytes are accepted
                    _ => {}
                }
            }
            if !report.more || report.written == 0 {
                return report.more;
            }
        }
        true
    }

    /// Drive and drain once; returns the drive report and whether events remain
    fn service(&mut self) -> Result<(DriveReport, bool)> {
        let report = self.drive()?;
        let more_events = self.drain();
        Ok((report, more_events))
    }

    /// Wait on the backend fd until it is ready, the next ntcp timer, or
    /// `deadline` (ticks), whichever comes first
    fn wait(&mut self, report: &DriveReport, deadline: u64) -> Result<()> {
        let now = self.now();
        let mut until = deadline;
        if let Some(timer) = report.next_deadline {
            until = until.min(timer);
        }
        let wait_ns = until.saturating_sub(now);
        if wait_ns == 0 {
            return Ok(());
        }
        let events = if report.tx_blocked {
            libc::POLLIN | libc::POLLOUT
        } else {
            libc::POLLIN
        };
        wait_fd(self.network.backend().as_raw_fd(), events, wait_ns)
    }

    /// Whether every locally closed connection has finished its close
    ///
    /// FIN-WAIT-2 counts as finished: our FIN was ACKed, and only the peer's
    /// FIN is missing, which a server that never closes does not send (Linux
    /// bounds that state with tcp_fin_timeout rather than wait for it).
    fn closes_done(&mut self) -> bool {
        let network = &self.network;
        self.closing.retain(|&id| match network.connection_info(id) {
            Ok(info) => !matches!(info.state, State::Closed | State::TimeWait | State::FinWait2),
            Err(_) => false,
        });
        self.closing.is_empty()
    }
}

/// `ppoll` one fd for `events`, up to `wait_ns` nanoseconds
fn wait_fd(fd: RawFd, events: libc::c_short, wait_ns: u64) -> Result<()> {
    let mut pollfd = libc::pollfd { fd, events, revents: 0 };
    let timeout = libc::timespec {
        tv_sec: (wait_ns / 1_000_000_000).min(i64::MAX as u64) as libc::time_t,
        tv_nsec: (wait_ns % 1_000_000_000) as libc::c_long,
    };
    // SAFETY: one valid pollfd and a valid timespec, both live for the call
    let ret = unsafe { libc::ppoll(&mut pollfd, 1, &timeout, std::ptr::null()) };
    if ret < 0 {
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e.into());
        }
    }
    Ok(())
}

impl<P: PacketIo + AsRawFd> ConnectionGroup for NtcpConnectionGroup<P> {
    fn add_connection(&mut self, target: &SocketAddr) -> Result<usize> {
        // Take an id only once ntcp accepted the connection, so that a
        // refused `connect` leaves nothing behind
        let conn = self.network.connect(*target).map_err(|e| network_error("ntcp connect", e))?;
        let id = self.slots.reserve();
        // Attach the handle right away: the drains below apply its
        // `Connected` or `Closed` event to the slot, just like other
        // connections' events
        self.slots.open(id, conn);
        let deadline = self.deadline_after(self.connect_timeout);

        // ntcp latches `Connected` once, when the handshake reaches
        // ESTABLISHED (or CLOSE-WAIT)
        loop {
            let (report, more_events) = match self.service() {
                Ok(service) => service,
                Err(e) => {
                    self.slots.remove(id);
                    let _ = self.network.abort(conn);
                    return Err(e);
                }
            };
            let state = self.slots.state_mut(id);
            if state.connected {
                break;
            }
            if state.closed {
                self.slots.remove(id);
                let reason = self.network.connection_info(conn).ok().and_then(|i| i.reason);
                let _ = self.network.close(conn);
                return Err(Error::Connection(format!(
                    "Connection to {target} failed: {}",
                    reason_text(reason)
                )));
            }
            if self.now() >= deadline {
                self.slots.remove(id);
                let _ = self.network.abort(conn);
                return Err(Error::Connection("Connection timeout".to_string()));
            }
            if !report.more_work && !more_events {
                self.wait(&report, deadline)?;
            }
        }

        // Requests are small and latency-bound: never hold them back (Nagle)
        if let Err(e) = self.network.set_nodelay(conn, true) {
            self.slots.remove(id);
            let _ = self.network.abort(conn);
            return Err(network_error("ntcp set_nodelay", e));
        }
        Ok(id)
    }

    fn send(&mut self, conn_id: usize, data: &[u8]) -> Result<SendReceipt> {
        let conn = self.slots.handle(conn_id)?;
        let mut sent = 0;
        let mut deadline = None;
        while sent < data.len() {
            match self.network.try_write(conn, &data[sent..], WriteOptions::default()) {
                Ok(receipt) => sent += receipt.accepted,
                Err(e) if e.is_would_block() => {}
                Err(e) => return Err(network_error("ntcp send", e)),
            }
            if sent < data.len() {
                // Only a partial write or a full buffer gets here: let the
                // stack transmit and take ACKs, within the send timeout
                let deadline = match deadline {
                    Some(deadline) => deadline,
                    None => *deadline.insert(self.deadline_after(self.send_timeout)),
                };
                if self.now() >= deadline {
                    return Err(Error::Connection(format!(
                        "Send timeout: {} of {} bytes accepted",
                        sent,
                        data.len()
                    )));
                }
                // A full drive: send space frees only as received ACKs are
                // processed
                self.drive()?;
            }
        }
        let timestamp = Timestamp::now();
        // Put the request on the wire now rather than at the next poll
        self.drive_with(FLUSH_BUDGET)?;
        Ok(SendReceipt { timestamp, tx: None })
    }

    fn recv(&mut self, conn_id: usize, buf: &mut [u8]) -> Result<RecvOutcome> {
        let conn = self.slots.handle(conn_id)?;
        match self.network.try_read(conn, buf) {
            Ok(ReadOutcome::Data { len, .. }) => {
                // ntcp's `try_read` copies min(buf.len(), readable bytes) as
                // of the call (ntcp-native `Network::try_read`, confirmed by
                // the ntcp maintainer), so a read that did not fill the
                // buffer took every byte that was there; it also rearms the
                // Readable event for the next arrival
                self.slots.state_mut(conn_id).readable = len == buf.len();
                Ok(RecvOutcome::Data { len, timestamp: Timestamp::now() })
            }
            Ok(ReadOutcome::Eof) => {
                self.slots.state_mut(conn_id).eof = true;
                Ok(RecvOutcome::Eof)
            }
            Ok(ReadOutcome::EmptyBuffer) => Ok(RecvOutcome::WouldBlock),
            Err(ntcp_native::Error::WouldBlock(BlockReason::Receive | BlockReason::Connecting)) => {
                self.slots.state_mut(conn_id).readable = false;
                Ok(RecvOutcome::WouldBlock)
            }
            Err(e) => Err(network_error("ntcp recv", e)),
        }
    }

    fn poll(&mut self, timeout: Duration, ready: &mut Vec<usize>) -> Result<()> {
        let deadline = self.deadline_after(timeout);
        loop {
            let (report, more_events) = self.service()?;
            if self.slots.any_ready() || self.now() >= deadline {
                break;
            }
            if !report.more_work && !more_events {
                self.wait(&report, deadline)?;
            }
        }
        self.slots.collect_ready(ready);
        Ok(())
    }

    fn len(&self) -> usize {
        self.slots.len()
    }

    fn close(&mut self, conn_id: usize) -> Result<()> {
        if let Some(conn) = self.slots.remove(conn_id) {
            // The FIN goes out on a later drive (poll or close_all). A
            // connection that already ended may refuse; it is gone either way
            if self.network.close(conn).is_ok() {
                self.closing.push(conn);
            }
        }
        Ok(())
    }

    fn close_all(&mut self) -> Result<()> {
        let ids: Vec<usize> = self.slots.live_handles().map(|(id, _)| id).collect();
        for id in ids {
            self.close(id)?;
        }

        // ntcp frees a closed connection's slot once its `Closed` event and
        // all its TX timestamp records were drained. None are requested yet,
        // but drain them anyway; the scratch records need a real ConnId
        let mut stamps = self.closing.first().map(|&connection| {
            let placeholder = TimestampEvent {
                connection,
                observation: TxTimestampEvent {
                    cookie: 0,
                    target: 0,
                    stage: TxTimestampStage::Transmit,
                    source: TxTimestampSource::Software,
                    outcome: TxTimestampOutcome::Cancelled,
                },
            };
            vec![placeholder; TIMESTAMP_BATCH]
        });

        // Keep driving so the FINs, their retransmissions and the final
        // ACKs reach the peers; dropping the network sends nothing
        let deadline = self.deadline_after(self.close_linger);
        loop {
            let (report, more_events) = match self.service() {
                Ok(service) => service,
                // A stopped network cannot send anything more
                Err(_) if self.network.is_stopped() => return Ok(()),
                Err(e) => return Err(e),
            };
            if let Some(stamps) = &mut stamps {
                while self.network.drain_timestamps(stamps).more {}
            }
            // Quiet once every close finished and the last drive had nothing
            // left to transmit. Not `more_work`: at the pinned ntcp revision
            // a closed connection in FIN-WAIT-2 makes every drive report it
            let done = self.closes_done();
            let quiet = done && !report.tx_blocked && report.transmitted == 0;
            if quiet || self.now() >= deadline {
                if !quiet {
                    tracing::debug!(
                        "ntcp: {} connection(s) still closing after the {:?} linger",
                        self.closing.len(),
                        self.close_linger
                    );
                }
                return Ok(());
            }
            if !report.more_work && !more_events {
                self.wait(&report, deadline)?;
            }
        }
    }
}

// =============================================================================
// TUN factory
// =============================================================================

/// Factory for ntcp connection groups over an existing Linux TUN device
///
/// The device must exist (`ip tuntap add dev NAME mode tun`, which implies
/// `IFF_NO_PI`) and be up, with a kernel route towards the targets. `local_ip`
/// is ntcp's own address on that link, not an address the kernel owns.
#[derive(Debug, Clone)]
pub struct NtcpTunFactory {
    ifname: String,
    local_ip: IpAddr,
    mtu: u32,
    max_connections: usize,
}

impl NtcpTunFactory {
    pub fn new(ifname: impl Into<String>, local_ip: IpAddr, mtu: u32) -> Self {
        Self {
            ifname: ifname.into(),
            local_ip,
            mtu,
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }

    /// Size each group's stack for `max_connections` connections (at least 1)
    ///
    /// Give the number of connections the group will open: at the pinned
    /// ntcp revision every `drive` costs time in proportion to this limit,
    /// whether or not the connections exist.
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections.max(1);
        self
    }
}

impl TransportFactory for NtcpTunFactory {
    type Group = NtcpConnectionGroup<ntcp_io_tun::Tun>;

    fn create_group(&self) -> Result<Self::Group> {
        let tun = ntcp_io_tun::Tun::open(&self.ifname).map_err(|e| {
            Error::Io(io::Error::new(e.kind(), format!("open TUN '{}': {e}", self.ifname)))
        })?;
        let mut config = NetworkConfig::new(self.local_ip, self.mtu);
        config.tcp.max_connections = self.max_connections;
        NtcpConnectionGroup::new(config, tun)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntcp_io::{PacketLayer, TxOutcome};
    use ntcp_native::{ListenerId, ReadOutcome};
    use std::cell::Cell;
    use std::net::SocketAddrV6;
    use std::os::unix::net::UnixDatagram;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    // =========================================================================
    // Slot table
    // =========================================================================

    #[test]
    fn ids_are_never_reused() {
        let mut table = SlotTable::<u32>::new();
        let a = table.reserve();
        table.open(a, 10);
        let failed = table.reserve();
        let b = table.reserve();
        table.open(b, 11);
        assert_eq!((a, failed, b), (0, 1, 2));
        assert_eq!(table.len(), 2);

        assert_eq!(table.remove(a), Some(10));
        assert_eq!(table.remove(a), None);
        assert_eq!(table.reserve(), 3);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn handle_lookup_distinguishes_closed_and_unknown() {
        let mut table = SlotTable::<u32>::new();
        let a = table.reserve();
        table.open(a, 7);
        assert_eq!(table.handle(a).unwrap(), 7);
        let failed = table.reserve();
        assert!(matches!(table.handle(failed), Err(Error::Connection(m)) if m.contains("closed")));
        assert!(matches!(table.handle(99), Err(Error::Connection(m)) if m.contains("not found")));
        table.remove(a);
        assert!(matches!(table.handle(a), Err(Error::Connection(m)) if m.contains("closed")));
    }

    #[test]
    fn readiness_is_level_triggered() {
        let mut table = SlotTable::<u32>::new();
        let mut ready = vec![usize::MAX];
        for handle in 0..3 {
            let id = table.reserve();
            table.open(id, handle);
        }
        table.collect_ready(&mut ready);
        assert!(ready.is_empty());
        assert!(!table.any_ready());

        // Stays ready until a receive clears it
        table.on_readable(1);
        table.on_readable(42); // unknown handles are ignored
        table.collect_ready(&mut ready);
        assert_eq!(ready, [1]);
        table.collect_ready(&mut ready);
        assert_eq!(ready, [1]);
        table.state_mut(1).readable = false;
        table.collect_ready(&mut ready);
        assert!(ready.is_empty());

        // EOF and close stay ready: the worker closes the connection
        table.state_mut(0).eof = true;
        table.on_closed(2);
        table.collect_ready(&mut ready);
        assert_eq!(ready, [0, 2]);
        table.state_mut(2).readable = false;
        table.collect_ready(&mut ready);
        assert_eq!(ready, [0, 2]);

        // Closed connections are never reported
        table.remove(0);
        table.remove(2);
        table.collect_ready(&mut ready);
        assert!(ready.is_empty());
        table.on_readable(2);
        assert!(!table.any_ready());
    }

    #[test]
    fn network_stop_ends_every_live_connection() {
        let mut table = SlotTable::<u32>::new();
        for handle in 0..3 {
            let id = table.reserve();
            table.open(id, handle);
        }
        table.remove(1);
        table.on_stopped();
        let mut ready = Vec::new();
        table.collect_ready(&mut ready);
        assert_eq!(ready, [0, 2]);
        assert!(table.state_mut(0).closed);
        assert!(!table.state_mut(1).closed);
    }

    // =========================================================================
    // Group over a simulated IP link
    // =========================================================================

    /// One end of a raw-IP link: a nonblocking Unix datagram socket pair
    struct Wire(UnixDatagram);

    fn link() -> (Wire, Wire) {
        let (a, b) = UnixDatagram::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        (Wire(a), Wire(b))
    }

    impl PacketIo for Wire {
        fn layer(&self) -> PacketLayer {
            PacketLayer::Ip
        }
        fn receive(&mut self, out: &mut [u8]) -> io::Result<Option<usize>> {
            match self.0.recv(out) {
                Ok(len) => Ok(Some(len)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
                Err(e) => Err(e),
            }
        }
        fn transmit(&mut self, packet: &[u8]) -> io::Result<TxOutcome> {
            match self.0.send(packet) {
                Ok(_) => Ok(TxOutcome::Submitted),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(TxOutcome::WouldBlock),
                Err(e) => Err(e),
            }
        }
    }

    impl AsRawFd for Wire {
        fn as_raw_fd(&self) -> RawFd {
            self.0.as_raw_fd()
        }
    }

    const CLIENT: &str = "10.99.0.2";
    const SERVER: &str = "10.99.0.1";
    const PORT: u16 = 7000;

    fn network_config(address: &str) -> NetworkConfig {
        let mut config = NetworkConfig::new(address.parse().unwrap(), 1500);
        config.tcp.max_connections = 16;
        config
    }

    /// When the echo server closes a connection
    #[derive(Clone, Copy)]
    enum ServerClose {
        /// When the client's FIN arrives
        OnEof,
        /// After echoing this many bytes
        After(usize),
        /// Never: the connection stays half-open after the client's FIN
        Never,
    }

    /// An ntcp echo server on its own thread
    struct EchoServer {
        stop: Arc<AtomicBool>,
        handle: Option<std::thread::JoinHandle<()>>,
    }

    impl EchoServer {
        fn start(wire: Wire, listen: bool, close: ServerClose) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let stop_flag = stop.clone();
            let handle =
                std::thread::spawn(move || run_echo_server(wire, listen, close, &stop_flag));
            Self { stop, handle: Some(handle) }
        }
    }

    impl Drop for EchoServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn run_echo_server(wire: Wire, listen: bool, close: ServerClose, stop: &AtomicBool) {
        let mut config = network_config(SERVER);
        config.tcp.connection.timebase.units_per_second = UNITS_PER_SECOND;
        let mut network =
            Network::new(config, wire, random_secret().unwrap(), now_ticks()).unwrap();
        let listener: Option<ListenerId> = listen.then(|| network.listen(PORT, 16).unwrap());
        let mut conns: Vec<(ConnId, usize)> = Vec::new();
        let mut events = [Event::NetworkStopped; 16];
        let mut buf = [0u8; 4096];
        while !stop.load(Ordering::Relaxed) {
            network.drive(now_ticks(), WORK_BUDGET).unwrap();
            network.drain_events(&mut events);
            if let Some(listener) = listener {
                while let Ok(id) = network.accept(listener) {
                    conns.push((id, 0));
                }
            }
            conns.retain_mut(|(id, echoed)| {
                let open = echo_once(&mut network, *id, &mut buf, echoed);
                let close_now = match close {
                    ServerClose::OnEof => !open,
                    ServerClose::After(n) => !open || *echoed >= n,
                    ServerClose::Never => false,
                };
                if close_now {
                    let _ = network.close(*id);
                }
                open && !close_now
            });
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    /// Echo what one read returns; false once the connection ended
    fn echo_once(
        network: &mut Network<Wire>,
        id: ConnId,
        buf: &mut [u8],
        echoed: &mut usize,
    ) -> bool {
        let len = match network.try_read(id, buf) {
            Ok(ReadOutcome::Data { len, .. }) => len,
            Ok(ReadOutcome::Eof) | Err(ntcp_native::Error::ConnectionClosed(_)) => return false,
            _ => return true,
        };
        let mut sent = 0;
        while sent < len {
            match network.try_write(id, &buf[sent..len], WriteOptions::default()) {
                Ok(receipt) => sent += receipt.accepted,
                Err(_) => {
                    network.drive(now_ticks(), WORK_BUDGET).unwrap();
                }
            }
        }
        *echoed += len;
        true
    }

    fn client(wire: Wire) -> NtcpConnectionGroup<Wire> {
        NtcpConnectionGroup::new(network_config(CLIENT), wire).unwrap()
    }

    fn server_addr() -> SocketAddr {
        SocketAddr::new(SERVER.parse().unwrap(), PORT)
    }

    /// Poll until `id` is ready, then receive until `want` bytes arrived
    fn recv_exact(group: &mut NtcpConnectionGroup<Wire>, id: usize, want: usize) -> Vec<u8> {
        let mut out = Vec::new();
        let mut ready = Vec::new();
        let mut buf = [0u8; 1024];
        let deadline = Instant::now() + Duration::from_secs(5);
        while out.len() < want {
            assert!(Instant::now() < deadline, "timed out with {} of {want} bytes", out.len());
            group.poll(Duration::from_millis(100), &mut ready).unwrap();
            if !ready.contains(&id) {
                continue;
            }
            if let RecvOutcome::Data { len, .. } = group.recv(id, &mut buf).unwrap() {
                out.extend_from_slice(&buf[..len]);
            }
        }
        out
    }

    #[test]
    fn echo_round_trips_and_readiness() {
        let (a, b) = link();
        let _server = EchoServer::start(b, true, ServerClose::OnEof);
        let mut group = client(a);

        let first = group.add_connection(&server_addr()).unwrap();
        let second = group.add_connection(&server_addr()).unwrap();
        assert_eq!((first, second), (0, 1));
        assert_eq!(group.len(), 2);

        let receipt = group.send(first, b"hello ntcp").unwrap();
        assert!(receipt.tx.is_none());
        assert_eq!(recv_exact(&mut group, first, 10), b"hello ntcp");

        // Drained: the next receive finds nothing and clears the readiness
        let mut buf = [0u8; 64];
        assert!(matches!(group.recv(first, &mut buf).unwrap(), RecvOutcome::WouldBlock));
        let mut ready = vec![usize::MAX];
        group.poll(Duration::ZERO, &mut ready).unwrap();
        assert!(ready.is_empty());

        // Larger than the 64 KiB send buffer: send drives until accepted
        let big: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        group.send(second, &big).unwrap();
        assert_eq!(recv_exact(&mut group, second, big.len()), big);

        group.close(first).unwrap();
        assert_eq!(group.len(), 1);
        assert!(matches!(group.send(first, b"x"), Err(Error::Connection(_))));
        assert!(matches!(group.recv(99, &mut buf), Err(Error::Connection(_))));
        group.close_all().unwrap();
        assert_eq!(group.len(), 0);
    }

    #[test]
    fn peer_close_reports_eof() {
        let (a, b) = link();
        let _server = EchoServer::start(b, true, ServerClose::After(4));
        let mut group = client(a);
        let id = group.add_connection(&server_addr()).unwrap();
        group.send(id, b"ping").unwrap();
        assert_eq!(recv_exact(&mut group, id, 4), b"ping");

        let mut ready = Vec::new();
        let mut buf = [0u8; 64];
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "no EOF");
            group.poll(Duration::from_millis(100), &mut ready).unwrap();
            if ready.contains(&id) && matches!(group.recv(id, &mut buf).unwrap(), RecvOutcome::Eof)
            {
                break;
            }
        }
        // EOF stays reported until the connection is closed
        group.poll(Duration::ZERO, &mut ready).unwrap();
        assert_eq!(ready, [id]);
        group.close(id).unwrap();
        group.poll(Duration::ZERO, &mut ready).unwrap();
        assert!(ready.is_empty());
    }

    #[test]
    fn refused_connection_fails_and_burns_its_id() {
        let (a, b) = link();
        // No listener: the server stack answers the SYN with a RST
        let _server = EchoServer::start(b, false, ServerClose::OnEof);
        let mut group = client(a);
        let err = group.add_connection(&server_addr()).unwrap_err();
        assert!(matches!(err, Error::Connection(_)), "{err}");
        assert_eq!(group.len(), 0);
        assert!(matches!(group.send(0, b"x"), Err(Error::Connection(_))));
    }

    #[test]
    fn unanswered_connect_times_out() {
        // Nobody reads the other end of the link
        let (a, _b) = link();
        let mut group = client(a);
        group.set_connect_timeout(Duration::from_millis(300));
        let started = Instant::now();
        let err = group.add_connection(&server_addr()).unwrap_err();
        assert!(matches!(&err, Error::Connection(m) if m.contains("timeout")), "{err}");
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn poll_waits_for_its_timeout_when_idle() {
        let (a, b) = link();
        let _server = EchoServer::start(b, true, ServerClose::OnEof);
        let mut group = client(a);
        group.add_connection(&server_addr()).unwrap();
        let mut ready = Vec::new();
        let started = Instant::now();
        group.poll(Duration::from_millis(50), &mut ready).unwrap();
        assert!(ready.is_empty());
        assert!(started.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn rejected_connect_takes_no_id() {
        let (a, b) = link();
        let _server = EchoServer::start(b, true, ServerClose::OnEof);
        let mut group = client(a);
        // ntcp refuses OS scope ids before opening anything
        let scoped = SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), PORT, 0, 1));
        for _ in 0..3 {
            assert!(group.add_connection(&scoped).is_err());
        }
        assert_eq!(group.len(), 0);
        assert_eq!(group.add_connection(&server_addr()).unwrap(), 0);
        assert_eq!(group.slots.slots.len(), 1);
    }

    #[test]
    fn connect_completes_on_its_event_and_keeps_other_events() {
        let (a, b) = link();
        let _server = EchoServer::start(b, true, ServerClose::OnEof);
        let mut group = client(a);
        let first = group.add_connection(&server_addr()).unwrap();
        assert!(group.slots.state_mut(first).connected);

        // The echo arrives while the next connect drives and drains the
        // network: its Readable event must reach the first slot
        group.send(first, b"early").unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let second = group.add_connection(&server_addr()).unwrap();
        assert!(group.slots.state_mut(second).connected);
        assert!(group.slots.state_mut(first).readable);
        let mut ready = Vec::new();
        group.poll(Duration::ZERO, &mut ready).unwrap();
        assert_eq!(ready, [first]);
        assert_eq!(recv_exact(&mut group, first, 5), b"early");
    }

    #[test]
    fn close_all_does_not_wait_for_a_peer_that_never_closes() {
        let (a, b) = link();
        let _server = EchoServer::start(b, true, ServerClose::Never);
        let mut group = client(a);
        for _ in 0..3 {
            group.add_connection(&server_addr()).unwrap();
        }
        group.set_close_linger(Duration::from_secs(5));
        let started = Instant::now();
        group.close_all().unwrap();
        assert!(started.elapsed() < Duration::from_secs(1), "{:?}", started.elapsed());
        assert!(group.closing.is_empty());
    }

    thread_local! {
        /// How far `stepped_clock` runs ahead of the real clock
        static CLOCK_AHEAD: Cell<u64> = const { Cell::new(0) };
    }

    fn stepped_clock() -> u64 {
        now_ticks() + CLOCK_AHEAD.with(Cell::get)
    }

    #[test]
    fn ticks_never_go_backwards() {
        let (a, b) = link();
        let _server = EchoServer::start(b, true, ServerClose::OnEof);
        let mut group = client(a);
        CLOCK_AHEAD.with(|c| c.set(50_000_000));
        group.clock = stepped_clock;
        let id = group.add_connection(&server_addr()).unwrap();
        let before = group.network().now();

        // The clock steps back 50 ms: ntcp keeps getting the last tick
        CLOCK_AHEAD.with(|c| c.set(0));
        group.send(id, b"tick").unwrap();
        let mut ready = Vec::new();
        group.poll(Duration::ZERO, &mut ready).unwrap();
        assert!(group.network().now() >= before);
        assert_eq!(recv_exact(&mut group, id, 4), b"tick");
        group.close_all().unwrap();
    }
}
