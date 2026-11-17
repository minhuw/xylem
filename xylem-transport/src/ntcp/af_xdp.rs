//! ntcp over AF_XDP sockets (`ntcp-af-xdp` feature)
//!
//! [`NtcpAfXdpPort`] attaches ntcp's XDP program to one interface, hands it
//! the members' IPv4 addresses, steers each address to its member's RX
//! queue (and ARP to the first member's queue) with ethtool ntuple rules,
//! and shares one ARP neighbour table among the members. [`NtcpAfXdpFactory`]
//! then builds a member's group on an AF_XDP socket of its queue, on the
//! worker thread.
//!
//! The kernel keeps the interface: the program hands only frames for the
//! members' addresses (IPv4 to them, ARP asking for them) to the sockets and
//! passes everything else on, so the interface's own addresses and traffic
//! keep working during a run. Sockets run in copy mode, with software
//! timestamps only: there is no NIC clock. Queues are busy-polled.
//!
//! Attaching the program and installing rules needs CAP_BPF, CAP_NET_ADMIN
//! and CAP_NET_RAW (or root). The program is detached when the port and
//! every group are dropped, or by the kernel when the process exits.
//!
//! Each socket registers a UMEM of ntcp's default geometry, 16384 frames of
//! 2048 bytes (32 MiB), whose frames ntcp holds by reference until the data
//! they carry is read. Without CAP_IPC_LOCK the kernel charges every UMEM
//! to the user's locked memory and refuses one past RLIMIT_MEMLOCK;
//! `create_group` then fails naming the limit and the size the port needs.

use super::{NtcpBackend, NtcpConnectionGroup, UNITS_PER_SECOND};
use crate::{Error, Result, TransportFactory};
/// Where ntcp's XDP program runs: in the driver or on socket buffers
pub use ntcp_io_af_xdp::XdpMode;
use ntcp_io_af_xdp::{Address, HostedQueue, Port, PortConfig, QueueHandle, Rule};
use ntcp_native::{EthernetNextHop, Link, NetworkConfig, SharedNeighborLimits, SharedNeighbors};
use std::collections::HashSet;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::os::fd::RawFd;
use std::sync::{Arc, Mutex};

/// Dynamic ARP entries of the port's shared table
const NEIGHBOR_ENTRIES: usize = 64;

/// ARP replies the shared table queues for transmission
const NEIGHBOR_REPLIES: usize = 16;

impl NtcpBackend for HostedQueue {
    fn wait_fd(&self) -> Option<RawFd> {
        None
    }
}

/// One member of the port: a worker thread with its own RX/TX queue pair,
/// IPv4 address and ntcp network
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NtcpAfXdpMember {
    /// The member's own address, steered to its queue
    pub local_ip: Ipv4Addr,
    /// The interface queue pair the member's socket binds to
    pub queue: u32,
    /// Connections the member opens: sizes its stack and its share of the
    /// neighbour table's held output
    pub max_connections: usize,
}

/// An interface and the members that share it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NtcpAfXdpConfig {
    /// Interface name, in the network namespace of the thread that starts
    /// the port
    pub interface: String,
    /// XDP in the driver (`Native`) or on socket buffers (`Generic`, for
    /// any device, such as a veth)
    pub mode: XdpMode,
    /// IP MTU of the members' networks and sockets
    pub mtu: u32,
    /// On-link prefix of the members' addresses: other destinations go
    /// through `gateway`
    pub prefix_len: u8,
    pub gateway: Option<Ipv4Addr>,
    pub members: Vec<NtcpAfXdpMember>,
}

/// An interface with ntcp's XDP program attached, shared by the members'
/// groups
///
/// Keep it until every group is dropped: it holds the members' addresses,
/// without which the program passes their frames to the kernel, and the
/// rules that steer them to the members' queues.
pub struct NtcpAfXdpPort {
    config: NtcpAfXdpConfig,
    neighbors: SharedNeighbors,
    /// Queue pairs not yet taken by a member's group
    queues: Vec<Mutex<Option<QueueHandle>>>,
    /// Bytes of each socket's UMEM, frame size times frame count
    umem_bytes: u64,
    /// Held, not read: deleted from the interface on drop
    _rules: Vec<Rule>,
    /// Held, not read: the program hands frames for an address to the
    /// sockets only while its `Address` lives
    _addresses: Vec<Address>,
    port: Port,
}

fn io_error(context: &str, e: io::Error) -> Error {
    Error::Io(io::Error::new(e.kind(), format!("{context}: {e}")))
}

fn ntcp_error(context: &str, e: ntcp_native::Error) -> Error {
    Error::Config(format!("{context}: {e}"))
}

/// The soft RLIMIT_MEMLOCK in bytes, which bounds the locked memory the
/// kernel charges AF_XDP UMEMs to; None if unlimited or unreadable
fn memlock_limit() -> Option<u64> {
    let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: writes one rlimit into `limit`
    if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) } != 0
        || limit.rlim_cur == libc::RLIM_INFINITY
    {
        return None;
    }
    Some(limit.rlim_cur)
}

/// The error of a socket that could not be opened on `queue`, of a port
/// with `queues` sockets of `umem_bytes` each, under RLIMIT_MEMLOCK
/// `memlock` ([`memlock_limit`])
///
/// Without CAP_IPC_LOCK, the kernel charges each UMEM to the user's locked
/// memory and refuses its registration (XDP_UMEM_REG) with ENOBUFS once
/// that would pass RLIMIT_MEMLOCK (xdp_umem_account_pages); ntcp passes
/// the error on as is. With the limit unlimited, ENOBUFS has another cause
/// and keeps its plain message.
fn socket_error(
    e: io::Error,
    queue: u32,
    queues: usize,
    umem_bytes: u64,
    memlock: Option<u64>,
) -> Error {
    let context = format!("open an AF_XDP socket on queue {queue}");
    let limit = match (e.raw_os_error(), memlock) {
        (Some(libc::ENOBUFS), Some(limit)) => limit,
        _ => return io_error(&context, e),
    };
    let needed = umem_bytes * queues as u64;
    Error::Io(io::Error::new(
        e.kind(),
        format!(
            "{context}: the kernel refused to lock its {} KiB UMEM in memory ({e}): without \
             CAP_IPC_LOCK it charges every AF_XDP UMEM to the user's locked memory, bounded \
             by RLIMIT_MEMLOCK, {} KiB here, and this port's {queues} sockets need {} KiB, \
             besides any other UMEM of the same user. Raise the memlock limit to at least {} \
             KiB (ulimit -l, LimitMEMLOCK= of a systemd unit, /etc/security/limits.conf) or \
             grant xylem CAP_IPC_LOCK",
            umem_bytes / 1024,
            limit / 1024,
            needed / 1024,
            needed / 1024,
        ),
    ))
}

/// Members need distinct addresses and distinct queues: a queue has one
/// socket, and an address belongs to one member
fn check_members(members: &[NtcpAfXdpMember]) -> Result<()> {
    if members.is_empty() {
        return Err(Error::Config("an AF_XDP port needs at least one member".to_string()));
    }
    let mut ips = HashSet::new();
    let mut queues = HashSet::new();
    for member in members {
        if !ips.insert(member.local_ip) {
            return Err(Error::Config(format!(
                "AF_XDP members share the address {}",
                member.local_ip
            )));
        }
        if !queues.insert(member.queue) {
            return Err(Error::Config(format!("AF_XDP members share queue {}", member.queue)));
        }
    }
    Ok(())
}

impl NtcpAfXdpPort {
    /// Attach the program to the interface, add the members' addresses and
    /// install their steering rules, on the calling thread
    pub fn start(config: NtcpAfXdpConfig) -> Result<Arc<Self>> {
        check_members(&config.members)?;
        let mut port_config = PortConfig::new(config.interface.clone());
        port_config.mode = config.mode;
        port_config.mtu = config.mtu;
        let umem_bytes = u64::from(port_config.frame_size) * u64::from(port_config.frame_count);
        let port = Port::attach(port_config).map_err(|e| {
            io_error(&format!("attach ntcp's XDP program to {}", config.interface), e)
        })?;
        let mac = port
            .mac_address()
            .map_err(|e| io_error("read the interface's MAC address", e))?;
        // Drivers without channel reporting give no count: rules are then
        // required and the sockets' binds find out about unusable queues
        let enabled = port.enabled_queues().ok();
        tracing::info!(
            "ntcp AF_XDP: {} (ifindex {}) mode={:?} mac={} queues={} enabled={enabled:?} \
             umem={} KiB per socket",
            config.interface,
            port.ifindex(),
            port.mode(),
            mac.map(|o| format!("{o:02x}")).join(":"),
            port.queues(),
            umem_bytes / 1024,
        );
        if let Some(enabled) = enabled {
            if let Some(member) = config.members.iter().find(|m| m.queue >= enabled) {
                return Err(Error::Config(format!(
                    "{} has {enabled} enabled RX queues; {} uses queue {}",
                    config.interface, member.local_ip, member.queue
                )));
            }
        }

        let members = config.members.len();
        let neighbors = SharedNeighbors::new(
            mac,
            UNITS_PER_SECOND,
            SharedNeighborLimits {
                members,
                entries: NEIGHBOR_ENTRIES,
                // Each member reserves max_connections + 1 output owners
                held: config.members.iter().map(|m| m.max_connections.max(1) + 1).sum(),
                replies: NEIGHBOR_REPLIES,
            },
        )
        .map_err(|e| ntcp_error("ntcp shared neighbour table", e))?;

        let mut addresses = Vec::with_capacity(members);
        for member in &config.members {
            let address = port
                .add_ipv4(member.local_ip)
                .map_err(|e| io_error(&format!("add {} to the XDP program", member.local_ip), e))?;
            addresses.push(address);
        }
        // With one enabled RX queue every frame arrives there, and devices
        // with one queue (a default veth) often have no ntuple filters
        let single = enabled == Some(1);
        let mut rules = Vec::with_capacity(members + 1);
        let mut steer = |what: String, rule: io::Result<Rule>| match rule {
            Ok(rule) => {
                tracing::info!("ntcp AF_XDP: rule {what} at location {}", rule.location());
                rules.push(rule);
                Ok(())
            }
            Err(e) if single && e.kind() == io::ErrorKind::Unsupported => {
                tracing::info!("ntcp AF_XDP: no rule {what} ({e}); the only RX queue receives all");
                Ok(())
            }
            Err(e) => Err(io_error(&format!("steer {what}"), e)),
        };
        for member in &config.members {
            steer(
                format!("{} -> queue {}", member.local_ip, member.queue),
                port.steer_ipv4(member.local_ip, member.queue),
            )?;
        }
        // Any member may handle ARP for all of them through the shared table
        let arp = config.members[0].queue;
        steer(format!("ARP -> queue {arp}"), port.steer_arp(arp))?;

        let queues = config
            .members
            .iter()
            .map(|member| {
                let handle = port
                    .take_queue(member.queue)
                    .map_err(|e| io_error(&format!("take AF_XDP queue {}", member.queue), e))?;
                Ok(Mutex::new(Some(handle)))
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new(Self {
            config,
            neighbors,
            queues,
            umem_bytes,
            _rules: rules,
            _addresses: addresses,
            port,
        }))
    }

    /// The factory of member `member`'s group
    pub fn factory(self: &Arc<Self>, member: usize) -> NtcpAfXdpFactory {
        assert!(member < self.config.members.len(), "no AF_XDP member {member}");
        NtcpAfXdpFactory { port: Arc::clone(self), member }
    }

    /// Frames for the members' addresses that arrived on a queue without a
    /// socket and were dropped, since the port started
    pub fn unsteered(&self) -> io::Result<u64> {
        self.port.unsteered()
    }
}

/// Builds one member's group on its queue pair of an [`NtcpAfXdpPort`]
///
/// A member has one queue pair, so its factory creates one group.
#[derive(Clone)]
pub struct NtcpAfXdpFactory {
    port: Arc<NtcpAfXdpPort>,
    member: usize,
}

impl TransportFactory for NtcpAfXdpFactory {
    type Group = NtcpConnectionGroup<HostedQueue>;

    /// Open the member's socket on the calling thread, which keeps it, and
    /// build its network
    fn create_group(&self) -> Result<Self::Group> {
        let port = &self.port.config;
        let member = &port.members[self.member];
        let handle = self.port.queues[self.member]
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| {
                Error::Config(format!("AF_XDP queue {} already has a group", member.queue))
            })?;
        let queue = handle.attach().map_err(|e| {
            socket_error(e, member.queue, port.members.len(), self.port.umem_bytes, memlock_limit())
        })?;

        let mut config = NetworkConfig::new(IpAddr::V4(member.local_ip), port.mtu);
        config.tcp.max_connections = member.max_connections.max(1);
        config.link = Link::Ethernet {
            local_mac: self.port.neighbors.local_mac(),
            next_hop: EthernetNextHop::Arp {
                prefix_len: port.prefix_len,
                gateway: port.gateway,
            },
        };
        NtcpConnectionGroup::with_shared_neighbors(config, queue, &self.port.neighbors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(ip: [u8; 4], queue: u32) -> NtcpAfXdpMember {
        NtcpAfXdpMember {
            local_ip: Ipv4Addr::from(ip),
            queue,
            max_connections: 4,
        }
    }

    #[test]
    fn members_need_distinct_addresses_and_queues() {
        assert!(check_members(&[]).is_err());
        assert!(check_members(&[member([10, 80, 0, 12], 0)]).is_ok());
        assert!(check_members(&[member([10, 80, 0, 12], 0), member([10, 80, 0, 13], 3)]).is_ok());
        assert!(check_members(&[member([10, 80, 0, 12], 0), member([10, 80, 0, 12], 1)]).is_err());
        assert!(check_members(&[member([10, 80, 0, 12], 1), member([10, 80, 0, 13], 1)]).is_err());
    }

    #[test]
    fn a_umem_refused_for_memlock_names_the_limit_and_the_fix() {
        let enobufs = || io::Error::from_raw_os_error(libc::ENOBUFS);
        let umem = 2048 * 16384;
        let message = socket_error(enobufs(), 1, 2, umem, Some(8 << 20)).to_string();
        for part in [
            "open an AF_XDP socket on queue 1",
            "32768 KiB UMEM",
            "RLIMIT_MEMLOCK, 8192 KiB here",
            "2 sockets need 65536 KiB",
            "at least 65536 KiB",
            "CAP_IPC_LOCK",
        ] {
            assert!(message.contains(part), "{part:?} not in {message}");
        }
        // Unlimited, ENOBUFS has another cause; other errors keep theirs
        let plain = socket_error(enobufs(), 1, 2, umem, None).to_string();
        assert!(!plain.contains("RLIMIT_MEMLOCK"), "{plain}");
        let eperm = io::Error::from_raw_os_error(libc::EPERM);
        let plain = socket_error(eperm, 1, 2, umem, Some(8 << 20)).to_string();
        assert!(!plain.contains("RLIMIT_MEMLOCK"), "{plain}");
    }
}
