//! Hardware timestamping helpers for Linux SO_TIMESTAMPING
//!
//! This module provides low-level primitives for enabling and extracting
//! hardware timestamps from NIC-capable sockets on Linux.
//!
//! # Overview
//!
//! Linux provides hardware timestamping via the SO_TIMESTAMPING socket option.
//! Timestamps can be captured at the NIC for both transmitted and received packets,
//! providing microsecond-precision latency measurements.
//!
//! # TX Timestamps
//!
//! TX timestamps are delivered asynchronously via the socket's error queue
//! (MSG_ERRQUEUE). For TCP, the SOF_TIMESTAMPING_OPT_ID flag enables matching
//! TX timestamps to specific bytes in the stream using a byte counter.
//!
//! # RX Timestamps
//!
//! RX timestamps are delivered as ancillary data (cmsg) with the received packet
//! via recvmsg().
//!
//! # References
//!
//! - Linux kernel timestamping docs: https://docs.kernel.org/networking/timestamping.html
//! - Lancet implementation: https://github.com/epfl-dcsl/lancet-tool

use crate::{Error, Result, Timestamp};
use std::io;
use std::mem;
use std::os::unix::io::RawFd;

// =============================================================================
// Constants from linux/net_tstamp.h
// =============================================================================

/// Hardware timestamping flags for SO_TIMESTAMPING
pub mod flags {
    /// Request TX hardware timestamps
    pub const SOF_TIMESTAMPING_TX_HARDWARE: u32 = 1 << 0;
    /// Request TX software timestamps
    pub const SOF_TIMESTAMPING_TX_SOFTWARE: u32 = 1 << 1;
    /// Request RX hardware timestamps
    pub const SOF_TIMESTAMPING_RX_HARDWARE: u32 = 1 << 2;
    /// Request RX software timestamps
    pub const SOF_TIMESTAMPING_RX_SOFTWARE: u32 = 1 << 3;
    /// Report software timestamps
    pub const SOF_TIMESTAMPING_SOFTWARE: u32 = 1 << 4;
    /// Report SYS (transformed) hardware timestamps
    pub const SOF_TIMESTAMPING_SYS_HARDWARE: u32 = 1 << 5;
    /// Report RAW hardware timestamps
    pub const SOF_TIMESTAMPING_RAW_HARDWARE: u32 = 1 << 6;
    /// Generate OPT_ID on TX for correlating TX timestamps
    pub const SOF_TIMESTAMPING_OPT_ID: u32 = 1 << 7;
    /// Only deliver timestamp, not original packet on TX
    pub const SOF_TIMESTAMPING_OPT_TSONLY: u32 = 1 << 11;
}

/// Hardware timestamp configuration for NIC (via SIOCSHWTSTAMP ioctl)
pub mod hwtstamp {
    /// Filter all packets for RX timestamps
    pub const HWTSTAMP_FILTER_ALL: i32 = 1;
    /// No RX timestamp filter
    pub const HWTSTAMP_FILTER_NONE: i32 = 0;
    /// Enable TX timestamps
    pub const HWTSTAMP_TX_ON: i32 = 1;
    /// Disable TX timestamps
    pub const HWTSTAMP_TX_OFF: i32 = 0;
}

/// Control message types
pub mod cmsg {
    /// Timestamp control message (from socket.h)
    pub const SCM_TIMESTAMPING: i32 = 37; // SO_TIMESTAMPING value on Linux
}

/// Socket error origin for TX timestamp identification
pub mod sock_err {
    /// Origin: timestamping
    pub const SO_EE_ORIGIN_TIMESTAMPING: u8 = 4;
}

// =============================================================================
// Structures
// =============================================================================

/// Hardware timestamp configuration (for ioctl SIOCSHWTSTAMP)
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct HwtstampConfig {
    pub flags: i32,
    pub tx_type: i32,
    pub rx_filter: i32,
}

/// Timestamp triplet returned by SCM_TIMESTAMPING
/// Contains software, hardware (transformed), and hardware (raw) timestamps
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ScmTimestamping {
    pub ts: [libc::timespec; 3],
}

impl Default for ScmTimestamping {
    fn default() -> Self {
        Self {
            ts: [
                libc::timespec { tv_sec: 0, tv_nsec: 0 },
                libc::timespec { tv_sec: 0, tv_nsec: 0 },
                libc::timespec { tv_sec: 0, tv_nsec: 0 },
            ],
        }
    }
}

impl ScmTimestamping {
    /// Get the best available timestamp (prefer raw hardware)
    pub fn best_timestamp(&self) -> Option<(i64, i64)> {
        // ts[2] = raw hardware timestamp (preferred)
        // ts[1] = transformed hardware timestamp
        // ts[0] = software timestamp
        if self.ts[2].tv_sec != 0 || self.ts[2].tv_nsec != 0 {
            Some((self.ts[2].tv_sec, self.ts[2].tv_nsec))
        } else if self.ts[1].tv_sec != 0 || self.ts[1].tv_nsec != 0 {
            Some((self.ts[1].tv_sec, self.ts[1].tv_nsec))
        } else if self.ts[0].tv_sec != 0 || self.ts[0].tv_nsec != 0 {
            Some((self.ts[0].tv_sec, self.ts[0].tv_nsec))
        } else {
            None
        }
    }

    /// Get raw hardware timestamp specifically
    pub fn raw_hw_timestamp(&self) -> Option<(i64, i64)> {
        if self.ts[2].tv_sec != 0 || self.ts[2].tv_nsec != 0 {
            Some((self.ts[2].tv_sec, self.ts[2].tv_nsec))
        } else {
            None
        }
    }
}

/// TX timestamp information with byte counter for TCP correlation
#[derive(Debug, Clone, Copy)]
pub struct TxTimestampInfo {
    /// Timestamp
    pub timestamp: Timestamp,
    /// Byte counter (OPT_ID) for matching with TX requests
    pub opt_id: u32,
}

/// Pending TX timestamps queue (Lancet-style)
///
/// For TCP, TX timestamps arrive asynchronously and must be matched
/// with the original send operation using byte counters.
#[derive(Debug)]
pub struct PendingTxTimestamps {
    /// Current TX byte counter (incremented on each send)
    pub tx_byte_counter: u32,
    /// Queue of pending timestamps (waiting for NIC timestamp)
    pending: Vec<PendingTxEntry>,
    /// Queue of received timestamps (waiting to be consumed)
    received: Vec<TxTimestampInfo>,
}

#[derive(Debug, Clone, Copy)]
struct PendingTxEntry {
    /// Expected byte counter value
    opt_id: u32,
}

impl PendingTxTimestamps {
    /// Create a new pending TX timestamps tracker
    pub fn new() -> Self {
        Self {
            tx_byte_counter: 0,
            pending: Vec::with_capacity(64),
            received: Vec::with_capacity(64),
        }
    }

    /// Record a TX operation (before sending)
    ///
    /// Returns the opt_id that will be used to match the TX timestamp.
    pub fn record_tx(&mut self, bytes: u32) -> u32 {
        self.tx_byte_counter = self.tx_byte_counter.wrapping_add(bytes);
        let opt_id = self.tx_byte_counter;
        self.pending.push(PendingTxEntry { opt_id });
        opt_id
    }

    /// Add a received TX timestamp
    pub fn add_received(&mut self, timestamp: Timestamp, opt_id: u32) {
        // Find and remove matching pending entry, then store timestamp
        if let Some(pos) = self.pending.iter().position(|e| e.opt_id == opt_id) {
            self.pending.remove(pos);
            self.received.push(TxTimestampInfo { timestamp, opt_id });
        } else {
            // Timestamp for unknown request - might be reordered, store anyway
            self.received.push(TxTimestampInfo { timestamp, opt_id });
        }
    }

    /// Pop the oldest received timestamp
    pub fn pop_received(&mut self) -> Option<TxTimestampInfo> {
        if self.received.is_empty() {
            None
        } else {
            Some(self.received.remove(0))
        }
    }

    /// Check if there are pending TX operations waiting for timestamps
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Number of pending TX operations
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Number of received timestamps ready to consume
    pub fn received_count(&self) -> usize {
        self.received.len()
    }
}

impl Default for PendingTxTimestamps {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Socket Operations
// =============================================================================

/// Enable hardware timestamping on a socket
///
/// This configures the socket to receive TX and RX hardware timestamps.
/// For TCP, also enables OPT_ID for correlating TX timestamps with byte positions.
///
/// # OPT_ID Counter Behavior
///
/// Per Linux kernel docs, the OPT_ID counter "starts at zero" and "is initialized
/// the first time that the socket option is enabled". This means:
/// - For TCP: ee_data contains cumulative bytes sent (starting from 0)
/// - For UDP: ee_data contains packet count (starting from 0)
///
/// IMPORTANT: This option must be set BEFORE sending any data on the socket,
/// otherwise the counter may be offset by data already in flight.
pub fn enable_socket_timestamping(fd: RawFd) -> Result<()> {
    use flags::*;

    let ts_mode: u32 = SOF_TIMESTAMPING_RX_HARDWARE
        | SOF_TIMESTAMPING_TX_HARDWARE
        | SOF_TIMESTAMPING_RAW_HARDWARE
        | SOF_TIMESTAMPING_OPT_TSONLY
        | SOF_TIMESTAMPING_OPT_ID;

    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPING,
            &ts_mode as *const u32 as *const libc::c_void,
            mem::size_of::<u32>() as libc::socklen_t,
        )
    };

    if ret < 0 {
        return Err(Error::Config(format!(
            "Failed to enable SO_TIMESTAMPING: {}",
            io::Error::last_os_error()
        )));
    }

    Ok(())
}

/// Enable software timestamping on a socket (fallback when HW not available)
pub fn enable_software_timestamping(fd: RawFd) -> Result<()> {
    use flags::*;

    let ts_mode: u32 = SOF_TIMESTAMPING_RX_SOFTWARE
        | SOF_TIMESTAMPING_TX_SOFTWARE
        | SOF_TIMESTAMPING_SOFTWARE
        | SOF_TIMESTAMPING_OPT_TSONLY
        | SOF_TIMESTAMPING_OPT_ID;

    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPING,
            &ts_mode as *const u32 as *const libc::c_void,
            mem::size_of::<u32>() as libc::socklen_t,
        )
    };

    if ret < 0 {
        return Err(Error::Config(format!(
            "Failed to enable software timestamping: {}",
            io::Error::last_os_error()
        )));
    }

    Ok(())
}

/// Bind socket to a specific network interface (required for HW timestamps)
pub fn bind_to_device(fd: RawFd, interface: &str) -> Result<()> {
    // Kernel expects NUL-terminated string for SO_BINDTODEVICE
    let c_interface = std::ffi::CString::new(interface).map_err(|_| {
        Error::Config(format!("Invalid interface name '{}': contains NUL byte", interface))
    })?;
    let bytes = c_interface.as_bytes_with_nul();

    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_BINDTODEVICE,
            bytes.as_ptr() as *const libc::c_void,
            bytes.len() as libc::socklen_t,
        )
    };

    if ret < 0 {
        return Err(Error::Config(format!(
            "Failed to bind to device '{}': {}",
            interface,
            io::Error::last_os_error()
        )));
    }

    Ok(())
}

// =============================================================================
// NIC Timestamping Configuration (via ioctl)
// =============================================================================

/// Enable hardware timestamping on a NIC
///
/// This requires root or CAP_NET_ADMIN capability.
pub fn enable_nic_timestamping(interface: &str) -> Result<()> {
    use hwtstamp::*;

    // Create a temporary UDP socket for the ioctl
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, libc::IPPROTO_UDP) };
    if fd < 0 {
        return Err(Error::Io(io::Error::last_os_error()));
    }

    let result = configure_nic_timestamping(fd, interface, HWTSTAMP_FILTER_ALL, HWTSTAMP_TX_ON);

    unsafe { libc::close(fd) };

    result
}

/// Disable hardware timestamping on a NIC
pub fn disable_nic_timestamping(interface: &str) -> Result<()> {
    use hwtstamp::*;

    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, libc::IPPROTO_UDP) };
    if fd < 0 {
        return Err(Error::Io(io::Error::last_os_error()));
    }

    let result = configure_nic_timestamping(fd, interface, HWTSTAMP_FILTER_NONE, HWTSTAMP_TX_OFF);

    unsafe { libc::close(fd) };

    result
}

fn configure_nic_timestamping(
    fd: RawFd,
    interface: &str,
    rx_filter: i32,
    tx_type: i32,
) -> Result<()> {
    let config = HwtstampConfig { flags: 0, tx_type, rx_filter };

    // Create ifreq structure
    let mut ifr: libc::ifreq = unsafe { mem::zeroed() };

    // Copy interface name (max 15 chars + null terminator)
    let if_bytes = interface.as_bytes();
    let copy_len = std::cmp::min(if_bytes.len(), 15);
    unsafe {
        std::ptr::copy_nonoverlapping(
            if_bytes.as_ptr(),
            ifr.ifr_name.as_mut_ptr() as *mut u8,
            copy_len,
        );
    }

    // Set ifr_data to point to config
    ifr.ifr_ifru.ifru_data = &config as *const HwtstampConfig as *mut libc::c_char;

    // SIOCSHWTSTAMP ioctl
    const SIOCSHWTSTAMP: libc::c_ulong = 0x89b0;

    let ret = unsafe { libc::ioctl(fd, SIOCSHWTSTAMP, &ifr) };

    if ret < 0 {
        let err = io::Error::last_os_error();
        return Err(Error::Config(format!(
            "Failed to configure NIC timestamping on '{}': {} (may require root/CAP_NET_ADMIN)",
            interface, err
        )));
    }

    Ok(())
}

// =============================================================================
// Timestamp Extraction from Control Messages
// =============================================================================

/// Buffer size for control messages (ancillary data)
pub const CMSG_BUFFER_SIZE: usize = 256;

/// Extract timestamp from recvmsg control messages
///
/// Parses the ancillary data from a recvmsg call to find SCM_TIMESTAMPING.
pub fn extract_rx_timestamp(cmsg_buf: &[u8], cmsg_len: usize) -> Option<Timestamp> {
    if cmsg_len == 0 {
        return None;
    }

    // Parse control messages
    let mut offset = 0;
    while offset + mem::size_of::<libc::cmsghdr>() <= cmsg_len {
        let cmsg_ptr = cmsg_buf.as_ptr().wrapping_add(offset) as *const libc::cmsghdr;
        let cmsg = unsafe { &*cmsg_ptr };

        if cmsg.cmsg_len == 0 {
            break;
        }

        // Check for SCM_TIMESTAMPING
        if cmsg.cmsg_level == libc::SOL_SOCKET && cmsg.cmsg_type == cmsg::SCM_TIMESTAMPING {
            // Data starts after cmsghdr
            let data_offset = offset + mem::size_of::<libc::cmsghdr>();
            if data_offset + mem::size_of::<ScmTimestamping>() <= cmsg_len {
                let ts_ptr = cmsg_buf.as_ptr().wrapping_add(data_offset) as *const ScmTimestamping;
                let scm_ts = unsafe { &*ts_ptr };

                if let Some((tv_sec, tv_nsec)) = scm_ts.best_timestamp() {
                    return Some(Timestamp::from_hardware(tv_sec, tv_nsec));
                }
            }
        }

        // Move to next cmsg (aligned)
        let aligned_len =
            (cmsg.cmsg_len + mem::size_of::<usize>() - 1) & !(mem::size_of::<usize>() - 1);
        offset += aligned_len;
    }

    None
}

/// Extract TX timestamp and opt_id from error queue message
///
/// TX timestamps arrive on the socket's error queue (MSG_ERRQUEUE).
/// Returns (timestamp, opt_id) if found.
pub fn extract_tx_timestamp(cmsg_buf: &[u8], cmsg_len: usize) -> Option<(Timestamp, u32)> {
    if cmsg_len == 0 {
        return None;
    }

    let mut timestamp: Option<Timestamp> = None;
    let mut opt_id: Option<u32> = None;

    // Parse control messages
    let mut offset = 0;
    while offset + mem::size_of::<libc::cmsghdr>() <= cmsg_len {
        let cmsg_ptr = cmsg_buf.as_ptr().wrapping_add(offset) as *const libc::cmsghdr;
        let cmsg = unsafe { &*cmsg_ptr };

        if cmsg.cmsg_len == 0 {
            break;
        }

        let data_offset = offset + mem::size_of::<libc::cmsghdr>();

        if cmsg.cmsg_level == libc::SOL_SOCKET && cmsg.cmsg_type == cmsg::SCM_TIMESTAMPING {
            if data_offset + mem::size_of::<ScmTimestamping>() <= cmsg_len {
                let ts_ptr = cmsg_buf.as_ptr().wrapping_add(data_offset) as *const ScmTimestamping;
                let scm_ts = unsafe { &*ts_ptr };

                if let Some((tv_sec, tv_nsec)) = scm_ts.best_timestamp() {
                    timestamp = Some(Timestamp::from_hardware(tv_sec, tv_nsec));
                }
            }
        } else if (cmsg.cmsg_level == libc::SOL_IP && cmsg.cmsg_type == libc::IP_RECVERR)
            || (cmsg.cmsg_level == libc::SOL_IPV6 && cmsg.cmsg_type == libc::IPV6_RECVERR)
        {
            // Extract sock_extended_err for opt_id (works for both IPv4 and IPv6)
            if data_offset + mem::size_of::<libc::sock_extended_err>() <= cmsg_len {
                let se_ptr =
                    cmsg_buf.as_ptr().wrapping_add(data_offset) as *const libc::sock_extended_err;
                let se = unsafe { &*se_ptr };

                if se.ee_errno == libc::ENOMSG as u32
                    && se.ee_origin == sock_err::SO_EE_ORIGIN_TIMESTAMPING
                {
                    opt_id = Some(se.ee_data);
                }
            }
        }

        // Move to next cmsg (aligned)
        let aligned_len =
            (cmsg.cmsg_len + mem::size_of::<usize>() - 1) & !(mem::size_of::<usize>() - 1);
        offset += aligned_len;
    }

    match (timestamp, opt_id) {
        (Some(ts), Some(id)) => Some((ts, id)),
        (Some(ts), None) => Some((ts, 0)), // UDP case: no opt_id needed
        _ => None,
    }
}

/// Receive data with timestamps via recvmsg
///
/// Returns (bytes_read, data, optional_timestamp).
pub fn recvmsg_with_timestamp(
    fd: RawFd,
    buf: &mut [u8],
    flags: i32,
) -> io::Result<(usize, Option<Timestamp>)> {
    let mut cmsg_buf = [0u8; CMSG_BUFFER_SIZE];
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };

    let mut hdr: libc::msghdr = unsafe { mem::zeroed() };
    hdr.msg_iov = &mut iov;
    hdr.msg_iovlen = 1;
    hdr.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    hdr.msg_controllen = CMSG_BUFFER_SIZE;

    let n = unsafe { libc::recvmsg(fd, &mut hdr, flags) };

    if n < 0 {
        return Err(io::Error::last_os_error());
    }

    let timestamp = extract_rx_timestamp(&cmsg_buf, hdr.msg_controllen);

    Ok((n as usize, timestamp))
}

/// Receive TX timestamp from error queue (non-blocking)
///
/// Returns Some((timestamp, opt_id)) if a TX timestamp is available.
pub fn recv_tx_timestamp(fd: RawFd) -> io::Result<Option<(Timestamp, u32)>> {
    let mut cmsg_buf = [0u8; CMSG_BUFFER_SIZE];
    let mut dummy_buf = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: dummy_buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: 0, // No actual data expected
    };

    let mut hdr: libc::msghdr = unsafe { mem::zeroed() };
    hdr.msg_iov = &mut iov;
    hdr.msg_iovlen = 1;
    hdr.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    hdr.msg_controllen = CMSG_BUFFER_SIZE;

    let n = unsafe { libc::recvmsg(fd, &mut hdr, libc::MSG_ERRQUEUE) };

    if n < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Ok(None);
        }
        return Err(err);
    }

    Ok(extract_tx_timestamp(&cmsg_buf, hdr.msg_controllen))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_tx_timestamps() {
        let mut pending = PendingTxTimestamps::new();

        // Record some TX operations
        let id1 = pending.record_tx(100);
        let id2 = pending.record_tx(200);
        let id3 = pending.record_tx(50);

        assert_eq!(pending.pending_count(), 3);
        assert_eq!(id1, 100);
        assert_eq!(id2, 300);
        assert_eq!(id3, 350);

        // Add received timestamps (out of order)
        pending.add_received(Timestamp::now(), id2);
        pending.add_received(Timestamp::now(), id1);

        assert_eq!(pending.pending_count(), 1); // Only id3 pending
        assert_eq!(pending.received_count(), 2);

        // Pop in order received
        let ts1 = pending.pop_received();
        assert!(ts1.is_some());
        assert_eq!(ts1.unwrap().opt_id, id2);

        let ts2 = pending.pop_received();
        assert!(ts2.is_some());
        assert_eq!(ts2.unwrap().opt_id, id1);
    }

    #[test]
    fn test_scm_timestamping_best() {
        let mut scm = ScmTimestamping::default();

        // No timestamps
        assert!(scm.best_timestamp().is_none());

        // Software timestamp
        scm.ts[0].tv_sec = 1000;
        scm.ts[0].tv_nsec = 500;
        assert_eq!(scm.best_timestamp(), Some((1000, 500)));

        // HW raw takes precedence
        scm.ts[2].tv_sec = 2000;
        scm.ts[2].tv_nsec = 1000;
        assert_eq!(scm.best_timestamp(), Some((2000, 1000)));
    }
}
