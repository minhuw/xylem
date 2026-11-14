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

use crate::{Error, KernelClock, KernelTimestamp, Result};
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
    /// Get the best available timestamp and its clock (prefer raw hardware)
    ///
    /// - `ts[2]`: raw hardware timestamp, the NIC's PHC (preferred)
    /// - `ts[1]`: legacy transformed hardware timestamp; always zero on
    ///   current kernels and never requested, so ignored
    /// - `ts[0]`: software timestamp, CLOCK_REALTIME
    pub fn best_timestamp(&self) -> Option<(i64, i64, KernelClock)> {
        let set = |ts: &libc::timespec| ts.tv_sec != 0 || ts.tv_nsec != 0;
        if set(&self.ts[2]) {
            Some((self.ts[2].tv_sec, self.ts[2].tv_nsec, KernelClock::NicHardware))
        } else if set(&self.ts[0]) {
            Some((self.ts[0].tv_sec, self.ts[0].tv_nsec, KernelClock::Realtime))
        } else {
            None
        }
    }
}

// =============================================================================
// Socket Operations
// =============================================================================

/// `SO_TIMESTAMPING` flags of NIC hardware timestamps, TX and RX
///
/// With `SOF_TIMESTAMPING_OPT_ID`, the kernel numbers TX timestamps for
/// correlation with sends.
///
/// # OPT_ID Counter Behavior
///
/// Per Linux kernel docs, the OPT_ID counter "starts at zero" and "is initialized
/// the first time that the socket option is enabled". This means:
/// - For TCP: ee_data contains cumulative bytes sent (starting from 0)
/// - For UDP: ee_data contains packet count (starting from 0)
///
/// IMPORTANT: The option must be set BEFORE sending any data on the socket,
/// otherwise the counter may be offset by data already in flight.
pub const HARDWARE_FLAGS: u32 = flags::SOF_TIMESTAMPING_RX_HARDWARE
    | flags::SOF_TIMESTAMPING_TX_HARDWARE
    | flags::SOF_TIMESTAMPING_RAW_HARDWARE
    | flags::SOF_TIMESTAMPING_OPT_TSONLY
    | flags::SOF_TIMESTAMPING_OPT_ID;

/// `SO_TIMESTAMPING` flags of kernel software timestamps, TX and RX; see
/// [`HARDWARE_FLAGS`] for the TX counter
pub const SOFTWARE_FLAGS: u32 = flags::SOF_TIMESTAMPING_RX_SOFTWARE
    | flags::SOF_TIMESTAMPING_TX_SOFTWARE
    | flags::SOF_TIMESTAMPING_SOFTWARE
    | flags::SOF_TIMESTAMPING_OPT_TSONLY
    | flags::SOF_TIMESTAMPING_OPT_ID;

/// Set `SO_TIMESTAMPING` to `ts_flags` on a socket
///
/// The error is the `setsockopt` call's, with its errno.
pub fn set_timestamping(fd: RawFd, ts_flags: u32) -> io::Result<()> {
    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPING,
            &ts_flags as *const u32 as *const libc::c_void,
            mem::size_of::<u32>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Enable hardware timestamping on a socket ([`HARDWARE_FLAGS`])
pub fn enable_socket_timestamping(fd: RawFd) -> Result<()> {
    set_timestamping(fd, HARDWARE_FLAGS).map_err(|e| {
        Error::Config(format!(
            "setsockopt(SOL_SOCKET, SO_TIMESTAMPING, {HARDWARE_FLAGS:#x}) failed: {e}"
        ))
    })
}

/// Enable software timestamping on a socket ([`SOFTWARE_FLAGS`])
pub fn enable_software_timestamping(fd: RawFd) -> Result<()> {
    set_timestamping(fd, SOFTWARE_FLAGS).map_err(|e| {
        Error::Config(format!(
            "setsockopt(SOL_SOCKET, SO_TIMESTAMPING, {SOFTWARE_FLAGS:#x}) failed: {e}"
        ))
    })
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
            "setsockopt(SOL_SOCKET, SO_BINDTODEVICE, '{}') failed: {}",
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

/// Whether `interface` currently has hardware timestamping switched on
///
/// Reads the NIC configuration with `SIOCGHWTSTAMP`, which needs no
/// privilege, and reports true only when TX timestamping is on and the RX
/// filter is not `HWTSTAMP_FILTER_NONE`. Socket-level `SO_TIMESTAMPING`
/// requests succeed whether or not the NIC is configured, so this is the
/// check that decides whether hardware timestamps will actually arrive.
///
/// Returns an error when the interface does not exist or its driver does
/// not support hardware timestamping.
pub fn nic_timestamping_enabled(interface: &str) -> Result<bool> {
    use hwtstamp::*;

    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, libc::IPPROTO_UDP) };
    if fd < 0 {
        return Err(Error::Io(io::Error::last_os_error()));
    }

    let mut config = HwtstampConfig::default();
    let mut ifr = ifreq_for(interface, &mut config);
    const SIOCGHWTSTAMP: libc::c_ulong = 0x89b1;
    let ret = unsafe { libc::ioctl(fd, SIOCGHWTSTAMP, &mut ifr) };
    let err = io::Error::last_os_error();
    unsafe { libc::close(fd) };

    if ret < 0 {
        return Err(Error::Config(format!(
            "Could not read hardware timestamping configuration of '{interface}': {err}"
        )));
    }
    Ok(config.tx_type != HWTSTAMP_TX_OFF && config.rx_filter != HWTSTAMP_FILTER_NONE)
}

/// An `ifreq` naming `interface` (truncated to 15 bytes) whose data points at `config`
fn ifreq_for(interface: &str, config: &mut HwtstampConfig) -> libc::ifreq {
    let mut ifr: libc::ifreq = unsafe { mem::MaybeUninit::zeroed().assume_init() };
    let if_bytes = interface.as_bytes();
    let copy_len = std::cmp::min(if_bytes.len(), 15);
    unsafe {
        std::ptr::copy_nonoverlapping(
            if_bytes.as_ptr(),
            ifr.ifr_name.as_mut_ptr() as *mut u8,
            copy_len,
        );
    }
    ifr.ifr_ifru.ifru_data = config as *mut HwtstampConfig as *mut libc::c_char;
    ifr
}

fn configure_nic_timestamping(
    fd: RawFd,
    interface: &str,
    rx_filter: i32,
    tx_type: i32,
) -> Result<()> {
    let mut config = HwtstampConfig { flags: 0, tx_type, rx_filter };
    let ifr = ifreq_for(interface, &mut config);

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

/// Align a control-message length without overflowing.
fn cmsg_align(len: usize) -> Option<usize> {
    let mask = mem::size_of::<usize>() - 1;
    Some(len.checked_add(mask)? & !mask)
}

/// Parse the complete ancillary buffer, rejecting malformed messages even after
/// a timestamp has been found. Input bytes need not be aligned.
fn parse_timestamps(
    cmsg_buf: &[u8],
    cmsg_len: usize,
    keep_first_timestamp: bool,
) -> Option<(Option<KernelTimestamp>, Option<u32>)> {
    let buf = cmsg_buf.get(..cmsg_len)?;
    let header_len = mem::size_of::<libc::cmsghdr>();
    let data_start = cmsg_align(header_len)?;
    let mut offset: usize = 0;
    let mut timestamp = None;
    let mut opt_id = None;

    while offset < buf.len() {
        let header = buf.get(offset..offset.checked_add(header_len)?)?;
        // SAFETY: the full header is in bounds; cmsghdr contains only integers.
        // read_unaligned does not require alignment of the caller's byte slice.
        let cmsg = unsafe { header.as_ptr().cast::<libc::cmsghdr>().read_unaligned() };
        if cmsg.cmsg_len < data_start {
            return None;
        }
        let end = offset.checked_add(cmsg.cmsg_len)?;
        let payload = buf.get(offset.checked_add(data_start)?..end)?;

        if cmsg.cmsg_level == libc::SOL_SOCKET && cmsg.cmsg_type == cmsg::SCM_TIMESTAMPING {
            let bytes = payload.get(..mem::size_of::<ScmTimestamping>())?;
            // SAFETY: a complete integer-only timestamp triplet is in this message.
            let scm_ts = unsafe { bytes.as_ptr().cast::<ScmTimestamping>().read_unaligned() };
            if let Some((sec, nsec, clock)) = scm_ts.best_timestamp() {
                if sec < 0 || !(0..1_000_000_000).contains(&nsec) {
                    return None;
                }
                let nanos = (sec as u64).checked_mul(1_000_000_000)?.checked_add(nsec as u64)?;
                if timestamp.is_none() || !keep_first_timestamp {
                    timestamp = Some(KernelTimestamp { nanos, clock });
                }
            }
        } else if (cmsg.cmsg_level == libc::SOL_IP && cmsg.cmsg_type == libc::IP_RECVERR)
            || (cmsg.cmsg_level == libc::SOL_IPV6 && cmsg.cmsg_type == libc::IPV6_RECVERR)
        {
            let bytes = payload.get(..mem::size_of::<libc::sock_extended_err>())?;
            // SAFETY: the complete integer-only error structure is in this message.
            let err = unsafe { bytes.as_ptr().cast::<libc::sock_extended_err>().read_unaligned() };
            if err.ee_errno == libc::ENOMSG as u32
                && err.ee_origin == sock_err::SO_EE_ORIGIN_TIMESTAMPING
            {
                opt_id = Some(err.ee_data);
            }
        }

        // The final message may omit some or all of its trailing alignment padding.
        offset = offset.checked_add(cmsg_align(cmsg.cmsg_len)?)?.min(buf.len());
    }
    Some((timestamp, opt_id))
}

/// Extract a timestamp from recvmsg control messages.
/// Returns None for missing timestamps or malformed ancillary data.
pub fn extract_rx_timestamp(cmsg_buf: &[u8], cmsg_len: usize) -> Option<KernelTimestamp> {
    parse_timestamps(cmsg_buf, cmsg_len, true)?.0
}

/// Extract a TX timestamp and opt_id from an error queue message.
/// Returns None for missing timestamps or malformed ancillary data.
/// When no opt_id is supplied (e.g. UDP), the ID defaults to zero.
pub fn extract_tx_timestamp(cmsg_buf: &[u8], cmsg_len: usize) -> Option<(KernelTimestamp, u32)> {
    let (timestamp, opt_id) = parse_timestamps(cmsg_buf, cmsg_len, false)?;
    Some((timestamp?, opt_id.unwrap_or(0)))
}

/// Receive data with timestamps via recvmsg
///
/// Returns (bytes_read, kernel timestamp if one was attached).
pub fn recvmsg_with_timestamp(
    fd: RawFd,
    buf: &mut [u8],
    flags: i32,
) -> io::Result<(usize, Option<KernelTimestamp>)> {
    let mut cmsg_buf = [0u8; CMSG_BUFFER_SIZE];
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };

    let mut hdr: libc::msghdr = unsafe { mem::MaybeUninit::zeroed().assume_init() };
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
/// Returns `Some((timestamp, opt_id))` for the next TX timestamp and `None`
/// once the error queue is empty. A message without a TX timestamp, or with
/// malformed ancillary data, is an error of kind `InvalidData`: with
/// `SO_TIMESTAMPING` alone, the error queue holds nothing else.
pub fn recv_tx_timestamp(fd: RawFd) -> io::Result<Option<(KernelTimestamp, u32)>> {
    let mut cmsg_buf = [0u8; CMSG_BUFFER_SIZE];
    let mut dummy_buf = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: dummy_buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: 0, // No actual data expected
    };

    let mut hdr: libc::msghdr = unsafe { mem::MaybeUninit::zeroed().assume_init() };
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

    match extract_tx_timestamp(&cmsg_buf, hdr.msg_controllen) {
        Some(stamp) => Ok(Some(stamp)),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "an error-queue message carried no TX timestamp ({} bytes of ancillary data{})",
                hdr.msg_controllen,
                if hdr.msg_flags & libc::MSG_CTRUNC != 0 {
                    ", truncated"
                } else {
                    ""
                }
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(buf: &mut [u8], offset: usize, bytes: &[u8]) {
        buf[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    fn message(level: i32, kind: i32, payload: &[u8]) -> Vec<u8> {
        let header_len = cmsg_align(mem::size_of::<libc::cmsghdr>()).unwrap();
        let len = header_len + payload.len();
        let mut buf = vec![0; cmsg_align(len).unwrap()];
        put(&mut buf, mem::offset_of!(libc::cmsghdr, cmsg_len), &len.to_ne_bytes());
        put(&mut buf, mem::offset_of!(libc::cmsghdr, cmsg_level), &level.to_ne_bytes());
        put(&mut buf, mem::offset_of!(libc::cmsghdr, cmsg_type), &kind.to_ne_bytes());
        put(&mut buf, header_len, payload);
        buf
    }

    fn timestamp_message() -> Vec<u8> {
        let mut payload = vec![0; mem::size_of::<ScmTimestamping>()];
        put(&mut payload, mem::offset_of!(libc::timespec, tv_sec), &1234i64.to_ne_bytes());
        put(&mut payload, mem::offset_of!(libc::timespec, tv_nsec), &567i64.to_ne_bytes());
        message(libc::SOL_SOCKET, cmsg::SCM_TIMESTAMPING, &payload)
    }

    fn sw(nanos: u64) -> KernelTimestamp {
        KernelTimestamp { nanos, clock: KernelClock::Realtime }
    }

    fn assert_rejected(buf: &[u8], len: usize) {
        assert!(extract_rx_timestamp(buf, len).is_none());
        assert!(extract_tx_timestamp(buf, len).is_none());
    }

    #[test]
    fn ancillary_rejects_invalid_lengths() {
        assert_rejected(&[], 0);
        assert_rejected(&[], 1);
        assert_rejected(&[0; 3], 3);
        let valid = timestamp_message();
        assert_rejected(&valid, valid.len() + 1);
        assert_rejected(&valid, usize::MAX);
        for len in [0, 1, mem::size_of::<libc::cmsghdr>() - 1, valid.len() + 1, usize::MAX] {
            let mut buf = valid.clone();
            put(&mut buf, mem::offset_of!(libc::cmsghdr, cmsg_len), &len.to_ne_bytes());
            assert_rejected(&buf, buf.len());
        }
        assert_eq!(cmsg_align(usize::MAX), None);
    }

    #[test]
    fn ancillary_valid_unaligned_ipv4_and_ipv6() {
        for (level, kind) in
            [(libc::SOL_IP, libc::IP_RECVERR), (libc::SOL_IPV6, libc::IPV6_RECVERR)]
        {
            let mut error = vec![0; mem::size_of::<libc::sock_extended_err>()];
            put(
                &mut error,
                mem::offset_of!(libc::sock_extended_err, ee_errno),
                &(libc::ENOMSG as u32).to_ne_bytes(),
            );
            error[mem::offset_of!(libc::sock_extended_err, ee_origin)] =
                sock_err::SO_EE_ORIGIN_TIMESTAMPING;
            put(
                &mut error,
                mem::offset_of!(libc::sock_extended_err, ee_data),
                &42u32.to_ne_bytes(),
            );
            // Exercise every possible alignment of the caller's slice.
            for prefix in 0..mem::align_of::<libc::cmsghdr>() {
                let mut buf = vec![0; prefix];
                buf.extend(timestamp_message());
                buf.extend(message(level, kind, &error));
                let buf = &buf[prefix..];
                assert_eq!(extract_rx_timestamp(buf, buf.len()).unwrap(), sw(1_234_000_000_567));
                let (ts, id) = extract_tx_timestamp(buf, buf.len()).unwrap();
                assert_eq!(ts, sw(1_234_000_000_567));
                assert_eq!(id, 42);
            }
        }
        let buf = timestamp_message();
        assert_eq!(extract_tx_timestamp(&buf, buf.len()).unwrap().1, 0);
    }

    #[test]
    fn ancillary_rejects_truncation_even_after_valid_timestamp() {
        let valid = timestamp_message();
        for len in 1..valid.len() {
            assert_rejected(&valid[..len], len);
        }
        let mut buf = valid;
        buf.extend([0; 3]); // Truncated next header
        assert_rejected(&buf, buf.len());
    }

    #[test]
    fn ancillary_payload_must_fit_current_message() {
        for (level, kind, size) in [
            (libc::SOL_SOCKET, cmsg::SCM_TIMESTAMPING, mem::size_of::<ScmTimestamping>()),
            (libc::SOL_IP, libc::IP_RECVERR, mem::size_of::<libc::sock_extended_err>()),
            (libc::SOL_IPV6, libc::IPV6_RECVERR, mem::size_of::<libc::sock_extended_err>()),
        ] {
            let mut buf = timestamp_message();
            buf.extend(message(level, kind, &vec![0; size - 1]));
            // There are enough bytes in the overall buffer, but not in this cmsg.
            buf.extend(timestamp_message());
            assert_rejected(&buf, buf.len());
        }
    }

    #[test]
    fn ancillary_preserves_rx_first_and_tx_last_timestamp_selection() {
        let mut buf = timestamp_message();
        let mut second = timestamp_message();
        let offset = cmsg_align(mem::size_of::<libc::cmsghdr>()).unwrap()
            + mem::offset_of!(libc::timespec, tv_sec);
        put(&mut second, offset, &2345i64.to_ne_bytes());
        buf.extend(second);
        assert_eq!(extract_rx_timestamp(&buf, buf.len()).unwrap(), sw(1_234_000_000_567));
        assert_eq!(extract_tx_timestamp(&buf, buf.len()).unwrap().0, sw(2_345_000_000_567));
    }

    #[test]
    fn ancillary_rejects_invalid_or_overflowing_timestamp_values() {
        for (sec, nsec) in [(-1i64, 0i64), (1, -1), (1, 1_000_000_000), (i64::MAX, 0)] {
            let mut buf = timestamp_message();
            let data = cmsg_align(mem::size_of::<libc::cmsghdr>()).unwrap();
            put(&mut buf, data + mem::offset_of!(libc::timespec, tv_sec), &sec.to_ne_bytes());
            put(&mut buf, data + mem::offset_of!(libc::timespec, tv_nsec), &nsec.to_ne_bytes());
            assert_rejected(&buf, buf.len());
        }
    }

    #[test]
    fn ancillary_accepts_unknown_message_and_final_padding() {
        let mut buf = message(123, 456, &[1]);
        buf.extend(timestamp_message());
        assert!(extract_rx_timestamp(&buf, buf.len()).is_some());
        buf.extend(message(123, 456, &[1]));
        // Omit final alignment padding, as recvmsg is permitted to do.
        buf.truncate(buf.len() - (mem::size_of::<usize>() - 1));
        assert!(extract_rx_timestamp(&buf, buf.len()).is_some());
    }

    /// Wait for the next TX timestamp on `fd`'s error queue
    pub(crate) fn wait_tx_timestamp(fd: RawFd) -> (KernelTimestamp, u32) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if let Some(stamp) = recv_tx_timestamp(fd).unwrap() {
                return stamp;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("no TX timestamp within 5 s");
    }

    /// Pins what Linux reports with SOF_TIMESTAMPING_OPT_ID on TCP: the
    /// counter starts at zero and each stamp's ID is the offset of the LAST
    /// byte of the write (`seq + len - 1`), not the exclusive end.
    /// Loopback generates software TX timestamps (loopback_xmit).
    #[test]
    fn loopback_tcp_opt_id_is_last_byte_offset() {
        use std::io::{Read, Write};
        use std::os::fd::AsRawFd;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        client.set_nodelay(true).unwrap();
        let fd = client.as_raw_fd();
        enable_software_timestamping(fd).unwrap();

        let mut ids = Vec::new();
        for _ in 0..2 {
            client.write_all(&[0; 10]).unwrap();
            let (ts, id) = wait_tx_timestamp(fd);
            assert_eq!(ts.clock, KernelClock::Realtime);
            ids.push(id);
            server.read_exact(&mut [0; 10]).unwrap();
        }
        assert_eq!(ids, vec![9, 19]);
        assert_eq!(recv_tx_timestamp(fd).unwrap(), None);
    }

    /// Pins what Linux reports with SOF_TIMESTAMPING_OPT_ID on UDP: one
    /// stamp per datagram, IDs counting datagrams from zero.
    #[test]
    fn loopback_udp_opt_id_counts_datagrams() {
        use std::os::fd::AsRawFd;
        let server = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let client = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let fd = client.as_raw_fd();
        enable_software_timestamping(fd).unwrap();

        let mut ids = Vec::new();
        for len in [10, 10, 3] {
            client.send_to(&vec![0; len], server.local_addr().unwrap()).unwrap();
            let (ts, id) = wait_tx_timestamp(fd);
            assert_eq!(ts.clock, KernelClock::Realtime);
            ids.push(id);
        }
        assert_eq!(ids, vec![0, 1, 2]);
        assert_eq!(recv_tx_timestamp(fd).unwrap(), None);
    }

    /// An error-queue message without a TX timestamp is an error, not the
    /// end of the queue: here an ICMP port unreachable, queued by IP_RECVERR
    #[test]
    fn an_error_queue_message_without_a_tx_timestamp_is_an_error() {
        use std::os::fd::AsRawFd;
        // A port nothing listens on any more
        let closed = std::net::UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        let client = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        client.connect(closed).unwrap();
        let fd = client.as_raw_fd();
        let on: libc::c_int = 1;
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_IP,
                libc::IP_RECVERR,
                &on as *const libc::c_int as *const libc::c_void,
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(ret, 0, "{}", io::Error::last_os_error());
        assert_eq!(recv_tx_timestamp(fd).unwrap(), None, "empty queue");
        client.send(b"x").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let error = loop {
            match recv_tx_timestamp(fd) {
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(1))
                }
                Ok(stamp) => panic!("expected an error, got {stamp:?}"),
                Err(error) => break error,
            }
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("carried no TX timestamp"), "{error}");
    }

    #[test]
    fn loopback_has_no_hardware_timestamping() {
        // lo has no PHC: either the driver rejects the query or reports it off
        assert!(!nic_timestamping_enabled("lo").unwrap_or(false));
        assert!(nic_timestamping_enabled("xylem-no-such-if").is_err());
    }

    #[test]
    fn test_scm_timestamping_best() {
        let mut scm = ScmTimestamping::default();

        // No timestamps
        assert!(scm.best_timestamp().is_none());

        // The legacy transformed hardware slot is ignored
        scm.ts[1].tv_sec = 3000;
        assert!(scm.best_timestamp().is_none());

        // Software timestamp
        scm.ts[0].tv_sec = 1000;
        scm.ts[0].tv_nsec = 500;
        assert_eq!(scm.best_timestamp(), Some((1000, 500, KernelClock::Realtime)));

        // HW raw takes precedence
        scm.ts[2].tv_sec = 2000;
        scm.ts[2].tv_nsec = 1000;
        assert_eq!(scm.best_timestamp(), Some((2000, 1000, KernelClock::NicHardware)));
    }
}
