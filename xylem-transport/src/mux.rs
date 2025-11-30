//! I/O Multiplexer abstraction with runtime-selectable backends
//! I/O Multiplexing abstraction layer.
//!
//! This module provides a unified interface for I/O multiplexing with platform-specific backends:
//!
//! **Linux:**
//! - `Epoll`: Most efficient, default on Linux
//! - `Poll`: Portable fallback, no fd limit
//! - `Select`: Portable fallback, ~1024 fd limit
//!
//! **Non-Linux (macOS, BSD, Windows):**
//! - `Mio`: Uses mio for cross-platform support (kqueue on macOS/BSD, IOCP on Windows)
//!
//! # Example
//!
//! ```rust,no_run
//! use xylem_transport::mux::{Multiplexer, MultiplexerType, Interest};
//! use std::time::Duration;
//!
//! let mut mux = Multiplexer::new(MultiplexerType::default()).unwrap();
//! // Register sockets using register_fd()...
//!
//! let ready = mux.wait(Some(Duration::from_millis(100))).unwrap();
//! for event in ready {
//!     println!("fd {} ready: read={}, write={}", event.id, event.readable, event.writable);
//! }
//! ```

use crate::{Error, Result};
use std::time::Duration;

// Linux: use nix for epoll/poll/select
#[cfg(target_os = "linux")]
use nix::poll::{PollFd, PollFlags, PollTimeout};
#[cfg(target_os = "linux")]
use nix::sys::epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags};
#[cfg(target_os = "linux")]
use nix::sys::select::{select, FdSet};
#[cfg(target_os = "linux")]
use nix::sys::time::TimeVal;
#[cfg(target_os = "linux")]
use std::os::fd::RawFd;

// Non-Linux: use mio
#[cfg(not(target_os = "linux"))]
use mio::{Events, Interest as MioInterest, Poll, Token};
#[cfg(not(target_os = "linux"))]
use std::collections::HashMap;

/// Available multiplexer backends
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiplexerType {
    /// Linux epoll - most efficient, Linux-only
    #[cfg(target_os = "linux")]
    Epoll,
    /// POSIX poll - Linux only (uses nix)
    #[cfg(target_os = "linux")]
    Poll,
    /// POSIX select - Linux only (uses nix), limited to ~1024 fds
    #[cfg(target_os = "linux")]
    Select,
    /// Mio - cross-platform (kqueue on macOS/BSD, IOCP on Windows)
    #[cfg(not(target_os = "linux"))]
    Mio,
}

// Can't derive Default because of conditional compilation
#[allow(clippy::derivable_impls)]
impl Default for MultiplexerType {
    fn default() -> Self {
        #[cfg(target_os = "linux")]
        {
            MultiplexerType::Epoll
        }
        #[cfg(not(target_os = "linux"))]
        {
            MultiplexerType::Mio
        }
    }
}

impl std::fmt::Display for MultiplexerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(target_os = "linux")]
            MultiplexerType::Epoll => write!(f, "epoll"),
            #[cfg(target_os = "linux")]
            MultiplexerType::Poll => write!(f, "poll"),
            #[cfg(target_os = "linux")]
            MultiplexerType::Select => write!(f, "select"),
            #[cfg(not(target_os = "linux"))]
            MultiplexerType::Mio => write!(f, "mio"),
        }
    }
}

impl std::str::FromStr for MultiplexerType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            #[cfg(target_os = "linux")]
            "epoll" => Ok(MultiplexerType::Epoll),
            #[cfg(target_os = "linux")]
            "poll" => Ok(MultiplexerType::Poll),
            #[cfg(target_os = "linux")]
            "select" => Ok(MultiplexerType::Select),
            #[cfg(not(target_os = "linux"))]
            "mio" => Ok(MultiplexerType::Mio),
            _ => Err(Error::Config(format!("Unknown multiplexer type: {}", s))),
        }
    }
}

/// Interest flags for registration
#[derive(Debug, Clone, Copy)]
pub struct Interest {
    pub readable: bool,
    pub writable: bool,
}

impl Interest {
    pub const READABLE: Interest = Interest { readable: true, writable: false };
    pub const WRITABLE: Interest = Interest { readable: false, writable: true };
    pub const BOTH: Interest = Interest { readable: true, writable: true };
}

/// Event returned by wait()
#[derive(Debug, Clone, Copy)]
pub struct Event {
    pub id: usize,
    pub readable: bool,
    pub writable: bool,
}

/// Registered file descriptor entry (Linux only - poll/select need to track these)
#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy)]
struct Registration {
    fd: RawFd,
    id: usize,
    interest: Interest,
}

/// Unified multiplexer that can use different backends at runtime
pub struct Multiplexer {
    backend: MultiplexerBackend,
    #[cfg(target_os = "linux")]
    registrations: Vec<Registration>,
}

enum MultiplexerBackend {
    #[cfg(target_os = "linux")]
    Epoll(EpollBackend),
    #[cfg(target_os = "linux")]
    Poll(PollBackend),
    #[cfg(target_os = "linux")]
    Select(SelectBackend),
    #[cfg(not(target_os = "linux"))]
    Mio(MioBackend),
}

// =============================================================================
// Epoll Backend (Linux only)
// =============================================================================

#[cfg(target_os = "linux")]
struct EpollBackend {
    epoll: Epoll,
    events: Vec<EpollEvent>,
}

#[cfg(target_os = "linux")]
impl EpollBackend {
    fn new() -> Result<Self> {
        let epoll = Epoll::new(EpollCreateFlags::EPOLL_CLOEXEC)?;
        Ok(Self { epoll, events: Vec::with_capacity(64) })
    }

    fn register(&mut self, fd: RawFd, id: usize, interest: Interest) -> Result<()> {
        let mut flags = EpollFlags::empty();
        if interest.readable {
            flags |= EpollFlags::EPOLLIN;
        }
        if interest.writable {
            flags |= EpollFlags::EPOLLOUT;
        }
        // Use level-triggered (default) - simpler, no need to drain sockets

        let event = EpollEvent::new(flags, id as u64);
        self.epoll.add(unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }, event)?;
        Ok(())
    }

    fn modify(&mut self, fd: RawFd, id: usize, interest: Interest) -> Result<()> {
        let mut flags = EpollFlags::empty();
        if interest.readable {
            flags |= EpollFlags::EPOLLIN;
        }
        if interest.writable {
            flags |= EpollFlags::EPOLLOUT;
        }
        // Level-triggered (default)

        let mut event = EpollEvent::new(flags, id as u64);
        self.epoll
            .modify(unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }, &mut event)?;
        Ok(())
    }

    fn deregister(&mut self, fd: RawFd) -> Result<()> {
        self.epoll.delete(unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) })?;
        Ok(())
    }

    fn wait(&mut self, timeout: Option<Duration>) -> Result<Vec<Event>> {
        self.events.clear();
        self.events.resize(64, EpollEvent::empty());

        let timeout_val = match timeout {
            Some(d) => {
                // Cap at u32::MAX ms (~49 days) to avoid overflow
                let ms = d.as_millis().min(u32::MAX as u128) as u32;
                nix::sys::epoll::EpollTimeout::try_from(ms)
                    .unwrap_or(nix::sys::epoll::EpollTimeout::NONE)
            }
            None => nix::sys::epoll::EpollTimeout::NONE,
        };

        let n = self.epoll.wait(&mut self.events, timeout_val)?;

        let events = self.events[..n]
            .iter()
            .map(|e| {
                let flags = e.events();
                Event {
                    id: e.data() as usize,
                    // Treat error conditions as readable so they surface to the worker
                    // and the connection can be properly torn down
                    readable: flags.contains(EpollFlags::EPOLLIN)
                        || flags.contains(EpollFlags::EPOLLHUP)
                        || flags.contains(EpollFlags::EPOLLERR)
                        || flags.contains(EpollFlags::EPOLLRDHUP),
                    writable: flags.contains(EpollFlags::EPOLLOUT),
                }
            })
            .collect();

        Ok(events)
    }
}

// =============================================================================
// Poll Backend (Linux only)
// =============================================================================

#[cfg(target_os = "linux")]
struct PollBackend {
    // We rebuild poll fds each time from registrations
}

#[cfg(target_os = "linux")]
impl PollBackend {
    fn new() -> Result<Self> {
        Ok(Self {})
    }

    fn wait(
        &self,
        registrations: &[Registration],
        timeout: Option<Duration>,
    ) -> Result<Vec<Event>> {
        if registrations.is_empty() {
            return Ok(Vec::new());
        }

        let mut poll_fds: Vec<PollFd> = registrations
            .iter()
            .map(|reg| {
                let mut flags = PollFlags::empty();
                if reg.interest.readable {
                    flags |= PollFlags::POLLIN;
                }
                if reg.interest.writable {
                    flags |= PollFlags::POLLOUT;
                }
                unsafe { PollFd::new(std::os::fd::BorrowedFd::borrow_raw(reg.fd), flags) }
            })
            .collect();

        let timeout_val = match timeout {
            Some(d) => PollTimeout::try_from(d).unwrap_or(PollTimeout::ZERO),
            None => PollTimeout::NONE,
        };

        nix::poll::poll(&mut poll_fds, timeout_val)?;

        let events = poll_fds
            .iter()
            .zip(registrations.iter())
            .filter_map(|(pfd, reg)| {
                let revents = pfd.revents()?;
                if revents.is_empty() {
                    return None;
                }
                Some(Event {
                    id: reg.id,
                    // Treat error conditions as readable so they surface to the worker
                    readable: revents.contains(PollFlags::POLLIN)
                        || revents.contains(PollFlags::POLLHUP)
                        || revents.contains(PollFlags::POLLERR),
                    writable: revents.contains(PollFlags::POLLOUT),
                })
            })
            .collect();

        Ok(events)
    }
}

// =============================================================================
// Select Backend (Linux only)
// =============================================================================

#[cfg(target_os = "linux")]
struct SelectBackend {
    // Select rebuilds fd_sets each time
}

#[cfg(target_os = "linux")]
impl SelectBackend {
    fn new() -> Result<Self> {
        Ok(Self {})
    }

    fn wait(
        &self,
        registrations: &[Registration],
        timeout: Option<Duration>,
    ) -> Result<Vec<Event>> {
        if registrations.is_empty() {
            return Ok(Vec::new());
        }

        let mut read_fds = FdSet::new();
        let mut write_fds = FdSet::new();
        let mut max_fd: RawFd = 0;

        for reg in registrations {
            if reg.interest.readable {
                read_fds.insert(unsafe { std::os::fd::BorrowedFd::borrow_raw(reg.fd) });
            }
            if reg.interest.writable {
                write_fds.insert(unsafe { std::os::fd::BorrowedFd::borrow_raw(reg.fd) });
            }
            max_fd = max_fd.max(reg.fd);
        }

        let mut timeout_val =
            timeout.map(|d| TimeVal::new(d.as_secs() as i64, d.subsec_micros() as i64));

        select(
            Some(max_fd + 1),
            Some(&mut read_fds),
            Some(&mut write_fds),
            None,
            timeout_val.as_mut(),
        )?;

        let events = registrations
            .iter()
            .filter_map(|reg| {
                let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(reg.fd) };
                let readable = read_fds.contains(fd);
                let writable = write_fds.contains(fd);
                if readable || writable {
                    Some(Event { id: reg.id, readable, writable })
                } else {
                    None
                }
            })
            .collect();

        Ok(events)
    }
}

// =============================================================================
// Mio Backend (Non-Linux)
// =============================================================================

#[cfg(not(target_os = "linux"))]
struct MioBackend {
    poll: Poll,
    events: Events,
    /// Map from token to source for deregistration
    sources: HashMap<usize, std::os::fd::RawFd>,
}

#[cfg(not(target_os = "linux"))]
impl MioBackend {
    fn new() -> Result<Self> {
        Ok(Self {
            poll: Poll::new()?,
            events: Events::with_capacity(64),
            sources: HashMap::new(),
        })
    }

    fn register(&mut self, fd: std::os::fd::RawFd, id: usize, interest: Interest) -> Result<()> {
        use std::os::fd::FromRawFd;

        let mut mio_interest = MioInterest::READABLE;
        if interest.writable {
            mio_interest = mio_interest | MioInterest::WRITABLE;
        }
        if !interest.readable {
            mio_interest = MioInterest::WRITABLE;
        }

        // Create a mio::unix::SourceFd wrapper
        let mut source = mio::unix::SourceFd(&fd);
        self.poll.registry().register(&mut source, Token(id), mio_interest)?;
        self.sources.insert(id, fd);
        Ok(())
    }

    fn modify(&mut self, fd: std::os::fd::RawFd, id: usize, interest: Interest) -> Result<()> {
        let mut mio_interest = MioInterest::READABLE;
        if interest.writable {
            mio_interest = mio_interest | MioInterest::WRITABLE;
        }
        if !interest.readable {
            mio_interest = MioInterest::WRITABLE;
        }

        let mut source = mio::unix::SourceFd(&fd);
        self.poll.registry().reregister(&mut source, Token(id), mio_interest)?;
        Ok(())
    }

    fn deregister(&mut self, fd: std::os::fd::RawFd) -> Result<()> {
        let mut source = mio::unix::SourceFd(&fd);
        self.poll.registry().deregister(&mut source)?;
        // Remove from sources map
        self.sources.retain(|_, v| *v != fd);
        Ok(())
    }

    fn wait(&mut self, timeout: Option<Duration>) -> Result<Vec<Event>> {
        self.events.clear();
        self.poll.poll(&mut self.events, timeout)?;

        let events = self
            .events
            .iter()
            .map(|e| Event {
                id: e.token().0,
                readable: e.is_readable(),
                writable: e.is_writable(),
            })
            .collect();

        Ok(events)
    }
}

// =============================================================================
// Unified Multiplexer Implementation
// =============================================================================

impl Multiplexer {
    /// Create a new multiplexer with the specified backend
    #[cfg(target_os = "linux")]
    pub fn new(mux_type: MultiplexerType) -> Result<Self> {
        let backend = match mux_type {
            MultiplexerType::Epoll => MultiplexerBackend::Epoll(EpollBackend::new()?),
            MultiplexerType::Poll => MultiplexerBackend::Poll(PollBackend::new()?),
            MultiplexerType::Select => MultiplexerBackend::Select(SelectBackend::new()?),
        };

        Ok(Self { backend, registrations: Vec::new() })
    }

    /// Create a new multiplexer with the specified backend (non-Linux)
    #[cfg(not(target_os = "linux"))]
    pub fn new(mux_type: MultiplexerType) -> Result<Self> {
        let backend = match mux_type {
            MultiplexerType::Mio => MultiplexerBackend::Mio(MioBackend::new()?),
        };

        Ok(Self { backend })
    }

    /// Get the multiplexer type
    #[cfg(target_os = "linux")]
    pub fn mux_type(&self) -> MultiplexerType {
        match &self.backend {
            MultiplexerBackend::Epoll(_) => MultiplexerType::Epoll,
            MultiplexerBackend::Poll(_) => MultiplexerType::Poll,
            MultiplexerBackend::Select(_) => MultiplexerType::Select,
        }
    }

    /// Get the multiplexer type (non-Linux)
    #[cfg(not(target_os = "linux"))]
    pub fn mux_type(&self) -> MultiplexerType {
        match &self.backend {
            MultiplexerBackend::Mio(_) => MultiplexerType::Mio,
        }
    }

    /// Register a file descriptor for events
    ///
    /// The `id` is returned in events to identify which fd is ready.
    #[cfg(target_os = "linux")]
    pub fn register(&mut self, fd: RawFd, id: usize, interest: Interest) -> Result<()> {
        // For epoll, register with the kernel
        if let MultiplexerBackend::Epoll(ref mut epoll) = self.backend {
            epoll.register(fd, id, interest)?;
        }

        // All backends track registrations
        self.registrations.push(Registration { fd, id, interest });
        Ok(())
    }

    /// Register a file descriptor for events (non-Linux)
    #[cfg(not(target_os = "linux"))]
    pub fn register(
        &mut self,
        fd: std::os::fd::RawFd,
        id: usize,
        interest: Interest,
    ) -> Result<()> {
        match &mut self.backend {
            MultiplexerBackend::Mio(mio) => mio.register(fd, id, interest),
        }
    }

    /// Modify the interest for a registered file descriptor
    #[cfg(target_os = "linux")]
    pub fn modify(&mut self, fd: RawFd, id: usize, interest: Interest) -> Result<()> {
        // For epoll, modify with the kernel
        if let MultiplexerBackend::Epoll(ref mut epoll) = self.backend {
            epoll.modify(fd, id, interest)?;
        }

        // Update registration
        if let Some(reg) = self.registrations.iter_mut().find(|r| r.fd == fd) {
            reg.interest = interest;
        }
        Ok(())
    }

    /// Modify the interest for a registered file descriptor (non-Linux)
    #[cfg(not(target_os = "linux"))]
    pub fn modify(&mut self, fd: std::os::fd::RawFd, id: usize, interest: Interest) -> Result<()> {
        match &mut self.backend {
            MultiplexerBackend::Mio(mio) => mio.modify(fd, id, interest),
        }
    }

    /// Deregister a file descriptor
    #[cfg(target_os = "linux")]
    pub fn deregister(&mut self, fd: RawFd) -> Result<()> {
        // For epoll, deregister from the kernel
        if let MultiplexerBackend::Epoll(ref mut epoll) = self.backend {
            epoll.deregister(fd)?;
        }

        // Remove from registrations
        self.registrations.retain(|r| r.fd != fd);
        Ok(())
    }

    /// Deregister a file descriptor (non-Linux)
    #[cfg(not(target_os = "linux"))]
    pub fn deregister(&mut self, fd: std::os::fd::RawFd) -> Result<()> {
        match &mut self.backend {
            MultiplexerBackend::Mio(mio) => mio.deregister(fd),
        }
    }

    /// Wait for events
    ///
    /// Returns a list of events indicating which file descriptors are ready.
    /// If `timeout` is `None`, blocks indefinitely.
    #[cfg(target_os = "linux")]
    pub fn wait(&mut self, timeout: Option<Duration>) -> Result<Vec<Event>> {
        match &mut self.backend {
            MultiplexerBackend::Epoll(epoll) => epoll.wait(timeout),
            MultiplexerBackend::Poll(poll) => poll.wait(&self.registrations, timeout),
            MultiplexerBackend::Select(sel) => sel.wait(&self.registrations, timeout),
        }
    }

    /// Wait for events (non-Linux)
    #[cfg(not(target_os = "linux"))]
    pub fn wait(&mut self, timeout: Option<Duration>) -> Result<Vec<Event>> {
        match &mut self.backend {
            MultiplexerBackend::Mio(mio) => mio.wait(timeout),
        }
    }

    /// Number of registered file descriptors
    #[cfg(target_os = "linux")]
    pub fn len(&self) -> usize {
        self.registrations.len()
    }

    /// Number of registered file descriptors (non-Linux)
    #[cfg(not(target_os = "linux"))]
    pub fn len(&self) -> usize {
        match &self.backend {
            MultiplexerBackend::Mio(mio) => mio.sources.len(),
        }
    }

    /// Check if no file descriptors are registered
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// =============================================================================
// Helper for registering socket types
// =============================================================================

impl Multiplexer {
    /// Register any type that implements AsRawFd
    pub fn register_fd<F: std::os::fd::AsRawFd>(
        &mut self,
        source: &F,
        id: usize,
        interest: Interest,
    ) -> Result<()> {
        self.register(source.as_raw_fd(), id, interest)
    }

    /// Modify interest for any type that implements AsRawFd
    pub fn modify_fd<F: std::os::fd::AsRawFd>(
        &mut self,
        source: &F,
        id: usize,
        interest: Interest,
    ) -> Result<()> {
        self.modify(source.as_raw_fd(), id, interest)
    }

    /// Deregister any type that implements AsRawFd
    pub fn deregister_fd<F: std::os::fd::AsRawFd>(&mut self, source: &F) -> Result<()> {
        self.deregister(source.as_raw_fd())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    fn test_multiplexer_backend(mux_type: MultiplexerType) {
        // Start echo server
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            loop {
                let n = socket.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
                socket.write_all(&buf[..n]).unwrap();
            }
        });

        thread::sleep(Duration::from_millis(10));

        // Connect client
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_nonblocking(true).unwrap();

        // Create multiplexer
        let mut mux = Multiplexer::new(mux_type).unwrap();
        mux.register_fd(&stream, 0, Interest::BOTH).unwrap();

        // Wait for writable (connected)
        let events = mux.wait(Some(Duration::from_millis(100))).unwrap();
        assert!(!events.is_empty());
        assert!(events[0].writable);

        // Send data
        stream.write_all(b"hello").unwrap();

        // Wait for readable
        thread::sleep(Duration::from_millis(10));
        let events = mux.wait(Some(Duration::from_millis(100))).unwrap();
        assert!(!events.is_empty());
        assert!(events[0].readable);

        // Read response
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello");

        // Cleanup
        mux.deregister_fd(&stream).unwrap();
        assert!(mux.is_empty());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_epoll_backend() {
        test_multiplexer_backend(MultiplexerType::Epoll);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_poll_backend() {
        test_multiplexer_backend(MultiplexerType::Poll);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_select_backend() {
        test_multiplexer_backend(MultiplexerType::Select);
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_mio_backend() {
        test_multiplexer_backend(MultiplexerType::Mio);
    }

    #[test]
    fn test_multiplexer_type_from_str() {
        #[cfg(target_os = "linux")]
        {
            assert_eq!("epoll".parse::<MultiplexerType>().unwrap(), MultiplexerType::Epoll);
            assert_eq!("poll".parse::<MultiplexerType>().unwrap(), MultiplexerType::Poll);
            assert_eq!("select".parse::<MultiplexerType>().unwrap(), MultiplexerType::Select);
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!("mio".parse::<MultiplexerType>().unwrap(), MultiplexerType::Mio);
        }
        assert!("invalid".parse::<MultiplexerType>().is_err());
    }
}
