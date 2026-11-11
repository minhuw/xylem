//! I/O multiplexer shared by the socket-based connection groups
//!
//! Linux uses epoll (via nix). Other platforms use mio, which wraps kqueue on
//! macOS and the BSDs. The backend is fixed at compile time.
//!
//! Registrations are level-triggered: a ready file descriptor is reported by
//! every `wait` until it is drained.

use crate::Result;
use std::os::fd::{AsRawFd, RawFd};
use std::time::Duration;

#[cfg(target_os = "linux")]
use nix::sys::epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags, EpollTimeout};
#[cfg(target_os = "linux")]
use std::os::fd::BorrowedFd;

#[cfg(not(target_os = "linux"))]
use mio::{Events, Interest as MioInterest, Poll, Token};

/// Interest flags for registration
#[derive(Debug, Clone, Copy)]
pub(crate) struct Interest {
    pub readable: bool,
    pub writable: bool,
}

impl Interest {
    pub const READABLE: Interest = Interest { readable: true, writable: false };
    pub const BOTH: Interest = Interest { readable: true, writable: true };
}

/// Event returned by `wait()`
#[derive(Debug, Clone, Copy)]
pub(crate) struct Event {
    pub id: usize,
    /// Readable, or an error or hang-up condition the reader must observe
    pub readable: bool,
    pub writable: bool,
}

/// Level-triggered readiness multiplexer (epoll on Linux, mio elsewhere)
pub(crate) struct Multiplexer {
    #[cfg(target_os = "linux")]
    epoll: Epoll,
    #[cfg(target_os = "linux")]
    events: Vec<EpollEvent>,
    #[cfg(not(target_os = "linux"))]
    poll: Poll,
    #[cfg(not(target_os = "linux"))]
    events: Events,
}

const EVENT_CAPACITY: usize = 64;

#[cfg(target_os = "linux")]
fn epoll_flags(interest: Interest) -> EpollFlags {
    let mut flags = EpollFlags::empty();
    if interest.readable {
        flags |= EpollFlags::EPOLLIN;
    }
    if interest.writable {
        flags |= EpollFlags::EPOLLOUT;
    }
    flags
}

#[cfg(not(target_os = "linux"))]
fn mio_interest(interest: Interest) -> MioInterest {
    match (interest.readable, interest.writable) {
        (true, true) => MioInterest::READABLE | MioInterest::WRITABLE,
        (false, true) => MioInterest::WRITABLE,
        _ => MioInterest::READABLE,
    }
}

#[cfg(target_os = "linux")]
impl Multiplexer {
    pub fn new() -> Result<Self> {
        Ok(Self {
            epoll: Epoll::new(EpollCreateFlags::EPOLL_CLOEXEC)?,
            events: vec![EpollEvent::empty(); EVENT_CAPACITY],
        })
    }

    /// Register `fd`; `id` is returned in its events
    pub fn register(&mut self, fd: RawFd, id: usize, interest: Interest) -> Result<()> {
        // SAFETY: the caller keeps `fd` open while it is registered.
        let fd = unsafe { BorrowedFd::borrow_raw(fd) };
        self.epoll.add(fd, EpollEvent::new(epoll_flags(interest), id as u64))?;
        Ok(())
    }

    pub fn deregister(&mut self, fd: RawFd) -> Result<()> {
        // SAFETY: `fd` is still open; it is being removed from the set.
        self.epoll.delete(unsafe { BorrowedFd::borrow_raw(fd) })?;
        Ok(())
    }

    /// Change the interest of the registered `fd`
    pub fn modify(&mut self, fd: RawFd, id: usize, interest: Interest) -> Result<()> {
        let mut event = EpollEvent::new(epoll_flags(interest), id as u64);
        // SAFETY: the caller keeps `fd` open while it is registered.
        self.epoll.modify(unsafe { BorrowedFd::borrow_raw(fd) }, &mut event)?;
        Ok(())
    }

    /// Wait up to `timeout` (`None` blocks) for ready file descriptors
    pub fn wait(&mut self, timeout: Option<Duration>) -> Result<Vec<Event>> {
        let timeout = match timeout {
            // Cap at u32::MAX ms (~49 days) to avoid overflow
            Some(d) => EpollTimeout::try_from(d.as_millis().min(u32::MAX as u128) as u32)
                .unwrap_or(EpollTimeout::NONE),
            None => EpollTimeout::NONE,
        };
        let n = self.epoll.wait(&mut self.events, timeout)?;
        Ok(self.events[..n]
            .iter()
            .map(|e| {
                let flags = e.events();
                Event {
                    id: e.data() as usize,
                    // Treat error conditions as readable so they surface to the worker
                    // and the connection can be properly torn down
                    readable: flags.intersects(
                        EpollFlags::EPOLLIN
                            | EpollFlags::EPOLLHUP
                            | EpollFlags::EPOLLERR
                            | EpollFlags::EPOLLRDHUP,
                    ),
                    writable: flags.contains(EpollFlags::EPOLLOUT),
                }
            })
            .collect())
    }
}

#[cfg(not(target_os = "linux"))]
impl Multiplexer {
    pub fn new() -> Result<Self> {
        Ok(Self {
            poll: Poll::new()?,
            events: Events::with_capacity(EVENT_CAPACITY),
        })
    }

    /// Register `fd`; `id` is returned in its events
    pub fn register(&mut self, fd: RawFd, id: usize, interest: Interest) -> Result<()> {
        let mut source = mio::unix::SourceFd(&fd);
        self.poll.registry().register(&mut source, Token(id), mio_interest(interest))?;
        Ok(())
    }

    pub fn deregister(&mut self, fd: RawFd) -> Result<()> {
        self.poll.registry().deregister(&mut mio::unix::SourceFd(&fd))?;
        Ok(())
    }

    /// Change the interest of the registered `fd`
    pub fn modify(&mut self, fd: RawFd, id: usize, interest: Interest) -> Result<()> {
        let mut source = mio::unix::SourceFd(&fd);
        self.poll
            .registry()
            .reregister(&mut source, Token(id), mio_interest(interest))?;
        Ok(())
    }

    /// Wait up to `timeout` (`None` blocks) for ready file descriptors
    pub fn wait(&mut self, timeout: Option<Duration>) -> Result<Vec<Event>> {
        self.poll.poll(&mut self.events, timeout)?;
        Ok(self
            .events
            .iter()
            .map(|e| Event {
                id: e.token().0,
                readable: e.is_readable() || e.is_read_closed() || e.is_error(),
                writable: e.is_writable(),
            })
            .collect())
    }
}

impl Multiplexer {
    /// Register any type that implements `AsRawFd`
    pub fn register_fd<F: AsRawFd>(
        &mut self,
        source: &F,
        id: usize,
        interest: Interest,
    ) -> Result<()> {
        self.register(source.as_raw_fd(), id, interest)
    }

    /// Deregister any type that implements `AsRawFd`
    pub fn deregister_fd<F: AsRawFd>(&mut self, source: &F) -> Result<()> {
        self.deregister(source.as_raw_fd())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    #[test]
    fn test_multiplexer_reports_writable_then_readable() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            while let Ok(n) = socket.read(&mut buf) {
                if n == 0 {
                    break;
                }
                socket.write_all(&buf[..n]).unwrap();
            }
        });

        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_nonblocking(true).unwrap();
        let mut mux = Multiplexer::new().unwrap();
        mux.register_fd(&stream, 7, Interest::BOTH).unwrap();

        // Connected: writable, nothing to read yet
        let events = mux.wait(Some(Duration::from_millis(100))).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, 7);
        assert!(events[0].writable);

        // Readable once the echo arrives; switch to read interest so the
        // level-triggered writable state does not end the wait early
        mux.deregister_fd(&stream).unwrap();
        mux.register_fd(&stream, 7, Interest::READABLE).unwrap();
        stream.write_all(b"hello").unwrap();
        let events = mux.wait(Some(Duration::from_secs(5))).unwrap();
        assert!(events.iter().any(|e| e.id == 7 && e.readable && !e.writable));
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello");

        // Deregistered: no more events
        mux.deregister_fd(&stream).unwrap();
        assert!(mux.wait(Some(Duration::ZERO)).unwrap().is_empty());
    }
}
