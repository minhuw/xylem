//! Unix domain socket transport implementation using non-blocking I/O
//!
//! This transport uses Unix domain sockets for local inter-process communication.
//!
//! `UnixConnectionGroup` manages many Unix stream connections behind one
//! multiplexer. Unix sockets use file paths, not network addresses, so
//! connections are added with `add_connection_path()` instead of
//! `add_connection()`.

use crate::group::{GroupCore, GroupSocket};
use crate::{
    ConnectionGroup, Error, RecvOutcome, Result, SendReceipt, Timestamp, TransportFactory,
};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

// =============================================================================
// Connection Group API
// =============================================================================

/// A single Unix socket connection within a connection group
pub(crate) struct UnixConn {
    stream: UnixStream,
    connected: bool,
}

impl UnixConn {
    fn new(stream: UnixStream) -> Self {
        Self { stream, connected: true }
    }
}

impl GroupSocket for UnixConn {
    fn socket_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }
}

impl UnixConn {
    fn send(&mut self, data: &[u8]) -> Result<SendReceipt> {
        if !self.connected {
            return Err(Error::Connection("Connection closed".to_string()));
        }

        let mut total_written = 0;
        while total_written < data.len() {
            match self.stream.write(&data[total_written..]) {
                Ok(n) => total_written += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::hint::spin_loop();
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Unix sockets report no TX timestamps
        Ok(SendReceipt { timestamp: Timestamp::now(), tx: None })
    }

    fn recv(&mut self, buf: &mut [u8]) -> Result<RecvOutcome> {
        // The peer closed the stream earlier; nothing more will arrive
        if !self.connected {
            return Ok(RecvOutcome::Eof);
        }

        match self.stream.read(buf) {
            Ok(0) => {
                self.connected = false;
                Ok(RecvOutcome::Eof)
            }
            Ok(len) => Ok(RecvOutcome::Data { len, timestamp: Timestamp::now() }),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(RecvOutcome::WouldBlock),
            Err(e) => Err(e.into()),
        }
    }
}

/// A group of Unix socket connections sharing a single multiplexer
pub struct UnixConnectionGroup {
    core: GroupCore<UnixConn>,
}

impl UnixConnectionGroup {
    pub fn new() -> Result<Self> {
        Ok(Self { core: GroupCore::new()? })
    }

    /// Add a connection by Unix socket path
    pub fn add_connection_path<P: AsRef<Path>>(&mut self, path: P) -> Result<usize> {
        let conn_id = self.core.reserve_id();

        let path = path.as_ref();
        let stream = UnixStream::connect(path)?;
        stream.set_nonblocking(true)?;

        self.core.wait_for_connect(&stream)?;

        // Connection successful - register with main mux
        self.core.register_readable(&stream, conn_id)?;
        let conn = UnixConn::new(stream);
        self.core.insert(conn_id, conn);
        Ok(conn_id)
    }
}

impl Default for UnixConnectionGroup {
    fn default() -> Self {
        Self::new().expect("Failed to create UnixConnectionGroup")
    }
}

impl ConnectionGroup for UnixConnectionGroup {
    fn add_connection(&mut self, _target: &SocketAddr) -> Result<usize> {
        Err(Error::Connection(
            "UnixConnectionGroup requires a path. Use add_connection_path() instead.".to_string(),
        ))
    }

    fn send(&mut self, conn_id: usize, data: &[u8]) -> Result<SendReceipt> {
        self.core.conn_mut(conn_id)?.send(data)
    }

    fn recv(&mut self, conn_id: usize, buf: &mut [u8]) -> Result<RecvOutcome> {
        self.core.conn_mut(conn_id)?.recv(buf)
    }

    fn poll(&mut self, timeout: Duration, ready: &mut Vec<usize>) -> Result<()> {
        self.core.poll(timeout, ready)
    }

    fn len(&self) -> usize {
        self.core.len()
    }

    fn close(&mut self, conn_id: usize) -> Result<()> {
        self.core.close(conn_id)
    }

    fn close_all(&mut self) -> Result<()> {
        self.core.close_all()
    }
}

/// Factory for creating Unix socket connection groups
///
/// Construct it with `UnixTransportFactory::default()`.
#[derive(Clone, Copy, Default)]
#[non_exhaustive]
pub struct UnixTransportFactory;

impl TransportFactory for UnixTransportFactory {
    type Group = UnixConnectionGroup;

    fn create_group(&self) -> Result<Self::Group> {
        UnixConnectionGroup::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;
    use std::thread;
    use tempfile::TempDir;

    // =========================================================================
    // Connection Group Tests
    // =========================================================================

    #[test]
    fn test_unix_group_create() {
        let group = UnixConnectionGroup::new();
        assert!(group.is_ok());
        let group = group.unwrap();
        assert_eq!(group.len(), 0);
        assert!(group.is_empty());
    }

    #[test]
    fn test_unix_group_add_connection_path() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("group.sock");

        let server_path = socket_path.clone();
        let listener = UnixListener::bind(&server_path).unwrap();

        thread::spawn(move || {
            for _ in 0..3 {
                let _ = listener.accept();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut group = UnixConnectionGroup::new().unwrap();

        let conn_id = group.add_connection_path(&socket_path);
        assert!(conn_id.is_ok());
        let conn_id = conn_id.unwrap();
        assert_eq!(conn_id, 0);
        assert_eq!(group.len(), 1);

        let conn_id2 = group.add_connection_path(&socket_path).unwrap();
        assert_eq!(conn_id2, 1);
        assert_eq!(group.len(), 2);

        assert!(group.core.get(0).is_some());
        assert!(group.core.get(1).is_some());
        assert!(group.core.get(99).is_none());
    }

    #[test]
    fn test_unix_group_add_connection_returns_error() {
        let mut group = UnixConnectionGroup::new().unwrap();
        let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();
        let result = group.add_connection(&addr);
        assert!(result.is_err());
    }

    #[test]
    fn test_unix_group_send_recv() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("echo_group.sock");

        let server_path = socket_path.clone();
        let listener = UnixListener::bind(&server_path).unwrap();

        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = vec![0u8; 1024];
            loop {
                let n = socket.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
                socket.write_all(&buf[..n]).unwrap();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut group = UnixConnectionGroup::new().unwrap();
        let conn_id = group.add_connection_path(&socket_path).unwrap();

        let test_data = b"Hello, Unix Group!";
        let receipt = group.send(conn_id, test_data).unwrap();

        thread::sleep(Duration::from_millis(10));

        let mut ready = vec![usize::MAX];
        group.poll(Duration::from_millis(100), &mut ready).unwrap();
        assert!(!ready.is_empty());
        assert!(ready.contains(&conn_id));
        assert!(!ready.contains(&usize::MAX));

        let mut buf = vec![0u8; 65536];
        let RecvOutcome::Data { len, timestamp } = group.recv(conn_id, &mut buf).unwrap() else {
            panic!("expected data");
        };
        assert_eq!(&buf[..len], test_data);
        assert!(timestamp.cycles() >= receipt.timestamp.cycles());
    }

    #[test]
    fn test_unix_group_close() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("close_group.sock");

        let server_path = socket_path.clone();
        let listener = UnixListener::bind(&server_path).unwrap();

        thread::spawn(move || {
            for _ in 0..2 {
                let _ = listener.accept();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut group = UnixConnectionGroup::new().unwrap();
        let conn1 = group.add_connection_path(&socket_path).unwrap();
        let conn2 = group.add_connection_path(&socket_path).unwrap();

        assert_eq!(group.len(), 2);

        group.close(conn1).unwrap();
        assert_eq!(group.len(), 1);
        assert!(group.core.get(conn1).is_none());
        assert!(group.core.get(conn2).is_some());

        group.close_all().unwrap();
        assert_eq!(group.len(), 0);
    }

    #[test]
    fn test_unix_group_ids_are_not_reused() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("ids_group.sock");

        let server_path = socket_path.clone();
        let listener = UnixListener::bind(&server_path).unwrap();

        thread::spawn(move || {
            for _ in 0..5 {
                let _ = listener.accept();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut group = UnixConnectionGroup::new().unwrap();
        assert_eq!(group.add_connection_path(&socket_path).unwrap(), 0);
        assert_eq!(group.add_connection_path(&socket_path).unwrap(), 1);
        assert_eq!(group.add_connection_path(&socket_path).unwrap(), 2);

        // Closing a middle id must not let the next connection reuse it or
        // the current length
        group.close(1).unwrap();
        assert!(group.core.get(1).is_none());
        assert!(group.core.get(1).is_none());
        assert_eq!(group.len(), 2);

        assert_eq!(group.add_connection_path(&socket_path).unwrap(), 3);
        assert_eq!(group.len(), 3);
        assert!(group.core.get(0).is_some());
        assert!(group.core.get(1).is_none());
        assert!(group.core.get(2).is_some());
        assert!(group.core.get(3).is_some());

        // Closing the highest id must not let the next connection reuse it
        group.close(3).unwrap();
        assert!(group.core.get(3).is_none());
        assert!(group.core.get(3).is_none());
        assert_eq!(group.len(), 2);

        assert_eq!(group.add_connection_path(&socket_path).unwrap(), 4);
        assert_eq!(group.len(), 3);
        assert!(group.core.get(0).is_some());
        assert!(group.core.get(1).is_none());
        assert!(group.core.get(2).is_some());
        assert!(group.core.get(3).is_none());
        assert!(group.core.get(4).is_some());
    }

    #[test]
    fn test_unix_group_recv_outcomes() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("outcomes.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (peer_tx, peer_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            peer_tx.send(socket).unwrap();
        });

        let mut group = UnixConnectionGroup::new().unwrap();
        let conn_id = group.add_connection_path(&socket_path).unwrap();
        let mut peer = peer_rx.recv().unwrap();
        let mut buf = vec![0u8; 64];
        let mut ready = Vec::new();

        assert!(matches!(group.recv(conn_id, &mut buf).unwrap(), RecvOutcome::WouldBlock));

        peer.write_all(b"data").unwrap();
        drop(peer);
        group.poll(Duration::from_millis(100), &mut ready).unwrap();
        assert_eq!(ready, vec![conn_id]);
        assert!(matches!(
            group.recv(conn_id, &mut buf).unwrap(),
            RecvOutcome::Data { len: 4, .. }
        ));
        assert_eq!(&buf[..4], b"data");
        assert!(matches!(group.recv(conn_id, &mut buf).unwrap(), RecvOutcome::Eof));
        assert!(matches!(group.recv(conn_id, &mut buf).unwrap(), RecvOutcome::Eof));

        assert!(matches!(group.recv(99, &mut buf), Err(Error::Connection(_))));
        assert!(matches!(group.send(99, b"x"), Err(Error::Connection(_))));
    }

    #[test]
    fn test_unix_group_echoes_messages_in_order() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("multi_group.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = vec![0u8; 1024];
            while let Ok(n) = socket.read(&mut buf) {
                if n == 0 || socket.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
        });

        let mut group = UnixConnectionGroup::new().unwrap();
        let conn_id = group.add_connection_path(&socket_path).unwrap();
        let mut buf = vec![0u8; 1024];
        let mut ready = Vec::new();
        for i in 0..5 {
            let message = format!("Message {i}");
            let receipt = group.send(conn_id, message.as_bytes()).unwrap();
            group.poll(Duration::from_secs(1), &mut ready).unwrap();
            assert_eq!(ready, vec![conn_id]);
            let RecvOutcome::Data { len, timestamp } = group.recv(conn_id, &mut buf).unwrap()
            else {
                panic!("expected data");
            };
            assert_eq!(&buf[..len], message.as_bytes());
            assert!(timestamp.cycles() >= receipt.timestamp.cycles());
        }
    }

    #[test]
    fn test_unix_transport_factory() {
        let factory = UnixTransportFactory::default();
        let group = factory.create_group();
        assert!(group.is_ok());
    }
}
