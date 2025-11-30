//! Unix domain socket transport implementation using non-blocking I/O
//!
//! This transport uses Unix domain sockets for local inter-process communication.
//!
//! This module provides two APIs:
//! - `UnixTransport`: Single-connection API (one multiplexer per connection)
//! - `UnixConnectionGroup`: Multi-connection API (one multiplexer per group)
//!
//! Note: Unix sockets use file paths, not network addresses. The ConnectionGroup API
//! uses `add_connection_path()` instead of `add_connection()`.

use crate::mux::{Interest, Multiplexer, MultiplexerType};
use crate::{ConnectionGroup, Error, GroupConnection, Result, Timestamp, TransportFactory};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_RECV_BUFFER_SIZE: usize = 8192;

/// Unix domain socket transport
pub struct UnixTransport {
    stream: Option<UnixStream>,
    path: Option<PathBuf>,
    mux: Multiplexer,
    recv_buffer: Vec<u8>,
}

impl UnixTransport {
    pub fn new() -> Self {
        Self::with_multiplexer(MultiplexerType::default())
    }

    pub fn with_multiplexer(mux_type: MultiplexerType) -> Self {
        Self {
            stream: None,
            path: None,
            mux: Multiplexer::new(mux_type).expect("Failed to create multiplexer"),
            recv_buffer: vec![0u8; DEFAULT_RECV_BUFFER_SIZE],
        }
    }

    /// Connect to a Unix domain socket at the given path
    pub fn connect_path<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path = path.as_ref();

        let stream = UnixStream::connect(path)?;
        stream.set_nonblocking(true)?;

        self.mux.register_fd(&stream, 0, Interest::BOTH)?;

        // Wait for connection to be established
        for _ in 0..50 {
            let events = self.mux.wait(Some(Duration::from_millis(100)))?;

            let connected = events.iter().any(|e| e.id == 0 && e.writable);
            if !connected {
                continue;
            }

            self.mux.modify_fd(&stream, 0, Interest::READABLE)?;
            self.stream = Some(stream);
            self.path = Some(path.to_path_buf());
            return Ok(());
        }

        // Timeout - deregister before returning error
        let _ = self.mux.deregister_fd(&stream);
        Err(Error::Connection("Connection timeout".to_string()))
    }

    /// Send data and return timestamp
    pub fn send(&mut self, data: &[u8]) -> Result<Timestamp> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        let mut total_written = 0;
        while total_written < data.len() {
            match stream.write(&data[total_written..]) {
                Ok(n) => total_written += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::hint::spin_loop();
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(Timestamp::now())
    }

    /// Receive data and return timestamp
    pub fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        match stream.read(&mut self.recv_buffer) {
            Ok(0) => Err(Error::Connection("Connection closed by peer".to_string())),
            Ok(n) => {
                let timestamp = Timestamp::now();
                let data = self.recv_buffer[..n].to_vec();
                Ok((data, timestamp))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((Vec::new(), Timestamp::now())),
            Err(e) => Err(e.into()),
        }
    }

    /// Check if there is data ready to read
    pub fn poll_readable(&mut self) -> Result<bool> {
        let events = self.mux.wait(Some(Duration::ZERO))?;
        Ok(!events.is_empty())
    }

    /// Close the connection
    pub fn close(&mut self) -> Result<()> {
        if let Some(stream) = self.stream.take() {
            let _ = self.mux.deregister_fd(&stream);
        }
        self.path = None;
        Ok(())
    }

    /// Get the path this transport is connected to
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

impl Default for UnixTransport {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Connection Group API
// =============================================================================

/// A single Unix socket connection within a connection group
pub struct UnixConn {
    stream: UnixStream,
    recv_buffer: Vec<u8>,
    connected: bool,
}

impl UnixConn {
    fn new(stream: UnixStream) -> Self {
        Self {
            stream,
            recv_buffer: vec![0u8; DEFAULT_RECV_BUFFER_SIZE],
            connected: true,
        }
    }
}

impl GroupConnection for UnixConn {
    fn send(&mut self, data: &[u8]) -> Result<Timestamp> {
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

        Ok(Timestamp::now())
    }

    fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        if !self.connected {
            return Err(Error::Connection("Connection closed".to_string()));
        }

        match self.stream.read(&mut self.recv_buffer) {
            Ok(0) => {
                self.connected = false;
                Err(Error::Connection("Connection closed by peer".to_string()))
            }
            Ok(n) => {
                let timestamp = Timestamp::now();
                let data = self.recv_buffer[..n].to_vec();
                Ok((data, timestamp))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((Vec::new(), Timestamp::now())),
            Err(e) => Err(e.into()),
        }
    }

    fn is_connected(&self) -> bool {
        self.connected
    }
}

/// A group of Unix socket connections sharing a single multiplexer
pub struct UnixConnectionGroup {
    mux: Multiplexer,
    connections: HashMap<usize, UnixConn>,
    next_id: usize,
}

impl UnixConnectionGroup {
    pub fn new() -> Result<Self> {
        Self::with_multiplexer(MultiplexerType::default())
    }

    pub fn with_multiplexer(mux_type: MultiplexerType) -> Result<Self> {
        Ok(Self {
            mux: Multiplexer::new(mux_type)?,
            connections: HashMap::new(),
            next_id: 0,
        })
    }

    /// Add a connection by Unix socket path
    pub fn add_connection_path<P: AsRef<Path>>(&mut self, path: P) -> Result<usize> {
        let conn_id = self.next_id;
        self.next_id += 1;

        let path = path.as_ref();
        let stream = UnixStream::connect(path)?;
        stream.set_nonblocking(true)?;

        // Use a temporary multiplexer for connection establishment to avoid
        // mixing connection events with other connections in the group
        let mut temp_mux = Multiplexer::new(self.mux.mux_type())?;
        temp_mux.register_fd(&stream, 0, Interest::BOTH)?;

        // Poll for connection complete
        for _ in 0..50 {
            let events = temp_mux.wait(Some(Duration::from_millis(100)))?;

            let connected = events.iter().any(|e| e.id == 0 && e.writable);
            if !connected {
                continue;
            }

            // Connection successful - deregister from temp and register with main mux
            let _ = temp_mux.deregister_fd(&stream);
            self.mux.register_fd(&stream, conn_id, Interest::READABLE)?;
            let conn = UnixConn::new(stream);
            self.connections.insert(conn_id, conn);
            return Ok(conn_id);
        }

        Err(Error::Connection("Connection timeout".to_string()))
    }
}

impl Default for UnixConnectionGroup {
    fn default() -> Self {
        Self::new().expect("Failed to create UnixConnectionGroup")
    }
}

impl ConnectionGroup for UnixConnectionGroup {
    type Conn = UnixConn;

    fn add_connection(&mut self, _target: &SocketAddr) -> Result<usize> {
        Err(Error::Connection(
            "UnixConnectionGroup requires a path. Use add_connection_path() instead.".to_string(),
        ))
    }

    fn get(&self, conn_id: usize) -> Option<&Self::Conn> {
        self.connections.get(&conn_id)
    }

    fn get_mut(&mut self, conn_id: usize) -> Option<&mut Self::Conn> {
        self.connections.get_mut(&conn_id)
    }

    fn poll(&mut self, timeout: Option<Duration>) -> Result<Vec<usize>> {
        let events = self.mux.wait(timeout)?;
        let ready: Vec<usize> = events
            .iter()
            .filter(|e| e.readable)
            .map(|e| e.id)
            .filter(|id| self.connections.contains_key(id))
            .collect();
        Ok(ready)
    }

    fn len(&self) -> usize {
        self.connections.len()
    }

    fn close(&mut self, conn_id: usize) -> Result<()> {
        if let Some(mut conn) = self.connections.remove(&conn_id) {
            conn.connected = false;
            let _ = self.mux.deregister_fd(&conn.stream);
        }
        Ok(())
    }

    fn close_all(&mut self) -> Result<()> {
        let conn_ids: Vec<usize> = self.connections.keys().copied().collect();
        for conn_id in conn_ids {
            self.close(conn_id)?;
        }
        Ok(())
    }
}

/// Factory for creating Unix socket connection groups
#[derive(Clone, Copy, Default)]
pub struct UnixTransportFactory {
    mux_type: MultiplexerType,
}

impl UnixTransportFactory {
    pub fn new(mux_type: MultiplexerType) -> Self {
        Self { mux_type }
    }
}

impl TransportFactory for UnixTransportFactory {
    type Group = UnixConnectionGroup;

    fn create_group(&self) -> Result<Self::Group> {
        UnixConnectionGroup::with_multiplexer(self.mux_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_unix_connect() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

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

        let mut transport = UnixTransport::new();
        assert!(transport.connect_path(&socket_path).is_ok());
        assert_eq!(transport.path(), Some(socket_path.as_path()));
    }

    #[test]
    fn test_unix_send_recv() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("echo.sock");

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

        let mut transport = UnixTransport::new();
        transport.connect_path(&socket_path).unwrap();

        let test_data = b"Hello, Unix!";
        let send_ts = transport.send(test_data).unwrap();

        thread::sleep(Duration::from_millis(10));

        for _ in 0..100 {
            if transport.poll_readable().unwrap() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        let (recv_data, recv_ts) = transport.recv().unwrap();
        assert_eq!(recv_data, test_data);
        assert!(recv_ts.cycles() >= send_ts.cycles());
    }

    #[test]
    fn test_unix_send_without_connect() {
        let mut transport = UnixTransport::new();
        let result = transport.send(b"test");
        assert!(result.is_err());
    }

    #[test]
    fn test_unix_recv_without_connect() {
        let mut transport = UnixTransport::new();
        let result = transport.recv();
        assert!(result.is_err());
    }

    #[test]
    fn test_unix_close() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("close.sock");

        let server_path = socket_path.clone();
        let listener = UnixListener::bind(&server_path).unwrap();

        thread::spawn(move || {
            let _ = listener.accept();
        });

        thread::sleep(Duration::from_millis(10));

        let mut transport = UnixTransport::new();
        transport.connect_path(&socket_path).unwrap();
        assert!(transport.close().is_ok());
        assert!(transport.stream.is_none());
        assert!(transport.path.is_none());
    }

    #[test]
    fn test_unix_multiple_messages() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("multi.sock");

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

        let mut transport = UnixTransport::new();
        transport.connect_path(&socket_path).unwrap();

        for i in 0..5 {
            let test_data = format!("Message {}", i);
            transport.send(test_data.as_bytes()).unwrap();

            thread::sleep(Duration::from_millis(5));

            for _ in 0..100 {
                if transport.poll_readable().unwrap() {
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }

            let (recv_data, _) = transport.recv().unwrap();
            assert_eq!(recv_data, test_data.as_bytes());
        }
    }

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

        assert!(group.get(0).is_some());
        assert!(group.get(1).is_some());
        assert!(group.get(99).is_none());
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
        let send_ts = group.get_mut(conn_id).unwrap().send(test_data).unwrap();

        thread::sleep(Duration::from_millis(10));

        let ready = group.poll(Some(Duration::from_millis(100))).unwrap();
        assert!(!ready.is_empty());
        assert!(ready.contains(&conn_id));

        let (recv_data, recv_ts) = group.get_mut(conn_id).unwrap().recv().unwrap();
        assert_eq!(recv_data, test_data);
        assert!(recv_ts.cycles() >= send_ts.cycles());
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
        assert!(group.get(conn1).is_none());
        assert!(group.get(conn2).is_some());

        group.close_all().unwrap();
        assert_eq!(group.len(), 0);
    }

    #[test]
    fn test_unix_transport_factory() {
        let factory = UnixTransportFactory::default();
        let group = factory.create_group();
        assert!(group.is_ok());
    }
}
