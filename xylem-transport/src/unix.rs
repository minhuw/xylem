//! Unix domain socket transport implementation using non-blocking I/O with mio
//!
//! This transport uses Unix domain sockets for local inter-process communication.
//! It implements a similar interface to TCP/UDP but uses file paths instead of network addresses.

use crate::{Error, Result, Timestamp};
use mio::net::UnixStream;
use mio::{Events, Interest, Poll, Token};
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SOCKET_TOKEN: Token = Token(0);

/// Unix domain socket transport
pub struct UnixTransport {
    stream: Option<UnixStream>,
    path: Option<PathBuf>,
    poll: Poll,
    recv_buffer: Vec<u8>,
}

impl UnixTransport {
    pub fn new() -> Self {
        Self {
            stream: None,
            path: None,
            poll: Poll::new().expect("Failed to create poll"),
            recv_buffer: vec![0u8; 8192], // 8KB buffer
        }
    }

    /// Connect to a Unix domain socket at the given path
    pub fn connect_path<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path = path.as_ref();

        // Create non-blocking Unix stream
        let std_stream = StdUnixStream::connect(path)?;
        std_stream.set_nonblocking(true)?;

        let mut stream = UnixStream::from_std(std_stream);

        // Register stream with poll
        self.poll.registry().register(
            &mut stream,
            SOCKET_TOKEN,
            Interest::WRITABLE | Interest::READABLE,
        )?;

        // Wait for connection to be established
        let mut events = Events::with_capacity(1);

        for _ in 0..50 {
            // Try for up to 5 seconds
            self.poll.poll(&mut events, Some(Duration::from_millis(100)))?;

            // Check if connection is ready
            let connected =
                events.iter().any(|event| event.token() == SOCKET_TOKEN && event.is_writable());

            if !connected {
                continue;
            }

            // Re-register for read events only
            self.poll.registry().reregister(&mut stream, SOCKET_TOKEN, Interest::READABLE)?;
            self.stream = Some(stream);
            self.path = Some(path.to_path_buf());
            return Ok(());
        }

        Err(Error::Connection("Connection timeout".to_string()))
    }

    /// Send data and return timestamp
    pub fn send(&mut self, data: &[u8]) -> Result<Timestamp> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        // Non-blocking write
        let mut total_written = 0;
        while total_written < data.len() {
            match stream.write(&data[total_written..]) {
                Ok(n) => total_written += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Socket buffer full, spin until ready
                    std::hint::spin_loop();
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Capture timestamp immediately after write completes
        let timestamp = Timestamp::now();

        Ok(timestamp)
    }

    /// Receive data and return timestamp
    pub fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        // Non-blocking read
        match stream.read(&mut self.recv_buffer) {
            Ok(0) => Err(Error::Connection("Connection closed by peer".to_string())),
            Ok(n) => {
                // Capture timestamp immediately after read
                let timestamp = Timestamp::now();

                // Copy data from buffer
                let data = self.recv_buffer[..n].to_vec();
                Ok((data, timestamp))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // No data available
                Ok((Vec::new(), Timestamp::now()))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Check if there is data ready to read
    pub fn poll_readable(&mut self) -> Result<bool> {
        let mut events = Events::with_capacity(1);

        // Zero timeout - return immediately
        self.poll.poll(&mut events, Some(Duration::ZERO))?;

        Ok(!events.is_empty())
    }

    /// Close the connection
    pub fn close(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            let _ = self.poll.registry().deregister(&mut stream);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_unix_connect() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        // Start server
        let server_path = socket_path.clone();
        let listener = UnixListener::bind(&server_path).unwrap();

        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            // Echo server
            let mut buf = vec![0u8; 1024];
            loop {
                let n = socket.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
                socket.write_all(&buf[..n]).unwrap();
            }
        });

        // Give server time to start
        thread::sleep(Duration::from_millis(10));

        // Test client
        let mut transport = UnixTransport::new();
        assert!(transport.connect_path(&socket_path).is_ok());
        assert_eq!(transport.path(), Some(socket_path.as_path()));
    }

    #[test]
    fn test_unix_send_recv() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("echo.sock");

        // Start echo server
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

        // Give server time to start
        thread::sleep(Duration::from_millis(10));

        // Test client
        let mut transport = UnixTransport::new();
        transport.connect_path(&socket_path).unwrap();

        let test_data = b"Hello, Unix!";
        let send_ts = transport.send(test_data).unwrap();

        // Wait a bit for echo response
        thread::sleep(Duration::from_millis(10));

        // Poll until data ready
        for _ in 0..100 {
            if transport.poll_readable().unwrap() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        let (recv_data, recv_ts) = transport.recv().unwrap();

        assert_eq!(recv_data, test_data);
        assert!(recv_ts.instant >= send_ts.instant);
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

        // Start echo server
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

        // Send and receive multiple messages
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
}
