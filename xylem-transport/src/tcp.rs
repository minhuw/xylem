//! TCP transport implementation using non-blocking I/O with mio

use super::Transport;
use crate::{Error, Result, Timestamp};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::time::Duration;

const SOCKET_TOKEN: Token = Token(0);

pub struct TcpTransport {
    stream: Option<TcpStream>,
    poll: Poll,
    recv_buffer: Vec<u8>,
}

impl TcpTransport {
    pub fn new() -> Self {
        Self {
            stream: None,
            poll: Poll::new().expect("Failed to create poll"),
            recv_buffer: vec![0u8; 8192], // 8KB buffer
        }
    }
}

impl Default for TcpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for TcpTransport {
    fn connect(&mut self, target: &SocketAddr) -> Result<()> {
        // Create non-blocking TCP stream
        let mut stream = TcpStream::connect(*target)?;

        // Wait for connection to be established
        let mut events = Events::with_capacity(1);
        self.poll.registry().register(
            &mut stream,
            SOCKET_TOKEN,
            Interest::WRITABLE | Interest::READABLE,
        )?;

        // Poll for connection complete (writable means connected)
        for _ in 0..50 {
            // Try for up to 5 seconds
            self.poll.poll(&mut events, Some(Duration::from_millis(100)))?;

            // Check if connection is ready
            let connected =
                events.iter().any(|event| event.token() == SOCKET_TOKEN && event.is_writable());

            if !connected {
                continue;
            }

            // Connection established - check for errors
            if let Some(err) = stream.take_error()? {
                return Err(Error::Connection(format!("Connection failed: {err}")));
            }

            // Re-register for read events only
            self.poll.registry().reregister(&mut stream, SOCKET_TOKEN, Interest::READABLE)?;
            self.stream = Some(stream);
            return Ok(());
        }

        Err(Error::Connection("Connection timeout".to_string()))
    }

    fn send(&mut self, data: &[u8]) -> Result<Timestamp> {
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

    fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
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

    fn poll_readable(&mut self) -> Result<bool> {
        let mut events = Events::with_capacity(1);

        // Zero timeout - return immediately
        self.poll.poll(&mut events, Some(Duration::ZERO))?;

        Ok(!events.is_empty())
    }

    fn close(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            let _ = self.poll.registry().deregister(&mut stream);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn test_tcp_connect() {
        // Start a test server
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn server thread
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
        let mut transport = TcpTransport::new();
        assert!(transport.connect(&addr).is_ok());
    }

    #[test]
    fn test_tcp_send_recv() {
        // Start echo server
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

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
        let mut transport = TcpTransport::new();
        transport.connect(&addr).unwrap();

        let test_data = b"Hello, World!";
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
    fn test_tcp_send_without_connect() {
        let mut transport = TcpTransport::new();
        let result = transport.send(b"test");
        assert!(result.is_err());
    }

    #[test]
    fn test_tcp_recv_without_connect() {
        let mut transport = TcpTransport::new();
        let result = transport.recv();
        assert!(result.is_err());
    }

    #[test]
    fn test_tcp_close() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let _ = listener.accept();
        });

        thread::sleep(Duration::from_millis(10));

        let mut transport = TcpTransport::new();
        transport.connect(&addr).unwrap();
        assert!(transport.close().is_ok());
        assert!(transport.stream.is_none());
    }
}
