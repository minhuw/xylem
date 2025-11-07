//! UDP transport implementation using non-blocking I/O with mio

use super::Transport;
use crate::{Error, Result, Timestamp};
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

const SOCKET_TOKEN: Token = Token(0);

pub struct UdpTransport {
    socket: Option<UdpSocket>,
    target: Option<SocketAddr>,
    poll: Poll,
    recv_buffer: Vec<u8>,
}

impl UdpTransport {
    pub fn new() -> Self {
        Self {
            socket: None,
            target: None,
            poll: Poll::new().expect("Failed to create poll"),
            recv_buffer: vec![0u8; 65535], // Max UDP packet size
        }
    }
}

impl Default for UdpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for UdpTransport {
    fn connect(&mut self, target: &SocketAddr) -> Result<()> {
        // Bind to any available port (let OS choose)
        let bind_addr: SocketAddr = if target.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };

        let mut socket = UdpSocket::bind(bind_addr)?;

        // Register socket with poll for readability
        self.poll.registry().register(&mut socket, SOCKET_TOKEN, Interest::READABLE)?;

        self.socket = Some(socket);
        self.target = Some(*target);

        Ok(())
    }

    fn send(&mut self, data: &[u8]) -> Result<Timestamp> {
        let socket = self
            .socket
            .as_mut()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        let target = self
            .target
            .ok_or_else(|| Error::Connection("No target address set".to_string()))?;

        // Non-blocking send
        loop {
            match socket.send_to(data, target) {
                Ok(_) => break,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Socket buffer full, spin until ready
                    std::hint::spin_loop();
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Capture timestamp immediately after send completes
        let timestamp = Timestamp::now();

        Ok(timestamp)
    }

    fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        let socket = self
            .socket
            .as_mut()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        // Non-blocking receive
        match socket.recv_from(&mut self.recv_buffer) {
            Ok((n, _src_addr)) => {
                // Capture timestamp immediately after receive
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
        if let Some(mut socket) = self.socket.take() {
            let _ = self.poll.registry().deregister(&mut socket);
        }
        self.target = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket as StdUdpSocket;
    use std::thread;

    #[test]
    fn test_udp_connect() {
        // Bind server socket to get an address
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        // Test client connect
        let mut transport = UdpTransport::new();
        assert!(transport.connect(&addr).is_ok());
        assert!(transport.target.is_some());
        assert_eq!(transport.target.unwrap(), addr);
    }

    #[test]
    fn test_udp_send_recv() {
        // Start echo server
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        thread::spawn(move || {
            let mut buf = vec![0u8; 65535];
            while let Ok((n, src)) = server.recv_from(&mut buf) {
                // Echo back
                server.send_to(&buf[..n], src).unwrap();
            }
        });

        // Give server time to start
        thread::sleep(Duration::from_millis(10));

        // Test client
        let mut transport = UdpTransport::new();
        transport.connect(&addr).unwrap();

        let test_data = b"Hello, UDP!";
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
    fn test_udp_send_without_connect() {
        let mut transport = UdpTransport::new();
        let result = transport.send(b"test");
        assert!(result.is_err());
    }

    #[test]
    fn test_udp_recv_without_connect() {
        let mut transport = UdpTransport::new();
        let result = transport.recv();
        assert!(result.is_err());
    }

    #[test]
    fn test_udp_close() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        let mut transport = UdpTransport::new();
        transport.connect(&addr).unwrap();
        assert!(transport.close().is_ok());
        assert!(transport.socket.is_none());
        assert!(transport.target.is_none());
    }

    #[test]
    fn test_udp_multiple_packets() {
        // Start echo server
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        thread::spawn(move || {
            let mut buf = vec![0u8; 65535];
            for _ in 0..5 {
                if let Ok((n, src)) = server.recv_from(&mut buf) {
                    server.send_to(&buf[..n], src).unwrap();
                } else {
                    break;
                }
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut transport = UdpTransport::new();
        transport.connect(&addr).unwrap();

        // Send and receive multiple packets
        for i in 0..5 {
            let test_data = format!("Packet {}", i);
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
