//! Memcached test fixture
//!
//! Each guard starts its own compose project on host ports Docker picks and
//! tears it down on drop.

use super::compose::{addr, answers, ComposeSpec, ComposeStack};
use std::net::SocketAddr;
/// Guard for Memcached in a compose project of its own, torn down on drop
pub struct MemcachedGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    port: u16,
    udp_port: u16,
}

impl MemcachedGuard {
    /// Start Memcached and wait until it answers
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Memcached",
            compose_file: "tests/memcached/docker-compose.test.yml",
            build_images: false,
            is_ready: |stack| {
                stack
                    .published_port("memcached", 11211, "tcp")
                    .is_some_and(|port| answers(port, b"stats\r\n", b"STAT"))
                    && stack.published_port("memcached", 11211, "udp").is_some()
            },
        })?;
        let port = stack
            .published_port("memcached", 11211, "tcp")
            .ok_or("Memcached TCP port not published")?;
        let udp_port = stack
            .published_port("memcached", 11211, "udp")
            .ok_or("Memcached UDP port not published")?;
        Ok(Self { _stack: stack, port, udp_port })
    }

    /// Host port Docker published for the TCP service
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// Host port Docker published for the UDP service
    pub fn get_udp_port(&self) -> u16 {
        self.udp_port
    }

    /// `127.0.0.1:<get_port()>`
    pub fn addr(&self) -> SocketAddr {
        addr(self.port)
    }

    /// The server's `stats` counters by name
    pub fn stats(&self) -> std::collections::HashMap<String, u64> {
        use std::io::{Read, Write};
        let mut socket = std::net::TcpStream::connect(self.addr()).unwrap();
        socket.write_all(b"stats\r\n").unwrap();
        let mut reply = Vec::new();
        let mut buf = [0; 4096];
        while !reply.ends_with(b"END\r\n") {
            let n = socket.read(&mut buf).unwrap();
            assert!(n > 0, "memcached closed the stats connection");
            reply.extend_from_slice(&buf[..n]);
        }
        String::from_utf8(reply)
            .unwrap()
            .lines()
            .filter_map(|line| {
                let mut fields = line.strip_prefix("STAT ")?.split(' ');
                let name = fields.next()?.to_string();
                Some((name, fields.next()?.parse().ok()?))
            })
            .collect()
    }
}
