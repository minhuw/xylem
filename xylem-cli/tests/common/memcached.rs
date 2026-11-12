//! Memcached test fixture
//!
//! Each guard starts its own compose project and tears it down on drop; the
//! server is reached at its container's address.

use super::compose::{answers_at, ComposeSpec, ComposeStack};
use std::net::SocketAddr;
/// Guard for Memcached in a compose project of its own, torn down on drop
pub struct MemcachedGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    addr: SocketAddr,
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
                    .container_addr("memcached", 11211)
                    .is_some_and(|addr| answers_at(addr, b"stats\r\n", b"STAT"))
            },
        })?;
        let addr = stack
            .container_addr("memcached", 11211)
            .ok_or("Memcached has no container address")?;
        Ok(Self { _stack: stack, addr })
    }

    /// Address of the TCP service, and of the UDP service on the same port
    pub fn addr(&self) -> SocketAddr {
        self.addr
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
