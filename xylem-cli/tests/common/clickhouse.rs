//! ClickHouse test fixture
//!
//! Each guard starts its own compose project and tears it down on drop. The
//! server publishes no host port: tests reach it at its container's address,
//! so no Docker port-forwarding process carries the load. The server is the version Stackbench builds, with
//! database `bench` holding the table `items` of `tests/clickhouse/init.sql`,
//! and user `xylem` with password `xylem-test`.

use super::compose::{answers_at, ComposeSpec, ComposeStack};
use std::net::SocketAddr;
use xylem_protocols::clickhouse::wire;

/// User the fixture creates
pub const USER: &str = "xylem";

/// Password of [`USER`]
pub const PASSWORD: &str = "xylem-test";

/// Rows of `bench.items`
pub const ITEMS: u64 = 100_000;

/// Guard for ClickHouse in a compose project of its own, torn down on drop
pub struct ClickHouseGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    addr: SocketAddr,
}

impl ClickHouseGuard {
    /// Start ClickHouse and wait until it answers a query on the loaded table
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "ClickHouse",
            compose_file: "tests/clickhouse/docker-compose.test.yml",
            image: None,
            is_ready: |stack| {
                // The server listens once the table is loaded; the marker is
                // in the result only, not in the query's exception
                let mut probe = wire::hello("bench", USER, PASSWORD);
                probe.extend(wire::query(
                    wire::REVISION,
                    "SELECT concat('xylem', '-ready') FROM items LIMIT 1",
                    &[],
                ));
                stack
                    .container_addr("clickhouse", 9000)
                    .is_some_and(|addr| answers_at(addr, &probe, b"xylem-ready"))
            },
        })?;
        let addr = stack
            .container_addr("clickhouse", 9000)
            .ok_or("ClickHouse has no container address")?;
        Ok(Self { _stack: stack, addr })
    }

    /// Address of the native protocol port in the container
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
