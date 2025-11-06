//! Connection management with pipelining support
//!
//! This module implements Lancet-style connection pooling, allowing multiple
//! outstanding requests per connection for higher throughput.

use crate::Result;
use std::collections::VecDeque;
use std::net::SocketAddr;
use xylem_transport::{Timestamp, Transport};

/// Maximum receive buffer size per connection
const MAX_PAYLOAD: usize = 16384;

/// Pending request tracking
#[derive(Debug)]
struct PendingRequest {
    /// Timestamp when request was sent
    send_ts: Timestamp,
}

/// A single TCP connection with pipelining support
pub struct Connection<T: Transport> {
    /// Transport layer (TCP socket)
    transport: T,
    /// Connection index
    idx: usize,
    /// Target address
    target: SocketAddr,
    /// Number of pending (in-flight) requests
    pending_requests: usize,
    /// Maximum pending requests allowed
    max_pending_requests: usize,
    /// Queue of pending request timestamps (FIFO)
    pending_queue: VecDeque<PendingRequest>,
    /// Receive buffer for partial responses
    recv_buffer: Vec<u8>,
    /// Current position in receive buffer
    buffer_pos: usize,
    /// Whether connection is closed
    closed: bool,
}

impl<T: Transport> Connection<T> {
    /// Create a new connection
    pub fn new(
        mut transport: T,
        idx: usize,
        target: SocketAddr,
        max_pending_requests: usize,
    ) -> Result<Self> {
        transport.connect(&target)?;

        Ok(Self {
            transport,
            idx,
            target,
            pending_requests: 0,
            max_pending_requests,
            pending_queue: VecDeque::with_capacity(max_pending_requests),
            recv_buffer: vec![0u8; MAX_PAYLOAD],
            buffer_pos: 0,
            closed: false,
        })
    }

    /// Check if this connection can accept more requests
    pub fn can_send(&self) -> bool {
        !self.closed && self.pending_requests < self.max_pending_requests
    }

    /// Send a request on this connection
    pub fn send(&mut self, data: &[u8]) -> Result<()> {
        if !self.can_send() {
            return Err(crate::Error::Connection(
                "Connection cannot accept more requests".to_string(),
            ));
        }

        let send_ts = self.transport.send(data)?;
        self.pending_queue.push_back(PendingRequest { send_ts });
        self.pending_requests += 1;

        Ok(())
    }

    /// Poll if this connection has data ready to read
    pub fn poll_readable(&mut self) -> Result<bool> {
        Ok(self.transport.poll_readable()?)
    }

    /// Receive and process responses, returning (completed_count, latencies)
    ///
    /// Returns the number of complete responses received and their latencies.
    /// Each response consumes one pending request from the queue.
    pub fn recv_responses<F>(
        &mut self,
        mut process_fn: F,
    ) -> Result<Vec<(usize, std::time::Duration)>>
    where
        F: FnMut(&[u8]) -> Result<usize>,
    {
        // Read data into buffer
        let (data, recv_ts) = self.transport.recv()?;
        if data.is_empty() {
            return Ok(Vec::new());
        }

        // Append to buffer
        let data_len = data.len();
        if self.buffer_pos + data_len > MAX_PAYLOAD {
            return Err(crate::Error::Protocol(
                "Response exceeds maximum payload size".to_string(),
            ));
        }

        self.recv_buffer[self.buffer_pos..self.buffer_pos + data_len].copy_from_slice(&data);
        self.buffer_pos += data_len;

        // Process complete responses in buffer
        let mut latencies = Vec::new();
        loop {
            // Try to parse response from buffer
            let consumed = process_fn(&self.recv_buffer[..self.buffer_pos])?;

            if consumed == 0 {
                // No complete response yet
                break;
            }

            // Pop the corresponding pending request and calculate latency
            if let Some(pending) = self.pending_queue.pop_front() {
                self.pending_requests -= 1;
                let latency = recv_ts.duration_since(&pending.send_ts);
                latencies.push((consumed, latency));
            } else {
                return Err(crate::Error::Protocol(
                    "Received response without pending request".to_string(),
                ));
            }

            // Remove consumed bytes from buffer
            if consumed == self.buffer_pos {
                // Consumed entire buffer
                self.buffer_pos = 0;
                break;
            } else if consumed < self.buffer_pos {
                // Partial consumption - move remaining data to front
                let remaining = self.buffer_pos - consumed;
                self.recv_buffer.copy_within(consumed..self.buffer_pos, 0);
                self.buffer_pos = remaining;
            } else {
                return Err(crate::Error::Protocol(
                    "Response parser consumed more bytes than available".to_string(),
                ));
            }
        }

        Ok(latencies)
    }

    /// Get connection index
    pub fn idx(&self) -> usize {
        self.idx
    }

    /// Get target address
    pub fn target(&self) -> SocketAddr {
        self.target
    }

    /// Get pending request count
    pub fn pending_count(&self) -> usize {
        self.pending_requests
    }

    /// Check if connection is closed
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Close the connection
    pub fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(self.transport.close()?)
    }
}

/// Connection pool managing multiple connections with round-robin selection
pub struct ConnectionPool<T: Transport> {
    /// All connections in the pool
    connections: Vec<Connection<T>>,
    /// Next connection index for round-robin
    next_idx: usize,
}

impl<T: Transport> ConnectionPool<T> {
    /// Create a new connection pool
    pub fn new(
        transport_factory: impl Fn() -> T,
        target: SocketAddr,
        conn_count: usize,
        max_pending_per_conn: usize,
    ) -> Result<Self> {
        let mut connections = Vec::with_capacity(conn_count);

        for idx in 0..conn_count {
            let transport = transport_factory();
            let conn = Connection::new(transport, idx, target, max_pending_per_conn)?;
            connections.push(conn);
        }

        Ok(Self { connections, next_idx: 0 })
    }

    /// Pick a connection that can accept more requests (round-robin)
    pub fn pick_connection(&mut self) -> Option<&mut Connection<T>> {
        let conn_count = self.connections.len();

        for _ in 0..conn_count {
            let idx = self.next_idx;
            self.next_idx = (self.next_idx + 1) % conn_count;

            if self.connections[idx].can_send() {
                return Some(&mut self.connections[idx]);
            }
        }

        None
    }

    /// Get all connections for polling
    pub fn connections_mut(&mut self) -> &mut [Connection<T>] {
        &mut self.connections
    }

    /// Get connection count
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Check if pool is empty
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    /// Close all connections
    pub fn close_all(&mut self) -> Result<()> {
        for conn in &mut self.connections {
            conn.close()?;
        }
        Ok(())
    }
}
