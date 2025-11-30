//! Benchmark using Unix domain sockets with echo protocol
//!
//! This benchmark directly uses UnixConnectionGroup to profile Unix socket performance.
//!
//! Usage:
//!   # Start echo server with Unix socket:
//!   cargo run -p xylem-echo-server --release -- --unix /tmp/echo.sock --threads 8
//!
//!   # Run benchmark:
//!   cargo run -p xylem-core --release --example echo_unix_benchmark -- \
//!       --socket /tmp/echo.sock --connections 1024 --duration 30

use std::collections::HashMap;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};
use xylem_transport::unix::UnixConnectionGroup;
use xylem_transport::{ConnectionGroup, GroupConnection, Timestamp};

const MESSAGE_SIZE: usize = 16;
const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_CONNECTIONS: usize = 1024;
const DEFAULT_PIPELINE: usize = 1;

// =============================================================================
// Echo Protocol (inline for simplicity)
// =============================================================================

struct PendingRequest {
    send_ts: Timestamp,
}

struct ConnectionState {
    pending: HashMap<u64, PendingRequest>,
    recv_buffer: Vec<u8>,
    buffer_pos: usize,
    next_req_id: u64,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            pending: HashMap::with_capacity(16),
            recv_buffer: vec![0u8; 8192],
            buffer_pos: 0,
            next_req_id: 0,
        }
    }

    fn generate_request(&mut self) -> (Vec<u8>, u64) {
        let req_id = self.next_req_id;
        self.next_req_id += 1;

        let mut buf = vec![0u8; MESSAGE_SIZE];
        buf[0..8].copy_from_slice(&req_id.to_le_bytes());

        (buf, req_id)
    }

    fn append_data(&mut self, data: &[u8]) {
        let end = self.buffer_pos + data.len();
        if end <= self.recv_buffer.len() {
            self.recv_buffer[self.buffer_pos..end].copy_from_slice(data);
            self.buffer_pos = end;
        }
    }

    fn parse_next_response(&mut self) -> Option<u64> {
        if self.buffer_pos < MESSAGE_SIZE {
            return None;
        }
        let req_id = u64::from_le_bytes([
            self.recv_buffer[0],
            self.recv_buffer[1],
            self.recv_buffer[2],
            self.recv_buffer[3],
            self.recv_buffer[4],
            self.recv_buffer[5],
            self.recv_buffer[6],
            self.recv_buffer[7],
        ]);
        self.recv_buffer.copy_within(MESSAGE_SIZE..self.buffer_pos, 0);
        self.buffer_pos -= MESSAGE_SIZE;
        Some(req_id)
    }
}

// =============================================================================
// Benchmark Worker
// =============================================================================

fn run_worker(
    socket_path: PathBuf,
    duration: Duration,
    num_connections: usize,
    max_pending: usize,
    core_id: usize,
) -> (u64, u64, u64, u64) {
    // Pin to core
    if let Some(core_ids) = core_affinity::get_core_ids() {
        if core_id < core_ids.len() {
            core_affinity::set_for_current(core_ids[core_id]);
        }
    }

    let mut group = UnixConnectionGroup::new().expect("Failed to create connection group");
    let mut states: Vec<ConnectionState> = Vec::with_capacity(num_connections);

    // Create connections
    for _ in 0..num_connections {
        let conn_id = group.add_connection_path(&socket_path).expect("Failed to connect");
        assert_eq!(conn_id, states.len());
        states.push(ConnectionState::new());
    }

    let mut total_tx = 0u64;
    let mut total_rx = 0u64;
    let mut total_latency_ns = 0u64;
    let mut max_latency_ns = 0u64;

    let start = Instant::now();
    let end_time = start + duration;

    // Initial send burst
    for (conn_id, state) in states.iter_mut().enumerate() {
        for _ in 0..max_pending {
            let (data, req_id) = state.generate_request();
            if let Some(conn) = group.get_mut(conn_id) {
                if let Ok(send_ts) = conn.send(&data) {
                    state.pending.insert(req_id, PendingRequest { send_ts });
                    total_tx += 1;
                }
            }
        }
    }

    // Main loop
    while Instant::now() < end_time {
        let ready = match group.poll(Some(Duration::from_micros(100))) {
            Ok(r) => r,
            Err(_) => continue,
        };

        for conn_id in ready {
            let state = &mut states[conn_id];
            let conn = match group.get_mut(conn_id) {
                Some(c) => c,
                None => continue,
            };

            let (data, recv_ts) = match conn.recv() {
                Ok(r) => r,
                Err(_) => continue,
            };

            state.append_data(&data);

            while let Some(req_id) = state.parse_next_response() {
                let Some(pending) = state.pending.remove(&req_id) else {
                    continue;
                };
                let latency_ns = recv_ts.duration_since(&pending.send_ts).as_nanos() as u64;
                total_latency_ns += latency_ns;
                max_latency_ns = max_latency_ns.max(latency_ns);
                total_rx += 1;

                // Send next request (closed loop)
                let (data, new_req_id) = state.generate_request();
                let Ok(send_ts) = conn.send(&data) else {
                    continue;
                };
                state.pending.insert(new_req_id, PendingRequest { send_ts });
                total_tx += 1;
            }
        }
    }

    let avg_latency_ns = if total_rx > 0 {
        total_latency_ns / total_rx
    } else {
        0
    };

    (total_tx, total_rx, avg_latency_ns, max_latency_ns)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let socket_path: PathBuf = args
        .iter()
        .position(|s| s == "--socket")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            eprintln!("Usage: echo_unix_benchmark --socket <path> [--threads N] [--connections N] [--pipeline N] [--duration N]");
            eprintln!("Example: echo_unix_benchmark --socket /tmp/echo.sock --connections 1024 --duration 30");
            std::process::exit(1);
        });

    let num_threads = args
        .iter()
        .position(|s| s == "--threads")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let num_connections = args
        .iter()
        .position(|s| s == "--connections")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CONNECTIONS);

    let max_pending = args
        .iter()
        .position(|s| s == "--pipeline")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PIPELINE);

    let duration_secs = args
        .iter()
        .position(|s| s == "--duration")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);

    let duration = Duration::from_secs(duration_secs);

    println!("Xylem Echo Unix Socket Benchmark");
    println!("===================================");
    println!("Socket: {}", socket_path.display());
    println!("Worker threads: {}", num_threads);
    println!("Connections per thread: {}", num_connections);
    println!("Pipeline depth: {}", max_pending);
    println!("Duration: {}s", duration_secs);
    println!();

    // Warmup
    println!("Warming up (2s)...");
    let _ =
        run_worker(socket_path.clone(), Duration::from_secs(2), num_connections, max_pending, 0);

    // Run benchmark
    println!("Running benchmark for {}s with {} threads...", duration_secs, num_threads);

    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let path = socket_path.clone();
            thread::spawn(move || run_worker(path, duration, num_connections, max_pending, i))
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    let total_elapsed = start.elapsed();
    let total_tx: u64 = results.iter().map(|(tx, _, _, _)| tx).sum();
    let total_rx: u64 = results.iter().map(|(_, rx, _, _)| rx).sum();

    println!();
    println!("Results:");
    println!("  Total TX: {} requests", total_tx);
    println!("  Total RX: {} requests", total_rx);
    println!("  Throughput: {:.0} req/s", total_tx as f64 / total_elapsed.as_secs_f64());
    println!(
        "  Avg latency: {:.2} μs",
        results.iter().map(|(_, _, avg, _)| avg).sum::<u64>() as f64 / num_threads as f64 / 1000.0
    );
    println!(
        "  Max latency: {} μs",
        results.iter().map(|(_, _, _, max)| max / 1000).max().unwrap_or(0)
    );

    if num_threads > 1 {
        println!("\nPer-thread results:");
        for (i, (tx, rx, avg, max)) in results.iter().enumerate() {
            println!(
                "  Thread {}: TX={}, RX={}, mean={:.2}μs, max={}μs",
                i,
                tx,
                rx,
                *avg as f64 / 1000.0,
                max / 1000
            );
        }
    }
}
