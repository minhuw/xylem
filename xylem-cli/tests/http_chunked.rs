//! HTTP chunked responses through the worker, against a stub server
//!
//! The stub answers every GET on a keep-alive connection with a fixed chunked
//! response, written in small pieces so the client sees partial reads. These
//! tests need no Docker; `http_integration` covers a real nginx server.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};
use xylem_core::threading::{Worker, WorkerConfig};
use xylem_protocols::http::HttpProtocol;
use xylem_protocols::HttpMethod;
use xylem_transport::TcpTransportFactory;

/// Bytes the stub writes per `write` call, to split responses across reads
const WRITE_PIECE: usize = 7;

/// Serve `response` for every request on 127.0.0.1; runs until the process exits
fn start_stub_server(response: Vec<u8>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind stub server");
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let response = response.clone();
            thread::spawn(move || serve(stream, &response));
        }
    });
    addr
}

/// Answer each bodyless request (ended by an empty line) with `response`
fn serve(mut stream: TcpStream, response: &[u8]) {
    stream.set_nodelay(true).ok();
    let mut pending = Vec::new();
    let mut buf = [0u8; 4096];
    while let Ok(n @ 1..) = stream.read(&mut buf) {
        pending.extend_from_slice(&buf[..n]);
        while let Some(end) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
            pending.drain(..end + 4);
            for piece in response.chunks(WRITE_PIECE) {
                if stream.write_all(piece).is_err() {
                    return;
                }
            }
        }
    }
}

/// Run a closed-loop GET worker against `target` for `duration`
fn run_worker(
    target: SocketAddr,
    duration: Duration,
    max_pending_per_conn: usize,
) -> (xylem_core::Result<()>, TupleStatsCollector) {
    let protocol = HttpProtocol::new(HttpMethod::Get, "/".to_string(), "localhost".to_string());
    let stats = TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));
    let config = WorkerConfig {
        target,
        duration,
        conn_count: 2,
        max_pending_per_conn,
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
    };
    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config)
            .expect("Failed to create worker");
    let result = worker.run();
    (result, worker.into_stats())
}

#[test]
fn chunked_responses_count_their_full_wire_bytes() {
    let response =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n\
                     5;ext=1\r\nhello\r\nA\r\n, chunked!\r\n0\r\nX-Trailer: done\r\n\r\n"
            .to_vec();
    let addr = start_stub_server(response.clone());

    for max_pending in [1, 8] {
        let (result, stats) = run_worker(addr, Duration::from_millis(500), max_pending);
        result.expect("Worker should complete without errors");
        let requests: u64 =
            stats.entries().map(|(_, entry)| entry).map(|entry| entry.request_count).sum();
        let rx_bytes: u64 =
            stats.entries().map(|(_, entry)| entry).map(|entry| entry.rx_bytes).sum();
        assert!(requests > 0, "Should have completed requests (max_pending {max_pending})");
        assert_eq!(rx_bytes, requests * response.len() as u64, "max_pending {max_pending}");
    }
}

#[test]
fn chunked_response_over_max_payload_fails() {
    // Two 40,000-byte chunks exceed the core's 64 KiB response limit
    let chunk = vec![b'y'; 40_000];
    let mut response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    for _ in 0..2 {
        response.extend_from_slice(b"9c40\r\n");
        response.extend_from_slice(&chunk);
        response.extend_from_slice(b"\r\n");
    }
    response.extend_from_slice(b"0\r\n\r\n");
    let addr = start_stub_server(response);

    let (result, _) = run_worker(addr, Duration::from_secs(2), 1);
    let error = result.expect_err("An oversize response should fail the worker");
    assert!(error.to_string().contains("maximum payload"), "Unexpected error: {error}");
}
