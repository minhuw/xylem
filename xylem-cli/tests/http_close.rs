//! End-to-end HTTP `1-req/conn` (`Connection: close`) tests against a real
//! loopback TCP server that drops every connection (sending FIN) immediately
//! after writing one HTTP/1.1 response.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use xylem_cli::output::DetailedExperimentResults;
use xylem_core::scheduler::UniformPolicyScheduler;
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};
use xylem_core::threading::{Worker, WorkerConfig};
use xylem_core::traffic_group::{OnError, PolicyConfig, TimestampSourceConfig, TrafficGroupConfig};
use xylem_protocols::http::HttpProtocol;
use xylem_protocols::HttpMethod;
use xylem_transport::TcpTransportFactory;

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";

struct CloseServer {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
    missing_close_header: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl CloseServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
        listener.set_nonblocking(true).expect("set nonblocking");
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let missing_close_header = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let accepted_c = Arc::clone(&accepted);
        let missing_c = Arc::clone(&missing_close_header);
        let stop_c = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            while !stop_c.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        accepted_c.fetch_add(1, Ordering::Relaxed);
                        let missing = Arc::clone(&missing_c);
                        thread::spawn(move || handle_one_request(stream, &missing));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_micros(200));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            addr,
            accepted,
            missing_close_header,
            stop,
            handle: Some(handle),
        }
    }

    fn accepted_count(&self) -> usize {
        self.accepted.load(Ordering::Relaxed)
    }

    fn missing_close_count(&self) -> usize {
        self.missing_close_header.load(Ordering::Relaxed)
    }
}

impl Drop for CloseServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Read one HTTP/1.1 request, verify `Connection: close`, reply, and immediately drop (FIN).
fn handle_one_request(mut stream: TcpStream, missing_close_header: &AtomicUsize) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let mut req = Vec::with_capacity(256);
    let mut buf = [0u8; 512];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                req.extend_from_slice(&buf[..n]);
                if req.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }
    let text = String::from_utf8_lossy(&req);
    if !text.contains("Connection: close\r\n") {
        missing_close_header.fetch_add(1, Ordering::Relaxed);
    }
    let _ = stream.write_all(RESPONSE);
    // Drop `stream` immediately to send TCP FIN.
}

fn group_config(addr: SocketAddr, timestamps: TimestampSourceConfig) -> TrafficGroupConfig {
    TrafficGroupConfig {
        name: "http-close".to_string(),
        protocol: "http".to_string(),
        target: addr.to_string(),
        transport: "tcp".to_string(),
        threads: vec![0],
        connections_per_thread: 4,
        max_pending_per_connection: 1,
        max_requests_per_connection: Some(1),
        request_timeout: None,
        on_error: OnError::Count,
        timestamps,
        traffic_policy: PolicyConfig::ClosedLoop,
        sampling_policy: SamplingPolicy::Unlimited,
        protocol_config: None,
    }
}

#[test]
fn http_connection_close_closed_loop_reconnects_and_reports_connect_breakdown() {
    let server = CloseServer::start();
    let protocol = HttpProtocol::with_options(
        HttpMethod::Get,
        "/".to_string(),
        "localhost".to_string(),
        0,
        false,
    );
    let stats = TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));
    let config = WorkerConfig {
        target: server.addr,
        duration: Duration::from_millis(200),
        conn_count: 4,
        max_pending_per_conn: 1,
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
    };
    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config)
            .expect("create worker");
    worker.set_max_requests_per_connection(0, Some(1));

    worker.run().expect("worker run succeeds without errors");
    let stats = worker.into_stats();
    let counts = stats.request_counts_global();

    assert_eq!(counts.failed, 0, "expected 0 failed requests: {counts:?}");
    assert_eq!(counts.errors, 0, "expected 0 error replies: {counts:?}");
    assert_eq!(counts.timed_out, 0, "expected 0 timed-out requests: {counts:?}");
    assert!(
        counts.completed >= 50,
        "expected >= 50 completed requests, got {}",
        counts.completed
    );
    assert!(
        server.accepted_count() >= 50,
        "expected >= 50 distinct TCP connections, got {}",
        server.accepted_count()
    );
    assert_eq!(server.missing_close_count(), 0, "every request must carry Connection: close");

    let results = DetailedExperimentResults::from_tuple_stats(
        "http-close-test".to_string(),
        None,
        None,
        server.addr.to_string(),
        "http".to_string(),
        "tcp".to_string(),
        Duration::from_millis(200),
        &stats,
        &[group_config(server.addr, TimestampSourceConfig::User)],
        false,
    );
    let bd = &results.global.latency_breakdown;
    let connect = bd.connect_us.as_ref().expect("connect_us must be populated");
    let connect_req = bd.connect_request_us.as_ref().expect("connect_request_us must be populated");
    assert!(connect.sample_count > 0);
    assert_eq!(connect.sample_count, connect_req.sample_count);
    assert!(
        connect_req.mean_us.unwrap() >= connect.mean_us.unwrap(),
        "connect_request_us ({:?}) must be >= connect_us ({:?})",
        connect_req.mean_us,
        connect.mean_us
    );
    assert!(
        connect_req.mean_us.unwrap() >= bd.service_us.mean_us.unwrap() * 0.5,
        "connect_request_us ({:?}) vs service_us ({:?})",
        connect_req.mean_us,
        bd.service_us.mean_us
    );

    // JSON includes connect_us and connect_request_us
    let json = serde_json::to_value(&results).unwrap();
    assert!(json["global"]["latency_breakdown"]["connect_us"].is_object());
    assert!(json["global"]["latency_breakdown"]["connect_request_us"].is_object());
}

#[test]
fn http_connection_close_open_loop_fixed_rate() {
    let server = CloseServer::start();
    let protocol = HttpProtocol::with_options(
        HttpMethod::Get,
        "/".to_string(),
        "localhost".to_string(),
        0,
        false,
    );
    let stats = TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));
    let config = WorkerConfig {
        target: server.addr,
        duration: Duration::from_millis(250),
        conn_count: 4,
        max_pending_per_conn: 1,
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
    };
    let mut worker = Worker::new(
        &TcpTransportFactory::default(),
        protocol,
        stats,
        config,
        Box::new(UniformPolicyScheduler::fixed_rate(400.0)),
    )
    .expect("create open-loop worker");
    worker.set_max_requests_per_connection(0, Some(1));

    worker.run().expect("open-loop worker run succeeds");
    let stats = worker.into_stats();
    let counts = stats.request_counts_global();

    assert_eq!(counts.failed, 0, "{counts:?}");
    assert_eq!(counts.errors, 0, "{counts:?}");
    assert!(counts.completed >= 50, "completed={}", counts.completed);
    assert!(server.accepted_count() >= 50, "accepted={}", server.accepted_count());
    assert_eq!(server.missing_close_count(), 0);
    assert!(stats.send_lag_global().sample_count >= 50);

    let bd = stats.latency_breakdown_global();
    assert!(bd.connect.unwrap().sample_count >= 45);
    assert!(bd.connect_request.unwrap().sample_count >= 45);
}

#[cfg(target_os = "linux")]
#[test]
fn http_connection_close_kernel_timestamps_have_zero_tx_misses() {
    let server = CloseServer::start();
    let protocol = HttpProtocol::with_options(
        HttpMethod::Get,
        "/".to_string(),
        "localhost".to_string(),
        0,
        false,
    );
    let stats = TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));
    let config = WorkerConfig {
        target: server.addr,
        duration: Duration::from_millis(200),
        conn_count: 4,
        max_pending_per_conn: 1,
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
    };
    let factory = TcpTransportFactory::with_timestamp_config(
        xylem_transport::TcpTimestampConfig::with_software_timestamps(),
    );
    let mut worker = Worker::with_closed_loop(&factory, protocol, stats, config)
        .expect("create kernel-ts worker");
    worker.set_max_requests_per_connection(0, Some(1));

    worker.run().expect("kernel-ts worker run succeeds");
    let stats = worker.into_stats();
    let counts = stats.request_counts_global();
    let bd = stats.latency_breakdown_global();

    assert_eq!(counts.failed, 0, "{counts:?}");
    assert_eq!(counts.errors, 0, "{counts:?}");
    assert!(counts.completed >= 50, "completed={}", counts.completed);
    assert!(server.accepted_count() >= 50, "accepted={}", server.accepted_count());
    assert_eq!(server.missing_close_count(), 0);

    assert_eq!(
        bd.kernel_split_missing.tx, 0,
        "draining before close must lose 0 TX timestamps: {bd:?}"
    );
    assert_eq!(bd.kernel_split_missing.rx, 0, "{bd:?}");
    assert_eq!(bd.kernel_split_count, counts.completed, "{bd:?}");
    assert!(bd.network_server.is_some());
    assert!(bd.connect.unwrap().sample_count > 0);
    assert!(bd.connect_request.unwrap().sample_count > 0);
}
