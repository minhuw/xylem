#![cfg(all(target_os = "linux", feature = "ntcp-dpdk"))]

use ntcp_io::PacketIo;
use ntcp_io_dpdk::{Eal, Port, PortConfig};
use ntcp_native::{
    EthernetNextHop, Event, Link, Network, NetworkConfig, ReadOutcome, WorkBudget, WriteOptions,
};
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use xylem_transport::{
    time_ns, ConnectionGroup, NtcpDpdkConfig, NtcpDpdkMember, NtcpDpdkPort, RecvOutcome,
    TransportFactory,
};

const HELPER_ENV: &str = "XYLEM_MEMIF_HELPER";
const SOCKET_ENV: &str = "XYLEM_MEMIF_SOCKET";
const PREFIX_ENV: &str = "XYLEM_MEMIF_PREFIX";
const PORT_ENV: &str = "XYLEM_MEMIF_PORT";

const SERVER_IP: Ipv4Addr = Ipv4Addr::new(10, 77, 0, 1);
const CLIENT_IP: Ipv4Addr = Ipv4Addr::new(10, 77, 0, 2);
const SERVER_PORT: u16 = 19000;
const UNITS_PER_SECOND: u64 = 1_000_000_000;
const WORK_BUDGET: WorkBudget = WorkBudget::new(64, 64, 64, 64);

/// Minimal stderr tracing subscriber so test output includes `NtcpDpdkPort`
/// and `NtcpConnectionGroup` logs without adding a `tracing-subscriber`
/// dev-dependency.
struct StderrSubscriber {
    next_id: AtomicU64,
}

impl StderrSubscriber {
    fn install() {
        let _ = tracing::subscriber::set_global_default(Self { next_id: AtomicU64::new(1) });
    }
}

struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        if field.name() == "message" {
            self.0.push_str(&format!("{value:?}"));
        } else {
            self.0.push_str(&format!("{}={value:?}", field.name()));
        }
    }
}

impl tracing::Subscriber for StderrSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::INFO
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        eprintln!("[{}] {}: {}", event.metadata().level(), event.metadata().target(), visitor.0);
    }

    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

struct SocketCleanup(PathBuf);

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Ensures a spawned helper child is terminated and reaped if the parent test
/// unwinds early.
struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn take_stdout(&mut self) -> Option<std::process::ChildStdout> {
        self.0.as_mut().and_then(|c| c.stdout.take())
    }

    fn wait(mut self) -> std::io::Result<std::process::ExitStatus> {
        self.0.take().expect("child already waited").wait()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn first_allowed_cpu() -> usize {
    // SAFETY: an all-zero cpu_set_t is a valid empty CPU set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: writes into `set` for the calling thread (pid 0).
    let rc = unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) };
    assert_eq!(rc, 0, "sched_getaffinity failed");
    (0..libc::CPU_SETSIZE as usize)
        // SAFETY: `cpu` is strictly below `CPU_SETSIZE`.
        .find(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
        .expect("calling thread has at least one allowed CPU")
}

/// Create a short temporary directory under `/tmp` for `XDG_RUNTIME_DIR` so
/// DPDK's `<runtime>/dpdk/<prefix>/dpdk_telemetry.v2` socket path stays well
/// within `UNIX_PATH_MAX` (108 bytes) even when `TMPDIR` is deeply nested,
/// and all DPDK runtime files are removed automatically when dropped.
fn dpdk_runtime_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("xm-")
        .tempdir_in("/tmp")
        .expect("create temporary DPDK runtime dir in /tmp")
}

fn drain_server_reads<P: PacketIo>(
    net: &mut Network<P>,
    conn: ntcp_native::ConnId,
    read_buf: &mut [u8],
    pending_out: &mut HashMap<ntcp_native::ConnId, Vec<u8>>,
) {
    loop {
        match net.try_read(conn, read_buf) {
            Ok(ReadOutcome::Data { len, .. }) => {
                if let Some(buf) = pending_out.get_mut(&conn) {
                    buf.extend_from_slice(&read_buf[..len]);
                }
            }
            Ok(ReadOutcome::Eof) => {
                let _ = net.close(conn);
                break;
            }
            Ok(ReadOutcome::EmptyBuffer) | Err(ntcp_native::Error::WouldBlock(_)) => break,
            Err(e) => panic!("server try_read failed: {e}"),
        }
    }
}

fn flush_server_writes<P: PacketIo>(
    net: &mut Network<P>,
    pending_out: &mut HashMap<ntcp_native::ConnId, Vec<u8>>,
) {
    let conns: Vec<_> = pending_out.keys().copied().collect();
    for conn in conns {
        let Some(buf) = pending_out.get_mut(&conn) else {
            continue;
        };
        while !buf.is_empty() {
            match net.try_write(conn, buf, WriteOptions::default()) {
                Ok(receipt) if receipt.accepted > 0 => {
                    buf.drain(..receipt.accepted);
                    let _ = net.push(conn, time_ns());
                }
                Ok(_) | Err(ntcp_native::Error::WouldBlock(_)) => break,
                Err(e) => panic!("server try_write failed: {e}"),
            }
        }
    }
}

#[test]
fn memif_server_helper() {
    if env::var(HELPER_ENV).ok().as_deref() != Some("server") {
        return;
    }
    StderrSubscriber::install();

    let sock = env::var(SOCKET_ENV).expect("socket env");
    let prefix = env::var(PREFIX_ENV).expect("prefix env");
    let tcp_port: u16 = env::var(PORT_ENV).expect("port env").parse().expect("port");
    let cpu = first_allowed_cpu();

    let device = format!("net_memif,role=server,socket={sock}");
    let eal_args = vec![
        "--vdev".to_string(),
        device.clone(),
        "--no-pci".to_string(),
        format!("--lcores=0@{cpu}"),
        "--no-huge".to_string(),
        "-m".to_string(),
        "512".to_string(),
        "--file-prefix".to_string(),
        prefix,
    ];
    let eal = Eal::init(eal_args).expect("server Eal::init");

    let mut port_config = PortConfig::new(device.clone());
    port_config.queues = 1;
    port_config.mtu = 1500;
    port_config.isolate = false;
    let port = Port::start(eal, port_config).expect("server Port::start");
    let mac = port.mac_address().expect("server mac_address");

    let queue = port.take_queue(0).expect("server take_queue").attach().expect("server attach");
    assert!(
        queue.rx_frame_budget().unwrap_or(0) > 0,
        "server queue should have positive rx_frame_budget"
    );

    let mut config = NetworkConfig::new(IpAddr::V4(SERVER_IP), 1500);
    config.tcp.max_connections = 8;
    config.tcp.connection.timebase.units_per_second = UNITS_PER_SECOND;
    config.link = Link::Ethernet {
        local_mac: mac,
        next_hop: EthernetNextHop::Arp { prefix_len: 24, gateway: None },
    };
    let secret = [0x5Au8; 32];
    let mut net = Network::new(config, queue, secret, time_ns()).expect("server Network::new");
    let listener = net.listen(tcp_port, 8).expect("server listen");

    println!("SERVER_READY");

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut events = vec![Event::NetworkStopped; 64];
    let mut pending_out: HashMap<ntcp_native::ConnId, Vec<u8>> = HashMap::new();
    let mut read_buf = vec![0u8; 65536];
    let mut accepted_any = false;
    let mut closed_all = false;

    while Instant::now() < deadline {
        net.drive(time_ns(), WORK_BUDGET).expect("server drive");

        let report = net.drain_events(&mut events);
        for &event in &events[..report.written] {
            match event {
                Event::Acceptable(l) if l == listener => {
                    while let Ok(conn) = net.accept(listener) {
                        accepted_any = true;
                        pending_out.insert(conn, Vec::new());
                    }
                }
                Event::Readable(conn) => {
                    drain_server_reads(&mut net, conn, &mut read_buf, &mut pending_out);
                }
                Event::Closed { connection, .. } => {
                    pending_out.remove(&connection);
                    if accepted_any && pending_out.is_empty() {
                        closed_all = true;
                    }
                }
                _ => {}
            }
        }

        flush_server_writes(&mut net, &mut pending_out);

        if closed_all {
            let linger = Instant::now() + Duration::from_millis(50);
            while Instant::now() < linger {
                let _ = net.drive(time_ns(), WORK_BUDGET);
                std::thread::yield_now();
            }
            break;
        }
        std::thread::yield_now();
    }

    assert!(accepted_any, "server never accepted a client connection");
    assert!(closed_all, "server connection did not cleanly close before deadline");
    drop(net);
    drop(port);
}

#[test]
fn memif_client_helper() {
    if env::var(HELPER_ENV).ok().as_deref() != Some("client") {
        return;
    }
    StderrSubscriber::install();

    let sock = env::var(SOCKET_ENV).expect("socket env");
    let prefix = env::var(PREFIX_ENV).expect("prefix env");
    let tcp_port: u16 = env::var(PORT_ENV).expect("port env").parse().expect("port");

    let port = NtcpDpdkPort::start(NtcpDpdkConfig {
        device: format!("net_memif,role=client,socket={sock}"),
        eal_args: vec![
            "--no-huge".to_string(),
            "-m".to_string(),
            "512".to_string(),
            "--file-prefix".to_string(),
            prefix,
        ],
        mtu: 1500,
        prefix_len: 24,
        gateway: None,
        mbufs_per_queue: None,
        members: vec![NtcpDpdkMember {
            local_ip: CLIENT_IP,
            max_connections: 4,
            max_pending: 4,
            nic_timestamps: false,
        }],
    })
    .expect("client NtcpDpdkPort::start");

    let mut group = port.factory(0).create_group().expect("client create_group");
    assert_eq!(
        group.network().backend().rx_frame_budget(),
        Some(12_895),
        "unexpected rx_frame_budget for default mbufs_per_queue=16383 on single queue"
    );

    let target = SocketAddr::new(IpAddr::V4(SERVER_IP), tcp_port);
    let conn_id = group.add_connection(&target).expect("client add_connection");

    // 1. Small single-segment echo
    let small = b"hello xylem net_memif copy mode!\n";
    group.send(conn_id, small).expect("client send small");
    let got_small = recv_exact(&mut group, conn_id, small.len(), Duration::from_secs(5));
    assert_eq!(got_small.as_slice(), small, "small echo mismatch");

    // 2. Multi-segment payload (64 KiB)
    let large: Vec<u8> = (0..65536u32).map(|i| (i.wrapping_mul(31) & 0xff) as u8).collect();
    let mut sent = 0;
    let mut received = Vec::with_capacity(large.len());
    let mut ready = Vec::new();
    let mut buf = vec![0u8; 16384];
    let deadline = Instant::now() + Duration::from_secs(10);

    while received.len() < large.len() {
        assert!(
            Instant::now() < deadline,
            "large echo timed out: sent={sent}, received={}",
            received.len()
        );
        if sent < large.len() {
            let chunk_end = (sent + 4096).min(large.len());
            let progress = group.try_send(conn_id, &large[sent..chunk_end]).expect("try_send");
            sent += progress.accepted;
        }
        ready.clear();
        group.poll(Duration::from_millis(10), &mut ready).expect("client poll");
        for &id in &ready {
            loop {
                match group.recv(id, &mut buf).expect("client recv") {
                    RecvOutcome::Data { len, .. } => received.extend_from_slice(&buf[..len]),
                    RecvOutcome::WouldBlock => break,
                    RecvOutcome::Eof => panic!("unexpected EOF during large echo"),
                }
            }
        }
    }
    assert_eq!(received, large, "large echo payload mismatch");

    group.close_all().expect("client close_all");
    drop(group);
    drop(port);
}

#[test]
fn memif_multi_queue_helper() {
    if env::var(HELPER_ENV).ok().as_deref() != Some("multi_queue") {
        return;
    }
    StderrSubscriber::install();

    let sock = env::var(SOCKET_ENV).expect("socket env");
    let prefix = env::var(PREFIX_ENV).expect("prefix env");

    let err = NtcpDpdkPort::start(NtcpDpdkConfig {
        device: format!("net_memif,role=server,socket={sock}"),
        eal_args: vec![
            "--no-huge".to_string(),
            "-m".to_string(),
            "512".to_string(),
            "--file-prefix".to_string(),
            prefix,
        ],
        mtu: 1500,
        prefix_len: 24,
        gateway: None,
        mbufs_per_queue: None,
        members: vec![
            NtcpDpdkMember {
                local_ip: Ipv4Addr::new(10, 77, 0, 2),
                max_connections: 4,
                max_pending: 4,
                nic_timestamps: false,
            },
            NtcpDpdkMember {
                local_ip: Ipv4Addr::new(10, 77, 0, 3),
                max_connections: 4,
                max_pending: 4,
                nic_timestamps: false,
            },
        ],
    })
    .err()
    .expect("net_memif with 2 members must fail without rte_flow steering");

    let msg = err.to_string();
    assert!(msg.contains("steer 10.77.0.2 to queue 0"), "unexpected error: {msg}");
}

fn recv_exact<G: ConnectionGroup>(
    group: &mut G,
    conn_id: usize,
    want: usize,
    timeout: Duration,
) -> Vec<u8> {
    let deadline = Instant::now() + timeout;
    let mut out = Vec::with_capacity(want);
    let mut ready = Vec::new();
    let mut buf = vec![0u8; 4096];
    while out.len() < want {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {want} bytes, got {}",
            out.len()
        );
        ready.clear();
        group.poll(Duration::from_millis(10), &mut ready).expect("poll");
        for &id in &ready {
            if id != conn_id {
                continue;
            }
            loop {
                match group.recv(id, &mut buf).expect("recv") {
                    RecvOutcome::Data { len, .. } => out.extend_from_slice(&buf[..len]),
                    RecvOutcome::WouldBlock => break,
                    RecvOutcome::Eof => {
                        panic!("unexpected EOF after {} of {want} bytes", out.len())
                    }
                }
            }
        }
    }
    out
}

#[test]
fn memif_copy_mode_echo() {
    if env::var(HELPER_ENV).is_ok() {
        return;
    }

    let pid = std::process::id();
    let sock_path = PathBuf::from(format!("/tmp/xylem-memif-test-{pid}.sock"));
    let _sock_guard = SocketCleanup(sock_path.clone());
    let _ = std::fs::remove_file(&sock_path);

    let runtime_dir = dpdk_runtime_dir();
    let exe = env::current_exe().expect("current_exe");

    let mut server = ChildGuard::new(
        Command::new(&exe)
            .args(["--exact", "memif_server_helper", "--nocapture"])
            .env(HELPER_ENV, "server")
            .env(SOCKET_ENV, &sock_path)
            .env(PREFIX_ENV, format!("xm-srv-{pid}"))
            .env(PORT_ENV, SERVER_PORT.to_string())
            .env("XDG_RUNTIME_DIR", runtime_dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn server helper"),
    );

    let server_stdout = server.take_stdout().expect("server stdout");
    let mut reader = BufReader::new(server_stdout);
    let mut ready = false;
    let mut line = String::new();
    while reader.read_line(&mut line).expect("read server stdout") > 0 {
        if line.trim() == "SERVER_READY" {
            ready = true;
            break;
        }
        line.clear();
    }
    assert!(ready, "server failed before printing SERVER_READY");

    let client_status = Command::new(&exe)
        .args(["--exact", "memif_client_helper", "--nocapture"])
        .env(HELPER_ENV, "client")
        .env(SOCKET_ENV, &sock_path)
        .env(PREFIX_ENV, format!("xm-cli-{pid}"))
        .env(PORT_ENV, SERVER_PORT.to_string())
        .env("XDG_RUNTIME_DIR", runtime_dir.path())
        .status()
        .expect("run client helper");

    let server_status = server.wait().expect("wait server helper");
    assert!(client_status.success(), "client helper failed: {client_status}");
    assert!(server_status.success(), "server helper failed: {server_status}");
}

#[test]
fn memif_multi_queue_requires_rte_flow() {
    if env::var(HELPER_ENV).is_ok() {
        return;
    }

    let pid = std::process::id();
    let sock_path = PathBuf::from(format!("/tmp/xylem-memif-mq-{pid}.sock"));
    let _sock_guard = SocketCleanup(sock_path.clone());
    let _ = std::fs::remove_file(&sock_path);

    let runtime_dir = dpdk_runtime_dir();
    let exe = env::current_exe().expect("current_exe");

    let status = Command::new(&exe)
        .args(["--exact", "memif_multi_queue_helper", "--nocapture"])
        .env(HELPER_ENV, "multi_queue")
        .env(SOCKET_ENV, &sock_path)
        .env(PREFIX_ENV, format!("xm-mq-{pid}"))
        .env("XDG_RUNTIME_DIR", runtime_dir.path())
        .status()
        .expect("run multi_queue helper");

    assert!(status.success(), "multi_queue helper failed: {status}");
}
