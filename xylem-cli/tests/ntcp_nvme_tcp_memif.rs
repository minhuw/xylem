#![cfg(all(target_os = "linux", feature = "ntcp-dpdk"))]

use ntcp_io::PacketIo;
use ntcp_io_dpdk::{Eal, Port, PortConfig};
use ntcp_native::{
    ConnId, EthernetNextHop, Event, Link, Network, NetworkConfig, ReadOutcome, WorkBudget,
    WriteOptions,
};
use std::collections::HashMap;
use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use xylem_transport::time_ns;

const HELPER_ENV: &str = "XYLEM_NVME_MEMIF_HELPER";
const SOCKET_ENV: &str = "XYLEM_NVME_MEMIF_SOCKET";
const PREFIX_ENV: &str = "XYLEM_NVME_MEMIF_PREFIX";
const PORT_ENV: &str = "XYLEM_NVME_MEMIF_PORT";
const CPU_ENV: &str = "XYLEM_NVME_MEMIF_CPU";

const SERVER_IP: Ipv4Addr = Ipv4Addr::new(10, 77, 1, 1);
const CLIENT_IP: Ipv4Addr = Ipv4Addr::new(10, 77, 1, 2);
const SERVER_PORT: u16 = 4420;
const SUBNQN: &str = "nqn.2016-06.io.spdk:xylem-memif";

const UNITS_PER_SECOND: u64 = 1_000_000_000;
const WORK_BUDGET: WorkBudget = WorkBudget::new(64, 64, 64, 64);

// NVMe/TCP PDU constants (NVMe/TCP Transport Specification 1.0)
const PDU_IC_REQ: u8 = 0x00;
const PDU_IC_RESP: u8 = 0x01;
const PDU_CAPSULE_CMD: u8 = 0x04;
const PDU_CAPSULE_RESP: u8 = 0x05;
const PDU_H2C_DATA: u8 = 0x06;
const PDU_C2H_DATA: u8 = 0x07;
const PDU_R2T: u8 = 0x09;

const FLAG_LAST_PDU: u8 = 0x04;
const FLAG_SUCCESS: u8 = 0x08;

const OPC_WRITE: u8 = 0x01;
const OPC_READ: u8 = 0x02;
const OPC_IDENTIFY: u8 = 0x06;
const OPC_KEEP_ALIVE: u8 = 0x18;
const OPC_FABRICS: u8 = 0x7f;

const FCTYPE_PROPERTY_SET: u8 = 0x00;
const FCTYPE_CONNECT: u8 = 0x01;
const FCTYPE_PROPERTY_GET: u8 = 0x04;

const PROP_CAP: u32 = 0x00;
const PROP_CC: u32 = 0x14;
const PROP_CSTS: u32 = 0x1c;

const LBA_SIZE: usize = 512;
const NSZE: u64 = 32768; // 16 MiB
const IN_CAPSULE_SIZE: u32 = 4096;
const MAXH2CDATA: u32 = 16384;
const C2H_SPLIT: usize = 16384;
const MQES: u16 = 127;

/// Serialize DPDK memif tests so concurrent tests do not allocate multiple
/// 512 MiB `--no-huge` pools or contend for the same pinned CPUs.
static SERIAL: Mutex<()> = Mutex::new(());
static RUN_SEQ: AtomicU64 = AtomicU64::new(0);

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

/// Short temporary directory under `/tmp` so both the memif socket path and
/// DPDK's `<XDG_RUNTIME_DIR>/dpdk/<prefix>/dpdk_telemetry.v2` stay well within
/// `UNIX_PATH_MAX` (108 bytes), with automatic RAII cleanup on drop.
fn short_tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("xnm-")
        .tempdir_in("/tmp")
        .expect("create temporary dir in /tmp")
}

fn allowed_cpus() -> Vec<usize> {
    let cpus = xylem_core::threading::get_available_core_ids()
        .expect("read allowed CPUs via sched_getaffinity");
    assert!(!cpus.is_empty(), "calling thread has no allowed CPUs");
    cpus
}

fn u16_at(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap())
}

fn u32_at(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap())
}

fn pdu_header(pdu_type: u8, flags: u8, hlen: u8, pdo: u8, plen: usize, out: &mut Vec<u8>) {
    out.extend_from_slice(&[pdu_type, flags, hlen, pdo]);
    out.extend_from_slice(&(plen as u32).to_le_bytes());
}

fn push_capsule_resp(cid: u16, dw0: u32, dw1: u32, status: (u8, u8), out: &mut Vec<u8>) {
    pdu_header(PDU_CAPSULE_RESP, 0, 24, 0, 24, out);
    out.extend_from_slice(&dw0.to_le_bytes());
    out.extend_from_slice(&dw1.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&cid.to_le_bytes());
    let status_field = ((u16::from(status.0) << 8) | u16::from(status.1)) << 1 | 1;
    out.extend_from_slice(&status_field.to_le_bytes());
}

struct ConnState {
    rx: Vec<u8>,
    tx: Vec<u8>,
    tx_offset: usize,
    cntlid: Option<u16>,
    /// Active R2T writes keyed by `cccid`: `(expected_len, received_bytes)`
    r2t_writes: HashMap<u16, (usize, usize)>,
}

impl ConnState {
    fn new() -> Self {
        Self {
            rx: Vec::with_capacity(65536),
            tx: Vec::with_capacity(65536),
            tx_offset: 0,
            cntlid: None,
            r2t_writes: HashMap::new(),
        }
    }

    fn pending_tx(&self) -> &[u8] {
        &self.tx[self.tx_offset..]
    }

    fn advance_tx(&mut self, n: usize) {
        self.tx_offset += n;
        if self.tx_offset == self.tx.len() {
            self.tx.clear();
            self.tx_offset = 0;
        } else if self.tx_offset >= 65536 {
            self.tx.drain(..self.tx_offset);
            self.tx_offset = 0;
        }
    }
}

/// Minimal in-process NVMe/TCP target matching Xylem's `NvmeTcpProtocol`.
struct FakeNvmeTarget {
    next_cntlid: u16,
    /// Controller ID -> enabled (CC.EN)
    controllers: HashMap<u16, bool>,
    conns: HashMap<ConnId, ConnState>,
    /// Reusable payload buffer for read responses (up to 256 KiB)
    read_payload: Vec<u8>,
}

impl FakeNvmeTarget {
    fn new() -> Self {
        Self {
            next_cntlid: 0x21,
            controllers: HashMap::new(),
            conns: HashMap::new(),
            read_payload: vec![0xA5u8; 256 * 1024],
        }
    }

    fn add_conn(&mut self, conn: ConnId) {
        self.conns.insert(conn, ConnState::new());
    }

    fn remove_conn(&mut self, conn: ConnId) {
        self.conns.remove(&conn);
    }

    fn is_empty(&self) -> bool {
        self.conns.is_empty()
    }

    fn receive(&mut self, conn: ConnId, data: &[u8]) {
        let mut pdus = Vec::new();
        {
            let Some(state) = self.conns.get_mut(&conn) else {
                return;
            };
            state.rx.extend_from_slice(data);

            let mut consumed = 0;
            while state.rx.len() - consumed >= 8 {
                let plen = u32_at(&state.rx, consumed + 4) as usize;
                assert!(plen >= 8, "invalid NVMe/TCP PDU length {plen}");
                if state.rx.len() - consumed < plen {
                    break;
                }
                pdus.push(state.rx[consumed..consumed + plen].to_vec());
                consumed += plen;
            }

            if consumed == state.rx.len() {
                state.rx.clear();
            } else if consumed > 0 {
                state.rx.drain(..consumed);
            }
        }

        for pdu in pdus {
            self.handle_pdu(conn, &pdu);
        }
    }

    fn handle_pdu(&mut self, conn: ConnId, pdu: &[u8]) {
        match pdu[0] {
            PDU_IC_REQ => {
                assert_eq!(pdu.len(), 128, "ICReq length");
                assert_eq!(pdu[11], 0, "host requests no digests");
                let state = self.conns.get_mut(&conn).unwrap();
                let start = state.tx.len();
                pdu_header(PDU_IC_RESP, 0, 128, 0, 128, &mut state.tx);
                state.tx.resize(start + 128, 0);
                state.tx[start + 12..start + 16].copy_from_slice(&MAXH2CDATA.to_le_bytes());
            }
            PDU_CAPSULE_CMD => {
                let pdo = pdu[3] as usize;
                let data = if pdo == 0 { &[][..] } else { &pdu[pdo..] };
                self.handle_command(conn, &pdu[8..72], data);
            }
            PDU_H2C_DATA => {
                let cccid = u16_at(pdu, 8);
                let ttag = u16_at(pdu, 10);
                let datao = u32_at(pdu, 12) as usize;
                let datal = u32_at(pdu, 16) as usize;
                let pdo = pdu[3] as usize;
                let last = (pdu[1] & FLAG_LAST_PDU) != 0;

                assert_eq!(ttag, cccid.wrapping_add(100), "H2CData ttag mismatch");
                assert!(datal <= MAXH2CDATA as usize, "H2CData exceeds MAXH2CDATA");
                assert_eq!(pdu.len(), pdo + datal, "H2CData length mismatch");

                let state = self.conns.get_mut(&conn).unwrap();
                let (expected_len, received) =
                    state.r2t_writes.get_mut(&cccid).expect("H2CData for active R2T write");
                assert_eq!(datao, *received, "H2CData out of order");
                *received += datal;
                assert_eq!(last, *received == *expected_len, "LAST_PDU flag mismatch");
                if last {
                    state.r2t_writes.remove(&cccid);
                    push_capsule_resp(cccid, 0, 0, (0, 0), &mut state.tx);
                }
            }
            other => panic!("unexpected NVMe/TCP host PDU type {other:#x}"),
        }
    }

    fn push_c2h(out: &mut Vec<u8>, cid: u16, data: &[u8], success_flag: bool) {
        let chunks: Vec<&[u8]> = data.chunks(C2H_SPLIT).collect();
        let mut offset = 0usize;
        for (idx, chunk) in chunks.iter().enumerate() {
            let last = idx + 1 == chunks.len();
            let mut flags = if last { FLAG_LAST_PDU } else { 0 };
            if last && success_flag {
                flags |= FLAG_SUCCESS;
            }
            pdu_header(PDU_C2H_DATA, flags, 24, 24, 24 + chunk.len(), out);
            out.extend_from_slice(&cid.to_le_bytes());
            out.extend_from_slice(&[0; 2]);
            out.extend_from_slice(&(offset as u32).to_le_bytes());
            out.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
            out.extend_from_slice(&[0; 4]);
            out.extend_from_slice(chunk);
            offset += chunk.len();
        }
        if !success_flag {
            push_capsule_resp(cid, 0, 0, (0, 0), out);
        }
    }

    fn handle_command(&mut self, conn: ConnId, sqe: &[u8], data: &[u8]) {
        let opc = sqe[0];
        let cid = u16_at(sqe, 2);
        let ok = (0, 0);

        match (opc, sqe[4]) {
            (OPC_FABRICS, FCTYPE_CONNECT) => {
                let qid = u16_at(sqe, 42);
                let cntlid = u16_at(data, 16);
                assert_eq!(&data[256..256 + SUBNQN.len()], SUBNQN.as_bytes(), "subnqn mismatch");

                if qid == 0 {
                    assert_eq!(cntlid, 0xffff, "dynamic controller connect");
                    let assigned = self.next_cntlid;
                    self.next_cntlid = self.next_cntlid.wrapping_add(1);
                    self.controllers.insert(assigned, false);
                    let state = self.conns.get_mut(&conn).unwrap();
                    state.cntlid = Some(assigned);
                    push_capsule_resp(cid, u32::from(assigned), 0, ok, &mut state.tx);
                } else {
                    let enabled = self.controllers.get(&cntlid).copied().unwrap_or(false);
                    let state = self.conns.get_mut(&conn).unwrap();
                    if enabled {
                        state.cntlid = Some(cntlid);
                        push_capsule_resp(cid, 0, 0, ok, &mut state.tx);
                    } else {
                        push_capsule_resp(cid, 0, 0, (1, 0x82), &mut state.tx);
                    }
                }
            }
            (OPC_FABRICS, FCTYPE_PROPERTY_GET) => {
                let offset = u32_at(sqe, 44);
                let cntlid = self.conns[&conn].cntlid;
                let enabled =
                    cntlid.and_then(|c| self.controllers.get(&c).copied()).unwrap_or(false);
                let value = match offset {
                    // MQES, CSS NVM (bit 37), MPSMIN 0
                    PROP_CAP => u64::from(MQES) | (1u64 << 37),
                    PROP_CSTS => u64::from(enabled),
                    other => panic!("unexpected property get {other:#x}"),
                };
                let state = self.conns.get_mut(&conn).unwrap();
                push_capsule_resp(cid, value as u32, (value >> 32) as u32, ok, &mut state.tx);
            }
            (OPC_FABRICS, FCTYPE_PROPERTY_SET) => {
                assert_eq!(u32_at(sqe, 44), PROP_CC);
                let value = u32_at(sqe, 48);
                let cntlid = self.conns[&conn].cntlid.expect("admin queue has cntlid");
                self.controllers.insert(cntlid, (value & 1) == 1);
                let state = self.conns.get_mut(&conn).unwrap();
                push_capsule_resp(cid, 0, 0, ok, &mut state.tx);
            }
            (OPC_IDENTIFY, _) => {
                let mut identify = [0u8; 4096];
                let state = self.conns.get_mut(&conn).unwrap();
                if sqe[40] == 1 {
                    // Identify Controller: MDTS = 6 (256 KiB), IOCCSZ = (IN_CAPSULE + 64) / 16
                    identify[77] = 6;
                    let ioccsz = (IN_CAPSULE_SIZE + 64) / 16;
                    identify[1792..1796].copy_from_slice(&ioccsz.to_le_bytes());
                    Self::push_c2h(&mut state.tx, cid, &identify, false);
                } else {
                    // Identify Namespace
                    identify[..8].copy_from_slice(&NSZE.to_le_bytes());
                    identify[128 + 2] = LBA_SIZE.trailing_zeros() as u8;
                    Self::push_c2h(&mut state.tx, cid, &identify, true);
                }
            }
            (OPC_KEEP_ALIVE, _) => {
                let state = self.conns.get_mut(&conn).unwrap();
                push_capsule_resp(cid, 0, 0, ok, &mut state.tx);
            }
            (OPC_READ, _) => {
                let nlb = u16_at(sqe, 48) as usize + 1;
                let len = nlb * LBA_SIZE;
                assert!(len <= self.read_payload.len(), "read length {len} exceeds payload buffer");
                let payload = &self.read_payload[..len];
                let state = self.conns.get_mut(&conn).unwrap();
                Self::push_c2h(&mut state.tx, cid, payload, true);
            }
            (OPC_WRITE, _) => {
                let nlb = u16_at(sqe, 48) as usize + 1;
                let len = nlb * LBA_SIZE;
                let state = self.conns.get_mut(&conn).unwrap();
                if sqe[39] == 0x01 {
                    assert_eq!(data.len(), len, "in-capsule write length");
                    push_capsule_resp(cid, 0, 0, ok, &mut state.tx);
                } else {
                    assert!(data.is_empty(), "R2T write carries no in-capsule data");
                    state.r2t_writes.insert(cid, (len, 0));
                    pdu_header(PDU_R2T, 0, 24, 0, 24, &mut state.tx);
                    state.tx.extend_from_slice(&cid.to_le_bytes());
                    state.tx.extend_from_slice(&cid.wrapping_add(100).to_le_bytes());
                    state.tx.extend_from_slice(&0u32.to_le_bytes());
                    state.tx.extend_from_slice(&(len as u32).to_le_bytes());
                    state.tx.extend_from_slice(&[0; 4]);
                }
            }
            other => panic!("unexpected NVMe/TCP command {other:?}"),
        }
    }
}

fn drain_server_reads<P: PacketIo>(
    net: &mut Network<P>,
    conn: ConnId,
    read_buf: &mut [u8],
    target: &mut FakeNvmeTarget,
) {
    loop {
        match net.try_read(conn, read_buf) {
            Ok(ReadOutcome::Data { len, .. }) => {
                target.receive(conn, &read_buf[..len]);
            }
            Ok(ReadOutcome::Eof) => {
                let _ = net.close(conn);
                break;
            }
            Ok(ReadOutcome::EmptyBuffer)
            | Err(ntcp_native::Error::WouldBlock(_))
            | Err(ntcp_native::Error::ConnectionClosed(_)) => break,
            Err(e) => panic!("server try_read failed: {e}"),
        }
    }
}

fn flush_server_writes<P: PacketIo>(net: &mut Network<P>, target: &mut FakeNvmeTarget) {
    let conns: Vec<ConnId> = target.conns.keys().copied().collect();
    for conn in conns {
        let Some(state) = target.conns.get_mut(&conn) else {
            continue;
        };
        while !state.pending_tx().is_empty() {
            match net.try_write(conn, state.pending_tx(), WriteOptions::default()) {
                Ok(receipt) if receipt.accepted > 0 => {
                    state.advance_tx(receipt.accepted);
                    let _ = net.push(conn, time_ns());
                }
                Ok(_)
                | Err(ntcp_native::Error::WouldBlock(_))
                | Err(ntcp_native::Error::ConnectionClosed(_)) => break,
                Err(e) => panic!("server try_write failed: {e}"),
            }
        }
    }
}

#[test]
fn nvme_tcp_memif_server_helper() {
    if env::var(HELPER_ENV).ok().as_deref() != Some("server") {
        return;
    }

    let sock = env::var(SOCKET_ENV).expect("socket env");
    let prefix = env::var(PREFIX_ENV).expect("prefix env");
    let tcp_port: u16 = env::var(PORT_ENV).expect("port env").parse().expect("port");
    let cpu: usize = env::var(CPU_ENV).expect("cpu env").parse().expect("cpu");

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

    let mut port_config = PortConfig::new(device);
    port_config.queues = 1;
    port_config.mtu = 1500;
    port_config.isolate = false;
    let port = Port::start(eal, port_config).expect("server Port::start");
    let mac = port.mac_address().expect("server mac_address");

    let queue = port.take_queue(0).expect("server take_queue").attach().expect("server attach");

    let mut config = NetworkConfig::new(IpAddr::V4(SERVER_IP), 1500);
    config.tcp.max_connections = 16;
    config.tcp.connection.timebase.units_per_second = UNITS_PER_SECOND;
    let (low, pressure, max) = NetworkConfig::memory_cap_for(512 << 20);
    config.tcp.buffer_low_bytes = Some(low);
    config.tcp.buffer_pressure_bytes = Some(pressure);
    config.tcp.max_buffer_bytes = max;
    config.link = Link::Ethernet {
        local_mac: mac,
        next_hop: EthernetNextHop::Arp { prefix_len: 24, gateway: None },
    };

    let secret = [0x5Au8; 32];
    let mut net = Network::new(config, queue, secret, time_ns()).expect("server Network::new");
    let listener = net.listen(tcp_port, 16).expect("server listen");

    println!("SERVER_READY");
    let _ = std::io::stdout().flush();

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut events = vec![Event::NetworkStopped; 64];
    let mut read_buf = vec![0u8; 65536];
    let mut target = FakeNvmeTarget::new();
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
                        target.add_conn(conn);
                    }
                }
                Event::Readable(conn) => {
                    drain_server_reads(&mut net, conn, &mut read_buf, &mut target);
                }
                Event::Closed { connection, .. } => {
                    target.remove_conn(connection);
                    if accepted_any && target.is_empty() {
                        closed_all = true;
                    }
                }
                _ => {}
            }
        }

        flush_server_writes(&mut net, &mut target);

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

    assert!(accepted_any, "server never accepted an NVMe/TCP client connection");
    assert!(closed_all, "server connections did not cleanly close before deadline");
    drop(net);
    drop(port);
}

fn run_nvme_memif_case(
    name: &str,
    connections_per_thread: usize,
    max_pending_per_connection: usize,
    block_size: u32,
    read_percent: u8,
) {
    if env::var(HELPER_ENV).is_ok() {
        return;
    }

    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let seq = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();

    let cpus = allowed_cpus();
    let server_cpu = cpus[0];
    let client_cpu = *cpus.get(1).unwrap_or(&cpus[0]);

    let tmp_dir = short_tempdir();
    let sock_path = tmp_dir.path().join("m.sock");
    let srv_prefix = format!("xnm-s-{pid}-{seq}");
    let cli_prefix = format!("xnm-c-{pid}-{seq}");
    let exe = env::current_exe().expect("current_exe");

    let mut server = ChildGuard::new(
        Command::new(&exe)
            .args(["--exact", "nvme_tcp_memif_server_helper", "--nocapture"])
            .env(HELPER_ENV, "server")
            .env(SOCKET_ENV, &sock_path)
            .env(PREFIX_ENV, &srv_prefix)
            .env(PORT_ENV, SERVER_PORT.to_string())
            .env(CPU_ENV, server_cpu.to_string())
            .env("XDG_RUNTIME_DIR", tmp_dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn NVMe/TCP memif server helper"),
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
    assert!(ready, "{name}: server failed before printing SERVER_READY");

    let results_path = tmp_dir.path().join("results.json");
    let profile_path = tmp_dir.path().join("profile.toml");
    let profile_toml = format!(
        r#"[experiment]
name = "{name}"
duration = "400ms"

[ntcp]
backend = "dpdk"
mtu = 1500

[ntcp.dpdk]
device = "net_memif,role=client,socket={sock}"
eal_args = ["--no-huge", "-m", "512", "--no-pci", "--lcores=0@{client_cpu}", "--file-prefix", "{cli_prefix}"]
local_ips = ["{client_ip}"]
prefix_len = 24

[[traffic_groups]]
name = "nvme"
protocol = "nvme-tcp"
transport = "ntcp"
target = "{server_ip}:{server_port}"
threads = [{client_cpu}]
connections_per_thread = {connections_per_thread}
max_pending_per_connection = {max_pending_per_connection}
traffic_policy = {{ type = "closed-loop" }}
protocol_config = {{ subnqn = "{subnqn}", block_size = {block_size}, read_percent = {read_percent} }}

[output]
format = "json"
file = "{results}"
"#,
        sock = sock_path.display(),
        client_ip = CLIENT_IP,
        server_ip = SERVER_IP,
        server_port = SERVER_PORT,
        subnqn = SUBNQN,
        results = results_path.display(),
    );
    std::fs::write(&profile_path, &profile_toml).expect("write profile.toml");

    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile_path)
        .env("XDG_RUNTIME_DIR", tmp_dir.path())
        .output()
        .expect("run xylem binary");

    let server_status = server.wait().expect("wait server helper");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{name}: xylem exited with {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    assert!(
        server_status.success(),
        "{name}: server helper exited with {server_status}\nxylem stderr:\n{stderr}"
    );

    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&results_path).expect("read results.json"))
            .expect("parse results.json");

    assert_eq!(json["verdict"], "pass", "{name}: unexpected verdict in {json}");
    let requests = &json["global"]["requests"];
    let completed = requests["completed"].as_u64().expect("completed count");
    let errors = requests["errors"].as_u64().expect("errors count");
    let failed = requests["failed"].as_u64().expect("failed count");
    let timed_out = requests["timed_out"].as_u64().expect("timed_out count");
    assert!(completed > 0, "{name}: expected completed > 0, got {requests}");
    assert_eq!(errors, 0, "{name}: expected errors == 0, got {requests}");
    assert_eq!(failed, 0, "{name}: expected failed == 0, got {requests}");
    assert_eq!(timed_out, 0, "{name}: expected timed_out == 0, got {requests}");
    eprintln!(
        "{name}: verdict=pass completed={completed} errors={errors} (conns={connections_per_thread}, in_flight={max_pending_per_connection}, block={block_size}, read={read_percent}%)"
    );
}

#[test]
fn memif_nvme_tcp_1conn_8inflight_4k_read() {
    run_nvme_memif_case("nvme-1c-8q-4k-r100", 1, 8, 4096, 100);
}

#[test]
fn memif_nvme_tcp_1conn_8inflight_128k_read() {
    run_nvme_memif_case("nvme-1c-8q-128k-r100", 1, 8, 131072, 100);
}

#[test]
fn memif_nvme_tcp_2conn_16inflight_4k_read() {
    run_nvme_memif_case("nvme-2c-16q-4k-r100", 2, 16, 4096, 100);
}

#[test]
fn memif_nvme_tcp_2conn_16inflight_128k_read() {
    run_nvme_memif_case("nvme-2c-16q-128k-r100", 2, 16, 131072, 100);
}

#[test]
fn memif_nvme_tcp_2conn_16inflight_4k_rw50() {
    run_nvme_memif_case("nvme-2c-16q-4k-r50", 2, 16, 4096, 50);
}

#[test]
fn memif_nvme_tcp_1conn_8inflight_128k_rw50() {
    run_nvme_memif_case("nvme-1c-8q-128k-r50", 1, 8, 131072, 50);
}
