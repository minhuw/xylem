//! NVMe/TCP against a scripted fake target that the tests drive the way the
//! worker drives a protocol

use super::*;
use pdu::{u16_at, u32_at, u64_at};
use std::collections::HashMap;

const CNTLID: u16 = 0x21;
const SUBNQN: &str = "nqn.2016-06.io.spdk:fake";

/// A one-namespace NVMe/TCP target that answers whole PDUs at once
struct FakeTarget {
    lba_size: usize,
    nsze: u64,
    /// Written LBAs
    storage: HashMap<u64, Vec<u8>>,
    in_capsule: u32,
    maxh2cdata: u32,
    mqes: u16,
    /// Digests the ICResp claims
    dgst: u8,
    /// CSTS reads that still report not ready
    not_ready_polls: u32,
    /// Most data bytes per C2HData PDU
    c2h_split: usize,
    /// Complete reads with C2HData's SUCCESS flag instead of a CapsuleResp
    success_flag: bool,
    /// Flip one byte of every read
    corrupt: bool,
    /// Fail every read with this status (SCT, SC)
    read_status: Option<(u8, u8)>,
    enabled: bool,
    /// What the host did, in order
    log: Vec<String>,
    /// Writes waiting for their H2CData: (slba, len, data so far)
    r2t_writes: HashMap<(usize, u16), (u64, usize, Vec<u8>)>,
    /// Unparsed host bytes and target bytes not yet delivered, per connection
    rx: HashMap<usize, Vec<u8>>,
    tx: HashMap<usize, Vec<u8>>,
}

impl Default for FakeTarget {
    fn default() -> Self {
        Self {
            lba_size: 512,
            nsze: 1024,
            storage: HashMap::new(),
            in_capsule: 4096,
            maxh2cdata: 16384,
            mqes: 127,
            dgst: 0,
            not_ready_polls: 1,
            c2h_split: usize::MAX,
            success_flag: true,
            corrupt: false,
            read_status: None,
            enabled: false,
            log: Vec::new(),
            r2t_writes: HashMap::new(),
            rx: HashMap::new(),
            tx: HashMap::new(),
        }
    }
}

fn header(pdu_type: u8, flags: u8, hlen: u8, pdo: u8, plen: usize) -> Vec<u8> {
    let mut out = vec![pdu_type, flags, hlen, pdo];
    out.extend_from_slice(&(plen as u32).to_le_bytes());
    out
}

fn capsule_resp(cid: u16, dw0: u32, dw1: u32, status: (u8, u8)) -> Vec<u8> {
    let mut out = header(pdu::PDU_CAPSULE_RESP, 0, 24, 0, 24);
    out.extend_from_slice(&dw0.to_le_bytes());
    out.extend_from_slice(&dw1.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&cid.to_le_bytes());
    let status = (u16::from(status.0) << 8 | u16::from(status.1)) << 1 | 1;
    out.extend_from_slice(&status.to_le_bytes());
    out
}

impl FakeTarget {
    /// Take the host's bytes on `conn`, answering every whole PDU
    fn receive(&mut self, conn: usize, data: &[u8]) {
        let mut buffer = self.rx.remove(&conn).unwrap_or_default();
        buffer.extend_from_slice(data);
        while buffer.len() >= 8 {
            let plen = u32_at(&buffer, 4) as usize;
            if buffer.len() < plen {
                break;
            }
            let pdu: Vec<u8> = buffer.drain(..plen).collect();
            self.handle(conn, &pdu);
        }
        self.rx.insert(conn, buffer);
    }

    fn send(&mut self, conn: usize, data: &[u8]) {
        self.tx.entry(conn).or_default().extend_from_slice(data);
    }

    fn handle(&mut self, conn: usize, pdu: &[u8]) {
        match pdu[0] {
            pdu::PDU_IC_REQ => {
                assert_eq!(pdu.len(), 128);
                assert_eq!(pdu[11], 0, "the host asks for no digests");
                self.log.push(format!("icreq {conn}"));
                let mut resp = header(pdu::PDU_IC_RESP, 0, 128, 0, 128);
                resp.resize(128, 0);
                resp[11] = self.dgst;
                resp[12..16].copy_from_slice(&self.maxh2cdata.to_le_bytes());
                self.send(conn, &resp);
            }
            pdu::PDU_CAPSULE_CMD => {
                let pdo = pdu[3] as usize;
                let data = if pdo == 0 { &[][..] } else { &pdu[pdo..] };
                self.command(conn, &pdu[8..72], data);
            }
            pdu::PDU_H2C_DATA => {
                let (cccid, ttag) = (u16_at(pdu, 8), u16_at(pdu, 10));
                let (datao, datal) = (u32_at(pdu, 12) as usize, u32_at(pdu, 16) as usize);
                assert_eq!(ttag, cccid + 100);
                assert!(datal <= self.maxh2cdata as usize, "H2CData within MAXH2CDATA");
                let last = pdu[1] & pdu::FLAG_LAST_PDU != 0;
                let (_, len, data) = self.r2t_writes.get_mut(&(conn, cccid)).unwrap();
                assert_eq!(datao, data.len(), "H2CData in order");
                data.extend_from_slice(&pdu[24..24 + datal]);
                assert_eq!(last, data.len() == *len, "LAST_PDU on the last H2CData");
                self.log.push(format!("h2c {conn} {cccid} {datao}+{datal}"));
                if last {
                    let (slba, _, data) = self.r2t_writes.remove(&(conn, cccid)).unwrap();
                    self.store(slba, &data);
                    self.send(conn, &capsule_resp(cccid, 0, 0, (0, 0)));
                }
            }
            other => panic!("unexpected host PDU type {other:#x}"),
        }
    }

    fn store(&mut self, slba: u64, data: &[u8]) {
        for (index, block) in data.chunks(self.lba_size).enumerate() {
            self.storage.insert(slba + index as u64, block.to_vec());
        }
    }

    /// Send `data` for command `cid` in C2HData PDUs, then complete it
    fn c2h(&mut self, conn: usize, cid: u16, data: &[u8], success: bool) {
        let chunks: Vec<&[u8]> = data.chunks(self.c2h_split).collect();
        let mut offset = 0;
        for (index, chunk) in chunks.iter().enumerate() {
            let last = index + 1 == chunks.len();
            let mut flags = if last { pdu::FLAG_LAST_PDU } else { 0 };
            if last && success {
                flags |= pdu::FLAG_SUCCESS;
            }
            let mut out = header(pdu::PDU_C2H_DATA, flags, 24, 24, 24 + chunk.len());
            out.extend_from_slice(&cid.to_le_bytes());
            out.extend_from_slice(&[0; 2]);
            out.extend_from_slice(&(offset as u32).to_le_bytes());
            out.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
            out.extend_from_slice(&[0; 4]);
            out.extend_from_slice(chunk);
            self.send(conn, &out);
            offset += chunk.len();
        }
        if !success {
            self.send(conn, &capsule_resp(cid, 0, 0, (0, 0)));
        }
    }

    fn command(&mut self, conn: usize, sqe: &[u8], data: &[u8]) {
        let (opc, cid) = (sqe[0], u16_at(sqe, 2));
        let ok = (0, 0);
        match (opc, sqe[4]) {
            (pdu::OPC_FABRICS, pdu::FCTYPE_CONNECT) => {
                let (qid, sqsize) = (u16_at(sqe, 42), u16_at(sqe, 44));
                assert_eq!(u32_at(sqe, 48), 0, "no keep alive timeout");
                let cntlid = u16_at(data, 16);
                assert_eq!(&data[256..256 + SUBNQN.len()], SUBNQN.as_bytes());
                self.log.push(format!("connect {conn} qid={qid} sqsize={sqsize}"));
                if qid == 0 {
                    assert_eq!(cntlid, 0xffff, "dynamic controller");
                    self.send(conn, &capsule_resp(cid, u32::from(CNTLID), 0, ok));
                } else if cntlid == CNTLID && self.enabled {
                    self.send(conn, &capsule_resp(cid, 0, 0, ok));
                } else {
                    self.send(conn, &capsule_resp(cid, 0, 0, (1, 0x82)));
                }
            }
            (pdu::OPC_FABRICS, pdu::FCTYPE_PROPERTY_GET) => {
                let offset = u32_at(sqe, 44);
                let value = match offset {
                    // MQES, CSS NVM (bit 37), MPSMIN 0
                    pdu::PROP_CAP => u64::from(self.mqes) | 1 << 37,
                    pdu::PROP_CSTS if self.not_ready_polls > 0 => {
                        self.not_ready_polls -= 1;
                        0
                    }
                    pdu::PROP_CSTS => u64::from(self.enabled),
                    other => panic!("property {other:#x}"),
                };
                self.log.push(format!("get {offset:#x}"));
                self.send(conn, &capsule_resp(cid, value as u32, (value >> 32) as u32, ok));
            }
            (pdu::OPC_FABRICS, pdu::FCTYPE_PROPERTY_SET) => {
                let value = u32_at(sqe, 48);
                assert_eq!(u32_at(sqe, 44), pdu::PROP_CC);
                assert_eq!((value >> 16 & 0xf, value >> 20 & 0xf), (6, 4), "IOSQES, IOCQES");
                self.enabled = value & 1 == 1;
                self.log.push(format!("set cc {value:#x}"));
                self.send(conn, &capsule_resp(cid, 0, 0, ok));
            }
            (pdu::OPC_IDENTIFY, _) => {
                let mut identify = vec![0u8; 4096];
                if sqe[40] == 1 {
                    identify[77] = 5;
                    let ioccsz = (self.in_capsule + 64) / 16;
                    identify[1792..1796].copy_from_slice(&ioccsz.to_le_bytes());
                    self.log.push("identify controller".into());
                    self.c2h(conn, cid, &identify, false);
                } else {
                    assert_eq!(u32_at(sqe, 4), 1, "namespace 1");
                    identify[..8].copy_from_slice(&self.nsze.to_le_bytes());
                    identify[128 + 2] = self.lba_size.trailing_zeros() as u8;
                    self.log.push("identify namespace".into());
                    self.c2h(conn, cid, &identify, true);
                }
            }
            (pdu::OPC_READ, _) => {
                let (slba, nlb) = (u64_at(sqe, 40), u16_at(sqe, 48) as u64 + 1);
                assert_eq!(sqe[39], 0x5a);
                if let Some(status) = self.read_status {
                    self.send(conn, &capsule_resp(cid, 0, 0, status));
                    return;
                }
                let mut data = Vec::new();
                for lba in slba..slba + nlb {
                    let zero = vec![0; self.lba_size];
                    data.extend_from_slice(self.storage.get(&lba).unwrap_or(&zero));
                }
                if self.corrupt {
                    data[100] ^= 0xff;
                }
                self.log.push(format!("read {conn} {slba}"));
                self.c2h(conn, cid, &data, self.success_flag);
            }
            (pdu::OPC_WRITE, _) => {
                let (slba, nlb) = (u64_at(sqe, 40), u16_at(sqe, 48) as usize + 1);
                let len = nlb * self.lba_size;
                assert_eq!(u32_at(sqe, 32) as usize, len);
                if sqe[39] == 0x01 {
                    assert_eq!(data.len(), len, "in-capsule data");
                    self.log.push(format!("write {conn} {slba} in-capsule"));
                    self.store(slba, data);
                    self.send(conn, &capsule_resp(cid, 0, 0, ok));
                } else {
                    assert!(data.is_empty());
                    self.log.push(format!("write {conn} {slba} r2t"));
                    self.r2t_writes.insert((conn, cid), (slba, len, Vec::new()));
                    let mut r2t = header(pdu::PDU_R2T, 0, 24, 0, 24);
                    r2t.extend_from_slice(&cid.to_le_bytes());
                    r2t.extend_from_slice(&(cid + 100).to_le_bytes());
                    r2t.extend_from_slice(&0u32.to_le_bytes());
                    r2t.extend_from_slice(&(len as u32).to_le_bytes());
                    r2t.extend_from_slice(&[0; 4]);
                    self.send(conn, &r2t);
                }
            }
            other => panic!("unexpected command {other:?}"),
        }
    }
}

/// A completed request: its ID and error kind
type Completed = (NvmeRequestId, Option<String>);

/// Drives a protocol against the fake target like the worker: requests in
/// full, responses in `chunk`-byte reads, the outbox after every read
struct Harness {
    protocol: NvmeTcpProtocol,
    target: FakeTarget,
    /// Received bytes not yet consumed, per connection
    inbox: HashMap<usize, Vec<u8>>,
    chunk: usize,
    woken: Vec<usize>,
    /// Requests in flight per connection, as the worker counts them
    in_flight: HashMap<usize, usize>,
    measured_sent: usize,
}

impl Harness {
    fn new(config: NvmeTcpConfig, queue_depth: usize, target: FakeTarget, load: usize) -> Self {
        let mut protocol = NvmeTcpProtocol::from_config(&config, queue_depth, Some(7)).unwrap();
        let roles = protocol.connection_roles(load);
        let ids: Vec<usize> = (10..10 + roles.len()).collect();
        protocol.bind_connections(&ids);
        Self {
            protocol,
            target,
            inbox: HashMap::new(),
            chunk: 7,
            woken: Vec::new(),
            in_flight: HashMap::new(),
            measured_sent: 0,
        }
    }

    fn send(&mut self, conn: usize) -> Request<NvmeRequestId> {
        assert!(self.protocol.can_send(conn), "connection {conn} may send");
        let request = self.protocol.next_request(conn);
        assert_eq!(request.request_id.0, conn);
        if !request.metadata.is_warmup {
            self.measured_sent += 1;
        }
        *self.in_flight.entry(conn).or_default() += 1;
        self.target.receive(conn, &request.data);
        request
    }

    /// Deliver everything the target sent on `conn`; returns the completions
    fn deliver(&mut self, conn: usize) -> Result<Vec<Completed>> {
        let mut completed = Vec::new();
        while let Some(bytes) = self.target.tx.remove(&conn) {
            for piece in bytes.chunks(self.chunk) {
                self.inbox.entry(conn).or_default().extend_from_slice(piece);
                self.parse_inbox(conn, &mut completed)?;
                let mut outbox = Outbox::default();
                self.protocol.drain_outbox(conn, &mut outbox);
                self.woken.append(&mut outbox.wake);
                if !outbox.data.is_empty() {
                    self.target.receive(conn, &outbox.data);
                }
            }
        }
        Ok(completed)
    }

    /// Parse the received bytes of `conn` like the pool: until the parser
    /// consumes nothing
    fn parse_inbox(&mut self, conn: usize, completed: &mut Vec<Completed>) -> Result<()> {
        let inbox = self.inbox.get_mut(&conn).unwrap();
        while !inbox.is_empty() {
            let parsed = self.protocol.parse_response(conn, inbox)?;
            let consumed = parsed.consumed();
            if let Parsed::Complete { id, error, .. } = parsed {
                completed.push((id, error.map(|error| error.kind.to_string())));
                *self.in_flight.get_mut(&conn).unwrap() -= 1;
            }
            if consumed == 0 {
                break;
            }
            inbox.drain(..consumed);
        }
        Ok(())
    }

    /// Run connection setup until no connection has a setup step it can take
    fn set_up(&mut self) -> Result<()> {
        let conns: Vec<usize> = {
            let mut ids: Vec<usize> = self.protocol.queues.keys().copied().collect();
            ids.sort_unstable();
            ids
        };
        loop {
            let mut progressed = false;
            for &conn in &conns {
                if self.protocol.setup_pending(conn) && self.protocol.can_send(conn) {
                    let request = self.send(conn);
                    assert!(request.metadata.is_setup);
                    self.deliver(conn)?;
                    progressed = true;
                }
            }
            if !progressed {
                return Ok(());
            }
        }
    }

    /// Send up to `depth` commands on every I/O queue, then deliver them all
    fn round(&mut self, depth: usize) -> Result<Vec<Completed>> {
        let io: Vec<usize> = self.protocol.controllers.iter().flat_map(|c| c.io.clone()).collect();
        for &conn in &io {
            while self.in_flight.get(&conn).copied().unwrap_or(0) < depth
                && self.protocol.can_send(conn)
            {
                self.send(conn);
            }
        }
        let mut completed = Vec::new();
        for &conn in &io {
            completed.extend(self.deliver(conn)?);
        }
        Ok(completed)
    }
}

fn config(extra: serde_json::Value) -> NvmeTcpConfig {
    let mut value = serde_json::json!({ "subnqn": SUBNQN });
    for (key, field) in extra.as_object().unwrap() {
        value[key] = field.clone();
    }
    serde_json::from_value(value).unwrap()
}

#[test]
fn connection_roles_put_an_admin_queue_before_each_controllers_io_queues() {
    use ConnectionRole::{Control as C, Load as L};
    let protocol = NvmeTcpProtocol::from_config(&config(serde_json::json!({})), 4, None).unwrap();
    assert_eq!(protocol.connection_roles(3), [C, L, L, L]);
    let per_two = config(serde_json::json!({ "io_queues_per_controller": 2 }));
    let protocol = NvmeTcpProtocol::from_config(&per_two, 4, None).unwrap();
    assert_eq!(protocol.connection_roles(5), [C, L, L, C, L, L, C, L]);
    assert!(protocol.connection_roles(0).is_empty());
}

#[test]
fn io_queues_connect_only_after_the_admin_queue_set_up_the_controller() {
    let mut harness = Harness::new(config(serde_json::json!({})), 8, FakeTarget::default(), 2);
    let (admin, io) = (10, [11, 12]);
    let protocol = &harness.protocol;
    assert!(io.iter().chain([&admin]).all(|&conn| protocol.setup_pending(conn)));

    // An I/O queue exchanges ICReq and ICResp at once, then waits
    let request = harness.send(io[0]);
    assert_eq!(request.request_id, (io[0], IC_REQ_TAG));
    assert!(request.metadata.is_setup);
    harness.deliver(io[0]).unwrap();
    assert!(!harness.protocol.can_send(io[0]), "no Connect before the controller is ready");
    assert!(harness.protocol.setup_pending(io[0]));

    harness.set_up().unwrap();
    let log = &harness.target.log;
    let position = |entry: &str| log.iter().position(|logged| logged == entry).unwrap();
    assert_eq!(
        log[..9],
        [
            "icreq 11",
            "icreq 10",
            "icreq 12",
            "connect 10 qid=0 sqsize=31",
            "get 0x0",
            "set cc 0x460001",
            "get 0x1c",
            "get 0x1c",
            "identify controller",
        ]
    );
    // CSTS.RDY was polled until set; both I/O queues connect after the
    // namespace is known, with the controller ID and the queue depth
    assert!(position("identify namespace") < position("connect 11 qid=1 sqsize=8"));
    assert!(position("identify namespace") < position("connect 12 qid=2 sqsize=8"));
    // Identify Namespace completed on the admin queue and woke both
    assert_eq!(harness.woken, io);
    let protocol = &harness.protocol;
    assert!(io.iter().all(|&conn| protocol.can_send(conn) && !protocol.setup_pending(conn)));
    assert!(!protocol.can_send(admin) && !protocol.setup_pending(admin));
    assert_eq!(protocol.controllers[0].in_capsule, 4096);
    assert_eq!(harness.measured_sent, 0, "setup is not measured");
}

#[test]
fn verify_mode_prefills_then_reads_back_the_pattern_at_queue_depth() {
    let config = config(serde_json::json!({
        "verify": true, "read_percent": 50, "lba_count": 64, "access": "random"
    }));
    let target = FakeTarget {
        c2h_split: 1000,
        success_flag: false,
        ..Default::default()
    };
    let mut harness = Harness::new(config, 4, target, 2);
    // The I/O queues preload from the start, before the namespace is known
    let preloading = |protocol: &NvmeTcpProtocol| {
        let mut queues: Vec<_> = protocol.queues.iter().collect();
        queues.sort_by_key(|(&conn, _)| conn);
        queues
            .iter()
            .map(|(&conn, _)| protocol.preload_pending(conn))
            .collect::<Vec<_>>()
    };
    assert_eq!(preloading(&harness.protocol), [false, true, true], "the admin queue does not");
    harness.set_up().unwrap();
    assert_eq!(preloading(&harness.protocol), [false, true, true]);

    // 64 LBAs of 512 bytes are 8 blocks of 4096: 8 prefill writes, in
    // capsule and unmeasured, then nothing until the last one completes
    let completed = harness.round(4).unwrap();
    assert_eq!(preloading(&harness.protocol), [false, false, false]);
    assert_eq!(completed.len(), 8);
    assert_eq!(harness.measured_sent, 0);
    assert_eq!(harness.target.storage.len(), 64);
    assert!(harness.target.log.iter().filter(|e| e.contains("in-capsule")).count() == 8);
    assert_eq!(harness.woken[2..], [11, 12], "the last prefill write wakes the I/O queues");

    // Measured reads and writes of the range, all verified, in 7-byte reads
    // of C2HData split into 1000-byte PDUs
    for _ in 0..10 {
        let completed = harness.round(4).unwrap();
        assert!(completed.iter().all(|(_, error)| error.is_none()));
    }
    assert_eq!(harness.measured_sent, 80);
    let reads = harness.target.log.iter().filter(|entry| entry.starts_with("read")).count();
    assert!(reads > 20 && reads < 60, "{reads} of 80 are reads");
    assert!(harness.protocol.queues.values().all(|queue| queue.commands.is_empty()));
}

#[test]
fn corrupted_readback_fails_with_the_lba() {
    let config = config(serde_json::json!({ "verify": true, "lba_count": 8 }));
    let mut harness = Harness::new(config, 1, FakeTarget::default(), 1);
    harness.set_up().unwrap();
    harness.round(1).unwrap();
    harness.target.corrupt = true;
    let error = harness.round(1).unwrap_err().to_string();
    assert!(error.contains("verify: LBA") && error.contains("byte 100"), "{error}");
}

#[test]
fn large_writes_answer_r2ts_with_h2c_data_within_maxh2cdata() {
    let config = config(serde_json::json!({ "block_size": 65536, "read_percent": 0 }));
    let mut harness = Harness::new(config, 2, FakeTarget::default(), 1);
    harness.set_up().unwrap();
    let request = harness.send(11);
    assert_eq!(request.data.len(), 72, "no data in the capsule");
    let completed = harness.deliver(11).unwrap();
    assert_eq!(completed, [(request.request_id, None)]);
    let cid = request.request_id.1;
    let h2c: Vec<_> = harness.target.log.iter().filter(|e| e.starts_with("h2c")).collect();
    assert_eq!(
        h2c,
        [0, 16384, 32768, 49152]
            .map(|offset| format!("h2c 11 {cid} {offset}+16384"))
            .iter()
            .collect::<Vec<_>>()
    );
    // The H2CData carried the write's data
    let slba = harness.target.log.iter().find_map(|e| e.strip_prefix("write 11 ")).unwrap();
    let slba: u64 = slba.trim_end_matches(" r2t").parse().unwrap();
    assert_eq!(harness.target.storage[&slba], harness.protocol.write_data[..512]);
}

#[test]
fn sequential_access_walks_the_range_and_wraps() {
    let config = config(serde_json::json!({
        "access": "sequential", "lba_start": 16, "lba_count": 24
    }));
    let mut harness = Harness::new(config, 1, FakeTarget::default(), 1);
    harness.set_up().unwrap();
    for _ in 0..4 {
        harness.round(1).unwrap();
    }
    let reads: Vec<_> = harness.target.log.iter().filter(|e| e.starts_with("read")).collect();
    assert_eq!(reads, ["read 11 16", "read 11 24", "read 11 32", "read 11 16"]);
}

#[test]
fn a_failed_command_is_an_error_reply_named_after_its_status() {
    let target = FakeTarget {
        read_status: Some((0, 0x80)),
        ..Default::default()
    };
    let mut harness = Harness::new(config(serde_json::json!({})), 1, target, 1);
    harness.set_up().unwrap();
    let completed = harness.round(1).unwrap();
    assert_eq!(completed[0].1.as_deref(), Some("LBA_OUT_OF_RANGE"));
}

#[test]
fn a_target_with_digests_fails_setup_clearly() {
    let target = FakeTarget { dgst: 2, ..Default::default() };
    let mut harness = Harness::new(config(serde_json::json!({})), 1, target, 1);
    let error = harness.set_up().unwrap_err().to_string();
    assert!(error.contains("enabled data digests"), "{error}");
}

#[test]
fn a_queue_depth_beyond_mqes_fails_setup() {
    let target = FakeTarget { mqes: 3, ..Default::default() };
    let mut harness = Harness::new(config(serde_json::json!({})), 4, target, 1);
    let error = harness.set_up().unwrap_err().to_string();
    assert!(error.contains("max_pending_per_connection 4 exceeds the 3"), "{error}");
}

#[test]
fn block_size_must_fit_the_namespace() {
    let config = config(serde_json::json!({ "block_size": 1000 }));
    let mut harness = Harness::new(config, 1, FakeTarget::default(), 1);
    let error = harness.set_up().unwrap_err().to_string();
    assert!(error.contains("block_size 1000"), "{error}");
}

#[test]
fn several_controllers_each_connect_their_own_io_queues() {
    let config = config(serde_json::json!({ "io_queues_per_controller": 1 }));
    // Ready at once, so both admin queues set up in step
    let target = FakeTarget { not_ready_polls: 0, ..Default::default() };
    let mut harness = Harness::new(config, 2, target, 2);
    harness.set_up().unwrap();
    let connects: Vec<_> =
        harness.target.log.iter().filter(|entry| entry.starts_with("connect")).collect();
    assert_eq!(
        connects,
        [
            "connect 10 qid=0 sqsize=31",
            "connect 12 qid=0 sqsize=31",
            "connect 11 qid=1 sqsize=2",
            "connect 13 qid=1 sqsize=2",
        ]
    );
    assert_eq!(harness.round(2).unwrap().len(), 4);
}

#[test]
fn a_command_that_never_completed_is_tracked_until_its_response() {
    let mut harness = Harness::new(config(serde_json::json!({})), 2, FakeTarget::default(), 1);
    harness.set_up().unwrap();
    harness.send(11);
    assert_eq!(harness.protocol.in_flight(11), 1);
    harness.deliver(11).unwrap();
    assert_eq!(harness.protocol.in_flight(11), 0);
}

#[test]
fn a_prefill_write_asked_for_after_the_last_block_rewrites_one() {
    let config = config(serde_json::json!({ "verify": true, "lba_count": 16 }));
    let mut harness = Harness::new(config, 4, FakeTarget::default(), 2);
    harness.set_up().unwrap();
    // Both queues may send; queue 11 takes both blocks, then the worker
    // asks queue 12, which it scheduled before, for its next request
    assert!(harness.protocol.can_send(12));
    harness.send(11);
    harness.send(11);
    assert!(!harness.protocol.can_send(12));
    let request = harness.protocol.next_request(12);
    assert!(request.metadata.is_warmup, "a prefill write");
    *harness.in_flight.entry(12).or_default() += 1;
    harness.target.receive(12, &request.data);
    harness.deliver(11).unwrap();
    assert!(harness.protocol.prefill.is_some(), "the rewrite is still in flight");
    harness.deliver(12).unwrap();
    assert!(harness.protocol.prefill.is_none());
    let writes: Vec<_> = harness.target.log.iter().filter(|e| e.starts_with("write")).collect();
    assert_eq!(
        writes,
        ["write 11 0 in-capsule", "write 11 8 in-capsule", "write 12 0 in-capsule"]
    );
    // Reads of the range verify
    for _ in 0..4 {
        assert!(harness.round(4).unwrap().iter().all(|(_, error)| error.is_none()));
    }
}

#[test]
fn the_pattern_is_the_same_however_a_transfer_is_cut() {
    let mut whole = vec![0; 4096];
    fill_pattern(512, 40, 0, &mut whole);
    let mut pieces = vec![0; 4096];
    for (start, end) in [(0, 3), (3, 517), (517, 1000), (1000, 4096)] {
        fill_pattern(512, 40, start as u32, &mut pieces[start..end]);
    }
    assert_eq!(whole, pieces);
    // The second LBA of a transfer from 40 is LBA 41 of one from 41
    let mut next = vec![0; 512];
    fill_pattern(512, 41, 0, &mut next);
    assert_eq!(whole[512..1024], next[..]);
    assert_ne!(whole[..512], next[..]);
}
