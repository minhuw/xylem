//! NVMe/TCP: NVM reads and writes on the I/O queues of NVMe over Fabrics
//! controllers reached over TCP
//!
//! # Sessions
//!
//! A controller spans connections ([`Protocol::connection_roles`]): an
//! admin queue (a control connection), then its I/O queues (load
//! connections). Each connection first sends an ICReq. The admin queue then
//! connects (Fabrics Connect, QID 0) and gets the controller ID, reads CAP,
//! enables the controller (CC.EN with 64-byte SQEs and 16-byte CQEs), waits
//! for CSTS.RDY, and identifies the controller (in-capsule data size, MDTS)
//! and the namespace (LBA size and count). Only then may the controller's
//! I/O queues connect with its controller ID, and the protocol wakes them
//! ([`Outbox::wake`]). Every step is a setup request, so none is measured.
//!
//! Connect sets no keep alive timeout (KATO 0). SPDK's target then starts
//! no keep alive timer, and the Linux target neither; a target that insists
//! on one would reject the Connect, which fails the run.
//!
//! # Commands
//!
//! The command ID is the request ID. A write of up to the target's
//! in-capsule data size carries its data in the command capsule; a larger
//! one waits for R2Ts, which the protocol answers with H2CData PDUs of at
//! most MAXH2CDATA bytes ([`Outbox::data`]). Read data arrives in C2HData
//! PDUs, which the parser streams ([`Parsed::Absorbed`]) however long they
//! are; one with the SUCCESS flag completes its read, else a CapsuleResp
//! does. A failed command is an error reply named after its status.
//! Header and data digests are off; a target that enables them fails the run.

pub mod config;
mod pdu;

pub use config::{NvmeAccess, NvmeTcpConfig};

use crate::workload::{CommandSelector, KeyGeneration, WeightedCommandSelector};
use anyhow::{anyhow, bail, ensure, Result};
use pdu::{
    CommonHeader, Cqe, DataHeader, DataPointer, CAPSULE_RESP_LEN, DATA_HLEN, IC_LEN, IDENTIFY_LEN,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use xylem_common::{ConnectionRole, ErrorReply, Outbox, Parsed, Protocol, Request, RequestMeta};
use xylem_core::seed::{components, derive_seed};

/// Request ID: the connection and the command ID, or [`IC_REQ_TAG`] for
/// the connection's ICReq
pub type NvmeRequestId = (usize, u32);

/// Request ID tag of a connection's ICReq, which has no command ID
pub const IC_REQ_TAG: u32 = 1 << 16;

/// Most commands an I/O queue can have in flight: its 0's based SQSIZE is
/// 16 bits wide
pub const MAX_QUEUE_DEPTH: usize = u16::MAX as usize;

/// 0's based size of the admin submission queue: 32 entries, the most
/// SPDK's and Linux's targets accept by default
const ADMIN_SQSIZE: u16 = 31;

/// Host identifier of every Connect
const HOST_ID: [u8; 16] = *b"xylem-nvme-tcp\0\x01";

/// Seed of the verify pattern. Every thread and run writes the same bytes
/// to an LBA, so any thread's write of it is what a read must return.
const PATTERN_SEED: u64 = 0x7879_6c65_6d2d_6e76;

/// Seed component of the read/write mix
const MIX_COMPONENT: &str = "nvme_tcp_read_write_mix";

/// Seed component of the data of writes outside verify mode
const WRITE_DATA_COMPONENT: &str = "nvme_tcp_write_data";

/// An I/O command of the read/write mix
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IoOp {
    Read,
    Write,
}

/// A connection's next connection setup step
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    IcReq,
    Connect,
    /// Admin queue: read CAP
    GetCap,
    /// Admin queue: set CC.EN
    EnableController,
    /// Admin queue: read CSTS until RDY
    WaitReady,
    IdentifyController,
    IdentifyNamespace,
    Done,
}

/// A command in flight on a queue
#[derive(Debug)]
enum Command {
    /// A connection setup command, with the data an Identify returns
    Setup {
        step: Step,
        data: Vec<u8>,
    },
    Read {
        slba: u64,
        len: u32,
        received: u32,
    },
    /// A write; `sent` counts the bytes R2Ts asked for. `prefill` writes
    /// the verify pattern before the measured commands.
    Write {
        slba: u64,
        len: u32,
        sent: u32,
        prefill: bool,
    },
}

/// Where the parser is in the received bytes of a queue
#[derive(Debug, Clone, Copy)]
enum Rx {
    /// At the start of a PDU
    Header,
    /// Inside the data of a C2HData PDU for command `cid`
    C2hData {
        cid: u16,
        offset: u32,
        remaining: u32,
        success: bool,
    },
}

/// One connection: an admin queue (QID 0) or an I/O queue
struct Queue {
    controller: usize,
    qid: u16,
    step: Step,
    /// A setup request is waiting for its reply
    awaiting: bool,
    maxh2cdata: u32,
    next_cid: u16,
    commands: HashMap<u16, Command>,
    rx: Rx,
    /// H2CData PDUs to send, taken by `drain_outbox`
    tx: Vec<u8>,
}

impl Queue {
    fn new(controller: usize, qid: u16) -> Self {
        Self {
            controller,
            qid,
            step: Step::IcReq,
            awaiting: false,
            maxh2cdata: 0,
            next_cid: 0,
            commands: HashMap::new(),
            rx: Rx::Header,
            tx: Vec::new(),
        }
    }

    /// A command ID not in flight
    fn allocate_cid(&mut self) -> u16 {
        loop {
            let cid = self.next_cid;
            self.next_cid = if cid == u16::MAX - 1 { 0 } else { cid + 1 };
            if !self.commands.contains_key(&cid) {
                return cid;
            }
        }
    }
}

/// What a controller's admin queue learned
#[derive(Debug, Default)]
struct Controller {
    io: Vec<usize>,
    cntlid: u16,
    /// CAP.MPSMIN: the smallest memory page is 4096 << mpsmin bytes
    mpsmin: u8,
    mdts: u8,
    /// Largest write that goes in the command capsule
    in_capsule: u32,
    /// The I/O queues may connect
    ready: bool,
}

/// The block range commands address, learned from the namespace
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Layout {
    lba_size: u32,
    /// LBAs per command
    nlb: u32,
    /// Commands' blocks in the range
    blocks: u64,
}

/// Verify mode's first pass over the range
///
/// A queue the worker scheduled while blocks were left may ask for a write
/// after others took the last ones. It rewrites a written block: the
/// pattern depends on the LBA only, so that changes nothing.
#[derive(Debug)]
struct Prefill {
    /// Prefill writes handed out; past the range's blocks, rewrites
    next: u64,
    outstanding: usize,
}

/// NVMe/TCP protocol; see the module documentation
pub struct NvmeTcpProtocol {
    config: NvmeTcpConfig,
    queue_depth: usize,
    seed: Option<u64>,
    queues: HashMap<usize, Queue>,
    controllers: Vec<Controller>,
    layout: Option<Layout>,
    blocks: Option<KeyGeneration>,
    mix: WeightedCommandSelector<IoOp>,
    /// Data of every write outside verify mode, `block_size` bytes
    write_data: Vec<u8>,
    prefill: Option<Prefill>,
    /// Connections to wake, taken by `drain_outbox`
    wake: Vec<usize>,
}

impl NvmeTcpProtocol {
    /// A protocol whose I/O queues hold `queue_depth` commands, the group's
    /// `max_pending_per_connection`
    pub fn from_config(
        config: &NvmeTcpConfig,
        queue_depth: usize,
        seed: Option<u64>,
    ) -> Result<Self> {
        config.validate()?;
        ensure!(
            (1..=MAX_QUEUE_DEPTH).contains(&queue_depth),
            "NVMe/TCP queue depth (max_pending_per_connection) must be from 1 to {MAX_QUEUE_DEPTH}"
        );
        let read = f64::from(config.read_percent);
        let mix = WeightedCommandSelector::with_seed(
            vec![(IoOp::Read, read), (IoOp::Write, 100.0 - read)],
            seed.map(|seed| derive_seed(seed, MIX_COMPONENT)),
        )?;
        let mut data_rng = match seed {
            Some(seed) => SmallRng::seed_from_u64(derive_seed(seed, WRITE_DATA_COMPONENT)),
            None => SmallRng::from_os_rng(),
        };
        let mut write_data = vec![0; config.block_size as usize];
        data_rng.fill(&mut write_data[..]);
        Ok(Self {
            config: config.clone(),
            queue_depth,
            seed,
            queues: HashMap::new(),
            controllers: Vec::new(),
            layout: None,
            blocks: None,
            mix,
            write_data,
            prefill: None,
            wake: Vec::new(),
        })
    }

    fn queue(&self, conn_id: usize) -> &Queue {
        self.queues.get(&conn_id).expect("the worker bound every connection it uses")
    }

    /// The next connection setup request of `conn_id`
    fn setup_request(&mut self, conn_id: usize, step: Step) -> Request<NvmeRequestId> {
        let config = &self.config;
        let queue = self.queues.get_mut(&conn_id).expect("bound connection");
        let controller = &self.controllers[queue.controller];
        queue.awaiting = true;
        if step == Step::IcReq {
            return Request::setup(pdu::ic_req(), (conn_id, IC_REQ_TAG));
        }
        let cid = queue.allocate_cid();
        let data = match step {
            Step::Connect if queue.qid == 0 => pdu::connect(
                cid,
                0,
                ADMIN_SQSIZE,
                &HOST_ID,
                0xffff,
                &config.subnqn,
                &config.hostnqn,
            ),
            Step::Connect => pdu::connect(
                cid,
                queue.qid,
                self.queue_depth as u16,
                &HOST_ID,
                controller.cntlid,
                &config.subnqn,
                &config.hostnqn,
            ),
            Step::GetCap => pdu::property_get(cid, pdu::PROP_CAP, true),
            Step::EnableController => {
                // EN, CSS NVM, MPS = MPSMIN, IOSQES 64 bytes, IOCQES 16 bytes
                let cc = 1 | (u32::from(controller.mpsmin) << 7) | (6 << 16) | (4 << 20);
                pdu::property_set(cid, pdu::PROP_CC, cc)
            }
            Step::WaitReady => pdu::property_get(cid, pdu::PROP_CSTS, false),
            Step::IdentifyController => pdu::identify(cid, 1, 0),
            Step::IdentifyNamespace => pdu::identify(cid, 0, config.nsid),
            Step::IcReq | Step::Done => unreachable!("no setup command"),
        };
        let identify = matches!(step, Step::IdentifyController | Step::IdentifyNamespace);
        let data_buffer = if identify {
            vec![0; IDENTIFY_LEN]
        } else {
            Vec::new()
        };
        queue.commands.insert(cid, Command::Setup { step, data: data_buffer });
        Request::setup(data, (conn_id, u32::from(cid)))
    }

    /// The next read or write on the I/O queue `conn_id`
    fn io_request(&mut self, conn_id: usize) -> Request<NvmeRequestId> {
        let layout = self.layout.expect("I/O queues send once the namespace is known");
        let (write, block, prefill) = match &mut self.prefill {
            Some(prefill) => {
                prefill.next += 1;
                prefill.outstanding += 1;
                (true, (prefill.next - 1) % layout.blocks, true)
            }
            None => {
                let write = self.mix.next_command() == IoOp::Write;
                let blocks = self.blocks.as_mut().expect("created with the layout");
                (write, blocks.next_key(), false)
            }
        };
        let slba = self.config.lba_start + block * u64::from(layout.nlb);
        let len = self.config.block_size;
        let nsid = self.config.nsid;
        let queue = self.queues.get_mut(&conn_id).expect("bound connection");
        let in_capsule = self.controllers[queue.controller].in_capsule;
        let cid = queue.allocate_cid();
        let data = if !write {
            queue.commands.insert(cid, Command::Read { slba, len, received: 0 });
            pdu::io_sqe(pdu::OPC_READ, cid, nsid, slba, layout.nlb, DataPointer::Transport(len))
                .capsule(&[])
        } else if len <= in_capsule {
            queue.commands.insert(cid, Command::Write { slba, len, sent: len, prefill });
            let sqe = pdu::io_sqe(
                pdu::OPC_WRITE,
                cid,
                nsid,
                slba,
                layout.nlb,
                DataPointer::InCapsule(len),
            );
            let mut data = Vec::with_capacity(pdu::CAPSULE_CMD_HLEN + len as usize);
            sqe.capsule_header(len as usize, &mut data);
            write_payload(
                self.config.verify,
                &self.write_data,
                layout.lba_size,
                slba,
                0,
                len,
                &mut data,
            );
            data
        } else {
            queue.commands.insert(cid, Command::Write { slba, len, sent: 0, prefill });
            pdu::io_sqe(pdu::OPC_WRITE, cid, nsid, slba, layout.nlb, DataPointer::Transport(len))
                .capsule(&[])
        };
        let id = (conn_id, u32::from(cid));
        if prefill {
            Request::new(data, id, RequestMeta::warmup())
        } else {
            Request::measurement(data, id)
        }
    }

    /// Answer an R2T with H2CData PDUs of the asked range
    fn answer_r2t(&mut self, conn_id: usize, r2t: DataHeader) -> Result<()> {
        let lba_size = self.layout.map_or(0, |layout| layout.lba_size);
        let queue = self.queues.get_mut(&conn_id).expect("bound connection");
        let Some(Command::Write { slba, len, sent, .. }) = queue.commands.get_mut(&r2t.cccid)
        else {
            bail!("NVMe/TCP R2T for command {}, which is no write in flight", r2t.cccid);
        };
        let end = r2t.offset.checked_add(r2t.len);
        ensure!(
            r2t.len > 0 && r2t.offset == *sent && end.is_some_and(|end| end <= *len),
            "NVMe/TCP R2T for command {} asks for {} bytes at offset {}; {} of its {} bytes \
             were asked for before",
            r2t.cccid,
            r2t.len,
            r2t.offset,
            sent,
            len
        );
        let end = r2t.offset + r2t.len;
        let mut offset = r2t.offset;
        while offset < end {
            let chunk = queue.maxh2cdata.min(end - offset);
            let last = offset + chunk == end;
            pdu::h2c_data_header(r2t.cccid, r2t.ttag, offset, chunk, last, &mut queue.tx);
            write_payload(
                self.config.verify,
                &self.write_data,
                lba_size,
                *slba,
                offset,
                chunk,
                &mut queue.tx,
            );
            offset += chunk;
        }
        *sent = end;
        Ok(())
    }

    /// Start a C2HData PDU whose header and padding are the first `pdo`
    /// bytes of `data`
    fn start_c2h_data(
        &mut self,
        conn_id: usize,
        header: CommonHeader,
        data: &[u8],
    ) -> Result<Parsed<'static, NvmeRequestId>> {
        let pdo = header.pdo as usize;
        let c2h = DataHeader::parse(data);
        ensure!(
            header.hlen as usize == DATA_HLEN && pdo >= DATA_HLEN,
            "NVMe/TCP C2HData has HLEN {} and PDO {}",
            header.hlen,
            pdo
        );
        ensure!(
            c2h.len > 0 && header.plen as u64 == pdo as u64 + u64::from(c2h.len),
            "NVMe/TCP C2HData PLEN {} is not PDO {pdo} plus DATAL {}",
            header.plen,
            c2h.len
        );
        let success = header.flags & pdu::FLAG_SUCCESS != 0;
        ensure!(
            !success || header.flags & pdu::FLAG_LAST_PDU != 0,
            "NVMe/TCP C2HData has the SUCCESS flag without LAST_PDU"
        );
        let queue = self.queues.get_mut(&conn_id).expect("bound connection");
        let (received, len) = match queue.commands.get(&c2h.cccid) {
            Some(Command::Read { received, len, .. }) => (*received, *len),
            Some(Command::Setup { data, .. }) if !data.is_empty() => (0, data.len() as u32),
            _ => bail!("NVMe/TCP C2HData for command {}, which reads nothing", c2h.cccid),
        };
        let end = c2h.offset.checked_add(c2h.len);
        ensure!(
            c2h.offset >= received && end.is_some_and(|end| end <= len),
            "NVMe/TCP C2HData for command {} carries {} bytes at offset {} of its {} bytes",
            c2h.cccid,
            c2h.len,
            c2h.offset,
            len
        );
        queue.rx = Rx::C2hData {
            cid: c2h.cccid,
            offset: c2h.offset,
            remaining: c2h.len,
            success,
        };
        Ok(Parsed::Absorbed(pdo))
    }

    /// Take the next part of the data of the C2HData PDU in progress
    fn c2h_chunk<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, NvmeRequestId>> {
        if data.is_empty() {
            return Ok(Parsed::Incomplete);
        }
        let verify = self.config.verify;
        let lba_size = self.layout.map_or(0, |layout| layout.lba_size);
        let queue = self.queues.get_mut(&conn_id).expect("bound connection");
        let Rx::C2hData { cid, offset, remaining, success } = queue.rx else {
            unreachable!("called inside C2HData only");
        };
        let n = remaining.min(data.len().min(u32::MAX as usize) as u32);
        let chunk = &data[..n as usize];
        match queue.commands.get_mut(&cid).expect("checked at the PDU header") {
            Command::Read { slba, received, .. } => {
                if verify {
                    check_pattern(lba_size, *slba, offset, chunk)?;
                }
                *received = offset + n;
            }
            Command::Setup { data: buffer, .. } => {
                buffer[offset as usize..(offset + n) as usize].copy_from_slice(chunk);
            }
            Command::Write { .. } => unreachable!("checked at the PDU header"),
        }
        if n < remaining {
            queue.rx = Rx::C2hData {
                cid,
                offset: offset + n,
                remaining: remaining - n,
                success,
            };
            return Ok(Parsed::Absorbed(n as usize));
        }
        queue.rx = Rx::Header;
        if !success {
            return Ok(Parsed::Absorbed(n as usize));
        }
        let cqe = Cqe { dw0: 0, dw1: 0, cid, status: 0 };
        self.complete(conn_id, cqe, n as usize)
    }

    /// Complete the command of `cqe`, whose response took `consumed` bytes
    fn complete<'a>(
        &mut self,
        conn_id: usize,
        cqe: Cqe,
        consumed: usize,
    ) -> Result<Parsed<'a, NvmeRequestId>> {
        let queue = self.queues.get_mut(&conn_id).expect("bound connection");
        let command = queue.commands.remove(&cqe.cid).ok_or_else(|| {
            anyhow!("NVMe/TCP completion for command {}, which is not in flight", cqe.cid)
        })?;
        let id = (conn_id, u32::from(cqe.cid));
        let failed = cqe.status != 0;
        let name = pdu::status_name(cqe.sct(), cqe.sc());
        match command {
            Command::Setup { step, data } => {
                if failed {
                    bail!(
                        "NVMe/TCP connection setup failed: {step:?} on queue {} returned {name} \
                         (SCT {:#x}, SC {:#04x})",
                        queue.qid,
                        cqe.sct(),
                        cqe.sc()
                    );
                }
                self.finish_setup_step(conn_id, step, cqe, &data)?;
                return Ok(Parsed::ok(consumed, id));
            }
            Command::Read { len, received, .. } => {
                ensure!(
                    failed || received == len,
                    "NVMe/TCP read {} completed after {received} of its {len} bytes",
                    cqe.cid
                );
            }
            Command::Write { len, sent, prefill, .. } => {
                ensure!(
                    failed || sent == len,
                    "NVMe/TCP write {} completed before R2Ts asked for all its data",
                    cqe.cid
                );
                if prefill {
                    ensure!(
                        !failed,
                        "NVMe/TCP verify: writing the pattern failed with {name} (SCT {:#x}, SC \
                         {:#04x})",
                        cqe.sct(),
                        cqe.sc()
                    );
                    self.finish_prefill_write();
                }
            }
        }
        Ok(if failed {
            Parsed::error(consumed, id, ErrorReply::new(name, name))
        } else {
            Parsed::ok(consumed, id)
        })
    }

    fn finish_prefill_write(&mut self) {
        let blocks = self.layout.expect("prefill follows the layout").blocks;
        let prefill = self.prefill.as_mut().expect("a prefill write was in flight");
        prefill.outstanding -= 1;
        if prefill.next >= blocks && prefill.outstanding == 0 {
            self.prefill = None;
            for controller in &self.controllers {
                self.wake.extend_from_slice(&controller.io);
            }
        }
    }

    fn finish_setup_step(
        &mut self,
        conn_id: usize,
        step: Step,
        cqe: Cqe,
        data: &[u8],
    ) -> Result<()> {
        let queue = self.queues.get_mut(&conn_id).expect("bound connection");
        let controller = &mut self.controllers[queue.controller];
        queue.awaiting = false;
        let mut layout = None;
        queue.step = match step {
            Step::Connect if queue.qid == 0 => {
                controller.cntlid = cqe.dw0 as u16;
                Step::GetCap
            }
            Step::Connect => Step::Done,
            Step::GetCap => {
                let cap = u64::from(cqe.dw0) | (u64::from(cqe.dw1) << 32);
                let mqes = (cap & 0xffff) as usize;
                ensure!(
                    (cap >> 37) & 1 == 1,
                    "The NVMe controller does not support the NVM command set (CAP.CSS)"
                );
                ensure!(
                    self.queue_depth <= mqes,
                    "max_pending_per_connection {} exceeds the {mqes} commands an I/O queue of \
                     the NVMe controller can hold (CAP.MQES)",
                    self.queue_depth
                );
                controller.mpsmin = ((cap >> 48) & 0xf) as u8;
                Step::EnableController
            }
            Step::EnableController => Step::WaitReady,
            Step::WaitReady => {
                ensure!(cqe.dw0 & 2 == 0, "The NVMe controller reports a fatal status (CSTS.CFS)");
                if cqe.dw0 & 1 == 1 {
                    Step::IdentifyController
                } else {
                    Step::WaitReady
                }
            }
            Step::IdentifyController => {
                controller.mdts = data[77];
                let ioccsz = u64::from(pdu::u32_at(data, 1792)) * 16;
                let icdoff = pdu::u16_at(data, 1800);
                // In-capsule data after an offset is not supported: such
                // writes wait for R2Ts instead
                controller.in_capsule = if icdoff == 0 {
                    ioccsz.saturating_sub(pdu::SQE_LEN as u64).min(u64::from(u32::MAX)) as u32
                } else {
                    0
                };
                Step::IdentifyNamespace
            }
            Step::IdentifyNamespace => {
                layout = Some(namespace_layout(&self.config, controller, data)?);
                controller.ready = true;
                self.wake.extend_from_slice(&controller.io);
                Step::Done
            }
            Step::IcReq | Step::Done => unreachable!("no setup command"),
        };
        match layout {
            Some(layout) => self.start_workload(layout),
            None => Ok(()),
        }
    }

    /// Use the namespace layout a controller learned: the first sets up the
    /// workload; the others must agree
    fn start_workload(&mut self, layout: Layout) -> Result<()> {
        if let Some(known) = self.layout {
            ensure!(
                known == layout,
                "NVMe controllers of one subsystem report different namespaces"
            );
            return Ok(());
        }
        self.layout = Some(layout);
        self.blocks = Some(match self.config.access {
            NvmeAccess::Sequential => KeyGeneration::round_robin(layout.blocks),
            NvmeAccess::Random => KeyGeneration::random_with_seed(
                layout.blocks,
                self.seed.map(|seed| derive_seed(seed, components::RANDOM_KEYS)),
            ),
        });
        if self.config.verify {
            self.prefill = Some(Prefill { next: 0, outstanding: 0 });
        }
        Ok(())
    }

    /// Commands the protocol tracks on `conn_id` (for tests)
    #[cfg(test)]
    fn in_flight(&self, conn_id: usize) -> usize {
        self.queue(conn_id).commands.len()
    }
}

/// The block range of the configured namespace from its Identify data
fn namespace_layout(
    config: &NvmeTcpConfig,
    controller: &Controller,
    data: &[u8],
) -> Result<Layout> {
    let nsid = config.nsid;
    let nsze = pdu::u64_at(data, 0);
    ensure!(nsze > 0, "NVMe namespace {nsid} of {} is not active", config.subnqn);
    let flbas = data[26];
    let format = usize::from((flbas & 0x0f) | ((flbas & 0x60) >> 1));
    let lbaf = pdu::u32_at(data, 128 + 4 * format);
    ensure!(
        lbaf & 0xffff == 0,
        "NVMe namespace {nsid} has metadata, which xylem does not send"
    );
    let lbads = (lbaf >> 16) & 0xff;
    ensure!(
        (9..32).contains(&lbads),
        "NVMe namespace {nsid} has an LBA data size of 2^{lbads}"
    );
    let lba_size = 1u32 << lbads;
    let block_size = config.block_size;
    ensure!(
        block_size.is_multiple_of(lba_size) && block_size / lba_size <= 1 << 16,
        "block_size {block_size} is not 1 to 65536 LBAs of namespace {nsid}'s {lba_size} bytes"
    );
    if controller.mdts != 0 {
        let max = (1u64 << controller.mdts) * (4096u64 << controller.mpsmin);
        ensure!(
            u64::from(block_size) <= max,
            "block_size {block_size} exceeds the {max} bytes the NVMe controller transfers per \
             command (MDTS)"
        );
    }
    let start = config.lba_start;
    let count = config.lba_count.unwrap_or(nsze.saturating_sub(start));
    ensure!(
        start < nsze && start.checked_add(count).is_some_and(|end| end <= nsze),
        "LBA range {start} + {count} exceeds namespace {nsid}'s {nsze} LBAs"
    );
    let nlb = block_size / lba_size;
    let blocks = count / u64::from(nlb);
    ensure!(blocks > 0, "LBA range of {count} LBAs holds no block of {block_size} bytes");
    Ok(Layout { lba_size, nlb, blocks })
}

/// Append bytes `offset..offset + len` of the data of a write at `slba`:
/// the verify pattern, or the fixed `write_data`
fn write_payload(
    verify: bool,
    write_data: &[u8],
    lba_size: u32,
    slba: u64,
    offset: u32,
    len: u32,
    out: &mut Vec<u8>,
) {
    let range = offset as usize..(offset + len) as usize;
    if verify {
        let start = out.len();
        out.resize(start + range.len(), 0);
        fill_pattern(lba_size, slba, offset, &mut out[start..]);
    } else {
        out.extend_from_slice(&write_data[range]);
    }
}

/// Fill `out` with the verify pattern of the data at `offset` of a transfer
/// starting at LBA `slba`: each 8-byte word of an LBA is a hash of the LBA
/// and the word's index, so each LBA's bytes depend on its address only.
/// LBA sizes are powers of two of at least 512, so no word spans two LBAs.
fn fill_pattern(lba_size: u32, slba: u64, offset: u32, out: &mut [u8]) {
    let lba_size = u64::from(lba_size);
    let mut position = u64::from(offset);
    let mut filled = 0;
    while filled < out.len() {
        let lba = slba + position / lba_size;
        let index = position % lba_size;
        let word = splitmix64(PATTERN_SEED ^ lba.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (index / 8));
        let skip = (index % 8) as usize;
        let len = (8 - skip).min(out.len() - filled);
        out[filled..filled + len].copy_from_slice(&word.to_le_bytes()[skip..skip + len]);
        filled += len;
        position += len as u64;
    }
}

/// Fail unless `data`, read at `offset` of a transfer from LBA `slba`, is
/// the verify pattern
fn check_pattern(lba_size: u32, slba: u64, offset: u32, data: &[u8]) -> Result<()> {
    let mut expected = vec![0; data.len()];
    fill_pattern(lba_size, slba, offset, &mut expected);
    let Some(index) = data.iter().zip(&expected).position(|(read, wrote)| read != wrote) else {
        return Ok(());
    };
    let position = u64::from(offset) + index as u64;
    let lba_size = u64::from(lba_size);
    bail!(
        "NVMe/TCP verify: LBA {} byte {} reads {:#04x}, but {:#04x} was written",
        slba + position / lba_size,
        position % lba_size,
        data[index],
        expected[index]
    )
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

impl Protocol for NvmeTcpProtocol {
    type RequestId = NvmeRequestId;

    fn connection_roles(&self, load: usize) -> Vec<ConnectionRole> {
        let per = self.config.io_queues_per_controller.unwrap_or(load).max(1);
        let mut roles = Vec::with_capacity(load + load.div_ceil(per));
        let mut left = load;
        while left > 0 {
            let io = left.min(per);
            roles.push(ConnectionRole::Control);
            roles.extend(std::iter::repeat_n(ConnectionRole::Load, io));
            left -= io;
        }
        roles
    }

    fn bind_connections(&mut self, conn_ids: &[usize]) {
        let per = self.config.io_queues_per_controller.unwrap_or(usize::MAX);
        let mut rest = conn_ids;
        while let Some((&admin, tail)) = rest.split_first() {
            let (io, tail) = tail.split_at(per.min(tail.len()));
            assert!(!io.is_empty(), "connection_roles gives every admin queue I/O queues");
            let controller = self.controllers.len();
            self.queues.insert(admin, Queue::new(controller, 0));
            for (qid, &conn_id) in (1..).zip(io) {
                self.queues.insert(conn_id, Queue::new(controller, qid));
            }
            self.controllers.push(Controller { io: io.to_vec(), ..Default::default() });
            rest = tail;
        }
    }

    fn next_request(&mut self, conn_id: usize) -> Request<NvmeRequestId> {
        match self.queue(conn_id).step {
            Step::Done => self.io_request(conn_id),
            step => self.setup_request(conn_id, step),
        }
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, NvmeRequestId>> {
        let queue = self
            .queues
            .get_mut(&conn_id)
            .ok_or_else(|| anyhow!("NVMe/TCP data on connection {conn_id}, which has no queue"))?;
        if let Rx::C2hData { .. } = queue.rx {
            return self.c2h_chunk(conn_id, data);
        }
        if data.len() < pdu::COMMON_HEADER_LEN {
            return Ok(Parsed::Incomplete);
        }
        let header = CommonHeader::parse(data);
        pdu::reject_digests(header)?;
        let fixed = |len: usize| -> Result<bool> {
            ensure!(
                header.hlen as usize == len && header.plen as usize == len,
                "NVMe/TCP PDU type {:#04x} has HLEN {} and PLEN {}, not {len}",
                header.pdu_type,
                header.hlen,
                header.plen
            );
            Ok(data.len() >= len)
        };
        match header.pdu_type {
            pdu::PDU_IC_RESP => {
                if !fixed(IC_LEN)? {
                    return Ok(Parsed::Incomplete);
                }
                let resp = pdu::parse_ic_resp(header, &data[..IC_LEN])?;
                ensure!(
                    queue.step == Step::IcReq && queue.awaiting,
                    "NVMe/TCP ICResp on connection {conn_id}, which sent no ICReq"
                );
                queue.maxh2cdata = resp.maxh2cdata;
                queue.awaiting = false;
                queue.step = Step::Connect;
                Ok(Parsed::ok(IC_LEN, (conn_id, IC_REQ_TAG)))
            }
            pdu::PDU_CAPSULE_RESP => {
                if !fixed(CAPSULE_RESP_LEN)? {
                    return Ok(Parsed::Incomplete);
                }
                self.complete(conn_id, Cqe::parse(data), CAPSULE_RESP_LEN)
            }
            pdu::PDU_R2T => {
                if !fixed(DATA_HLEN)? {
                    return Ok(Parsed::Incomplete);
                }
                self.answer_r2t(conn_id, DataHeader::parse(data))?;
                Ok(Parsed::Absorbed(DATA_HLEN))
            }
            pdu::PDU_C2H_DATA => {
                if data.len() < (header.pdo as usize).max(DATA_HLEN) {
                    return Ok(Parsed::Incomplete);
                }
                self.start_c2h_data(conn_id, header, data)
            }
            pdu::PDU_C2H_TERM_REQ => {
                if data.len() < DATA_HLEN {
                    return Ok(Parsed::Incomplete);
                }
                bail!(pdu::term_req_error(data))
            }
            other => bail!("Unexpected NVMe/TCP PDU type {other:#04x} from the target"),
        }
    }

    fn can_send(&self, conn_id: usize) -> bool {
        let Some(queue) = self.queues.get(&conn_id) else {
            return false;
        };
        if queue.awaiting {
            return false;
        }
        match queue.step {
            // An admin queue carries no load
            Step::Done if queue.qid == 0 => false,
            Step::Done => {
                let blocks = self.layout.map_or(0, |layout| layout.blocks);
                self.prefill.as_ref().is_none_or(|prefill| prefill.next < blocks)
            }
            Step::Connect if queue.qid != 0 => self.controllers[queue.controller].ready,
            _ => true,
        }
    }

    fn setup_pending(&self, conn_id: usize) -> bool {
        self.queues.get(&conn_id).is_some_and(|queue| queue.step != Step::Done)
    }

    fn drain_outbox(&mut self, conn_id: usize, outbox: &mut Outbox) {
        if let Some(queue) = self.queues.get_mut(&conn_id) {
            if outbox.data.is_empty() {
                std::mem::swap(&mut outbox.data, &mut queue.tx);
            } else {
                outbox.data.append(&mut queue.tx);
            }
        }
        outbox.wake.append(&mut self.wake);
    }

    fn name(&self) -> &'static str {
        "nvme-tcp"
    }

    fn reset(&mut self) {
        if let Some(blocks) = &mut self.blocks {
            blocks.reset();
        }
        self.mix.reset();
    }
}

#[cfg(test)]
mod tests;
