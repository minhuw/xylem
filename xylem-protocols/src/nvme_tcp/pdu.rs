//! NVMe/TCP PDUs (NVMe/TCP Transport Specification 1.0) and the NVMe
//! submission and completion queue entries they carry, with header and data
//! digests off

use anyhow::{bail, ensure, Result};

/// Length of the common header every PDU starts with
pub const COMMON_HEADER_LEN: usize = 8;
/// Length of an ICReq and an ICResp PDU
pub const IC_LEN: usize = 128;
/// Header length of a CapsuleCmd PDU: the common header and a 64-byte SQE
pub const CAPSULE_CMD_HLEN: usize = COMMON_HEADER_LEN + SQE_LEN;
/// Length of a CapsuleResp PDU: the common header and a 16-byte CQE
pub const CAPSULE_RESP_LEN: usize = COMMON_HEADER_LEN + 16;
/// Header length of the H2CData, C2HData and R2T PDUs
pub const DATA_HLEN: usize = 24;
/// Length of a submission queue entry
pub const SQE_LEN: usize = 64;
/// Length of the data of a Fabrics Connect command
pub const CONNECT_DATA_LEN: usize = 1024;
/// Length of Identify data
pub const IDENTIFY_LEN: usize = 4096;

pub const PDU_IC_REQ: u8 = 0x00;
pub const PDU_IC_RESP: u8 = 0x01;
pub const PDU_C2H_TERM_REQ: u8 = 0x03;
pub const PDU_CAPSULE_CMD: u8 = 0x04;
pub const PDU_CAPSULE_RESP: u8 = 0x05;
pub const PDU_H2C_DATA: u8 = 0x06;
pub const PDU_C2H_DATA: u8 = 0x07;
pub const PDU_R2T: u8 = 0x09;

/// Common header flag: the PDU has a header digest
pub const FLAG_HDGSTF: u8 = 0x01;
/// Common header flag: the PDU has a data digest
pub const FLAG_DDGSTF: u8 = 0x02;
/// H2CData and C2HData flag: the last data PDU of the command's transfer
pub const FLAG_LAST_PDU: u8 = 0x04;
/// C2HData flag: the command succeeded and no CapsuleResp follows
pub const FLAG_SUCCESS: u8 = 0x08;

/// Admin opcodes
pub const OPC_IDENTIFY: u8 = 0x06;
/// Fabrics command opcode; the command type is in byte 4
pub const OPC_FABRICS: u8 = 0x7f;
/// NVM command set opcodes
pub const OPC_WRITE: u8 = 0x01;
pub const OPC_READ: u8 = 0x02;

/// Fabrics command types
pub const FCTYPE_PROPERTY_SET: u8 = 0x00;
pub const FCTYPE_CONNECT: u8 = 0x01;
pub const FCTYPE_PROPERTY_GET: u8 = 0x04;

/// Controller property offsets
pub const PROP_CAP: u32 = 0x00;
pub const PROP_CC: u32 = 0x14;
pub const PROP_CSTS: u32 = 0x1c;

/// SGL descriptor identifier: Data Block, Offset subtype (in-capsule data)
const SGL_DATA_BLOCK_OFFSET: u8 = 0x01;
/// SGL descriptor identifier: Transport SGL Data Block, transport-specific
/// subtype (data moves in H2CData or C2HData PDUs)
const SGL_TRANSPORT_DATA_BLOCK: u8 = 0x5a;
/// CDW0 PSDT: SGLs for the data
const PSDT_SGL: u8 = 0x40;

/// The common header of a PDU
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommonHeader {
    pub pdu_type: u8,
    pub flags: u8,
    pub hlen: u8,
    pub pdo: u8,
    pub plen: u32,
}

impl CommonHeader {
    /// Read the common header at the start of `data`, which holds at least
    /// [`COMMON_HEADER_LEN`] bytes
    pub fn parse(data: &[u8]) -> Self {
        Self {
            pdu_type: data[0],
            flags: data[1],
            hlen: data[2],
            pdo: data[3],
            plen: u32_at(data, 4),
        }
    }

    fn write(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&[self.pdu_type, self.flags, self.hlen, self.pdo]);
        out.extend_from_slice(&self.plen.to_le_bytes());
    }
}

/// An ICReq PDU: PDU format version 0, no padding, no digests, one R2T in
/// flight per command
pub fn ic_req() -> Vec<u8> {
    let mut out = Vec::with_capacity(IC_LEN);
    CommonHeader {
        pdu_type: PDU_IC_REQ,
        flags: 0,
        hlen: IC_LEN as u8,
        pdo: 0,
        plen: IC_LEN as u32,
    }
    .write(&mut out);
    // PFV 0, HPDA 0, DGST none, MAXR2T 0 (0's based: one)
    out.resize(IC_LEN, 0);
    out
}

/// What an ICResp grants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IcResp {
    /// Most data bytes in one H2CData PDU
    pub maxh2cdata: u32,
}

/// Read an ICResp PDU, whose common header is `header`, from `data`
/// (its [`IC_LEN`] bytes)
pub fn parse_ic_resp(header: CommonHeader, data: &[u8]) -> Result<IcResp> {
    ensure!(
        header.hlen as usize == IC_LEN && header.plen as usize == IC_LEN,
        "NVMe/TCP ICResp has HLEN {} and PLEN {}, not {IC_LEN}",
        header.hlen,
        header.plen
    );
    let pfv = u16_at(data, 8);
    ensure!(pfv == 0, "NVMe/TCP ICResp has PDU format version {pfv}; xylem speaks version 0");
    let dgst = data[11];
    if dgst != 0 {
        bail!(
            "The NVMe/TCP target enabled {} on the connection; xylem supports neither header \
             nor data digests",
            digest_names(dgst & 1 != 0, dgst & 2 != 0)
        );
    }
    let maxh2cdata = u32_at(data, 12);
    ensure!(
        maxh2cdata >= 4096 && maxh2cdata.is_multiple_of(4),
        "NVMe/TCP ICResp MAXH2CDATA {maxh2cdata} is not a multiple of 4 of at least 4096"
    );
    Ok(IcResp { maxh2cdata })
}

/// Fail on a PDU whose header flags announce a digest
pub fn reject_digests(header: CommonHeader) -> Result<()> {
    let header_digest = header.flags & FLAG_HDGSTF != 0;
    let data_digest = header.flags & FLAG_DDGSTF != 0;
    if header_digest || data_digest {
        bail!(
            "The NVMe/TCP target sent a PDU (type {:#04x}) with {}; xylem supports neither \
             header nor data digests",
            header.pdu_type,
            digest_names(header_digest, data_digest)
        );
    }
    Ok(())
}

fn digest_names(header: bool, data: bool) -> &'static str {
    match (header, data) {
        (true, true) => "header and data digests",
        (true, false) => "header digests",
        _ => "data digests",
    }
}

/// Describe a C2HTermReq PDU's fatal error
pub fn term_req_error(data: &[u8]) -> String {
    let fes = u16_at(data, 8);
    let fei = u32_at(data, 10);
    let reason = match fes {
        0x01 => "invalid PDU header field",
        0x02 => "PDU sequence error",
        0x03 => "header digest error",
        0x04 => "data transfer out of range",
        0x05 => "data transfer limit exceeded",
        0x06 => "unsupported parameter",
        _ => "unknown error",
    };
    format!(
        "The NVMe/TCP target terminated the connection: {reason} (FES {fes:#06x}, FEI {fei:#x})"
    )
}

/// How a command's data moves
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataPointer {
    /// No data
    None,
    /// `len` bytes right after the SQE in the CapsuleCmd PDU
    InCapsule(u32),
    /// `len` bytes in H2CData PDUs the target asks for with R2Ts, or in
    /// C2HData PDUs the target sends
    Transport(u32),
}

/// A submission queue entry under construction
pub struct Sqe([u8; SQE_LEN]);

impl Sqe {
    /// An SQE with opcode `opc`, command ID `cid` and data `dptr`
    pub fn new(opc: u8, cid: u16, dptr: DataPointer) -> Self {
        let mut sqe = [0; SQE_LEN];
        sqe[0] = opc;
        sqe[1] = PSDT_SGL;
        sqe[2..4].copy_from_slice(&cid.to_le_bytes());
        let (len, kind) = match dptr {
            DataPointer::None => (0, SGL_TRANSPORT_DATA_BLOCK),
            DataPointer::InCapsule(len) => (len, SGL_DATA_BLOCK_OFFSET),
            DataPointer::Transport(len) => (len, SGL_TRANSPORT_DATA_BLOCK),
        };
        // SGL1 at bytes 24..40: address 0, length, descriptor identifier
        sqe[32..36].copy_from_slice(&len.to_le_bytes());
        sqe[39] = kind;
        Self(sqe)
    }

    /// A Fabrics command of type `fctype`
    pub fn fabrics(fctype: u8, cid: u16, dptr: DataPointer) -> Self {
        let mut sqe = Self::new(OPC_FABRICS, cid, dptr);
        sqe.0[4] = fctype;
        sqe
    }

    pub fn nsid(mut self, nsid: u32) -> Self {
        self.0[4..8].copy_from_slice(&nsid.to_le_bytes());
        self
    }

    /// Set the little-endian `value` at byte `offset` (40 for CDW10, ...)
    pub fn put(mut self, offset: usize, value: &[u8]) -> Self {
        self.0[offset..offset + value.len()].copy_from_slice(value);
        self
    }

    /// The CapsuleCmd PDU carrying this SQE and `in_capsule` data
    pub fn capsule(&self, in_capsule: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(CAPSULE_CMD_HLEN + in_capsule.len());
        self.capsule_header(in_capsule.len(), &mut out);
        out.extend_from_slice(in_capsule);
        out
    }

    /// Write the header of a CapsuleCmd PDU carrying this SQE and
    /// `in_capsule_len` bytes of data, which the caller appends
    pub fn capsule_header(&self, in_capsule_len: usize, out: &mut Vec<u8>) {
        CommonHeader {
            pdu_type: PDU_CAPSULE_CMD,
            flags: 0,
            hlen: CAPSULE_CMD_HLEN as u8,
            pdo: if in_capsule_len == 0 {
                0
            } else {
                CAPSULE_CMD_HLEN as u8
            },
            plen: (CAPSULE_CMD_HLEN + in_capsule_len) as u32,
        }
        .write(out);
        out.extend_from_slice(&self.0);
    }
}

/// A Fabrics Connect command for queue `qid` with `sqsize` (0's based)
/// entries and no keep alive timeout, and its data
pub fn connect(
    cid: u16,
    qid: u16,
    sqsize: u16,
    hostid: &[u8; 16],
    cntlid: u16,
    subnqn: &str,
    hostnqn: &str,
) -> Vec<u8> {
    let sqe = Sqe::fabrics(FCTYPE_CONNECT, cid, DataPointer::InCapsule(CONNECT_DATA_LEN as u32))
        // RECFMT 0, QID, SQSIZE, CATTR 0, KATO 0 (no keep alive)
        .put(42, &qid.to_le_bytes())
        .put(44, &sqsize.to_le_bytes());
    let mut data = [0u8; CONNECT_DATA_LEN];
    data[..16].copy_from_slice(hostid);
    data[16..18].copy_from_slice(&cntlid.to_le_bytes());
    data[256..256 + subnqn.len()].copy_from_slice(subnqn.as_bytes());
    data[512..512 + hostnqn.len()].copy_from_slice(hostnqn.as_bytes());
    sqe.capsule(&data)
}

/// A Fabrics Property Get of the property at `offset`, 8 bytes wide if `wide`
pub fn property_get(cid: u16, offset: u32, wide: bool) -> Vec<u8> {
    Sqe::fabrics(FCTYPE_PROPERTY_GET, cid, DataPointer::None)
        .put(40, &[u8::from(wide)])
        .put(44, &offset.to_le_bytes())
        .capsule(&[])
}

/// A Fabrics Property Set of the 4-byte property at `offset` to `value`
pub fn property_set(cid: u16, offset: u32, value: u32) -> Vec<u8> {
    Sqe::fabrics(FCTYPE_PROPERTY_SET, cid, DataPointer::None)
        .put(44, &offset.to_le_bytes())
        .put(48, &value.to_le_bytes())
        .capsule(&[])
}

/// An Identify command for the data structure `cns` of namespace `nsid`
pub fn identify(cid: u16, cns: u8, nsid: u32) -> Vec<u8> {
    Sqe::new(OPC_IDENTIFY, cid, DataPointer::Transport(IDENTIFY_LEN as u32))
        .nsid(nsid)
        .put(40, &[cns])
        .capsule(&[])
}

/// The SQE of an NVM Read or Write of `nlb` blocks from `slba`
pub fn io_sqe(opc: u8, cid: u16, nsid: u32, slba: u64, nlb: u32, dptr: DataPointer) -> Sqe {
    Sqe::new(opc, cid, dptr)
        .nsid(nsid)
        .put(40, &slba.to_le_bytes())
        .put(48, &((nlb - 1) as u16).to_le_bytes())
}

/// The header of an H2CData PDU of `datal` bytes at offset `datao` of the
/// transfer of command `cccid`, answering the R2T with tag `ttag`
pub fn h2c_data_header(
    cccid: u16,
    ttag: u16,
    datao: u32,
    datal: u32,
    last: bool,
    out: &mut Vec<u8>,
) {
    CommonHeader {
        pdu_type: PDU_H2C_DATA,
        flags: if last { FLAG_LAST_PDU } else { 0 },
        hlen: DATA_HLEN as u8,
        pdo: DATA_HLEN as u8,
        plen: DATA_HLEN as u32 + datal,
    }
    .write(out);
    out.extend_from_slice(&cccid.to_le_bytes());
    out.extend_from_slice(&ttag.to_le_bytes());
    out.extend_from_slice(&datao.to_le_bytes());
    out.extend_from_slice(&datal.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
}

/// A completion queue entry
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cqe {
    pub dw0: u32,
    pub dw1: u32,
    pub cid: u16,
    /// Status field without the phase tag: SC in bits 0-7, SCT in 8-10
    pub status: u16,
}

impl Cqe {
    /// Read the CQE of a CapsuleResp PDU (its [`CAPSULE_RESP_LEN`] bytes)
    pub fn parse(pdu: &[u8]) -> Self {
        Self {
            dw0: u32_at(pdu, 8),
            dw1: u32_at(pdu, 12),
            cid: u16_at(pdu, 20),
            status: u16_at(pdu, 22) >> 1,
        }
    }

    pub fn sct(&self) -> u8 {
        ((self.status >> 8) & 0x7) as u8
    }

    pub fn sc(&self) -> u8 {
        (self.status & 0xff) as u8
    }
}

/// Stable name of a failed command's status, by which error replies count
pub fn status_name(sct: u8, sc: u8) -> &'static str {
    match (sct, sc) {
        (0, 0x01) => "INVALID_OPCODE",
        (0, 0x02) => "INVALID_FIELD",
        (0, 0x04) => "DATA_TRANSFER_ERROR",
        (0, 0x06) => "INTERNAL_ERROR",
        (0, 0x07) => "ABORTED",
        (0, 0x0b) => "INVALID_NAMESPACE",
        (0, 0x0d) => "SGL_INVALID",
        (0, 0x80) => "LBA_OUT_OF_RANGE",
        (0, 0x81) => "CAPACITY_EXCEEDED",
        (0, 0x82) => "NAMESPACE_NOT_READY",
        (0, _) => "GENERIC_STATUS",
        (1, _) => "COMMAND_SPECIFIC",
        (2, _) => "MEDIA_ERROR",
        (3, _) => "PATH_ERROR",
        (7, _) => "VENDOR_SPECIFIC",
        _ => "UNKNOWN_STATUS",
    }
}

/// The parts of an R2T or a C2HData PDU header after the common header
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataHeader {
    pub cccid: u16,
    /// Transfer tag (R2T only)
    pub ttag: u16,
    /// Offset in the command's data (R2TO or DATAO)
    pub offset: u32,
    /// Length (R2TL or DATAL)
    pub len: u32,
}

impl DataHeader {
    /// Read the header of an R2T or C2HData PDU (at least [`DATA_HLEN`] bytes)
    pub fn parse(pdu: &[u8]) -> Self {
        Self {
            cccid: u16_at(pdu, 8),
            ttag: u16_at(pdu, 10),
            offset: u32_at(pdu, 12),
            len: u32_at(pdu, 16),
        }
    }
}

pub fn u16_at(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(data[offset..offset + 2].try_into().expect("two bytes"))
}

pub fn u32_at(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().expect("four bytes"))
}

pub fn u64_at(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(data[offset..offset + 8].try_into().expect("eight bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ic_req_asks_for_no_digests_and_one_r2t() {
        let pdu = ic_req();
        assert_eq!(pdu.len(), IC_LEN);
        assert_eq!(&pdu[..8], &[0, 0, 128, 0, 128, 0, 0, 0]);
        assert!(pdu[8..].iter().all(|&b| b == 0));
    }

    fn ic_resp(dgst: u8, maxh2cdata: u32) -> Vec<u8> {
        let mut pdu = vec![0; IC_LEN];
        pdu[..8].copy_from_slice(&[PDU_IC_RESP, 0, 128, 0, 128, 0, 0, 0]);
        pdu[11] = dgst;
        pdu[12..16].copy_from_slice(&maxh2cdata.to_le_bytes());
        pdu
    }

    #[test]
    fn ic_resp_with_digests_is_rejected_clearly() {
        let pdu = ic_resp(0, 131072);
        let header = CommonHeader::parse(&pdu);
        assert_eq!(parse_ic_resp(header, &pdu).unwrap().maxh2cdata, 131072);
        for (dgst, name) in [(1, "header digests"), (2, "data digests"), (3, "header and data")] {
            let pdu = ic_resp(dgst, 131072);
            let error = parse_ic_resp(CommonHeader::parse(&pdu), &pdu).unwrap_err().to_string();
            assert!(error.contains(name) && error.contains("supports neither"), "{error}");
        }
        let pdu = ic_resp(0, 100);
        assert!(parse_ic_resp(CommonHeader::parse(&pdu), &pdu).is_err());
    }

    #[test]
    fn write_in_capsule_has_offset_sgl_and_data_after_the_sqe() {
        let sqe = io_sqe(OPC_WRITE, 7, 1, 0x1122_3344_5566, 8, DataPointer::InCapsule(4096));
        let pdu = sqe.capsule(&[0xab; 4096]);
        let header = CommonHeader::parse(&pdu);
        assert_eq!((header.pdu_type, header.hlen, header.pdo), (PDU_CAPSULE_CMD, 72, 72));
        assert_eq!(header.plen as usize, 72 + 4096);
        let sqe = &pdu[8..72];
        assert_eq!((sqe[0], sqe[1], u16_at(sqe, 2), u32_at(sqe, 4)), (OPC_WRITE, 0x40, 7, 1));
        assert_eq!(u64_at(sqe, 40), 0x1122_3344_5566);
        assert_eq!(u16_at(sqe, 48), 7, "NLB is 0's based");
        assert_eq!((u64_at(sqe, 24), u32_at(sqe, 32), sqe[39]), (0, 4096, 0x01));
        assert!(pdu[72..].iter().all(|&b| b == 0xab));
    }

    #[test]
    fn read_uses_a_transport_sgl_and_no_capsule_data() {
        let pdu = io_sqe(OPC_READ, 3, 2, 16, 8, DataPointer::Transport(4096)).capsule(&[]);
        let header = CommonHeader::parse(&pdu);
        assert_eq!((header.pdo, header.plen), (0, 72));
        assert_eq!((u32_at(&pdu, 8 + 32), pdu[8 + 39]), (4096, 0x5a));
    }

    #[test]
    fn connect_carries_nqns_and_cntlid_in_capsule() {
        let pdu = connect(1, 2, 31, &[9; 16], 0x0042, "nqn.sub", "nqn.host");
        assert_eq!(pdu.len(), 72 + 1024);
        let sqe = &pdu[8..72];
        assert_eq!((sqe[0], sqe[4]), (OPC_FABRICS, FCTYPE_CONNECT));
        assert_eq!((u16_at(sqe, 42), u16_at(sqe, 44), u32_at(sqe, 48)), (2, 31, 0));
        let data = &pdu[72..];
        assert_eq!((&data[..16], u16_at(data, 16)), (&[9; 16][..], 0x42));
        assert_eq!(&data[256..263], b"nqn.sub");
        assert_eq!(&data[512..520], b"nqn.host");
    }

    #[test]
    fn property_commands_name_offset_and_value() {
        let set = property_set(4, PROP_CC, 0x0046_0001);
        assert_eq!(
            (set[8 + 4], u32_at(&set, 8 + 44), u32_at(&set, 8 + 48)),
            (0, 0x14, 0x0046_0001)
        );
        let get = property_get(5, PROP_CAP, true);
        assert_eq!((get[8 + 4], get[8 + 40], u32_at(&get, 8 + 44)), (4, 1, 0));
    }

    #[test]
    fn h2c_data_header_marks_the_last_pdu() {
        let mut out = Vec::new();
        h2c_data_header(7, 3, 8192, 4096, true, &mut out);
        let header = CommonHeader::parse(&out);
        assert_eq!(
            header,
            CommonHeader {
                pdu_type: 6,
                flags: 4,
                hlen: 24,
                pdo: 24,
                plen: 4120
            }
        );
        let data = DataHeader::parse(&out);
        assert_eq!(
            data,
            DataHeader {
                cccid: 7,
                ttag: 3,
                offset: 8192,
                len: 4096
            }
        );
    }

    #[test]
    fn cqe_status_drops_the_phase_tag() {
        let mut pdu = vec![0; CAPSULE_RESP_LEN];
        pdu[20..22].copy_from_slice(&9u16.to_le_bytes());
        // SCT 0, SC 0x80 (LBA out of range), phase 1
        pdu[22..24].copy_from_slice(&((0x80u16 << 1) | 1).to_le_bytes());
        let cqe = Cqe::parse(&pdu);
        assert_eq!((cqe.cid, cqe.sct(), cqe.sc()), (9, 0, 0x80));
        assert_eq!(status_name(cqe.sct(), cqe.sc()), "LBA_OUT_OF_RANGE");
    }
}
