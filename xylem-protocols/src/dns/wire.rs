//! DNS message encoding and decoding (RFC 1035 section 4, RFC 6891 for OPT)
//!
//! Only what a query load needs: building a one-question query and checking
//! a response's header, question and record structure.

use anyhow::{bail, ensure, Result};

/// Size of the message header
pub const HEADER_LEN: usize = 12;

/// Longest name in wire form, length bytes and root label included
pub const MAX_NAME_LEN: usize = 255;

/// Longest label
const MAX_LABEL_LEN: usize = 63;

/// Class IN
pub const CLASS_IN: u16 = 1;

/// Type of the EDNS(0) OPT pseudo-record
pub const TYPE_OPT: u16 = 41;

/// Header flag bits
pub const FLAG_QR: u16 = 0x8000;
pub const FLAG_TC: u16 = 0x0200;
pub const FLAG_RD: u16 = 0x0100;

/// The DO (DNSSEC OK) bit of the OPT record's TTL field
const OPT_DO: u32 = 0x8000;

/// RCODE values the protocol treats specially
pub const RCODE_NOERROR: u16 = 0;
pub const RCODE_NXDOMAIN: u16 = 3;

/// Query types by mnemonic (RFC 1035, 3596, 2782, 4034, 6844, 9460)
const QTYPES: &[(&str, u16)] = &[
    ("A", 1),
    ("NS", 2),
    ("CNAME", 5),
    ("SOA", 6),
    ("PTR", 12),
    ("MX", 15),
    ("TXT", 16),
    ("AAAA", 28),
    ("SRV", 33),
    ("NAPTR", 35),
    ("DS", 43),
    ("RRSIG", 46),
    ("NSEC", 47),
    ("DNSKEY", 48),
    ("SVCB", 64),
    ("HTTPS", 65),
    ("CAA", 257),
    ("ANY", 255),
];

/// Types that are not queries for records over one message exchange: OPT
/// and the TKEY, TSIG, IXFR, AXFR, MAILB and MAILA meta-types
const UNSUPPORTED_QTYPES: &[(&str, u16)] = &[
    ("OPT", TYPE_OPT),
    ("TKEY", 249),
    ("TSIG", 250),
    ("IXFR", 251),
    ("AXFR", 252),
    ("MAILB", 253),
    ("MAILA", 254),
];

/// Parse a query type: a mnemonic such as "AAAA" (any case) or the RFC 3597
/// generic form "TYPE65"
pub fn parse_qtype(text: &str) -> Result<u16> {
    let upper = text.to_ascii_uppercase();
    let mut known = QTYPES.iter().chain(UNSUPPORTED_QTYPES);
    let qtype = match known.find(|(name, _)| *name == upper) {
        Some(&(_, qtype)) => qtype,
        None => match upper.strip_prefix("TYPE").and_then(|n| n.parse::<u16>().ok()) {
            Some(qtype) => qtype,
            None => {
                let names: Vec<&str> = QTYPES.iter().map(|(name, _)| *name).collect();
                bail!("Unknown DNS qtype '{text}': use one of {} or TYPE<n>", names.join(", "))
            }
        },
    };
    ensure!(
        !UNSUPPORTED_QTYPES.iter().any(|&(_, unsupported)| unsupported == qtype),
        "DNS qtype '{text}' ({qtype}) is not supported: zone transfers, TSIG, TKEY and OPT \
         are out of scope"
    );
    Ok(qtype)
}

/// Encode a name such as "www.example.test." (the final dot is optional)
/// in wire form, without compression. "." is the root.
pub fn encode_name(name: &str) -> Result<Vec<u8>> {
    let mut wire = Vec::with_capacity(name.len() + 2);
    let trimmed = name.strip_suffix('.').unwrap_or(name);
    if !trimmed.is_empty() {
        for label in trimmed.split('.') {
            ensure!(!label.is_empty(), "DNS name '{name}' has an empty label");
            ensure!(
                label.len() <= MAX_LABEL_LEN,
                "DNS name '{name}' has a label longer than {MAX_LABEL_LEN} bytes"
            );
            ensure!(
                label.bytes().all(|b| b.is_ascii_graphic() && b != b'\\'),
                "DNS name '{name}' must be printable ASCII without spaces or escapes"
            );
            wire.push(label.len() as u8);
            wire.extend_from_slice(label.as_bytes());
        }
    }
    wire.push(0);
    ensure!(
        wire.len() <= MAX_NAME_LEN,
        "DNS name '{name}' is {} bytes in wire form, longer than {MAX_NAME_LEN}",
        wire.len()
    );
    Ok(wire)
}

/// EDNS(0) settings of a query's OPT record
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Edns {
    /// Largest UDP response the client accepts
    pub udp_size: u16,
    /// Set the DO (DNSSEC OK) bit
    pub dnssec_ok: bool,
}

/// Append a query with one question (`name` in wire form, `qtype`, class IN)
/// to `buf`
pub fn write_query(
    buf: &mut Vec<u8>,
    id: u16,
    flags: u16,
    name: &[u8],
    qtype: u16,
    edns: Option<Edns>,
) {
    buf.extend_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(&flags.to_be_bytes());
    let additional = u16::from(edns.is_some());
    for count in [1, 0, 0, additional] {
        buf.extend_from_slice(&count.to_be_bytes());
    }
    buf.extend_from_slice(name);
    buf.extend_from_slice(&qtype.to_be_bytes());
    buf.extend_from_slice(&CLASS_IN.to_be_bytes());
    if let Some(edns) = edns {
        buf.push(0); // root owner name
        buf.extend_from_slice(&TYPE_OPT.to_be_bytes());
        buf.extend_from_slice(&edns.udp_size.to_be_bytes());
        // Extended RCODE 0, version 0, then the flags
        let ttl = if edns.dnssec_ok { OPT_DO } else { 0 };
        buf.extend_from_slice(&ttl.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes()); // no options
    }
}

/// Length of a query message with a `name_len`-byte name
pub fn query_len(name_len: usize, edns: bool) -> usize {
    HEADER_LEN + name_len + 4 + if edns { 11 } else { 0 }
}

/// The fields of a response the protocol checks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Response<'a> {
    pub id: u16,
    pub flags: u16,
    /// RCODE with the extended bits of the OPT record, if any
    pub rcode: u16,
    /// The question, `None` when the response has none
    pub question: Option<Question<'a>>,
}

/// One question in wire form
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Question<'a> {
    /// The name in wire form, without compression
    pub name: &'a [u8],
    pub qtype: u16,
    pub qclass: u16,
}

impl Question<'_> {
    /// Whether this question asks for `name` (wire form) and `qtype` in class
    /// IN; names compare case-insensitively (RFC 4343)
    pub fn matches(&self, name: &[u8], qtype: u16) -> bool {
        self.qtype == qtype && self.qclass == CLASS_IN && self.name.eq_ignore_ascii_case(name)
    }
}

impl Response<'_> {
    /// The 4-bit opcode
    pub fn opcode(&self) -> u16 {
        (self.flags >> 11) & 0xf
    }
}

/// Read a big-endian u16 at `at`
fn u16_at(message: &[u8], at: usize) -> Result<u16> {
    match message.get(at..at + 2) {
        Some(bytes) => Ok(u16::from_be_bytes([bytes[0], bytes[1]])),
        None => bail!("DNS message of {} bytes ends inside a field", message.len()),
    }
}

/// Offset just past the name at `at`, and whether it ends in a compression
/// pointer
fn skip_name(message: &[u8], mut at: usize) -> Result<(usize, bool)> {
    let start = at;
    loop {
        let Some(&len) = message.get(at) else {
            bail!("DNS message of {} bytes ends inside a name", message.len());
        };
        match len & 0xc0 {
            0x00 if len == 0 => return Ok((at + 1, false)),
            0x00 => at += 1 + len as usize,
            0xc0 => {
                ensure!(at + 2 <= message.len(), "DNS message ends inside a name pointer");
                return Ok((at + 2, true));
            }
            _ => bail!("DNS name has unsupported label type 0x{len:02x}"),
        }
        ensure!(at - start < MAX_NAME_LEN, "DNS name is longer than {MAX_NAME_LEN} bytes");
    }
}

/// Decode a complete response message: its header and question, and every
/// record, which must end exactly at the end of `message`
pub fn decode_response(message: &[u8]) -> Result<Response<'_>> {
    ensure!(
        message.len() >= HEADER_LEN,
        "DNS message of {} bytes is shorter than its {HEADER_LEN}-byte header",
        message.len()
    );
    let id = u16_at(message, 0)?;
    let flags = u16_at(message, 2)?;
    let [qdcount, ancount, nscount, arcount] =
        [4, 6, 8, 10].map(|at| u16_at(message, at).expect("the header is complete"));
    ensure!(qdcount <= 1, "DNS response has {qdcount} questions; a query has one");

    let mut at = HEADER_LEN;
    let question = if qdcount == 1 {
        let (end, compressed) = skip_name(message, at)?;
        ensure!(
            !compressed,
            "DNS response question name is compressed; it must echo the query's name"
        );
        let question = Question {
            name: &message[at..end],
            qtype: u16_at(message, end)?,
            qclass: u16_at(message, end + 2)?,
        };
        at = end + 4;
        Some(question)
    } else {
        None
    };

    let mut extended_rcode = None;
    let records = ancount as usize + nscount as usize + arcount as usize;
    for index in 0..records {
        let owner = at;
        at = skip_name(message, at)?.0;
        let rtype = u16_at(message, at)?;
        let rdlength = u16_at(message, at + 8)? as usize;
        if rtype == TYPE_OPT {
            ensure!(
                index >= records - arcount as usize,
                "DNS response has an OPT record outside its additional section"
            );
            ensure!(message[owner] == 0, "DNS response OPT record is not owned by the root");
            ensure!(extended_rcode.is_none(), "DNS response has more than one OPT record");
            // The TTL field's top byte holds the RCODE's upper 8 bits
            extended_rcode = Some(message[at + 4]);
        }
        at += 10 + rdlength;
        ensure!(
            at <= message.len(),
            "DNS message of {} bytes ends inside a record",
            message.len()
        );
    }
    ensure!(
        at == message.len(),
        "DNS message of {} bytes has {} bytes after its last record",
        message.len(),
        message.len() - at
    );
    let rcode = (u16::from(extended_rcode.unwrap_or(0)) << 4) | (flags & 0xf);
    Ok(Response { id, flags, rcode, question })
}

/// The mnemonic of an RCODE (IANA DNS parameters), "UNASSIGNED" for others
pub fn rcode_name(rcode: u16) -> &'static str {
    match rcode {
        0 => "NOERROR",
        1 => "FORMERR",
        2 => "SERVFAIL",
        3 => "NXDOMAIN",
        4 => "NOTIMP",
        5 => "REFUSED",
        6 => "YXDOMAIN",
        7 => "YXRRSET",
        8 => "NXRRSET",
        9 => "NOTAUTH",
        10 => "NOTZONE",
        11 => "DSOTYPENI",
        16 => "BADVERS",
        17 => "BADKEY",
        18 => "BADTIME",
        19 => "BADMODE",
        20 => "BADNAME",
        21 => "BADALG",
        22 => "BADTRUNC",
        23 => "BADCOOKIE",
        _ => "UNASSIGNED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_encode_as_labels() {
        assert_eq!(encode_name("www.example.test.").unwrap(), b"\x03www\x07example\x04test\x00");
        assert_eq!(encode_name("www.example.test").unwrap(), b"\x03www\x07example\x04test\x00");
        assert_eq!(encode_name(".").unwrap(), b"\x00");
        assert_eq!(encode_name("").unwrap(), b"\x00");
        assert_eq!(encode_name("*.wild.test").unwrap(), b"\x01*\x04wild\x04test\x00");
    }

    #[test]
    fn invalid_names_are_rejected() {
        for name in ["a..b", ".a", "a b.test", "a\\.b", "é.test"] {
            assert!(encode_name(name).is_err(), "{name}");
        }
        let long_label = "x".repeat(64);
        assert!(encode_name(&long_label).is_err());
        assert!(encode_name(&"x".repeat(63)).is_ok());
        // 4 labels of 63 bytes take 4 * 64 + 1 = 257 bytes
        let long_name = [&long_label[..63]; 4].join(".");
        assert!(encode_name(&long_name).is_err());
        let longest = format!("{}.{}", [&long_label[..63]; 3].join("."), "x".repeat(61));
        assert_eq!(encode_name(&longest).unwrap().len(), 255);
    }

    #[test]
    fn qtypes_parse_by_mnemonic_or_number() {
        assert_eq!(parse_qtype("A").unwrap(), 1);
        assert_eq!(parse_qtype("aaaa").unwrap(), 28);
        assert_eq!(parse_qtype("MX").unwrap(), 15);
        assert_eq!(parse_qtype("ANY").unwrap(), 255);
        assert_eq!(parse_qtype("TYPE65").unwrap(), 65);
        assert!(parse_qtype("BOGUS").is_err());
        assert!(parse_qtype("TYPE70000").is_err());
        for unsupported in ["AXFR", "ixfr", "TSIG", "TYPE251", "TYPE41", "TYPE250"] {
            let error = parse_qtype(unsupported).unwrap_err().to_string();
            assert!(error.contains("out of scope"), "{unsupported}: {error}");
        }
    }

    #[test]
    fn query_matches_rfc_1035_layout() {
        let name = encode_name("www.example.test.").unwrap();
        let mut buf = Vec::new();
        write_query(&mut buf, 0x1234, FLAG_RD, &name, 28, None);
        let mut expected = vec![0x12, 0x34, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0];
        expected.extend_from_slice(&name);
        expected.extend_from_slice(&[0, 28, 0, 1]);
        assert_eq!(buf, expected);
        assert_eq!(buf.len(), query_len(name.len(), false));
    }

    #[test]
    fn query_with_edns_carries_an_opt_record() {
        let name = encode_name("a.test").unwrap();
        let mut buf = Vec::new();
        let edns = Edns { udp_size: 1232, dnssec_ok: true };
        write_query(&mut buf, 7, 0, &name, 1, Some(edns));
        assert_eq!(&buf[10..12], &[0, 1], "ARCOUNT");
        let opt = &buf[HEADER_LEN + name.len() + 4..];
        assert_eq!(opt, &[0, 0, 41, 0x04, 0xd0, 0, 0, 0x80, 0, 0, 0]);
        assert_eq!(buf.len(), query_len(name.len(), true));
        // A query is itself a well-formed message
        let decoded = decode_response(&buf).unwrap();
        assert_eq!(decoded.id, 7);
        assert!(decoded.question.unwrap().matches(&name, 1));
    }

    /// A response to `name`/`qtype` with `answers` A records, compressed
    fn response(id: u16, flags: u16, name: &[u8], qtype: u16, answers: u16) -> Vec<u8> {
        let mut buf = Vec::new();
        for field in [id, flags, 1, answers, 0, 0] {
            buf.extend_from_slice(&field.to_be_bytes());
        }
        buf.extend_from_slice(name);
        buf.extend_from_slice(&qtype.to_be_bytes());
        buf.extend_from_slice(&CLASS_IN.to_be_bytes());
        for _ in 0..answers {
            buf.extend_from_slice(&[0xc0, 12, 0, 1, 0, 1, 0, 0, 1, 44, 0, 4, 192, 0, 2, 80]);
        }
        buf
    }

    #[test]
    fn responses_decode_with_question_and_records() {
        let name = encode_name("www.example.test").unwrap();
        let message = response(9, FLAG_QR | 0x0400, &name, 1, 2);
        let decoded = decode_response(&message).unwrap();
        assert_eq!(decoded.id, 9);
        assert_eq!(decoded.rcode, 0);
        assert_eq!(decoded.opcode(), 0);
        let question = decoded.question.unwrap();
        assert!(question.matches(&name, 1));
        assert!(question.matches(&encode_name("WWW.Example.TEST").unwrap(), 1));
        assert!(!question.matches(&name, 28));
        assert!(!question.matches(&encode_name("ww.example.test").unwrap(), 1));
    }

    #[test]
    fn extended_rcode_comes_from_the_opt_record() {
        let name = encode_name("a.test").unwrap();
        let mut message = response(1, FLAG_QR, &name, 1, 0);
        message[11] = 1; // ARCOUNT
        message.extend_from_slice(&[0, 0, 41, 4, 0xd0, 1, 0, 0, 0, 0, 0]);
        let decoded = decode_response(&message).unwrap();
        assert_eq!(decoded.rcode, 16);
        assert_eq!(rcode_name(decoded.rcode), "BADVERS");
    }

    #[test]
    fn malformed_responses_are_errors() {
        let name = encode_name("a.test").unwrap();
        let good = response(1, FLAG_QR, &name, 1, 1);
        assert!(decode_response(&good).is_ok());
        // Short header, cut record, trailing bytes, counts beyond the data
        assert!(decode_response(&good[..11]).is_err());
        assert!(decode_response(&good[..good.len() - 1]).is_err());
        let mut trailing = good.clone();
        trailing.push(0);
        assert!(decode_response(&trailing).is_err());
        let mut more = good.clone();
        more[7] = 2;
        assert!(decode_response(&more).is_err());
        // Two questions
        let mut two = good.clone();
        two[5] = 2;
        assert!(decode_response(&two).is_err());
        // A compressed question name
        assert!(decode_response(&response(1, FLAG_QR, b"\xc0\x0c", 1, 0)).is_err());
        assert!(decode_response(&response(1, FLAG_QR, b"\xc0\x00", 1, 0)).is_err());
        // An extended label type
        assert!(decode_response(&response(1, FLAG_QR, b"\x41a\x00", 1, 0)).is_err());
        // Two OPT records, an OPT record in the answer section
        let mut two_opt = response(1, FLAG_QR, &name, 1, 0);
        two_opt[11] = 2;
        for _ in 0..2 {
            two_opt.extend_from_slice(&[0, 0, 41, 4, 0xd0, 0, 0, 0, 0, 0, 0]);
        }
        assert!(decode_response(&two_opt).is_err());
        let mut answer_opt = response(1, FLAG_QR, &name, 1, 0);
        answer_opt[7] = 1;
        answer_opt.extend_from_slice(&[0, 0, 41, 4, 0xd0, 0, 0, 0, 0, 0, 0]);
        assert!(decode_response(&answer_opt).is_err());
    }

    #[test]
    fn a_response_may_have_no_question() {
        let mut message = vec![0, 5, 0x80, 0x01, 0, 0, 0, 0, 0, 0, 0, 0];
        let decoded = decode_response(&message).unwrap();
        assert_eq!((decoded.id, decoded.rcode, decoded.question), (5, 1, None));
        message.push(0);
        assert!(decode_response(&message).is_err());
    }

    #[test]
    fn rcodes_have_their_iana_names() {
        assert_eq!(rcode_name(0), "NOERROR");
        assert_eq!(rcode_name(2), "SERVFAIL");
        assert_eq!(rcode_name(3), "NXDOMAIN");
        assert_eq!(rcode_name(5), "REFUSED");
        assert_eq!(rcode_name(12), "UNASSIGNED");
    }
}
