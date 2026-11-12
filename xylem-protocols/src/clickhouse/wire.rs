//! ClickHouse native protocol encoding: LEB128 integers, length-prefixed
//! strings, the client's packets and the server's small packets
//!
//! The field lists follow ClickHouse 26.8's `TCPHandler`, `ClientInfo`,
//! `ProgressValues`, `ProfileInfo`, `BlockInfo` and `writeException`.

use anyhow::{bail, ensure, Result};

/// The protocol revision xylem sends: the last before custom column
/// serializations (54454), which let the server send sparse columns
pub const REVISION: u64 = 54453;

/// The oldest server revision xylem accepts: the first that takes query
/// settings as strings
pub const MIN_REVISION: u64 = 54429;

// Revisions that add fields xylem reads or writes, as ProtocolDefines.h
// names them (DBMS_MIN_REVISION_WITH_* and DBMS_MIN_PROTOCOL_VERSION_WITH_*)
const WITH_INTERSERVER_SECRET: u64 = 54441;
const WITH_OPENTELEMETRY: u64 = 54442;
const WITH_DISTRIBUTED_DEPTH: u64 = 54448;
const WITH_INITIAL_QUERY_START_TIME: u64 = 54449;
const WITH_PARALLEL_REPLICAS: u64 = 54453;

/// Most bytes one parse step may consume: the worker's frame limit. A
/// packet part that must be parsed whole (a header, an exception) cannot
/// be longer.
pub const MAX_STEP: usize = 64 * 1024;

/// Client packet types (`Protocol::Client`)
pub mod client {
    pub const HELLO: u64 = 0;
    pub const QUERY: u64 = 1;
    pub const DATA: u64 = 2;
}

/// Server packet types (`Protocol::Server`)
pub mod server {
    pub const HELLO: u64 = 0;
    pub const DATA: u64 = 1;
    pub const EXCEPTION: u64 = 2;
    pub const PROGRESS: u64 = 3;
    pub const PONG: u64 = 4;
    pub const END_OF_STREAM: u64 = 5;
    pub const PROFILE_INFO: u64 = 6;
    pub const TOTALS: u64 = 7;
    pub const EXTREMES: u64 = 8;
    pub const TABLES_STATUS_RESPONSE: u64 = 9;
    pub const LOG: u64 = 10;
    pub const TABLE_COLUMNS: u64 = 11;
    pub const PART_UUIDS: u64 = 12;
    pub const READ_TASK_REQUEST: u64 = 13;
    pub const PROFILE_EVENTS: u64 = 14;
    pub const MERGE_TREE_ALL_RANGES_ANNOUNCEMENT: u64 = 15;
    pub const MERGE_TREE_READ_TASK_REQUEST: u64 = 16;
    pub const TIMEZONE_UPDATE: u64 = 17;
    pub const SSH_CHALLENGE: u64 = 18;

    /// The name of server packet type `packet`, for error messages
    pub fn name(packet: u64) -> &'static str {
        match packet {
            HELLO => "Hello",
            DATA => "Data",
            EXCEPTION => "Exception",
            PROGRESS => "Progress",
            PONG => "Pong",
            END_OF_STREAM => "EndOfStream",
            PROFILE_INFO => "ProfileInfo",
            TOTALS => "Totals",
            EXTREMES => "Extremes",
            TABLES_STATUS_RESPONSE => "TablesStatusResponse",
            LOG => "Log",
            TABLE_COLUMNS => "TableColumns",
            PART_UUIDS => "PartUUIDs",
            READ_TASK_REQUEST => "ReadTaskRequest",
            PROFILE_EVENTS => "ProfileEvents",
            MERGE_TREE_ALL_RANGES_ANNOUNCEMENT => "MergeTreeAllRangesAnnouncement",
            MERGE_TREE_READ_TASK_REQUEST => "MergeTreeReadTaskRequest",
            TIMEZONE_UPDATE => "TimezoneUpdate",
            SSH_CHALLENGE => "SSHChallenge",
            _ => "unknown",
        }
    }
}

/// Client name in Hello and in every query's client info
const CLIENT_NAME: &str = "xylem";

/// Query processing stage `Complete`
const STAGE_COMPLETE: u64 = 2;

/// Unwrap the `Option` of a read, or return `Ok(None)`: the data ends first
macro_rules! take {
    ($read:expr) => {
        match $read {
            Some(value) => value,
            None => return Ok(None),
        }
    };
}
pub(crate) use take;

/// Reads the start of received data; `None` means the data ends first
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Bytes read so far
    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let bytes = self.data.get(self.pos..self.pos.checked_add(n)?)?;
        self.pos += n;
        Some(bytes)
    }

    pub fn u8(&mut self) -> Option<u8> {
        self.bytes(1).map(|bytes| bytes[0])
    }

    pub fn i32(&mut self) -> Option<i32> {
        self.bytes(4).map(|bytes| i32::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> Option<u64> {
        self.bytes(8).map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    /// A LEB128 unsigned integer of at most 10 bytes
    pub fn varuint(&mut self) -> Result<Option<u64>> {
        let (value, len) = take!(varuint(&self.data[self.pos..])?);
        self.pos += len;
        Ok(Some(value))
    }

    /// A string: its length as a varuint, then its bytes. A string longer
    /// than [`MAX_STEP`] cannot be parsed whole and fails.
    pub fn string(&mut self) -> Result<Option<&'a [u8]>> {
        let start = self.pos;
        let len = take!(self.varuint()?);
        ensure!(
            len <= MAX_STEP as u64,
            "ClickHouse sent a {len}-byte string where xylem takes at most {MAX_STEP} bytes"
        );
        match self.bytes(len as usize) {
            Some(bytes) => Ok(Some(bytes)),
            None => {
                self.pos = start;
                Ok(None)
            }
        }
    }
}

/// The LEB128 unsigned integer at the start of `data` and its length
pub fn varuint(data: &[u8]) -> Result<Option<(u64, usize)>> {
    let mut value = 0u64;
    for (i, &byte) in data.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            ensure!(i < 9 || byte <= 1, "ClickHouse sent a varint beyond 64 bits");
            return Ok(Some((value, i + 1)));
        }
    }
    ensure!(data.len() < 10, "ClickHouse sent a varint longer than 10 bytes");
    Ok(None)
}

pub fn put_varuint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub fn put_string(out: &mut Vec<u8>, value: &[u8]) {
    put_varuint(out, value.len() as u64);
    out.extend_from_slice(value);
}

/// xylem's version: major, minor, patch
fn client_version() -> [u64; 3] {
    [
        env!("CARGO_PKG_VERSION_MAJOR"),
        env!("CARGO_PKG_VERSION_MINOR"),
        env!("CARGO_PKG_VERSION_PATCH"),
    ]
    .map(|part| part.parse().expect("Cargo versions are numbers"))
}

/// The client Hello: name, version, [`REVISION`], database and credentials
pub fn hello(database: &str, user: &str, password: &str) -> Vec<u8> {
    let [major, minor, _] = client_version();
    let mut out = Vec::new();
    put_varuint(&mut out, client::HELLO);
    put_string(&mut out, CLIENT_NAME.as_bytes());
    put_varuint(&mut out, major);
    put_varuint(&mut out, minor);
    put_varuint(&mut out, REVISION);
    for field in [database, user, password] {
        put_string(&mut out, field.as_bytes());
    }
    out
}

/// A Query packet for `sql` at `revision` that the server processes to
/// completion without compression, with `settings` (name, value) marked
/// important, followed by the empty Data block that ends the external
/// tables (there are none)
pub fn query(revision: u64, sql: &str, settings: &[(String, String)]) -> Vec<u8> {
    let [major, minor, patch] = client_version();
    let mut out = Vec::with_capacity(64 + sql.len());
    put_varuint(&mut out, client::QUERY);
    put_string(&mut out, b""); // query ID: the server picks one
                               // Client info of an initial query from a TCP client
    out.push(1); // query kind: initial query
    put_string(&mut out, b""); // initial user
    put_string(&mut out, b""); // initial query ID
    put_string(&mut out, b"0.0.0.0:0"); // initial address: the server uses the peer's
    if revision >= WITH_INITIAL_QUERY_START_TIME {
        out.extend_from_slice(&0u64.to_le_bytes());
    }
    out.push(1); // interface: TCP
    put_string(&mut out, b""); // OS user
    put_string(&mut out, b""); // client host name
    put_string(&mut out, CLIENT_NAME.as_bytes());
    put_varuint(&mut out, major);
    put_varuint(&mut out, minor);
    put_varuint(&mut out, revision);
    put_string(&mut out, b""); // quota key
    if revision >= WITH_DISTRIBUTED_DEPTH {
        put_varuint(&mut out, 0);
    }
    put_varuint(&mut out, patch);
    if revision >= WITH_OPENTELEMETRY {
        out.push(0); // no trace context
    }
    if revision >= WITH_PARALLEL_REPLICAS {
        // Collaborate with initiator, replica count, replica number
        out.extend_from_slice(&[0, 0, 0]);
    }
    // Settings as strings with flags, ended by an empty name
    for (name, value) in settings {
        put_string(&mut out, name.as_bytes());
        put_varuint(&mut out, 1); // IMPORTANT: an unknown setting fails the query
        put_string(&mut out, value.as_bytes());
    }
    put_string(&mut out, b"");
    if revision >= WITH_INTERSERVER_SECRET {
        put_string(&mut out, b"");
    }
    put_varuint(&mut out, STAGE_COMPLETE);
    put_varuint(&mut out, 0); // compression off
    put_string(&mut out, sql.as_bytes());
    // The empty block that ends the external tables
    put_varuint(&mut out, client::DATA);
    put_string(&mut out, b"");
    put_block_info(&mut out);
    put_varuint(&mut out, 0); // columns
    put_varuint(&mut out, 0); // rows
    out
}

/// A default BlockInfo: not overflows, bucket -1
pub fn put_block_info(out: &mut Vec<u8>) {
    put_varuint(out, 1);
    out.push(0);
    put_varuint(out, 2);
    out.extend_from_slice(&(-1i32).to_le_bytes());
    put_varuint(out, 0);
}

/// What the server's Hello tells
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerHello {
    pub name: String,
    pub version: [u64; 3],
    pub revision: u64,
    pub timezone: String,
    pub display_name: String,
}

impl ServerHello {
    /// The revision both sides speak
    pub fn negotiated(&self) -> u64 {
        self.revision.min(REVISION)
    }

    /// Read a Hello after its packet type
    pub fn read(r: &mut Reader) -> Result<Option<Self>> {
        let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
        let name = text(take!(r.string()?));
        let major = take!(r.varuint()?);
        let minor = take!(r.varuint()?);
        let revision = take!(r.varuint()?);
        ensure!(
            revision >= MIN_REVISION,
            "ClickHouse server {name} {major}.{minor} speaks protocol revision {revision}; \
             xylem needs {MIN_REVISION} or later"
        );
        // Fields of revisions 54058 (time zone), 54372 (display name) and
        // 54401 (patch), all before MIN_REVISION
        let timezone = text(take!(r.string()?));
        let display_name = text(take!(r.string()?));
        let patch = take!(r.varuint()?);
        Ok(Some(Self {
            name,
            version: [major, minor, patch],
            revision,
            timezone,
            display_name,
        }))
    }
}

/// The first exception of an Exception packet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exception<'a> {
    pub code: i32,
    pub name: &'a [u8],
    pub message: &'a [u8],
}

impl<'a> Exception<'a> {
    /// Read an Exception packet after its type: each exception's code,
    /// name, message, stack trace and whether a nested one follows
    pub fn read(r: &mut Reader<'a>) -> Result<Option<Self>> {
        let mut first = None;
        loop {
            let code = take!(r.i32());
            let name = take!(r.string()?);
            let message = take!(r.string()?);
            take!(r.string()?); // stack trace
            first.get_or_insert(Self { code, name, message });
            match take!(r.u8()) {
                0 => return Ok(first),
                1 => {}
                other => bail!("ClickHouse Exception has nested flag {other}"),
            }
        }
    }
}

/// Read a Progress packet after its type: rows, bytes and total rows read,
/// rows and bytes written (revision 54420, before MIN_REVISION). Its later
/// fields come after REVISION.
pub fn skip_progress(r: &mut Reader) -> Result<Option<()>> {
    for _ in 0..5 {
        take!(r.varuint()?);
    }
    Ok(Some(()))
}

/// Read a ProfileInfo packet after its type: rows, blocks, bytes, whether a
/// limit applied, rows before the limit and an obsolete flag
pub fn skip_profile_info(r: &mut Reader) -> Result<Option<()>> {
    for _ in 0..3 {
        take!(r.varuint()?);
    }
    take!(r.u8());
    take!(r.varuint()?);
    take!(r.u8());
    Ok(Some(()))
}

/// Read a block's BlockInfo and dimensions: (columns, rows)
pub fn block_header(r: &mut Reader) -> Result<Option<(u64, u64)>> {
    loop {
        match take!(r.varuint()?) {
            0 => break,
            1 => {
                take!(r.u8()); // is_overflows
            }
            2 => {
                take!(r.i32()); // bucket_num
            }
            field => bail!("ClickHouse block has unknown BlockInfo field {field}"),
        }
    }
    let columns = take!(r.varuint()?);
    let rows = take!(r.varuint()?);
    Ok(Some((columns, rows)))
}
