//! HPACK header compression (RFC 7541)
//!
//! The decoder keeps the dynamic table a peer's encoder builds. The encoder
//! keeps no state: it sends each field as an index into the static table or
//! as a literal without indexing, so every request's header block is the
//! same bytes and can be built once.

mod huffman;

use anyhow::{anyhow, bail, ensure, Result};
use std::collections::VecDeque;

/// Default SETTINGS_HEADER_TABLE_SIZE, the dynamic table size xylem's
/// decoder allows: it never advertises another
pub const DEFAULT_TABLE_SIZE: usize = 4096;

/// Per-entry overhead of the dynamic table size (RFC 7541 section 4.1)
const ENTRY_OVERHEAD: usize = 32;

/// The static table (RFC 7541 Appendix A); entry `i` is index `i + 1`
pub(super) const STATIC_TABLE: [(&str, &str); 61] = [
    (":authority", ""),
    (":method", "GET"),
    (":method", "POST"),
    (":path", "/"),
    (":path", "/index.html"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
];

/// A field of the dynamic table
#[derive(Debug)]
struct Entry {
    name: Box<[u8]>,
    value: Box<[u8]>,
}

impl Entry {
    fn size(&self) -> usize {
        self.name.len() + self.value.len() + ENTRY_OVERHEAD
    }
}

/// An HPACK decoder: the dynamic table of one direction of a connection
#[derive(Debug)]
pub struct Decoder {
    /// Newest entry first
    table: VecDeque<Entry>,
    size: usize,
    /// Current maximum size, set by the encoder's size updates
    max_size: usize,
    /// Most the encoder may set: the SETTINGS_HEADER_TABLE_SIZE advertised
    limit: usize,
    /// Huffman-decoded name and value of the field being decoded
    name: Vec<u8>,
    value: Vec<u8>,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new(DEFAULT_TABLE_SIZE)
    }
}

impl Decoder {
    /// A decoder whose dynamic table holds at most `limit` bytes, the
    /// SETTINGS_HEADER_TABLE_SIZE its connection advertised
    pub fn new(limit: usize) -> Self {
        Self {
            table: VecDeque::new(),
            size: 0,
            max_size: limit,
            limit,
            name: Vec::new(),
            value: Vec::new(),
        }
    }

    /// Bytes the dynamic table holds, by RFC 7541's measure
    #[cfg(test)]
    pub fn table_size(&self) -> usize {
        self.size
    }

    /// Decode the header block `block`, calling `field` with each name and
    /// value in order
    ///
    /// Every header block of the connection must be decoded in order, also
    /// those whose fields are not needed, to keep the dynamic table in step
    /// with the encoder's. Any error is a connection error
    /// (COMPRESSION_ERROR): the table can no longer be trusted.
    pub fn decode(
        &mut self,
        block: &[u8],
        mut field: impl FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut pos = 0;
        // Size updates may only start a block
        let mut fields_seen = false;
        while pos < block.len() {
            let first = block[pos];
            if first & 0x80 != 0 {
                // Indexed field
                let index = decode_int(block, &mut pos, 7)?;
                ensure!(index != 0, "HPACK indexed field with index 0");
                let (name, value) = self.lookup(index)?;
                field(name, value)?;
                fields_seen = true;
            } else if first & 0xe0 == 0x20 {
                ensure!(!fields_seen, "HPACK dynamic table size update after a header field");
                let size = decode_int(block, &mut pos, 5)?;
                ensure!(
                    size <= self.limit,
                    "HPACK dynamic table size update to {size} exceeds the {} allowed",
                    self.limit
                );
                self.max_size = size;
                self.evict(0);
            } else {
                self.literal(block, &mut pos, &mut field)?;
                fields_seen = true;
            }
        }
        Ok(())
    }

    /// Decode the literal field at `pos` of `block`, pass it to `field`, and
    /// add it to the dynamic table if it is to be indexed
    fn literal(
        &mut self,
        block: &[u8],
        pos: &mut usize,
        field: &mut impl FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        // With incremental indexing (01), without indexing (0000) or never
        // indexed (0001)
        let indexing = block[*pos] & 0xc0 == 0x40;
        let index = decode_int(block, pos, if indexing { 6 } else { 4 })?;
        // Scratch buffers for Huffman strings and indexed names; an error
        // drops them, but it ends the connection anyway
        let mut name_buffer = std::mem::take(&mut self.name);
        let mut value_buffer = std::mem::take(&mut self.value);
        let name: &[u8] = if index == 0 {
            decode_string(block, pos, &mut name_buffer)?
        } else {
            name_buffer.clear();
            name_buffer.extend_from_slice(self.lookup(index)?.0);
            &name_buffer
        };
        let value = decode_string(block, pos, &mut value_buffer)?;
        field(name, value)?;
        let entry = indexing.then(|| Entry { name: name.into(), value: value.into() });
        self.name = name_buffer;
        self.value = value_buffer;
        if let Some(entry) = entry {
            self.insert(entry);
        }
        Ok(())
    }

    /// The field at `index` (1-based) of the static then the dynamic table
    fn lookup(&self, index: usize) -> Result<(&[u8], &[u8])> {
        if let Some(&(name, value)) = STATIC_TABLE.get(index - 1) {
            return Ok((name.as_bytes(), value.as_bytes()));
        }
        let entry = self
            .table
            .get(index - 1 - STATIC_TABLE.len())
            .ok_or_else(|| anyhow!("HPACK index {index} is past the dynamic table"))?;
        Ok((&entry.name, &entry.value))
    }

    fn insert(&mut self, entry: Entry) {
        let size = entry.size();
        if size > self.max_size {
            // An entry larger than the table empties it (RFC 7541 section 4.4)
            self.table.clear();
            self.size = 0;
            return;
        }
        self.evict(size);
        self.size += size;
        self.table.push_front(entry);
    }

    /// Evict the oldest entries until `room` more bytes fit
    fn evict(&mut self, room: usize) {
        while self.size + room > self.max_size {
            let entry = self.table.pop_back().expect("a table over its size has entries");
            self.size -= entry.size();
        }
    }
}

/// Decode the integer with an `prefix`-bit prefix at `pos` of `data`
/// (RFC 7541 section 5.1), moving `pos` past it
fn decode_int(data: &[u8], pos: &mut usize, prefix: u32) -> Result<usize> {
    let max = (1usize << prefix) - 1;
    let mut value = usize::from(data[*pos]) & max;
    *pos += 1;
    if value < max {
        return Ok(value);
    }
    let mut shift = 0;
    loop {
        let byte = *data.get(*pos).ok_or_else(|| anyhow!("HPACK integer is cut short"))?;
        *pos += 1;
        ensure!(shift <= 28, "HPACK integer overflows");
        value += usize::from(byte & 0x7f) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
}

/// Decode the string literal at `pos` of `data` (RFC 7541 section 5.2),
/// moving `pos` past it: a raw string borrows from `data`, a Huffman one is
/// decoded into `buffer`
fn decode_string<'a>(data: &'a [u8], pos: &mut usize, buffer: &'a mut Vec<u8>) -> Result<&'a [u8]> {
    let Some(&first) = data.get(*pos) else {
        bail!("HPACK string literal is missing");
    };
    let len = decode_int(data, pos, 7)?;
    let end = pos.checked_add(len).filter(|&end| end <= data.len());
    let end = end.ok_or_else(|| anyhow!("HPACK string literal is cut short"))?;
    let raw = &data[*pos..end];
    *pos = end;
    if first & 0x80 == 0 {
        return Ok(raw);
    }
    buffer.clear();
    huffman::decode(raw, buffer)?;
    Ok(buffer)
}

/// Append the integer `value` with an `prefix`-bit prefix whose other bits
/// in the first byte are `flags`
fn encode_int(out: &mut Vec<u8>, flags: u8, prefix: u32, value: usize) {
    let max = (1usize << prefix) - 1;
    if value < max {
        out.push(flags | value as u8);
        return;
    }
    out.push(flags | max as u8);
    let mut rest = value - max;
    while rest >= 0x80 {
        out.push(0x80 | (rest & 0x7f) as u8);
        rest >>= 7;
    }
    out.push(rest as u8);
}

/// Append a raw (not Huffman-coded) string literal
fn encode_string(out: &mut Vec<u8>, data: &[u8]) {
    encode_int(out, 0, 7, data.len());
    out.extend_from_slice(data);
}

/// Append the header field `name: value`: an index for a field of the
/// static table, else a literal without indexing, naming a static table
/// name by its index
///
/// `name` must be lowercase, as HTTP/2 requires.
pub fn encode_field(out: &mut Vec<u8>, name: &str, value: &str) {
    let mut name_index = None;
    for (i, &(table_name, table_value)) in STATIC_TABLE.iter().enumerate() {
        if table_name != name {
            continue;
        }
        if table_value == value {
            encode_int(out, 0x80, 7, i + 1);
            return;
        }
        name_index.get_or_insert(i + 1);
    }
    match name_index {
        Some(index) => encode_int(out, 0x00, 4, index),
        None => {
            out.push(0x00);
            encode_string(out, name.as_bytes());
        }
    }
    encode_string(out, value.as_bytes());
}

#[cfg(test)]
mod tests;
