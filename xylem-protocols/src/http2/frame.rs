//! HTTP/2 frame layout (RFC 9113 section 4 and 6)

/// The client connection preface, sent before the client's SETTINGS
pub const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Length of a frame header
pub const HEADER_LEN: usize = 9;

/// Smallest and default SETTINGS_MAX_FRAME_SIZE, the largest frame payload
/// xylem accepts: it never advertises more
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 16_384;

/// Largest SETTINGS_MAX_FRAME_SIZE a peer may set
pub const MAX_MAX_FRAME_SIZE: u32 = (1 << 24) - 1;

/// Initial flow-control window of every stream and of the connection until
/// SETTINGS or WINDOW_UPDATE change it
pub const DEFAULT_WINDOW: u32 = 65_535;

/// Largest flow-control window
pub const MAX_WINDOW: u32 = (1 << 31) - 1;

/// Largest stream identifier
pub const MAX_STREAM_ID: u32 = (1 << 31) - 1;

pub const TYPE_DATA: u8 = 0x0;
pub const TYPE_HEADERS: u8 = 0x1;
pub const TYPE_RST_STREAM: u8 = 0x3;
pub const TYPE_SETTINGS: u8 = 0x4;
pub const TYPE_PUSH_PROMISE: u8 = 0x5;
pub const TYPE_PING: u8 = 0x6;
pub const TYPE_GOAWAY: u8 = 0x7;
pub const TYPE_WINDOW_UPDATE: u8 = 0x8;
pub const TYPE_CONTINUATION: u8 = 0x9;

pub const FLAG_END_STREAM: u8 = 0x1;
/// The ACK flag of SETTINGS and PING
pub const FLAG_ACK: u8 = 0x1;
pub const FLAG_END_HEADERS: u8 = 0x4;
pub const FLAG_PADDED: u8 = 0x8;
pub const FLAG_PRIORITY: u8 = 0x20;

pub const SETTINGS_ENABLE_PUSH: u16 = 0x2;
pub const SETTINGS_MAX_CONCURRENT_STREAMS: u16 = 0x3;
pub const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 0x4;
pub const SETTINGS_MAX_FRAME_SIZE: u16 = 0x5;

pub const NO_ERROR: u32 = 0x0;

/// A frame header
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub len: usize,
    pub kind: u8,
    pub flags: u8,
    pub stream: u32,
}

impl FrameHeader {
    /// The header at the start of `data`, which holds at least
    /// [`HEADER_LEN`] bytes; the stream identifier's reserved bit is ignored
    pub fn parse(data: &[u8]) -> Self {
        Self {
            len: usize::from(data[0]) << 16 | usize::from(data[1]) << 8 | usize::from(data[2]),
            kind: data[3],
            flags: data[4],
            stream: u32_at(data, 5) & MAX_STREAM_ID,
        }
    }

    pub fn has(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }
}

/// The big-endian `u32` at `offset` of `data`
pub fn u32_at(data: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(data[offset..offset + 4].try_into().expect("four bytes"))
}

/// Append a frame header for a payload of `len` bytes
pub fn write_header(out: &mut Vec<u8>, len: usize, kind: u8, flags: u8, stream: u32) {
    debug_assert!(len <= MAX_MAX_FRAME_SIZE as usize);
    out.extend_from_slice(&(len as u32).to_be_bytes()[1..]);
    out.push(kind);
    out.push(flags);
    out.extend_from_slice(&stream.to_be_bytes());
}

/// Append a SETTINGS frame with `settings`
pub fn write_settings(out: &mut Vec<u8>, settings: &[(u16, u32)]) {
    write_header(out, settings.len() * 6, TYPE_SETTINGS, 0, 0);
    for &(id, value) in settings {
        out.extend_from_slice(&id.to_be_bytes());
        out.extend_from_slice(&value.to_be_bytes());
    }
}

/// Append a SETTINGS acknowledgement
pub fn write_settings_ack(out: &mut Vec<u8>) {
    write_header(out, 0, TYPE_SETTINGS, FLAG_ACK, 0);
}

/// Append a PING acknowledgement echoing `payload`
pub fn write_ping_ack(out: &mut Vec<u8>, payload: &[u8; 8]) {
    write_header(out, 8, TYPE_PING, FLAG_ACK, 0);
    out.extend_from_slice(payload);
}

/// Append a WINDOW_UPDATE of `increment` for `stream` (0: the connection)
pub fn write_window_update(out: &mut Vec<u8>, stream: u32, increment: u32) {
    debug_assert!((1..=MAX_WINDOW).contains(&increment));
    write_header(out, 4, TYPE_WINDOW_UPDATE, 0, stream);
    out.extend_from_slice(&increment.to_be_bytes());
}

/// Append a RST_STREAM of `stream` with `code`
pub fn write_rst_stream(out: &mut Vec<u8>, stream: u32, code: u32) {
    write_header(out, 4, TYPE_RST_STREAM, 0, stream);
    out.extend_from_slice(&code.to_be_bytes());
}

/// Append the header block `block` for `stream` as a HEADERS frame and as
/// many CONTINUATION frames as frames of `max_frame` bytes need
pub fn write_headers(
    out: &mut Vec<u8>,
    stream: u32,
    block: &[u8],
    end_stream: bool,
    max_frame: usize,
) {
    let mut chunks = block.chunks(max_frame).peekable();
    let first = chunks.next().unwrap_or_default();
    let mut flags = if end_stream { FLAG_END_STREAM } else { 0 };
    if chunks.peek().is_none() {
        flags |= FLAG_END_HEADERS;
    }
    write_header(out, first.len(), TYPE_HEADERS, flags, stream);
    out.extend_from_slice(first);
    while let Some(chunk) = chunks.next() {
        let flags = if chunks.peek().is_none() {
            FLAG_END_HEADERS
        } else {
            0
        };
        write_header(out, chunk.len(), TYPE_CONTINUATION, flags, stream);
        out.extend_from_slice(chunk);
    }
}

/// Name of an error code (RFC 9113 section 7), for error replies and errors
pub fn error_name(code: u32) -> &'static str {
    match code {
        0x0 => "NO_ERROR",
        0x1 => "PROTOCOL_ERROR",
        0x2 => "INTERNAL_ERROR",
        0x3 => "FLOW_CONTROL_ERROR",
        0x4 => "SETTINGS_TIMEOUT",
        0x5 => "STREAM_CLOSED",
        0x6 => "FRAME_SIZE_ERROR",
        0x7 => "REFUSED_STREAM",
        0x8 => "CANCEL",
        0x9 => "COMPRESSION_ERROR",
        0xa => "CONNECT_ERROR",
        0xb => "ENHANCE_YOUR_CALM",
        0xc => "INADEQUATE_SECURITY",
        0xd => "HTTP_1_1_REQUIRED",
        _ => "UNKNOWN_ERROR",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips_and_ignores_the_reserved_bit() {
        let mut out = Vec::new();
        write_header(&mut out, 0x01_02_03, TYPE_HEADERS, FLAG_END_HEADERS, 7);
        assert_eq!(out, [1, 2, 3, 1, 4, 0, 0, 0, 7]);
        out[5] |= 0x80;
        assert_eq!(
            FrameHeader::parse(&out),
            FrameHeader {
                len: 0x01_02_03,
                kind: TYPE_HEADERS,
                flags: FLAG_END_HEADERS,
                stream: 7
            }
        );
    }

    #[test]
    fn control_frames_have_their_wire_layout() {
        let mut out = Vec::new();
        write_settings(
            &mut out,
            &[(SETTINGS_ENABLE_PUSH, 0), (SETTINGS_INITIAL_WINDOW_SIZE, 1 << 20)],
        );
        assert_eq!(out, [0, 0, 12, 4, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 4, 0, 0x10, 0, 0]);
        out.clear();
        write_settings_ack(&mut out);
        assert_eq!(out, [0, 0, 0, 4, 1, 0, 0, 0, 0]);
        out.clear();
        write_ping_ack(&mut out, b"12345678");
        assert_eq!(out, [&[0, 0, 8, 6, 1, 0, 0, 0, 0][..], b"12345678"].concat());
        out.clear();
        write_window_update(&mut out, 3, 0x7fff_ffff);
        assert_eq!(out, [0, 0, 4, 8, 0, 0, 0, 0, 3, 0x7f, 0xff, 0xff, 0xff]);
        out.clear();
        write_rst_stream(&mut out, 5, 8);
        assert_eq!(out, [0, 0, 4, 3, 0, 0, 0, 0, 5, 0, 0, 0, 8]);
    }

    #[test]
    fn long_header_blocks_continue_in_continuation_frames() {
        let block: Vec<u8> = (0..10).collect();
        let mut out = Vec::new();
        write_headers(&mut out, 1, &block, true, 4);
        let frames: Vec<_> = {
            let mut frames = Vec::new();
            let mut rest = &out[..];
            while !rest.is_empty() {
                let header = FrameHeader::parse(rest);
                frames.push((header, rest[HEADER_LEN..HEADER_LEN + header.len].to_vec()));
                rest = &rest[HEADER_LEN + header.len..];
            }
            frames
        };
        let kinds: Vec<_> = frames.iter().map(|(h, _)| (h.kind, h.flags, h.stream)).collect();
        assert_eq!(
            kinds,
            [
                (TYPE_HEADERS, FLAG_END_STREAM, 1),
                (TYPE_CONTINUATION, 0, 1),
                (TYPE_CONTINUATION, FLAG_END_HEADERS, 1)
            ]
        );
        assert_eq!(frames.iter().flat_map(|(_, p)| p.clone()).collect::<Vec<_>>(), block);
        out.clear();
        write_headers(&mut out, 3, &block, false, 16);
        assert_eq!(FrameHeader::parse(&out).flags, FLAG_END_HEADERS);
        assert_eq!(out.len(), HEADER_LEN + block.len());
    }
}
