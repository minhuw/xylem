//! HTTP/2 over cleartext with prior knowledge (h2c, RFC 9113), and gRPC
//! unary calls on top of it
//!
//! # Connections
//!
//! Each connection's first request is a setup request: the connection
//! preface, SETTINGS (no server push, a [`STREAM_WINDOW`]-byte stream
//! window) and a WINDOW_UPDATE that opens the connection window to
//! [`CONNECTION_WINDOW`] bytes. The server's SETTINGS is its reply. SETTINGS
//! and PING from the server are acknowledged through [`Outbox::data`].
//!
//! # Streams
//!
//! Every request is a stream whose ID is the request ID, so up to
//! `max_pending_per_connection` streams are in flight on a connection, and
//! never more than the server's SETTINGS_MAX_CONCURRENT_STREAMS
//! ([`Protocol::can_send`]). Frames of different streams may interleave;
//! the parser takes one frame per call ([`Parsed::Absorbed`]) and completes
//! a stream at its END_STREAM. Header blocks may continue in CONTINUATION
//! frames and frames may be padded.
//!
//! Header blocks are decoded with HPACK, keeping the server's dynamic
//! table; requests use static table indices and literals without indexing
//! only, so their header block is built once.
//!
//! # Flow control
//!
//! Received DATA is discarded as it arrives, and a WINDOW_UPDATE gives its
//! window back once half of a window is used: the connection's, and a
//! stream's that has not ended. Request bodies respect the server's
//! connection and stream windows and its SETTINGS_MAX_FRAME_SIZE: what does
//! not fit waits for the server's WINDOW_UPDATE and then goes out through
//! the outbox.
//!
//! # Errors
//!
//! A response whose `:status` is not 200 is an error reply whose kind is
//! the status (`"503"`); for gRPC, so is a `grpc-status` other than 0, from
//! the trailers or a trailers-only response, with the code as kind
//! (`"14"`). A gRPC response without `grpc-status` fails the run. An
//! RST_STREAM ends its stream as an error reply named after its error code
//! (`"REFUSED_STREAM"`).
//!
//! A GOAWAY with NO_ERROR stops new streams on its connection, while the
//! streams it accepted complete; xylem does not reconnect. One with another
//! error code, or one that refuses streams in flight, fails the run, as do
//! connection errors of any kind (malformed frames, flow-control
//! violations, HPACK errors).

mod config;
mod frame;
mod hpack;
mod request;

pub use config::{GrpcConfig, Http2Config};

use crate::http::STATUS_DIGITS;
use anyhow::{anyhow, bail, ensure, Result};
use frame::FrameHeader;
use std::collections::VecDeque;
use xylem_common::{ErrorReply, FastHashMap, Outbox, Parsed, Protocol, Request};

/// Request ID: the connection and the stream ID, 0 for the connection's
/// setup request
pub type Http2RequestId = (usize, u32);

/// SETTINGS_INITIAL_WINDOW_SIZE xylem advertises: the receive window of
/// each stream
pub const STREAM_WINDOW: u32 = 1 << 20;

/// Receive window of each connection
pub const CONNECTION_WINDOW: u32 = 1 << 24;

/// What a response must carry to be a success
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Exchange {
    /// `:status` 200
    Http,
    /// `:status` 200 and `grpc-status` 0
    Grpc,
}

/// Where a connection is in its setup
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// The preface went out; waiting for the server's SETTINGS
    AwaitSettings,
    Open,
}

/// A stream in flight
#[derive(Debug)]
struct Stream {
    /// `:status` of the final response, once its headers arrived
    status: Option<u16>,
    grpc_status: Option<u32>,
    /// Bytes the server may still send on the stream
    recv_window: i64,
    /// Bytes received since the stream's last WINDOW_UPDATE
    recv_unacked: u32,
    /// Bytes xylem may still send on the stream
    send_window: i64,
    /// The request body (an index into the protocol's bodies) and how many
    /// of its bytes went out
    body: usize,
    sent: usize,
}

/// A header block that continues in CONTINUATION frames
#[derive(Debug)]
struct Continuation {
    stream: u32,
    end_stream: bool,
    block: Vec<u8>,
}

/// One connection's session
#[derive(Debug)]
struct Connection {
    phase: Phase,
    next_stream: u32,
    streams: FastHashMap<u32, Stream>,
    /// Streams with body bytes waiting for window, oldest first
    blocked: VecDeque<u32>,
    peer_max_concurrent: u32,
    peer_initial_window: i64,
    peer_max_frame: usize,
    send_window: i64,
    recv_window: i64,
    recv_unacked: u32,
    decoder: hpack::Decoder,
    continuation: Option<Continuation>,
    /// The last stream ID of the server's GOAWAY
    goaway: Option<u32>,
    /// Frames to send, taken by `drain_outbox`
    tx: Vec<u8>,
}

impl Connection {
    /// A connection whose preface went out, with a receive window of
    /// `recv_window` bytes once the server takes its WINDOW_UPDATE
    fn new(recv_window: u32) -> Self {
        Self {
            phase: Phase::AwaitSettings,
            next_stream: 1,
            streams: FastHashMap::default(),
            blocked: VecDeque::new(),
            peer_max_concurrent: u32::MAX,
            peer_initial_window: i64::from(frame::DEFAULT_WINDOW),
            peer_max_frame: frame::DEFAULT_MAX_FRAME_SIZE as usize,
            send_window: i64::from(frame::DEFAULT_WINDOW),
            recv_window: i64::from(recv_window),
            recv_unacked: 0,
            decoder: hpack::Decoder::default(),
            continuation: None,
            goaway: None,
            tx: Vec::new(),
        }
    }
}

/// HTTP/2 or gRPC protocol; see the module documentation
pub struct Http2Protocol {
    exchange: Exchange,
    /// HPACK header block of every request
    headers: Vec<u8>,
    /// Request bodies, sent in turn; one empty body for a request without
    bodies: Vec<Vec<u8>>,
    next_body: usize,
    stream_window: u32,
    connection_window: u32,
    connections: FastHashMap<usize, Connection>,
}

/// Name of a gRPC status code (0 to 16), its kind as an error reply
const GRPC_CODES: [(&str, &str); 17] = [
    ("0", "OK"),
    ("1", "CANCELLED"),
    ("2", "UNKNOWN"),
    ("3", "INVALID_ARGUMENT"),
    ("4", "DEADLINE_EXCEEDED"),
    ("5", "NOT_FOUND"),
    ("6", "ALREADY_EXISTS"),
    ("7", "PERMISSION_DENIED"),
    ("8", "RESOURCE_EXHAUSTED"),
    ("9", "FAILED_PRECONDITION"),
    ("10", "ABORTED"),
    ("11", "OUT_OF_RANGE"),
    ("12", "UNIMPLEMENTED"),
    ("13", "INTERNAL"),
    ("14", "UNAVAILABLE"),
    ("15", "DATA_LOSS"),
    ("16", "UNAUTHENTICATED"),
];

impl Http2Protocol {
    fn new(exchange: Exchange, headers: Vec<u8>, bodies: Vec<Vec<u8>>) -> Self {
        assert!(!bodies.is_empty(), "a request has a body, if an empty one");
        Self {
            exchange,
            headers,
            bodies,
            next_body: 0,
            stream_window: STREAM_WINDOW,
            connection_window: CONNECTION_WINDOW,
            connections: FastHashMap::default(),
        }
    }

    /// Use other receive windows (for tests of flow control)
    #[cfg(test)]
    fn with_windows(mut self, stream: u32, connection: u32) -> Self {
        assert!(stream > 0 && connection >= frame::DEFAULT_WINDOW);
        self.stream_window = stream;
        self.connection_window = connection;
        self
    }

    /// The setup request: preface, SETTINGS, and the connection window
    fn preface(&self) -> Vec<u8> {
        let mut data = frame::PREFACE.to_vec();
        frame::write_settings(
            &mut data,
            &[
                (frame::SETTINGS_ENABLE_PUSH, 0),
                (frame::SETTINGS_INITIAL_WINDOW_SIZE, self.stream_window),
            ],
        );
        let increment = self.connection_window - frame::DEFAULT_WINDOW;
        if increment > 0 {
            frame::write_window_update(&mut data, 0, increment);
        }
        data
    }

    fn connection(&mut self, conn_id: usize) -> Result<&mut Connection> {
        self.connections
            .get_mut(&conn_id)
            .ok_or_else(|| anyhow!("HTTP/2 data on connection {conn_id}, which sent no preface"))
    }

    /// Streams the protocol tracks on `conn_id` (for tests)
    #[cfg(test)]
    fn open_streams(&self, conn_id: usize) -> usize {
        self.connections[&conn_id].streams.len()
    }

    /// Parse the frame of `header` whose payload is `payload`
    fn parse_frame<'a>(
        &mut self,
        conn_id: usize,
        header: FrameHeader,
        payload: &[u8],
        consumed: usize,
    ) -> Result<Parsed<'a, Http2RequestId>> {
        let exchange = self.exchange;
        let (stream_window, connection_window) = (self.stream_window, self.connection_window);
        let bodies = &self.bodies;
        let conn = self.connections.get_mut(&conn_id).expect("checked by the caller");
        if let Some(continuation) = &conn.continuation {
            ensure!(
                header.kind == frame::TYPE_CONTINUATION && header.stream == continuation.stream,
                "HTTP/2 frame type {:#04x} on stream {} interrupts the header block of stream {}",
                header.kind,
                header.stream,
                continuation.stream
            );
        }
        if conn.phase == Phase::AwaitSettings {
            ensure!(
                header.kind == frame::TYPE_SETTINGS && !header.has(frame::FLAG_ACK),
                "The HTTP/2 server did not start with SETTINGS (frame type {:#04x})",
                header.kind
            );
        }
        let absorbed = Parsed::Absorbed(consumed);
        let control = |kind: &str| -> Result<()> {
            ensure!(header.stream == 0, "HTTP/2 {kind} on stream {}", header.stream);
            Ok(())
        };
        match header.kind {
            frame::TYPE_SETTINGS => {
                control("SETTINGS")?;
                if header.has(frame::FLAG_ACK) {
                    ensure!(payload.is_empty(), "HTTP/2 SETTINGS acknowledgement with a payload");
                    return Ok(absorbed);
                }
                apply_settings(conn, payload, bodies)?;
                frame::write_settings_ack(&mut conn.tx);
                if conn.phase == Phase::AwaitSettings {
                    conn.phase = Phase::Open;
                    return Ok(Parsed::ok(consumed, (conn_id, 0)));
                }
                Ok(absorbed)
            }
            frame::TYPE_PING => {
                control("PING")?;
                let payload: &[u8; 8] = payload
                    .try_into()
                    .map_err(|_| anyhow!("HTTP/2 PING of {} bytes", payload.len()))?;
                if !header.has(frame::FLAG_ACK) {
                    frame::write_ping_ack(&mut conn.tx, payload);
                }
                Ok(absorbed)
            }
            frame::TYPE_GOAWAY => {
                control("GOAWAY")?;
                ensure!(payload.len() >= 8, "HTTP/2 GOAWAY of {} bytes", payload.len());
                let last = frame::u32_at(payload, 0) & frame::MAX_STREAM_ID;
                let code = frame::u32_at(payload, 4);
                let debug = String::from_utf8_lossy(&payload[8..]);
                ensure!(
                    code == frame::NO_ERROR,
                    "The HTTP/2 server sent GOAWAY {} ({code:#x}): {debug:?}",
                    frame::error_name(code)
                );
                let refused = conn.streams.keys().filter(|&&id| id > last).count();
                ensure!(
                    refused == 0,
                    "The HTTP/2 server sent GOAWAY refusing {refused} streams in flight on \
                     connection {conn_id}, and xylem does not reconnect: {debug:?}"
                );
                conn.goaway = Some(conn.goaway.map_or(last, |old| old.min(last)));
                Ok(absorbed)
            }
            frame::TYPE_WINDOW_UPDATE => {
                ensure!(payload.len() == 4, "HTTP/2 WINDOW_UPDATE of {} bytes", payload.len());
                let increment = i64::from(frame::u32_at(payload, 0) & frame::MAX_WINDOW);
                ensure!(increment > 0, "HTTP/2 WINDOW_UPDATE of 0 on stream {}", header.stream);
                let window = if header.stream == 0 {
                    &mut conn.send_window
                } else {
                    match conn.streams.get_mut(&header.stream) {
                        Some(stream) => &mut stream.send_window,
                        // A stream may end while an update for it is on its way
                        None => return Ok(absorbed),
                    }
                };
                *window += increment;
                ensure!(
                    *window <= i64::from(frame::MAX_WINDOW),
                    "HTTP/2 WINDOW_UPDATE overflows the window of stream {}",
                    header.stream
                );
                release_blocked(conn, bodies);
                Ok(absorbed)
            }
            frame::TYPE_RST_STREAM => {
                ensure!(header.stream != 0, "HTTP/2 RST_STREAM on stream 0");
                ensure!(payload.len() == 4, "HTTP/2 RST_STREAM of {} bytes", payload.len());
                let code = frame::u32_at(payload, 0);
                if conn.streams.remove(&header.stream).is_none() {
                    ensure_opened(conn, header.stream, "RST_STREAM")?;
                    return Ok(absorbed);
                }
                conn.blocked.retain(|&id| id != header.stream);
                let name = frame::error_name(code);
                Ok(Parsed::error(consumed, (conn_id, header.stream), ErrorReply::new(name, name)))
            }
            frame::TYPE_DATA => {
                unpad(header, payload)?;
                let len = payload.len() as u32;
                conn.recv_window -= i64::from(len);
                ensure!(conn.recv_window >= 0, "The HTTP/2 server overran the connection window");
                conn.recv_unacked += len;
                if conn.recv_unacked >= connection_window / 2 {
                    frame::write_window_update(&mut conn.tx, 0, conn.recv_unacked);
                    conn.recv_window += i64::from(conn.recv_unacked);
                    conn.recv_unacked = 0;
                }
                let id = header.stream;
                let Some(stream) = conn.streams.get_mut(&id) else {
                    bail!("HTTP/2 DATA on stream {id}, which is not open");
                };
                ensure!(stream.status.is_some(), "HTTP/2 DATA on stream {id} before its headers");
                stream.recv_window -= i64::from(len);
                ensure!(stream.recv_window >= 0, "The HTTP/2 server overran stream {id}'s window");
                if header.has(frame::FLAG_END_STREAM) {
                    return complete(conn, conn_id, id, consumed, exchange);
                }
                stream.recv_unacked += len;
                if stream.recv_unacked >= stream_window / 2 {
                    frame::write_window_update(&mut conn.tx, id, stream.recv_unacked);
                    stream.recv_window += i64::from(stream.recv_unacked);
                    stream.recv_unacked = 0;
                }
                Ok(absorbed)
            }
            frame::TYPE_HEADERS => {
                let mut fragment = unpad(header, payload)?;
                if header.has(frame::FLAG_PRIORITY) {
                    fragment = fragment
                        .get(5..)
                        .ok_or_else(|| anyhow!("HTTP/2 HEADERS too short for its priority"))?;
                }
                let end_stream = header.has(frame::FLAG_END_STREAM);
                if !header.has(frame::FLAG_END_HEADERS) {
                    conn.continuation = Some(Continuation {
                        stream: header.stream,
                        end_stream,
                        block: fragment.to_vec(),
                    });
                    return Ok(absorbed);
                }
                header_block(conn, conn_id, header.stream, end_stream, fragment, consumed, exchange)
            }
            frame::TYPE_CONTINUATION => {
                let Some(continuation) = &mut conn.continuation else {
                    bail!("HTTP/2 CONTINUATION on stream {} without a header block", header.stream);
                };
                continuation.block.extend_from_slice(payload);
                if !header.has(frame::FLAG_END_HEADERS) {
                    return Ok(absorbed);
                }
                let continuation = conn.continuation.take().expect("checked above");
                let Continuation { stream, end_stream, block } = continuation;
                header_block(conn, conn_id, stream, end_stream, &block, consumed, exchange)
            }
            frame::TYPE_PUSH_PROMISE => bail!("HTTP/2 PUSH_PROMISE, but xylem disabled push"),
            // PRIORITY, and unknown frame types, which must be ignored
            _ => Ok(absorbed),
        }
    }
}

/// Apply the server's SETTINGS `payload`
fn apply_settings(conn: &mut Connection, payload: &[u8], bodies: &[Vec<u8>]) -> Result<()> {
    ensure!(payload.len().is_multiple_of(6), "HTTP/2 SETTINGS of {} bytes", payload.len());
    for setting in payload.as_chunks::<6>().0 {
        let id = u16::from_be_bytes([setting[0], setting[1]]);
        let value = frame::u32_at(setting, 2);
        match id {
            frame::SETTINGS_ENABLE_PUSH => {
                ensure!(value <= 1, "HTTP/2 SETTINGS_ENABLE_PUSH of {value}");
            }
            frame::SETTINGS_MAX_CONCURRENT_STREAMS => conn.peer_max_concurrent = value,
            frame::SETTINGS_INITIAL_WINDOW_SIZE => {
                ensure!(
                    value <= frame::MAX_WINDOW,
                    "HTTP/2 SETTINGS_INITIAL_WINDOW_SIZE of {value} exceeds the largest window"
                );
                // The change applies to the windows of the open streams too
                let delta = i64::from(value) - conn.peer_initial_window;
                conn.peer_initial_window = i64::from(value);
                for stream in conn.streams.values_mut() {
                    stream.send_window += delta;
                    ensure!(
                        stream.send_window <= i64::from(frame::MAX_WINDOW),
                        "HTTP/2 SETTINGS_INITIAL_WINDOW_SIZE overflows a stream window"
                    );
                }
            }
            frame::SETTINGS_MAX_FRAME_SIZE => {
                ensure!(
                    (frame::DEFAULT_MAX_FRAME_SIZE..=frame::MAX_MAX_FRAME_SIZE).contains(&value),
                    "HTTP/2 SETTINGS_MAX_FRAME_SIZE of {value}"
                );
                conn.peer_max_frame = value as usize;
            }
            // The header table size bounds an encoder xylem does not use;
            // unknown settings must be ignored
            _ => {}
        }
    }
    release_blocked(conn, bodies);
    Ok(())
}

/// The data of a DATA or HEADERS frame, without its padding
fn unpad(header: FrameHeader, payload: &[u8]) -> Result<&[u8]> {
    if header.stream == 0 || header.stream.is_multiple_of(2) {
        bail!("HTTP/2 frame type {:#04x} on stream {}", header.kind, header.stream);
    }
    if !header.has(frame::FLAG_PADDED) {
        return Ok(payload);
    }
    let (&pad, rest) = payload
        .split_first()
        .ok_or_else(|| anyhow!("HTTP/2 padded frame without padding"))?;
    ensure!(usize::from(pad) <= rest.len(), "HTTP/2 padding exceeds its frame");
    Ok(&rest[..rest.len() - usize::from(pad)])
}

/// Fail unless `stream` is one xylem opened: frames for streams that ended
/// may still arrive, but no stream starts at the server
fn ensure_opened(conn: &Connection, stream: u32, kind: &str) -> Result<()> {
    ensure!(
        !stream.is_multiple_of(2) && stream < conn.next_stream,
        "HTTP/2 {kind} on stream {stream}, which xylem never opened"
    );
    Ok(())
}

/// Decode the header block `block` of `id`, which ends the stream if
/// `end_stream`
fn header_block<'a>(
    conn: &mut Connection,
    conn_id: usize,
    id: u32,
    end_stream: bool,
    block: &[u8],
    consumed: usize,
    exchange: Exchange,
) -> Result<Parsed<'a, Http2RequestId>> {
    let mut status = None;
    let mut grpc_status = None;
    // Decode first: the dynamic table must follow every block
    conn.decoder.decode(block, |name, value| {
        let number = || -> Result<u32> {
            let text = std::str::from_utf8(value).ok().filter(|text| {
                !text.is_empty() && text.len() <= 9 && text.bytes().all(|b| b.is_ascii_digit())
            });
            let text = text.ok_or_else(|| {
                anyhow!(
                    "HTTP/2 {} of {:?} on stream {id}",
                    String::from_utf8_lossy(name),
                    String::from_utf8_lossy(value)
                )
            })?;
            Ok(text.parse().expect("nine digits fit"))
        };
        match name {
            b":status" => status = Some(number()?),
            b"grpc-status" => grpc_status = Some(number()?),
            _ => {}
        }
        Ok(())
    })?;
    let Some(stream) = conn.streams.get_mut(&id) else {
        ensure_opened(conn, id, "HEADERS")?;
        bail!("HTTP/2 HEADERS on stream {id}, which ended");
    };
    if stream.status.is_some() {
        ensure!(status.is_none(), "HTTP/2 trailers on stream {id} carry :status");
        ensure!(end_stream, "HTTP/2 trailers on stream {id} do not end it");
    } else {
        let status =
            status.ok_or_else(|| anyhow!("HTTP/2 response on stream {id} has no :status"))?;
        ensure!((100..600).contains(&status), "HTTP/2 :status {status} on stream {id}");
        ensure!(status != 101, "HTTP/2 :status 101 on stream {id}");
        if status < 200 {
            // Informational: the final response follows
            ensure!(!end_stream, "HTTP/2 informational response ends stream {id}");
            return Ok(Parsed::Absorbed(consumed));
        }
        stream.status = Some(status as u16);
    }
    if grpc_status.is_some() {
        stream.grpc_status = grpc_status;
    }
    if end_stream {
        return complete(conn, conn_id, id, consumed, exchange);
    }
    Ok(Parsed::Absorbed(consumed))
}

/// Complete stream `id`, whose END_STREAM came in the last `consumed` bytes
fn complete<'a>(
    conn: &mut Connection,
    conn_id: usize,
    id: u32,
    consumed: usize,
    exchange: Exchange,
) -> Result<Parsed<'a, Http2RequestId>> {
    let stream = conn.streams.remove(&id).expect("the caller found it");
    if conn.blocked.contains(&id) {
        // The server answered before reading the whole body: stop sending it
        conn.blocked.retain(|&blocked| blocked != id);
        frame::write_rst_stream(&mut conn.tx, id, frame::NO_ERROR);
    }
    let status = stream.status.expect("a stream ends after its headers");
    let request = (conn_id, id);
    if status != 200 {
        let kind =
            std::str::from_utf8(&STATUS_DIGITS[usize::from(status)]).expect("digits are UTF-8");
        return Ok(Parsed::error(consumed, request, ErrorReply::new(kind, kind)));
    }
    if exchange == Exchange::Grpc {
        let code = stream
            .grpc_status
            .ok_or_else(|| anyhow!("gRPC response on stream {id} ended without grpc-status"))?;
        if code != 0 {
            let (kind, name) =
                GRPC_CODES.get(code as usize).copied().unwrap_or(("unknown", "unknown"));
            return Ok(Parsed::error(consumed, request, ErrorReply::new(kind, name)));
        }
    }
    Ok(Parsed::ok(consumed, request))
}

/// Append to `out` as much of `stream`'s body as the windows and the
/// largest frame allow; returns whether all of it went out
fn send_body(
    out: &mut Vec<u8>,
    id: u32,
    stream: &mut Stream,
    body: &[u8],
    connection_window: &mut i64,
    max_frame: usize,
) -> bool {
    while stream.sent < body.len() {
        let window = (*connection_window).min(stream.send_window);
        let len = (body.len() - stream.sent).min(max_frame).min(window.max(0) as usize);
        if len == 0 {
            return false;
        }
        let end = stream.sent + len;
        let flags = if end == body.len() {
            frame::FLAG_END_STREAM
        } else {
            0
        };
        frame::write_header(out, len, frame::TYPE_DATA, flags, id);
        out.extend_from_slice(&body[stream.sent..end]);
        stream.sent = end;
        stream.send_window -= len as i64;
        *connection_window -= len as i64;
    }
    true
}

/// Send what the windows now allow of the blocked streams' bodies, oldest
/// first, through the outbox
fn release_blocked(conn: &mut Connection, bodies: &[Vec<u8>]) {
    let mut left = conn.blocked.len();
    while left > 0 && conn.send_window > 0 {
        left -= 1;
        let id = conn.blocked.pop_front().expect("counted");
        let stream = conn.streams.get_mut(&id).expect("blocked streams are open");
        let body = &bodies[stream.body];
        if !send_body(&mut conn.tx, id, stream, body, &mut conn.send_window, conn.peer_max_frame) {
            conn.blocked.push_back(id);
        }
    }
}

impl Protocol for Http2Protocol {
    type RequestId = Http2RequestId;

    fn next_request(&mut self, conn_id: usize) -> Request<Http2RequestId> {
        if !self.connections.contains_key(&conn_id) {
            self.connections.insert(conn_id, Connection::new(self.connection_window));
            return Request::setup(self.preface(), (conn_id, 0));
        }
        let body = self.next_body;
        self.next_body = (body + 1) % self.bodies.len();
        let body_bytes = &self.bodies[body];
        let conn = self.connections.get_mut(&conn_id).expect("checked above");
        debug_assert!(conn.phase == Phase::Open, "can_send waits for the server's SETTINGS");
        let id = conn.next_stream;
        conn.next_stream += 2;
        let mut data = Vec::with_capacity(
            2 * frame::HEADER_LEN + self.headers.len() + body_bytes.len().min(conn.peer_max_frame),
        );
        frame::write_headers(
            &mut data,
            id,
            &self.headers,
            body_bytes.is_empty(),
            frame::DEFAULT_MAX_FRAME_SIZE as usize,
        );
        let mut stream = Stream {
            status: None,
            grpc_status: None,
            recv_window: i64::from(self.stream_window),
            recv_unacked: 0,
            send_window: conn.peer_initial_window,
            body,
            sent: 0,
        };
        let max_frame = conn.peer_max_frame;
        if !send_body(&mut data, id, &mut stream, body_bytes, &mut conn.send_window, max_frame) {
            conn.blocked.push_back(id);
        }
        conn.streams.insert(id, stream);
        Request::measurement(data, (conn_id, id))
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, Http2RequestId>> {
        let conn = self.connection(conn_id)?;
        if conn.phase == Phase::AwaitSettings && data.starts_with(b"HTTP/1.") {
            bail!(
                "The server answered in HTTP/1.x: it does not speak cleartext HTTP/2 with prior \
                 knowledge (h2c) on this port"
            );
        }
        if data.len() < frame::HEADER_LEN {
            return Ok(Parsed::Incomplete);
        }
        let header = FrameHeader::parse(data);
        // xylem never raises SETTINGS_MAX_FRAME_SIZE above the default
        ensure!(
            header.len <= frame::DEFAULT_MAX_FRAME_SIZE as usize,
            "HTTP/2 frame of {} bytes exceeds the {} allowed (FRAME_SIZE_ERROR)",
            header.len,
            frame::DEFAULT_MAX_FRAME_SIZE
        );
        let consumed = frame::HEADER_LEN + header.len;
        if data.len() < consumed {
            return Ok(Parsed::Incomplete);
        }
        self.parse_frame(conn_id, header, &data[frame::HEADER_LEN..consumed], consumed)
    }

    fn can_send(&self, conn_id: usize) -> bool {
        let Some(conn) = self.connections.get(&conn_id) else {
            return true;
        };
        match conn.phase {
            Phase::AwaitSettings => false,
            Phase::Open => {
                conn.goaway.is_none()
                    && conn.next_stream <= frame::MAX_STREAM_ID
                    && conn.streams.len() < conn.peer_max_concurrent as usize
            }
        }
    }

    fn setup_pending(&self, conn_id: usize) -> bool {
        self.connections.get(&conn_id).is_none_or(|conn| conn.phase != Phase::Open)
    }

    fn drain_outbox(&mut self, conn_id: usize, outbox: &mut Outbox) {
        if let Some(conn) = self.connections.get_mut(&conn_id) {
            if outbox.data.is_empty() {
                std::mem::swap(&mut outbox.data, &mut conn.tx);
            } else {
                outbox.data.append(&mut conn.tx);
            }
        }
    }

    fn name(&self) -> &'static str {
        match self.exchange {
            Exchange::Http => "http2",
            Exchange::Grpc => "grpc",
        }
    }

    fn reset(&mut self) {
        self.next_body = 0;
    }
}

#[cfg(test)]
mod tests;
