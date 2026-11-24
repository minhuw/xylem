//! HTTP/1.1 protocol implementation
//!
//! Supports GET, POST, PUT with Content-Length, chunked, or bodyless responses.
//! Other transfer codings and close-delimited bodies are explicitly unsupported.
//! Informational responses are consumed without completing a request.
//! A response with status 400 or above is an error reply whose kind is its
//! status code (`"503"`); 1xx, 2xx and 3xx responses are not.
//! Uses httparse for zero-copy, fast HTTP response parsing.
//!
//! Request sequence tracking is done per-connection to support pipelining.

mod config;

use crate::workload::KeyGeneration;
use crate::{ErrorReply, Parsed, Protocol};
use anyhow::{anyhow, Result};
use std::collections::VecDeque;
use xylem_common::{FastHashMap, VerificationMismatch, VerificationReport};

/// Maximum bytes consumed per `parse_response` call so `xylem-core`'s
/// `MAX_PAYLOAD` (64 KiB) per-step guard is never tripped while streaming
/// `Content-Length` responses larger than 64 KiB.
const MAX_CHUNK: usize = 65_536;

const FNV1A_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A_PRIME: u64 = 0x0000_0100_0000_01b3;

#[inline]
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h = FNV1A_OFFSET_BASIS;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV1A_PRIME);
    }
    h
}

#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Deterministic base word derived from `(seed, path, body_size)`.
#[inline]
pub fn pattern_base(seed: u64, path: &[u8], body_size: usize) -> u64 {
    splitmix64(seed ^ fnv1a_64(path) ^ (body_size as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

#[inline]
fn pattern_word(base: u64, word_index: u64) -> u64 {
    splitmix64(base.wrapping_add(word_index.wrapping_mul(0x9e37_79b9_7f4a_7c15)))
}

#[inline]
fn first_word_mismatch(word_start_offset: u64, expected: u64, actual: u64) -> (u64, u8, u8) {
    let exp = expected.to_le_bytes();
    let act = actual.to_le_bytes();
    for i in 0..8 {
        if exp[i] != act[i] {
            return (word_start_offset.wrapping_add(i as u64), exp[i], act[i]);
        }
    }
    unreachable!("first_word_mismatch called with equal words");
}

/// Fill `out` with the deterministic HTTP GET verification pattern for
/// `(seed, path, body_size)` starting at body byte offset 0.
pub fn fill_verify_pattern(seed: u64, path: &[u8], body_size: usize, out: &mut [u8]) {
    if out.is_empty() {
        return;
    }
    let base = pattern_base(seed, path, body_size);
    let mut pos = 0usize;
    let mut word_idx = 0u64;

    // 4x-unrolled 32-byte body
    while pos + 32 <= out.len() {
        let w0 = pattern_word(base, word_idx).to_le_bytes();
        let w1 = pattern_word(base, word_idx.wrapping_add(1)).to_le_bytes();
        let w2 = pattern_word(base, word_idx.wrapping_add(2)).to_le_bytes();
        let w3 = pattern_word(base, word_idx.wrapping_add(3)).to_le_bytes();
        out[pos..pos + 8].copy_from_slice(&w0);
        out[pos + 8..pos + 16].copy_from_slice(&w1);
        out[pos + 16..pos + 24].copy_from_slice(&w2);
        out[pos + 24..pos + 32].copy_from_slice(&w3);
        word_idx = word_idx.wrapping_add(4);
        pos += 32;
    }

    // Remaining full 8-byte words
    while pos + 8 <= out.len() {
        let w = pattern_word(base, word_idx).to_le_bytes();
        out[pos..pos + 8].copy_from_slice(&w);
        word_idx = word_idx.wrapping_add(1);
        pos += 8;
    }

    // Trailing partial word (< 8 bytes)
    if pos < out.len() {
        let w = pattern_word(base, word_idx).to_le_bytes();
        let tail = out.len() - pos;
        out[pos..].copy_from_slice(&w[..tail]);
    }
}

/// Verify `chunk` against the deterministic HTTP GET verification pattern with
/// precomputed `base`, starting at 0-based body byte `offset`.
///
/// Returns `Some((body_byte_offset, expected_byte, actual_byte))` for the
/// first mismatched byte, or `None` if every byte matches.
pub fn check_verify_pattern_chunk(base: u64, offset: usize, chunk: &[u8]) -> Option<(u64, u8, u8)> {
    if chunk.is_empty() {
        return None;
    }
    let mut pos = 0usize;
    let mut off = offset as u64;

    // Unaligned head up to the next 8-byte word boundary
    let rem_head = (off & 7) as usize;
    if rem_head != 0 {
        let word = pattern_word(base, off >> 3).to_le_bytes();
        let take = (8 - rem_head).min(chunk.len());
        for i in 0..take {
            let expected = word[rem_head + i];
            let actual = chunk[i];
            if expected != actual {
                return Some((off.wrapping_add(i as u64), expected, actual));
            }
        }
        pos += take;
        off = off.wrapping_add(take as u64);
    }

    let mut word_idx = off >> 3;

    // 4x-unrolled 32-byte body
    let (quads, quads_rem) = chunk[pos..].as_chunks::<32>();
    for quad in quads {
        let e0 = pattern_word(base, word_idx);
        let e1 = pattern_word(base, word_idx.wrapping_add(1));
        let e2 = pattern_word(base, word_idx.wrapping_add(2));
        let e3 = pattern_word(base, word_idx.wrapping_add(3));
        let a0 = u64::from_le_bytes(quad[0..8].try_into().unwrap());
        let a1 = u64::from_le_bytes(quad[8..16].try_into().unwrap());
        let a2 = u64::from_le_bytes(quad[16..24].try_into().unwrap());
        let a3 = u64::from_le_bytes(quad[24..32].try_into().unwrap());
        if ((e0 ^ a0) | (e1 ^ a1) | (e2 ^ a2) | (e3 ^ a3)) != 0 {
            if e0 != a0 {
                return Some(first_word_mismatch(word_idx << 3, e0, a0));
            }
            if e1 != a1 {
                return Some(first_word_mismatch(word_idx.wrapping_add(1) << 3, e1, a1));
            }
            if e2 != a2 {
                return Some(first_word_mismatch(word_idx.wrapping_add(2) << 3, e2, a2));
            }
            return Some(first_word_mismatch(word_idx.wrapping_add(3) << 3, e3, a3));
        }
        word_idx = word_idx.wrapping_add(4);
    }

    // Remaining full 8-byte words
    let (words, tail) = quads_rem.as_chunks::<8>();
    for word_bytes in words {
        let expected = pattern_word(base, word_idx);
        let actual = u64::from_le_bytes(*word_bytes);
        if expected != actual {
            return Some(first_word_mismatch(word_idx << 3, expected, actual));
        }
        word_idx = word_idx.wrapping_add(1);
    }

    // Trailing partial word (< 8 bytes)
    if !tail.is_empty() {
        let word = pattern_word(base, word_idx).to_le_bytes();
        let base_off = word_idx << 3;
        for (i, &actual) in tail.iter().enumerate() {
            let expected = word[i];
            if expected != actual {
                return Some((base_off.wrapping_add(i as u64), expected, actual));
            }
        }
    }

    None
}

/// Verify a complete response body `actual` against the deterministic HTTP GET
/// verification pattern for `(seed, path, body_size)`.
///
/// Returns `Some((body_byte_offset, expected_byte, actual_byte))` for the
/// first mismatched byte, or `None` if every byte matches.
pub fn check_verify_pattern(
    seed: u64,
    path: &[u8],
    body_size: usize,
    actual: &[u8],
) -> Option<(u64, u8, u8)> {
    check_verify_pattern_chunk(pattern_base(seed, path, body_size), 0, actual)
}

/// The three digits of every status code below 600, so the kind of an error
/// reply is a static string
pub(crate) static STATUS_DIGITS: [[u8; 3]; 600] = {
    let mut digits = [[0; 3]; 600];
    let mut code = 0;
    while code < 600 {
        digits[code] = [
            b'0' + (code / 100) as u8,
            b'0' + (code / 10 % 10) as u8,
            b'0' + (code % 10) as u8,
        ];
        code += 1;
    }
    digits
};

/// The error reply of a response with status `code` (400 to 599) whose head
/// starts with `head`: the code is its kind, the status line its message
fn error_reply(code: u16, head: &[u8]) -> ErrorReply<'_> {
    let kind = &STATUS_DIGITS[usize::from(code)];
    let line = &head[..head.iter().position(|&b| b == b'\n').unwrap_or(head.len())];
    ErrorReply::from_bytes(kind, line.strip_suffix(b"\r").unwrap_or(line))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
}

impl HttpMethod {
    fn as_str(&self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
        }
    }
}

/// Per-connection in-flight streaming `Content-Length` receive state.
struct HttpRxState {
    remaining_body: usize,
    body_offset: usize,
    status_code: u16,
    content_length: usize,
    target_path: Option<String>,
    base: u64,
    first_body_mismatch: Option<(u64, u8, u8)>,
}

/// Opt-in GET response-body verification state.
struct HttpVerifyState {
    seed: u64,
    expected_body_size: usize,
    static_base: u64,
    /// FIFO queue of request target paths per connection when `key_gen` is set.
    pending_paths: FastHashMap<usize, VecDeque<String>>,
    report: VerificationReport,
}

impl HttpVerifyState {
    fn new(seed: u64, expected_body_size: usize, static_base: u64) -> Self {
        Self {
            seed,
            expected_body_size,
            static_base,
            pending_paths: FastHashMap::default(),
            report: VerificationReport::default(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_failure(
        &mut self,
        kind: &'static str,
        key: &str,
        offset: u64,
        expected: Option<u8>,
        actual: Option<u8>,
        expected_len: Option<usize>,
        actual_len: Option<usize>,
        detail: impl FnOnce() -> String,
        reply_message: &'static str,
    ) -> ErrorReply<'static> {
        self.report.failures += 1;
        if self.report.first_mismatch.is_none() {
            self.report.first_mismatch = Some(VerificationMismatch {
                kind: kind.to_string(),
                key: Some(key.to_string()),
                offset,
                expected,
                actual,
                expected_len,
                actual_len,
                detail: detail(),
            });
        }
        ErrorReply::new(kind, reply_message)
    }
}

pub struct HttpProtocol {
    method: HttpMethod,
    path: String,
    host: String,
    body_size: usize,
    keep_alive: bool,
    /// Request bytes, built once at construction and sent for every request
    /// when `key_gen` is `None`.
    request: Vec<u8>,
    /// Optional per-request key generator and prefix appended to `path`.
    key_gen: Option<(KeyGeneration, String)>,
    /// Per-connection sequence numbers for request tracking (TX side)
    conn_seq: FastHashMap<usize, u64>,
    /// Per-connection expected response sequence (RX side - responses are FIFO)
    conn_response_seq: FastHashMap<usize, u64>,
    /// Per-connection in-flight streaming `Content-Length` state.
    conn_rx: FastHashMap<usize, HttpRxState>,
    /// Opt-in GET response-body verification state.
    verify: Option<HttpVerifyState>,
}

impl HttpProtocol {
    pub fn new(method: HttpMethod, path: String, host: String) -> Self {
        Self::with_body_size(method, path, host, 64)
    }

    pub fn with_body_size(
        method: HttpMethod,
        path: String,
        host: String,
        body_size: usize,
    ) -> Self {
        Self::with_options(method, path, host, body_size, true)
    }

    pub fn with_options(
        method: HttpMethod,
        path: String,
        host: String,
        body_size: usize,
        keep_alive: bool,
    ) -> Self {
        let request = Self::build_request(method, &path, &host, body_size, keep_alive);
        Self {
            method,
            path,
            host,
            body_size,
            keep_alive,
            request,
            key_gen: None,
            conn_seq: FastHashMap::default(),
            conn_response_seq: FastHashMap::default(),
            conn_rx: FastHashMap::default(),
            verify: None,
        }
    }

    /// Append `{prefix}{key_num}` from `key_gen` to `path` on each request.
    pub fn with_keys(mut self, key_gen: KeyGeneration, prefix: String) -> Self {
        self.key_gen = Some((key_gen, prefix));
        self
    }

    /// Enable opt-in deterministic GET response-body verification.
    pub fn with_verify(mut self, seed: u64, expected_body_size: usize) -> Self {
        let static_base = pattern_base(seed, self.path.as_bytes(), expected_body_size);
        self.verify = Some(HttpVerifyState::new(seed, expected_body_size, static_base));
        self
    }

    fn next_seq(&mut self, conn_id: usize) -> u64 {
        let seq = self.conn_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        *seq = seq.wrapping_add(1);
        result
    }

    fn next_response_seq(&mut self, conn_id: usize) -> u64 {
        let response_seq = self.conn_response_seq.entry(conn_id).or_insert(0);
        let seq = *response_seq;
        *response_seq = response_seq.wrapping_add(1);
        seq
    }

    fn pop_pending_path(&mut self, conn_id: usize) -> Option<String> {
        self.key_gen.as_ref()?;
        self.verify
            .as_mut()
            .and_then(|v| v.pending_paths.get_mut(&conn_id)?.pop_front())
    }

    fn verify_body_prefix(
        &self,
        status_code: u16,
        body_length: usize,
        base: u64,
        prefix: &[u8],
    ) -> Option<(u64, u8, u8)> {
        let verify = self.verify.as_ref()?;
        if status_code == 200 && body_length == verify.expected_body_size && !prefix.is_empty() {
            check_verify_pattern_chunk(base, 0, prefix)
        } else {
            None
        }
    }

    fn verify_base_for(&self, target_path: Option<&str>) -> u64 {
        let Some(ref v) = self.verify else {
            return 0;
        };
        match target_path {
            Some(path) => pattern_base(v.seed, path.as_bytes(), v.expected_body_size),
            None => v.static_base,
        }
    }

    fn finalize_content_length_response<'a>(
        &mut self,
        status_code: u16,
        body_length: usize,
        target_path: Option<&str>,
        first_mismatch: Option<(u64, u8, u8)>,
        head: Option<&'a [u8]>,
    ) -> Option<ErrorReply<'a>> {
        let Some(ref mut v) = self.verify else {
            return (status_code >= 400).then(|| match head {
                Some(head) => error_reply(status_code, head),
                None => {
                    let kind = &STATUS_DIGITS[usize::from(status_code)];
                    ErrorReply::from_bytes(kind, kind)
                }
            });
        };
        let key = target_path.unwrap_or(&self.path);
        if status_code != 200 {
            return Some(v.record_failure(
                "BadStatus",
                key,
                0,
                None,
                None,
                None,
                None,
                || format!("HTTP status {status_code} (expected 200)"),
                "Unexpected HTTP status code (expected 200)",
            ));
        }
        let expected_size = v.expected_body_size;
        if body_length != expected_size {
            return Some(v.record_failure(
                "ContentLengthMismatch",
                key,
                0,
                None,
                None,
                Some(expected_size),
                Some(body_length),
                || {
                    format!(
                        "HTTP Content-Length {body_length} does not match expected {expected_size}"
                    )
                },
                "HTTP Content-Length mismatch",
            ));
        }
        if let Some((offset, expected, actual)) = first_mismatch {
            return Some(v.record_failure(
                "BodyMismatch",
                key,
                offset,
                Some(expected),
                Some(actual),
                Some(expected_size),
                Some(expected_size),
                || {
                    format!(
                        "HTTP response body byte mismatch at offset {offset}: \
                         expected 0x{expected:02x}, actual 0x{actual:02x}"
                    )
                },
                "HTTP response body mismatch",
            ));
        }
        v.report.verified += 1;
        None
    }

    fn finalize_chunked_response<'a>(
        &mut self,
        status_code: u16,
        wire_body: &[u8],
        target_path: Option<&str>,
        head: &'a [u8],
    ) -> Option<ErrorReply<'a>> {
        let Some(ref mut v) = self.verify else {
            return (status_code >= 400).then(|| error_reply(status_code, head));
        };
        let key = target_path.unwrap_or(&self.path);
        if status_code != 200 {
            return Some(v.record_failure(
                "BadStatus",
                key,
                0,
                None,
                None,
                None,
                None,
                || format!("HTTP status {status_code} (expected 200)"),
                "Unexpected HTTP status code (expected 200)",
            ));
        }
        let expected_size = v.expected_body_size;
        let base = match target_path {
            Some(path) => pattern_base(v.seed, path.as_bytes(), expected_size),
            None => v.static_base,
        };
        let mut pos = 0usize;
        let mut decoded_len = 0usize;
        let mut first_mismatch: Option<(u64, u8, u8)> = None;
        loop {
            let line_len = crlf_line(&wire_body[pos..])
                .expect("chunked body already validated")
                .expect("chunked body already validated");
            let size = parse_chunk_size(&wire_body[pos..pos + line_len])
                .expect("chunked body already validated");
            pos += line_len + 2;
            if size == 0 {
                break;
            }
            let chunk_data = &wire_body[pos..pos + size];
            if decoded_len < expected_size && first_mismatch.is_none() {
                let check_len = size.min(expected_size - decoded_len);
                first_mismatch =
                    check_verify_pattern_chunk(base, decoded_len, &chunk_data[..check_len]);
            }
            decoded_len = decoded_len.saturating_add(size);
            pos += size + 2;
        }
        if decoded_len < expected_size {
            return Some(v.record_failure(
                "ShortBody",
                key,
                decoded_len as u64,
                None,
                None,
                Some(expected_size),
                Some(decoded_len),
                || {
                    format!(
                        "HTTP chunked body length {decoded_len} shorter than expected {expected_size}"
                    )
                },
                "HTTP response body shorter than expected",
            ));
        }
        if decoded_len > expected_size {
            return Some(v.record_failure(
                "ContentLengthMismatch",
                key,
                expected_size as u64,
                None,
                None,
                Some(expected_size),
                Some(decoded_len),
                || {
                    format!(
                        "HTTP chunked body length {decoded_len} longer than expected {expected_size}"
                    )
                },
                "HTTP response body length mismatch",
            ));
        }
        if let Some((offset, expected, actual)) = first_mismatch {
            return Some(v.record_failure(
                "BodyMismatch",
                key,
                offset,
                Some(expected),
                Some(actual),
                Some(expected_size),
                Some(expected_size),
                || {
                    format!(
                        "HTTP response body byte mismatch at offset {offset}: \
                         expected 0x{expected:02x}, actual 0x{actual:02x}"
                    )
                },
                "HTTP response body mismatch",
            ));
        }
        v.report.verified += 1;
        None
    }

    /// Generate HTTP request, with a body of `body_size` 'x' bytes for POST/PUT
    fn build_request(
        method: HttpMethod,
        path: &str,
        host: &str,
        body_size: usize,
        keep_alive: bool,
    ) -> Vec<u8> {
        let connection = if keep_alive { "keep-alive" } else { "close" };
        let mut request = format!(
            "{} {} HTTP/1.1\r\n\
             Host: {}\r\n\
             Connection: {}\r\n",
            method.as_str(),
            path,
            host,
            connection
        );

        match method {
            HttpMethod::Get => {
                request.push_str("\r\n");
                request.into_bytes()
            }
            HttpMethod::Post | HttpMethod::Put => {
                request.push_str(&format!("Content-Length: {}\r\n", body_size));
                request.push_str("Content-Type: application/octet-stream\r\n");
                request.push_str("\r\n");

                let mut bytes = request.into_bytes();
                bytes.resize(bytes.len() + body_size, b'x');
                bytes
            }
        }
    }
}

impl Default for HttpProtocol {
    fn default() -> Self {
        Self::new(HttpMethod::Get, "/".to_string(), "localhost".to_string())
    }
}

impl Protocol for HttpProtocol {
    type RequestId = (usize, u64);

    fn next_request(&mut self, conn_id: usize) -> crate::Request<Self::RequestId> {
        let seq = self.next_seq(conn_id);
        let data = if let Some((ref mut key_gen, ref prefix)) = self.key_gen {
            let key_num = key_gen.next_key();
            let target = format!("{}{}{}", self.path, prefix, key_num);
            let bytes = Self::build_request(
                self.method,
                &target,
                &self.host,
                self.body_size,
                self.keep_alive,
            );
            if let Some(ref mut v) = self.verify {
                v.pending_paths.entry(conn_id).or_default().push_back(target);
            }
            bytes
        } else {
            self.request.clone()
        };
        // HTTP protocol doesn't have a warmup phase
        crate::Request::measurement(data, (conn_id, seq))
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, Self::RequestId>> {
        if data.is_empty() {
            return Ok(Parsed::Incomplete);
        }

        // Continuing an in-flight streaming Content-Length body
        if let Some(state) = self.conn_rx.get_mut(&conn_id) {
            let n = data.len().min(state.remaining_body).min(MAX_CHUNK);
            if n == 0 {
                return Ok(Parsed::Incomplete);
            }
            if let Some(ref verify) = self.verify {
                if state.status_code == 200
                    && state.content_length == verify.expected_body_size
                    && state.first_body_mismatch.is_none()
                {
                    state.first_body_mismatch =
                        check_verify_pattern_chunk(state.base, state.body_offset, &data[..n]);
                }
            }
            state.body_offset += n;
            state.remaining_body -= n;
            if state.remaining_body > 0 {
                return Ok(Parsed::Absorbed(n));
            }
            let state = self.conn_rx.remove(&conn_id).expect("in-flight HTTP RX state");
            let error = self.finalize_content_length_response(
                state.status_code,
                state.content_length,
                state.target_path.as_deref(),
                state.first_body_mismatch,
                None,
            );
            let seq = self.next_response_seq(conn_id);
            return Ok(Parsed::Complete { consumed: n, id: (conn_id, seq), error });
        }

        let mut offset = 0;
        loop {
            let remaining = &data[offset..];
            // Parse HTTP response headers
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut response = httparse::Response::new(&mut headers);

            let headers_len = match response.parse(remaining) {
                Ok(httparse::Status::Complete(length)) => length,
                Ok(httparse::Status::Partial) => return Ok(Parsed::Incomplete),
                Err(error) => return Err(anyhow!("HTTP parse error: {}", error)),
            };
            let status_code = response.code.unwrap_or(0);
            anyhow::ensure!(
                (100..600).contains(&status_code),
                "Invalid HTTP status code: {}",
                status_code
            );
            anyhow::ensure!(status_code != 101, "HTTP protocol upgrades are unsupported");
            let mut content_length = None;
            let mut transfer_encoding = false;
            for header in response.headers.iter() {
                if header.name.eq_ignore_ascii_case("transfer-encoding") {
                    transfer_encoding = true;
                    continue;
                }
                if !header.name.eq_ignore_ascii_case("content-length") {
                    continue;
                }
                let text = std::str::from_utf8(header.value)?.trim();
                anyhow::ensure!(
                    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()),
                    "Invalid HTTP Content-Length"
                );
                let length: usize = text.parse()?;
                anyhow::ensure!(
                    content_length.is_none_or(|old| old == length),
                    "Conflicting HTTP Content-Length headers"
                );
                content_length = Some(length);
            }
            if status_code < 200 {
                offset += headers_len;
                continue;
            }
            // RFC 9112 section 6.3: 204 and 304 responses end at the header
            // section whatever their framing headers say
            if status_code == 204 || status_code == 304 {
                let total_len = offset + headers_len;
                let target_path = self.pop_pending_path(conn_id);
                let error = self.finalize_content_length_response(
                    status_code,
                    0,
                    target_path.as_deref(),
                    None,
                    Some(remaining),
                );
                let seq = self.next_response_seq(conn_id);
                return Ok(Parsed::Complete {
                    consumed: total_len,
                    id: (conn_id, seq),
                    error,
                });
            }
            if transfer_encoding {
                // Transfer-Encoding overrides Content-Length, but a response with
                // both "ought to be handled as an error"; the sender broke a MUST NOT
                anyhow::ensure!(
                    content_length.is_none(),
                    "HTTP response has both Transfer-Encoding and Content-Length"
                );
                ensure_chunked_only(response.headers)?;
                let Some(wire_body_len) = chunked_body_len(&remaining[headers_len..])? else {
                    return Ok(Parsed::Incomplete);
                };
                let total_len = offset
                    .checked_add(headers_len)
                    .and_then(|n| n.checked_add(wire_body_len))
                    .ok_or_else(|| anyhow!("HTTP response length overflow"))?;
                let target_path = self.pop_pending_path(conn_id);
                let error = self.finalize_chunked_response(
                    status_code,
                    &remaining[headers_len..headers_len + wire_body_len],
                    target_path.as_deref(),
                    remaining,
                );
                let seq = self.next_response_seq(conn_id);
                return Ok(Parsed::Complete {
                    consumed: total_len,
                    id: (conn_id, seq),
                    error,
                });
            }

            let body_length = content_length.ok_or_else(|| {
                anyhow!(
                    "HTTP response requires Content-Length or chunked Transfer-Encoding; \
                     close-delimited bodies are unsupported"
                )
            })?;
            let head_end = offset
                .checked_add(headers_len)
                .ok_or_else(|| anyhow!("HTTP response length overflow"))?;
            let total_len = head_end
                .checked_add(body_length)
                .ok_or_else(|| anyhow!("HTTP response length overflow"))?;

            // Fast one-shot path: entire response fits in `data` and <= MAX_CHUNK
            if total_len <= data.len() && total_len <= MAX_CHUNK {
                let target_path = self.pop_pending_path(conn_id);
                let base = self.verify_base_for(target_path.as_deref());
                let first_mismatch = self.verify_body_prefix(
                    status_code,
                    body_length,
                    base,
                    &data[head_end..total_len],
                );
                let error = self.finalize_content_length_response(
                    status_code,
                    body_length,
                    target_path.as_deref(),
                    first_mismatch,
                    Some(remaining),
                );
                let seq = self.next_response_seq(conn_id);
                return Ok(Parsed::Complete {
                    consumed: total_len,
                    id: (conn_id, seq),
                    error,
                });
            }

            // Preserve exact `Parsed::Incomplete` behavior for `verify = false`
            // on responses that fit within `MAX_CHUNK`.
            if self.verify.is_none() && total_len <= MAX_CHUNK {
                return Ok(Parsed::Incomplete);
            }

            if head_end > MAX_CHUNK {
                return Ok(Parsed::Absorbed(head_end));
            }

            // Streaming Content-Length path via `Parsed::Absorbed` (<= MAX_CHUNK)
            let body_avail = (data.len() - head_end).min(body_length);
            let take_body = body_avail.min(MAX_CHUNK - head_end);
            let target_path = self.pop_pending_path(conn_id);
            let base = self.verify_base_for(target_path.as_deref());
            let first_body_mismatch = self.verify_body_prefix(
                status_code,
                body_length,
                base,
                &data[head_end..head_end + take_body],
            );
            let remaining_body = body_length - take_body;
            debug_assert!(remaining_body > 0);
            self.conn_rx.insert(
                conn_id,
                HttpRxState {
                    remaining_body,
                    body_offset: take_body,
                    status_code,
                    content_length: body_length,
                    target_path,
                    base,
                    first_body_mismatch,
                },
            );
            return Ok(Parsed::Absorbed(head_end + take_body));
        }
    }

    fn verification_report(&self) -> Option<VerificationReport> {
        self.verify.as_ref().map(|v| v.report.clone())
    }

    fn on_connection_closed(&mut self, conn_id: usize) -> Option<ErrorReply<'static>> {
        let in_flight = self.conn_rx.remove(&conn_id);
        let verify = self.verify.as_mut()?;
        verify.pending_paths.remove(&conn_id);
        let state = in_flight?;
        if state.remaining_body == 0 {
            return None;
        }
        let key = state.target_path.as_deref().unwrap_or(&self.path);
        let expected_size = verify.expected_body_size;
        let received = state.body_offset;
        let expected_cl = state.content_length;
        Some(verify.record_failure(
            "ShortBody",
            key,
            received as u64,
            None,
            None,
            Some(expected_size),
            Some(received),
            || format!("Connection closed after {received} of {expected_cl} body bytes"),
            "Connection closed before HTTP response body completed",
        ))
    }

    fn name(&self) -> &'static str {
        "http"
    }

    fn reset(&mut self) {
        self.conn_seq.clear();
        self.conn_response_seq.clear();
        self.conn_rx.clear();
        if let Some((ref mut key_gen, _)) = self.key_gen {
            key_gen.reset();
        }
        if let Some(ref mut v) = self.verify {
            v.pending_paths.clear();
            v.report = VerificationReport::default();
        }
    }
}

/// Ensure the Transfer-Encoding headers list exactly one coding, `chunked`
///
/// Any other coding leaves the body close-delimited or encoded (RFC 9112
/// section 6.3), and `chunked` must be applied once, last (section 6.1).
fn ensure_chunked_only(headers: &[httparse::Header<'_>]) -> Result<()> {
    let values = || {
        headers
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case("transfer-encoding"))
            .map(|header| header.value)
    };
    // Split the combined list, ignoring empty elements (RFC 9110 section 5.6.1)
    let mut codings = values()
        .flat_map(|value| value.split(|&b| b == b','))
        .map(|coding| coding.trim_ascii())
        .filter(|coding| !coding.is_empty());
    let chunked = codings.next().is_some_and(|coding| coding.eq_ignore_ascii_case(b"chunked"));
    if chunked && codings.next().is_none() {
        return Ok(());
    }
    let list = values().map(String::from_utf8_lossy).collect::<Vec<_>>().join(", ");
    Err(anyhow!(
        "Unsupported HTTP Transfer-Encoding {list:?}; only a single chunked coding is supported"
    ))
}

/// Length of the chunked body at the start of `data` (RFC 9112 section 7.1)
///
/// Counts every chunk, the last-chunk, the trailer section and its final CRLF,
/// or returns `None` until the whole body has arrived. Chunk extensions and
/// trailer fields are skipped. The core bounds the length of a response, so
/// an incomplete body cannot grow without limit.
fn chunked_body_len(data: &[u8]) -> Result<Option<usize>> {
    let mut pos = 0;
    loop {
        let Some(line_len) = crlf_line(&data[pos..])? else {
            return Ok(None);
        };
        let size = parse_chunk_size(&data[pos..pos + line_len])?;
        pos += line_len + 2;
        if size == 0 {
            break;
        }
        let end = pos
            .checked_add(size)
            .and_then(|end| end.checked_add(2))
            .ok_or_else(|| anyhow!("HTTP chunk size overflow"))?;
        if data.len() < end {
            return Ok(None);
        }
        anyhow::ensure!(&data[end - 2..end] == b"\r\n", "HTTP chunk data must end with CRLF");
        pos = end;
    }
    // Trailer section, ended by an empty line
    loop {
        let Some(line_len) = crlf_line(&data[pos..])? else {
            return Ok(None);
        };
        let line = &data[pos..pos + line_len];
        pos += line_len + 2;
        if line.is_empty() {
            return Ok(Some(pos));
        }
        let name_len = line.iter().take_while(|&&b| is_tchar(b)).count();
        anyhow::ensure!(
            name_len > 0
                && line.get(name_len) == Some(&b':')
                && line[name_len + 1..].iter().all(|&b| b == b'\t' || !b.is_ascii_control()),
            "Invalid HTTP trailer field"
        );
    }
}

/// Length of the CRLF-terminated line at the start of `data`, without the
/// CRLF, or `None` until the line ends; a bare CR or LF is an error
fn crlf_line(data: &[u8]) -> Result<Option<usize>> {
    let Some(end) = data.iter().position(|&b| b == b'\r' || b == b'\n') else {
        return Ok(None);
    };
    anyhow::ensure!(data[end] == b'\r', "HTTP chunked framing requires CRLF line endings");
    match data.get(end + 1) {
        None => Ok(None),
        Some(b'\n') => Ok(Some(end)),
        Some(_) => Err(anyhow!("HTTP chunked framing requires CRLF line endings")),
    }
}

/// Parse a chunk-size line, `1*HEXDIG [chunk-ext]`, ignoring the extensions
fn parse_chunk_size(line: &[u8]) -> Result<usize> {
    let digits = line.iter().take_while(|b| b.is_ascii_hexdigit()).count();
    let (hex, extensions) = line.split_at(digits);
    // chunk-ext = *( BWS ";" BWS name [ BWS "=" BWS value ] ), so anything
    // after the size starts with optional whitespace and a semicolon
    let semicolon = extensions.trim_ascii_start().first() == Some(&b';');
    anyhow::ensure!(
        digits > 0
            && (extensions.is_empty() || semicolon)
            && extensions.iter().all(|&b| b == b'\t' || !b.is_ascii_control()),
        "Invalid HTTP chunk size line {:?}",
        String::from_utf8_lossy(line)
    );
    hex.iter()
        .try_fold(0usize, |size, &b| {
            let digit = (b as char).to_digit(16)? as usize;
            size.checked_mul(16)?.checked_add(digit)
        })
        .ok_or_else(|| anyhow!("HTTP chunk size overflow"))
}

/// RFC 9110 token character
fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_request() {
        let mut proto =
            HttpProtocol::new(HttpMethod::Get, "/api/test".to_string(), "example.com".to_string());

        let request = proto.next_request(0);
        let (conn_id, seq) = request.request_id;
        let req_str = String::from_utf8_lossy(&request.data);

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0);
        assert!(req_str.contains("GET /api/test HTTP/1.1"));
        assert!(req_str.contains("Host: example.com"));
        assert!(req_str.contains("Connection: keep-alive"));
        assert!(!req_str.contains("Content-Length")); // GET has no body
    }

    #[test]
    fn test_post_request() {
        let mut proto =
            HttpProtocol::new(HttpMethod::Post, "/data".to_string(), "api.example.com".to_string());

        let request = proto.next_request(0);
        let (conn_id, seq) = request.request_id;
        let req_str = String::from_utf8_lossy(&request.data);

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0);
        assert!(req_str.contains("POST /data HTTP/1.1"));
        assert!(req_str.contains("Host: api.example.com"));
        assert!(req_str.contains("Connection: keep-alive"));
        // HTTP doesn't actually use the value_size from next_request since it uses internal config
        // Just verify Content-Length exists
        assert!(req_str.contains("Content-Length:"));
        assert!(req_str.contains("Content-Type: application/octet-stream"));
    }

    #[test]
    fn test_sequence_numbers() {
        let mut proto =
            HttpProtocol::new(HttpMethod::Get, "/".to_string(), "localhost".to_string());

        // Using same conn_id increments sequence
        let request1 = proto.next_request(0);
        let (_, seq1) = request1.request_id;
        let request2 = proto.next_request(0);
        let (_, seq2) = request2.request_id;
        let request3 = proto.next_request(0);
        let (_, seq3) = request3.request_id;

        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
        assert_eq!(seq3, 2);
    }

    #[test]
    fn test_response_parsing() {
        let mut proto =
            HttpProtocol::new(HttpMethod::Get, "/".to_string(), "localhost".to_string());

        let response =
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: keep-alive\r\n\r\nOK\n";

        let parsed = proto.parse_response(0, response).unwrap();

        assert_eq!(parsed, Parsed::ok(response.len(), (0, 0)));
    }

    /// A 200 response with a chunked body made of `body`
    fn chunked(body: &str) -> Vec<u8> {
        format!("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{body}").into_bytes()
    }

    /// Parse `data` on a fresh connection, expecting one whole response
    fn parse_one(data: &[u8]) -> Result<Parsed<'_, (usize, u64)>> {
        HttpProtocol::default().parse_response(0, data)
    }

    #[test]
    fn test_chunked_hex_sizes_in_either_case() {
        let alphabet = "0123456789abcdefghijklmnopqrstuvwxyz";
        for size in ["1a", "1A", "01A", "1f", "1F"] {
            let len = usize::from_str_radix(size, 16).unwrap();
            let response = chunked(&format!("{size}\r\n{}\r\n0\r\n\r\n", &alphabet[..len]));
            assert_eq!(parse_one(&response).unwrap(), Parsed::ok(response.len(), (0, 0)), "{size}");
        }
        let response = [chunked("aB\r\n"), vec![b'x'; 0xab], b"\r\n0\r\n\r\n".to_vec()].concat();
        assert_eq!(parse_one(&response).unwrap().consumed(), response.len());
    }

    #[test]
    fn test_chunked_extensions_are_ignored() {
        for body in [
            "5;name=value\r\nhello\r\n0\r\n\r\n",
            "5;a;b=\"quoted; value\"\r\nhello\r\n0;last\r\n\r\n",
            "5 ; a = b\r\nhello\r\n000\t;x\r\n\r\n",
        ] {
            let response = chunked(body);
            assert_eq!(
                parse_one(&response).unwrap(),
                Parsed::ok(response.len(), (0, 0)),
                "{body:?}"
            );
        }
    }

    #[test]
    fn test_chunked_trailers_are_skipped() {
        let response = chunked("3\r\nabc\r\n0\r\nExpires: never\r\nX-Checksum:\t1234\r\n\r\n");
        assert_eq!(parse_one(&response).unwrap(), Parsed::ok(response.len(), (0, 0)));
        // A trailer that looks like a response does not end the message early
        let response = chunked("0\r\nX-Next: HTTP/1.1 200 OK\r\n\r\n");
        assert_eq!(parse_one(&response).unwrap(), Parsed::ok(response.len(), (0, 0)));
    }

    #[test]
    fn test_chunked_zero_length_body() {
        for body in ["0\r\n\r\n", "000\r\n\r\n"] {
            let response = chunked(body);
            assert_eq!(parse_one(&response).unwrap(), Parsed::ok(response.len(), (0, 0)));
        }
    }

    #[test]
    fn test_chunked_every_split_point_is_incomplete() {
        // Interim response, extensions, several chunks and trailers, all counted
        let mut response = b"HTTP/1.1 100 Continue\r\n\r\n".to_vec();
        response.extend(chunked(
            "4;x=y\r\nWiki\r\n6\r\npedia \r\nE\r\nin \r\n\r\nchunks.\r\n0\r\nT: v\r\n\r\n",
        ));
        let next = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        let mut proto = HttpProtocol::default();
        for end in 0..response.len() {
            assert_eq!(
                proto.parse_response(1, &response[..end]).unwrap(),
                Parsed::Incomplete,
                "{end}"
            );
        }
        assert_eq!(proto.parse_response(1, &response).unwrap(), Parsed::ok(response.len(), (1, 0)));
        let coalesced = [response.as_slice(), next].concat();
        assert_eq!(
            proto.parse_response(1, &coalesced).unwrap(),
            Parsed::ok(response.len(), (1, 1))
        );
    }

    #[test]
    fn test_chunked_pipelined_responses() {
        let responses = [
            chunked("2\r\nhi\r\n0\r\n\r\n"),
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc".to_vec(),
            chunked("0\r\nTrailer: x\r\n\r\n"),
            chunked("1\r\n0\r\n0\r\n\r\n"),
        ];
        let wire = responses.concat();
        let mut proto = HttpProtocol::default();
        let mut offset = 0;
        for (seq, response) in responses.iter().enumerate() {
            let parsed = proto.parse_response(2, &wire[offset..]).unwrap();
            assert_eq!(parsed, Parsed::ok(response.len(), (2, seq as u64)));
            offset += parsed.consumed();
        }
        assert_eq!(offset, wire.len());
    }

    #[test]
    fn test_chunked_rejects_malformed_sizes() {
        for body in [
            "\r\n",
            "g\r\n",
            "-1\r\n",
            "+1\r\n",
            "0x5\r\n",
            " 5\r\n",
            "5 \r\n",
            "5x\r\nhello\r\n0\r\n\r\n",
            "5;\x01\r\nhello\r\n0\r\n\r\n",
            "10000000000000000\r\n",
            "ffffffffffffffffff\r\n",
        ] {
            assert!(parse_one(&chunked(body)).is_err(), "{body:?}");
        }
        // Huge sizes that fit are incomplete; the core limits the buffered length
        assert_eq!(parse_one(&chunked("ffffffff\r\nabc")).unwrap(), Parsed::Incomplete);
    }

    #[test]
    fn test_chunked_rejects_missing_crlf() {
        for body in [
            "5\nhello\r\n0\r\n\r\n",
            "5\rhello\r\n0\r\n\r\n",
            "5\r\nhelloX\r\n0\r\n\r\n",
            "5\r\nhello\n0\r\n\r\n",
            "5\r\nhello\r\n0\n\r\n",
            "5\r\nhello\r\n0\r\n\n",
            "5\r\nhello\r\n0\r\nX: y\n\r\n",
            "5\r\nhello\r\n0\r\nnot a field\r\n\r\n",
            "5\r\nhello\r\n0\r\n folded: x\r\n\r\n",
        ] {
            assert!(parse_one(&chunked(body)).is_err(), "{body:?}");
        }
    }

    #[test]
    fn test_chunked_oversize_response_is_one_frame() {
        // The parser frames responses of any size; the core rejects frames
        // over its MAX_PAYLOAD (64 KiB) using the consumed length
        let chunk = "y".repeat(40_000);
        let body = format!("9c40\r\n{chunk}\r\n9c40\r\n{chunk}\r\n0\r\n\r\n");
        let response = chunked(&body);
        assert!(response.len() > 80_000);
        assert_eq!(parse_one(&response[..70_000]).unwrap(), Parsed::Incomplete);
        assert_eq!(parse_one(&response).unwrap(), Parsed::ok(response.len(), (0, 0)));
    }

    #[test]
    fn test_transfer_encoding_must_be_chunked_only() {
        for value in ["Chunked", "chunked ", " chunked, ", ",chunked"] {
            let response =
                format!("HTTP/1.1 200 OK\r\nTransfer-Encoding: {value}\r\n\r\n0\r\n\r\n");
            assert_eq!(
                parse_one(response.as_bytes()).unwrap().consumed(),
                response.len(),
                "{value:?}"
            );
        }
        for headers in [
            "Transfer-Encoding: gzip\r\n",
            "Transfer-Encoding: gzip, chunked\r\n",
            "Transfer-Encoding: chunked, gzip\r\n",
            "Transfer-Encoding: chunked, chunked\r\n",
            "Transfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n",
            "Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n",
            "Transfer-Encoding: identity\r\n",
            "Transfer-Encoding: chunked;q=1\r\n",
            "Transfer-Encoding: \r\n",
        ] {
            let response = format!("HTTP/1.1 200 OK\r\n{headers}\r\n0\r\n\r\n");
            let error = parse_one(response.as_bytes()).unwrap_err().to_string();
            assert!(error.contains("Transfer-Encoding"), "{headers:?}: {error}");
        }
    }

    #[test]
    fn test_transfer_encoding_with_content_length_is_rejected() {
        for headers in [
            "Transfer-Encoding: chunked\r\nContent-Length: 5\r\n",
            "Content-Length: 5\r\nTransfer-Encoding: chunked\r\n",
        ] {
            let response = format!("HTTP/1.1 200 OK\r\n{headers}\r\n0\r\n\r\n");
            let error = parse_one(response.as_bytes()).unwrap_err().to_string();
            assert!(error.contains("both"), "{headers:?}: {error}");
        }
    }

    #[test]
    fn test_bodyless_responses_ignore_framing_headers() {
        let next = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        for head in [
            "HTTP/1.1 204 No Content\r\nTransfer-Encoding: chunked\r\n\r\n",
            "HTTP/1.1 304 Not Modified\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
            "HTTP/1.1 304 Not Modified\r\nContent-Length: 10\r\n\r\n",
        ] {
            let wire = [head.as_bytes(), next].concat();
            assert_eq!(parse_one(&wire).unwrap(), Parsed::ok(head.len(), (0, 0)), "{head:?}");
        }
        // Interim responses are counted in the final response's bytes
        let interim = "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nTransfer-Encoding: chunked\r\n\r\n";
        let response = [interim.as_bytes(), &chunked("1\r\nx\r\n0\r\n\r\n")].concat();
        assert_eq!(parse_one(&response).unwrap(), Parsed::ok(response.len(), (0, 0)));
    }

    #[test]
    fn test_keep_alive_header_and_config_default() {
        // HttpConfig defaults keep_alive to true (both Default and serde)
        let default_cfg = crate::configs::HttpConfig::default();
        assert!(default_cfg.keep_alive);
        let parsed_default: crate::configs::HttpConfig =
            serde_json::from_str(r#"{"method":"GET","path":"/"}"#).unwrap();
        assert!(parsed_default.keep_alive);

        let mut keep_alive_proto = HttpProtocol::from_config(&parsed_default, None).unwrap();
        let req = String::from_utf8(keep_alive_proto.next_request(0).data).unwrap();
        assert!(req.contains("Connection: keep-alive\r\n"), "{req}");
        assert!(!req.contains("Connection: close\r\n"), "{req}");

        let parsed_close: crate::configs::HttpConfig =
            serde_json::from_str(r#"{"method":"GET","path":"/health","keep_alive":false}"#)
                .unwrap();
        assert!(!parsed_close.keep_alive);
        let mut close_proto = HttpProtocol::from_config(&parsed_close, None).unwrap();
        let req = String::from_utf8(close_proto.next_request(0).data).unwrap();
        assert!(req.contains("Connection: close\r\n"), "{req}");
        assert!(!req.contains("Connection: keep-alive\r\n"), "{req}");
    }

    #[test]
    fn test_verify_pattern_reference_vectors_and_unaligned_chunks() {
        // Independent byte-by-byte reference implementation of the documented formula
        fn reference_body(seed: u64, path: &[u8], body_size: usize) -> Vec<u8> {
            let mut h = 0xcbf2_9ce4_8422_2325u64;
            for &b in path {
                h ^= u64::from(b);
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            let sm = |mut x: u64| {
                x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
                x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                x ^ (x >> 31)
            };
            let base = sm(seed ^ h ^ (body_size as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
            let mut out = vec![0u8; body_size];
            for (i, b) in out.iter_mut().enumerate() {
                let w = (i / 8) as u64;
                let word = sm(base.wrapping_add(w.wrapping_mul(0x9e37_79b9_7f4a_7c15)));
                *b = word.to_le_bytes()[i % 8];
            }
            out
        }

        // Fixed reference vector check (locks down exact byte output)
        let mut vec16 = [0u8; 16];
        fill_verify_pattern(0, b"/", 16, &mut vec16);
        assert_eq!(vec16, reference_body(0, b"/", 16).as_slice());
        assert_eq!(check_verify_pattern(0, b"/", 16, &vec16), None);

        // Distinct seeds, paths, and body sizes produce distinct patterns
        let mut vec16_seed = [0u8; 16];
        fill_verify_pattern(42, b"/", 16, &mut vec16_seed);
        assert_ne!(vec16, vec16_seed);

        let mut vec16_path = [0u8; 16];
        fill_verify_pattern(0, b"/a", 16, &mut vec16_path);
        assert_ne!(vec16, vec16_path);

        // Arbitrary sizes and unaligned chunk splits
        for size in [1usize, 7, 13, 64, 4096, 65536, 98304] {
            let seed = 1337u64;
            let path = b"/obj/k42";
            let expected = reference_body(seed, path, size);
            let mut actual = vec![0u8; size];
            fill_verify_pattern(seed, path, size, &mut actual);
            assert_eq!(actual, expected, "size={size}");
            assert_eq!(check_verify_pattern(seed, path, size, &actual), None, "size={size}");

            let base = pattern_base(seed, path, size);
            for split in [1usize, 7, 13, 4096, 65536] {
                let mut off = 0usize;
                while off < size {
                    let end = (off + split).min(size);
                    assert_eq!(
                        check_verify_pattern_chunk(base, off, &actual[off..end]),
                        None,
                        "size={size} split={split} off={off}"
                    );
                    off = end;
                }
            }

            // Flip one byte at start, middle, and end; check detection across unaligned split
            for bad_off in [0usize, size / 2, size - 1] {
                let mut corrupted = actual.clone();
                corrupted[bad_off] ^= 0x55;
                let expected_mismatch = Some((bad_off as u64, actual[bad_off], corrupted[bad_off]));
                assert_eq!(
                    check_verify_pattern(seed, path, size, &corrupted),
                    expected_mismatch,
                    "size={size} bad_off={bad_off}"
                );
                // Verify chunk-by-chunk with awkward split=13
                let found = (0..size).step_by(13).find_map(|off| {
                    let end = (off + 13).min(size);
                    check_verify_pattern_chunk(base, off, &corrupted[off..end])
                });
                assert_eq!(found, expected_mismatch, "chunked size={size} bad_off={bad_off}");
            }
        }
    }

    #[test]
    fn test_streaming_content_length_verify_and_errors() {
        // 96 KiB body streamed in 4096-byte chunks
        let size = 98_304usize;
        let seed = 99u64;
        let mut proto =
            HttpProtocol::new(HttpMethod::Get, "/blob".to_string(), "localhost".to_string())
                .with_verify(seed, size);
        let _ = proto.next_request(0);

        let mut body = vec![0u8; size];
        fill_verify_pattern(seed, b"/blob", size, &mut body);
        let mut wire = format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\n\r\n").into_bytes();
        wire.extend_from_slice(&body);

        let mut pos = 0usize;
        let mut total_consumed = 0usize;
        let mut completed = None;
        while pos < wire.len() {
            let end = (pos + 4096).min(wire.len());
            let parsed = proto.parse_response(0, &wire[pos..end]).unwrap();
            let c = parsed.consumed();
            assert!(c <= MAX_CHUNK);
            total_consumed += c;
            pos += c;
            if let Parsed::Complete { id, error, .. } = parsed {
                completed = Some((id, error));
                break;
            }
        }
        assert_eq!(total_consumed, wire.len());
        assert_eq!(completed, Some(((0, 0), None)));
        let report = proto.verification_report().unwrap();
        assert_eq!(report.verified, 1);
        assert_eq!(report.failures, 0);

        // ShortBody on connection close mid-body
        let _ = proto.next_request(1);
        let partial = &wire[..wire.len() - 50_000];
        let parsed = proto.parse_response(1, partial).unwrap();
        assert!(matches!(parsed, Parsed::Absorbed(_)));
        let err = proto.on_connection_closed(1).expect("expected ShortBody");
        assert_eq!(err.kind, "ShortBody");
        let report = proto.verification_report().unwrap();
        assert_eq!(report.failures, 1);
        let first = report.first_mismatch.unwrap();
        assert_eq!(first.kind, "ShortBody");
        assert_eq!(first.expected_len, Some(size));

        // Reset clears verification counters and in-flight state
        proto.reset();
        assert_eq!(proto.verification_report(), Some(VerificationReport::default()));
        assert_eq!(proto.on_connection_closed(1), None);
    }

    #[test]
    fn test_chunked_verify_ok_short_long_and_corrupt() {
        let seed = 7u64;
        let size = 32usize;
        let mut body = vec![0u8; size];
        fill_verify_pattern(seed, b"/", size, &mut body);

        let make_chunked = |parts: &[&[u8]]| -> Vec<u8> {
            let mut out = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
            for p in parts {
                out.extend_from_slice(format!("{:x}\r\n", p.len()).as_bytes());
                out.extend_from_slice(p);
                out.extend_from_slice(b"\r\n");
            }
            out.extend_from_slice(b"0\r\n\r\n");
            out
        };

        // Valid split across 3 chunks
        let mut proto = HttpProtocol::default().with_verify(seed, size);
        let ok_wire = make_chunked(&[&body[..5], &body[5..19], &body[19..]]);
        assert_eq!(proto.parse_response(0, &ok_wire).unwrap(), Parsed::ok(ok_wire.len(), (0, 0)));
        assert_eq!(proto.verification_report().unwrap().verified, 1);

        // Short chunked body -> ShortBody
        let mut proto = HttpProtocol::default().with_verify(seed, size);
        let short_wire = make_chunked(&[&body[..10], &body[10..20]]);
        let parsed = proto.parse_response(0, &short_wire).unwrap();
        assert!(matches!(parsed, Parsed::Complete { error: Some(e), .. } if e.kind == "ShortBody"));

        // Long chunked body -> ContentLengthMismatch
        let mut proto = HttpProtocol::default().with_verify(seed, size);
        let long_wire = make_chunked(&[&body, b"extra"]);
        let parsed = proto.parse_response(0, &long_wire).unwrap();
        assert!(
            matches!(parsed, Parsed::Complete { error: Some(e), .. } if e.kind == "ContentLengthMismatch")
        );

        // Corrupted byte -> BodyMismatch
        let mut proto = HttpProtocol::default().with_verify(seed, size);
        let mut bad = body.clone();
        bad[17] ^= 0xff;
        let bad_wire = make_chunked(&[&bad[..11], &bad[11..]]);
        let parsed = proto.parse_response(0, &bad_wire).unwrap();
        assert!(
            matches!(parsed, Parsed::Complete { error: Some(e), .. } if e.kind == "BodyMismatch")
        );
        let first = proto.verification_report().unwrap().first_mismatch.unwrap();
        assert_eq!(first.offset, 17);
        assert_eq!(first.expected, Some(body[17]));
        assert_eq!(first.actual, Some(bad[17]));
    }
}
