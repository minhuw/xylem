//! HTTP/1.1 protocol implementation
//!
//! Supports GET, POST, PUT with Content-Length, chunked, or bodyless responses.
//! Other transfer codings and close-delimited bodies are explicitly unsupported.
//! Informational responses are consumed without completing a request.
//! Uses httparse for zero-copy, fast HTTP response parsing.
//!
//! Request sequence tracking is done per-connection to support pipelining.

mod config;

use crate::Protocol;
use anyhow::{anyhow, Result};
use std::collections::HashMap;

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

pub struct HttpProtocol {
    /// Request bytes, built once at construction and sent for every request
    request: Vec<u8>,
    /// Per-connection sequence numbers for request tracking (TX side)
    conn_seq: HashMap<usize, u64>,
    /// Per-connection expected response sequence (RX side - responses are FIFO)
    conn_response_seq: HashMap<usize, u64>,
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
        Self {
            request: Self::build_request(method, &path, &host, body_size),
            conn_seq: HashMap::new(),
            conn_response_seq: HashMap::new(),
        }
    }

    fn next_seq(&mut self, conn_id: usize) -> u64 {
        let seq = self.conn_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        *seq = seq.wrapping_add(1);
        result
    }

    /// Generate HTTP request, with a body of `body_size` 'x' bytes for POST/PUT
    fn build_request(method: HttpMethod, path: &str, host: &str, body_size: usize) -> Vec<u8> {
        let mut request = format!(
            "{} {} HTTP/1.1\r\n\
             Host: {}\r\n\
             Connection: keep-alive\r\n",
            method.as_str(),
            path,
            host
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
        // HTTP protocol doesn't have a warmup phase
        crate::Request::measurement(self.request.clone(), (conn_id, seq))
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        if data.is_empty() {
            return Ok((0, None));
        }

        let mut offset = 0;
        loop {
            let remaining = &data[offset..];
            // Parse HTTP response headers
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut response = httparse::Response::new(&mut headers);

            let headers_len = match response.parse(remaining) {
                Ok(httparse::Status::Complete(length)) => length,
                Ok(httparse::Status::Partial) => return Ok((0, None)),
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
            let body_length = if status_code == 204 || status_code == 304 {
                0
            } else if transfer_encoding {
                // Transfer-Encoding overrides Content-Length, but a response with
                // both "ought to be handled as an error"; the sender broke a MUST NOT
                anyhow::ensure!(
                    content_length.is_none(),
                    "HTTP response has both Transfer-Encoding and Content-Length"
                );
                ensure_chunked_only(response.headers)?;
                match chunked_body_len(&remaining[headers_len..])? {
                    Some(length) => length,
                    None => return Ok((0, None)),
                }
            } else {
                content_length.ok_or_else(|| anyhow!("HTTP response requires Content-Length or chunked Transfer-Encoding; close-delimited bodies are unsupported"))?
            };
            let total_len = headers_len
                .checked_add(body_length)
                .ok_or_else(|| anyhow!("HTTP response length overflow"))?;
            if remaining.len() < total_len {
                return Ok((0, None));
            }
            let response_seq = self.conn_response_seq.entry(conn_id).or_insert(0);
            let seq = *response_seq;
            *response_seq = response_seq.wrapping_add(1);
            return Ok((offset + total_len, Some((conn_id, seq))));
        }
    }

    fn name(&self) -> &'static str {
        "http"
    }

    fn reset(&mut self) {
        self.conn_seq.clear();
        self.conn_response_seq.clear();
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

        let (bytes_consumed, req_id) = proto.parse_response(0, response).unwrap();

        assert_eq!(bytes_consumed, response.len());
        assert_eq!(req_id, Some((0, 0)));
    }

    /// A 200 response with a chunked body made of `body`
    fn chunked(body: &str) -> Vec<u8> {
        format!("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{body}").into_bytes()
    }

    /// Parse `data` on a fresh connection, expecting one whole response
    fn parse_one(data: &[u8]) -> Result<(usize, Option<(usize, u64)>)> {
        HttpProtocol::default().parse_response(0, data)
    }

    #[test]
    fn test_chunked_hex_sizes_in_either_case() {
        let alphabet = "0123456789abcdefghijklmnopqrstuvwxyz";
        for size in ["1a", "1A", "01A", "1f", "1F"] {
            let len = usize::from_str_radix(size, 16).unwrap();
            let response = chunked(&format!("{size}\r\n{}\r\n0\r\n\r\n", &alphabet[..len]));
            assert_eq!(parse_one(&response).unwrap(), (response.len(), Some((0, 0))), "{size}");
        }
        let response = [chunked("aB\r\n"), vec![b'x'; 0xab], b"\r\n0\r\n\r\n".to_vec()].concat();
        assert_eq!(parse_one(&response).unwrap().0, response.len());
    }

    #[test]
    fn test_chunked_extensions_are_ignored() {
        for body in [
            "5;name=value\r\nhello\r\n0\r\n\r\n",
            "5;a;b=\"quoted; value\"\r\nhello\r\n0;last\r\n\r\n",
            "5 ; a = b\r\nhello\r\n000\t;x\r\n\r\n",
        ] {
            let response = chunked(body);
            assert_eq!(parse_one(&response).unwrap(), (response.len(), Some((0, 0))), "{body:?}");
        }
    }

    #[test]
    fn test_chunked_trailers_are_skipped() {
        let response = chunked("3\r\nabc\r\n0\r\nExpires: never\r\nX-Checksum:\t1234\r\n\r\n");
        assert_eq!(parse_one(&response).unwrap(), (response.len(), Some((0, 0))));
        // A trailer that looks like a response does not end the message early
        let response = chunked("0\r\nX-Next: HTTP/1.1 200 OK\r\n\r\n");
        assert_eq!(parse_one(&response).unwrap(), (response.len(), Some((0, 0))));
    }

    #[test]
    fn test_chunked_zero_length_body() {
        for body in ["0\r\n\r\n", "000\r\n\r\n"] {
            let response = chunked(body);
            assert_eq!(parse_one(&response).unwrap(), (response.len(), Some((0, 0))));
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
            assert_eq!(proto.parse_response(1, &response[..end]).unwrap(), (0, None), "{end}");
        }
        assert_eq!(proto.parse_response(1, &response).unwrap(), (response.len(), Some((1, 0))));
        let coalesced = [response.as_slice(), next].concat();
        assert_eq!(proto.parse_response(1, &coalesced).unwrap(), (response.len(), Some((1, 1))));
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
            let (consumed, id) = proto.parse_response(2, &wire[offset..]).unwrap();
            assert_eq!((consumed, id), (response.len(), Some((2, seq as u64))));
            offset += consumed;
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
        assert_eq!(parse_one(&chunked("ffffffff\r\nabc")).unwrap(), (0, None));
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
        assert_eq!(parse_one(&response[..70_000]).unwrap(), (0, None));
        assert_eq!(parse_one(&response).unwrap(), (response.len(), Some((0, 0))));
    }

    #[test]
    fn test_transfer_encoding_must_be_chunked_only() {
        for value in ["Chunked", "chunked ", " chunked, ", ",chunked"] {
            let response =
                format!("HTTP/1.1 200 OK\r\nTransfer-Encoding: {value}\r\n\r\n0\r\n\r\n");
            assert_eq!(parse_one(response.as_bytes()).unwrap().0, response.len(), "{value:?}");
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
            assert_eq!(parse_one(&wire).unwrap(), (head.len(), Some((0, 0))), "{head:?}");
        }
        // Interim responses are counted in the final response's bytes
        let interim = "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nTransfer-Encoding: chunked\r\n\r\n";
        let response = [interim.as_bytes(), &chunked("1\r\nx\r\n0\r\n\r\n")].concat();
        assert_eq!(parse_one(&response).unwrap(), (response.len(), Some((0, 0))));
    }
}
