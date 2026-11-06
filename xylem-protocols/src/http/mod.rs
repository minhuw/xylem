//! HTTP/1.1 protocol implementation
//!
//! Supports GET, POST, PUT with Content-Length or bodyless responses.
//! Transfer-Encoding and close-delimited bodies are explicitly unsupported.
//! Informational responses are consumed without completing a request.
//! Uses httparse for zero-copy, fast HTTP response parsing.
//!
//! Request sequence tracking is done per-connection to support pipelining.

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
            for header in response.headers.iter() {
                anyhow::ensure!(
                    !header.name.eq_ignore_ascii_case("transfer-encoding"),
                    "HTTP Transfer-Encoding is unsupported"
                );
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
            let body_length = if status_code == 204 || status_code == 304 {
                0
            } else {
                content_length.ok_or_else(|| anyhow!("HTTP response requires Content-Length; close-delimited bodies are unsupported"))?
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
}
