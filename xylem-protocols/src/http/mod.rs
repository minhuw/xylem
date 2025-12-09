//! HTTP/1.1 protocol implementation
//!
//! Supports GET, POST, PUT methods with proper HTTP parsing.
//! Uses httparse for zero-copy, fast HTTP response parsing.
//!
//! Request sequence tracking is done per-connection to support pipelining.

use crate::Protocol;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use zeropool::BufferPool;

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
    method: HttpMethod,
    path: String,
    host: String,
    /// Body size for POST/PUT requests
    body_size: usize,
    /// Per-connection sequence numbers for request tracking (TX side)
    conn_seq: HashMap<usize, u64>,
    /// Per-connection expected response sequence (RX side - responses are FIFO)
    conn_response_seq: HashMap<usize, u64>,
    /// Buffer pool for request generation
    pool: BufferPool,
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
            method,
            path,
            host,
            body_size,
            conn_seq: HashMap::new(),
            conn_response_seq: HashMap::new(),
            pool: BufferPool::new(),
        }
    }

    fn next_seq(&mut self, conn_id: usize) -> u64 {
        let seq = self.conn_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        *seq = seq.wrapping_add(1);
        result
    }

    /// Generate HTTP request with optional body for POST/PUT
    fn build_request(&self, body: Option<&[u8]>) -> Vec<u8> {
        let mut request = format!(
            "{} {} HTTP/1.1\r\n\
             Host: {}\r\n\
             Connection: keep-alive\r\n",
            self.method.as_str(),
            self.path,
            self.host
        );

        if let Some(body_data) = body {
            request.push_str(&format!("Content-Length: {}\r\n", body_data.len()));
            request.push_str("Content-Type: application/octet-stream\r\n");
            request.push_str("\r\n");

            let mut bytes = request.into_bytes();
            bytes.extend_from_slice(body_data);
            bytes
        } else {
            request.push_str("\r\n");
            request.into_bytes()
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

    fn next_request(&mut self, conn_id: usize) -> (Vec<u8>, Self::RequestId, crate::RequestMeta) {
        let seq = self.next_seq(conn_id);

        let body = match self.method {
            HttpMethod::Get => None,
            HttpMethod::Post | HttpMethod::Put => {
                // Generate body filled with 'x' for POST/PUT using pool
                let mut buf = self.pool.get(self.body_size);
                buf.clear();
                buf.resize(self.body_size, b'x');
                Some(buf.to_vec())
            }
        };

        let request = self.build_request(body.as_deref());
        // HTTP protocol doesn't have a warmup phase
        (request, (conn_id, seq), crate::RequestMeta::measurement())
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        if data.is_empty() {
            return Ok((0, None));
        }

        // Parse HTTP response headers
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut response = httparse::Response::new(&mut headers);

        match response.parse(data) {
            Ok(httparse::Status::Complete(headers_len)) => {
                // Successfully parsed headers
                let status_code = response.code.unwrap_or(0);

                if !(200..600).contains(&status_code) {
                    return Err(anyhow!("Invalid HTTP status code: {}", status_code));
                }

                // Find Content-Length header to determine body size
                let content_length = response
                    .headers
                    .iter()
                    .find(|h| h.name.eq_ignore_ascii_case("content-length"))
                    .and_then(|h| std::str::from_utf8(h.value).ok())
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(0);

                // Check if we have the complete response (headers + body)
                let total_len = headers_len + content_length;
                if data.len() >= total_len {
                    // Complete response received
                    // HTTP/1.1 responses arrive in order (FIFO), so use sequential response tracking
                    let response_seq = self.conn_response_seq.entry(conn_id).or_insert(0);
                    let seq = *response_seq;
                    *response_seq = response_seq.wrapping_add(1);
                    Ok((total_len, Some((conn_id, seq))))
                } else {
                    // Incomplete response - need more data
                    Ok((0, None))
                }
            }
            Ok(httparse::Status::Partial) => {
                // Need more data to complete header parsing
                Ok((0, None))
            }
            Err(e) => Err(anyhow!("HTTP parse error: {}", e)),
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

        let (req, (conn_id, seq), _meta) = proto.next_request(0);
        let req_str = String::from_utf8_lossy(&req);

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

        let (req, (conn_id, seq), _meta) = proto.next_request(0);
        let req_str = String::from_utf8_lossy(&req);

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
        let (_, (_, seq1), _) = proto.next_request(0);
        let (_, (_, seq2), _) = proto.next_request(0);
        let (_, (_, seq3), _) = proto.next_request(0);

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
