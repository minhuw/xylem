//! Requests built from the HTTP/2 and gRPC configs

use super::config::{GrpcConfig, Http2Config};
use super::{hpack, Exchange, Http2Protocol};
use anyhow::{anyhow, bail, ensure, Context, Result};

/// Request headers xylem sets itself, which `metadata` may not
const RESERVED: [&str; 9] = [
    "content-type",
    "te",
    "user-agent",
    "host",
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "upgrade",
];

/// gRPC's message prefix: the compression flag and the length
const GRPC_PREFIX_LEN: usize = 5;

/// Fail if `value` of the request's `what` could not go in a header field
fn ensure_field_value(what: &str, value: &str) -> Result<()> {
    ensure!(
        !value.bytes().any(|b| b == b'\r' || b == b'\n' || b == 0),
        "{what} {value:?} contains CR, LF or NUL"
    );
    Ok(())
}

/// Fail unless `value` is a path or authority without whitespace or
/// control characters
fn ensure_plain(what: &str, value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && !value.bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control()),
        "Invalid HTTP/2 {what} {value:?}"
    );
    Ok(())
}

impl Http2Config {
    /// Check the config, as [`Http2Protocol::from_config`] does
    pub fn validate(&self) -> Result<()> {
        parse_method(&self.method)?;
        ensure!(self.path.starts_with('/'), "HTTP/2 path {:?} must start with '/'", self.path);
        ensure_plain("path", &self.path)?;
        if let Some(authority) = &self.authority {
            ensure_plain("authority", authority)?;
        }
        Ok(())
    }
}

/// The method's name, if it is GET, POST or PUT, and whether it has a body
fn parse_method(method: &str) -> Result<(&'static str, bool)> {
    match method.to_ascii_uppercase().as_str() {
        "GET" => Ok(("GET", false)),
        "POST" => Ok(("POST", true)),
        "PUT" => Ok(("PUT", true)),
        _ => bail!("Unknown HTTP/2 method: {method}. Supported: GET, POST, PUT"),
    }
}

impl GrpcConfig {
    /// The `:path` of a call: "/package.Service/Method"
    pub fn path(&self) -> Result<String> {
        let call = self.call.strip_prefix('/').unwrap_or(&self.call);
        let valid = call.split_once('/').is_some_and(|(service, method)| {
            !service.is_empty() && !method.is_empty() && !method.contains('/')
        });
        ensure!(
            valid,
            "gRPC call {:?} is not a full method name \"package.Service/Method\"",
            self.call
        );
        ensure_plain("gRPC call", call)?;
        Ok(format!("/{call}"))
    }

    /// The request messages, each with its 5-byte gRPC prefix, read from
    /// the configured source
    pub fn messages(&self) -> Result<Vec<Vec<u8>>> {
        let sources = [
            self.request_file.is_some(),
            self.request_hex.is_some(),
            self.request_base64.is_some(),
            self.messages_file.is_some(),
        ];
        ensure!(
            sources.iter().filter(|&&set| set).count() <= 1,
            "gRPC protocol_config sets more than one of request_file, request_hex, \
             request_base64 and messages_file"
        );
        let message = if let Some(path) = &self.request_file {
            std::fs::read(path)
                .with_context(|| format!("Cannot read gRPC request_file {}", path.display()))?
        } else if let Some(hex) = &self.request_hex {
            decode_hex(hex).context("Invalid gRPC request_hex")?
        } else if let Some(base64) = &self.request_base64 {
            decode_base64(base64).context("Invalid gRPC request_base64")?
        } else if let Some(path) = &self.messages_file {
            let data = std::fs::read(path)
                .with_context(|| format!("Cannot read gRPC messages_file {}", path.display()))?;
            return split_messages(&data)
                .with_context(|| format!("Invalid gRPC messages_file {}", path.display()));
        } else {
            Vec::new()
        };
        Ok(vec![frame_message(&message)?])
    }

    /// Check the config, reading the request messages
    pub fn validate(&self) -> Result<()> {
        self.path()?;
        if let Some(authority) = &self.authority {
            ensure_plain("authority", authority)?;
        }
        for (name, value) in &self.metadata {
            ensure!(
                !name.is_empty()
                    && name.bytes().all(|b| b.is_ascii_lowercase()
                        || b.is_ascii_digit()
                        || b"-_.".contains(&b)),
                "gRPC metadata name {name:?} must be lowercase letters, digits, '-', '_' or '.'"
            );
            ensure!(
                !RESERVED.contains(&name.as_str()) && !name.starts_with("grpc-"),
                "gRPC metadata {name:?} is set by xylem or reserved by gRPC"
            );
            ensure_field_value("gRPC metadata", value)?;
        }
        self.messages().map(drop)
    }
}

/// `message` with gRPC's prefix: uncompressed, and its length
fn frame_message(message: &[u8]) -> Result<Vec<u8>> {
    let len = u32::try_from(message.len()).map_err(|_| anyhow!("gRPC message over 4 GiB"))?;
    let mut framed = Vec::with_capacity(GRPC_PREFIX_LEN + message.len());
    framed.push(0);
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(message);
    Ok(framed)
}

/// The prefixed messages that make up `data`
fn split_messages(mut data: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut messages = Vec::new();
    while !data.is_empty() {
        ensure!(data.len() >= GRPC_PREFIX_LEN, "it ends inside a message prefix");
        ensure!(data[0] == 0, "message {} has compression flag {}", messages.len(), data[0]);
        let len = u32::from_be_bytes(data[1..5].try_into().expect("four bytes")) as usize;
        let end = GRPC_PREFIX_LEN + len;
        ensure!(data.len() >= end, "message {} is cut short", messages.len());
        messages.push(data[..end].to_vec());
        data = &data[end..];
    }
    ensure!(!messages.is_empty(), "it holds no message");
    Ok(messages)
}

fn decode_hex(text: &str) -> Result<Vec<u8>> {
    let digits: Vec<u8> = text.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    ensure!(digits.len().is_multiple_of(2), "odd number of hex digits");
    let (pairs, _) = digits.as_chunks::<2>();
    pairs
        .iter()
        .map(|pair| {
            let digit = |b: u8| {
                (b as char)
                    .to_digit(16)
                    .ok_or_else(|| anyhow!("{:?} is no hex digit", b as char))
            };
            Ok((digit(pair[0])? << 4 | digit(pair[1])?) as u8)
        })
        .collect()
}

/// Decode standard base64 (RFC 4648 section 4); padding is optional and
/// whitespace is skipped
fn decode_base64(text: &str) -> Result<Vec<u8>> {
    let symbols: Vec<u8> = text.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    let data = symbols
        .strip_suffix(b"==")
        .or_else(|| symbols.strip_suffix(b"="))
        .unwrap_or(&symbols);
    ensure!(data.len() % 4 != 1, "base64 of {} symbols", data.len());
    let mut out = Vec::with_capacity(data.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0);
    for &symbol in data {
        let value = match symbol {
            b'A'..=b'Z' => symbol - b'A',
            b'a'..=b'z' => symbol - b'a' + 26,
            b'0'..=b'9' => symbol - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => bail!("{:?} is no base64 symbol", symbol as char),
        };
        acc = acc << 6 | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    ensure!(acc & ((1 << bits) - 1) == 0, "base64 has nonzero trailing bits");
    Ok(out)
}

impl Http2Protocol {
    /// An HTTP/2 workload; `authority` is used unless the config sets one
    pub fn from_config(config: &Http2Config, authority: &str) -> Result<Self> {
        config.validate()?;
        let authority = config.authority.as_deref().unwrap_or(authority);
        ensure_plain("authority", authority)?;
        let (method, has_body) = parse_method(&config.method)?;
        let mut headers = Vec::new();
        hpack::encode_field(&mut headers, ":method", method);
        hpack::encode_field(&mut headers, ":scheme", "http");
        hpack::encode_field(&mut headers, ":path", &config.path);
        hpack::encode_field(&mut headers, ":authority", authority);
        hpack::encode_field(&mut headers, "user-agent", "xylem");
        let body = if has_body {
            hpack::encode_field(&mut headers, "content-type", "application/octet-stream");
            hpack::encode_field(&mut headers, "content-length", &config.body_size.to_string());
            vec![b'x'; config.body_size]
        } else {
            Vec::new()
        };
        Ok(Self::new(Exchange::Http, headers, vec![body]))
    }

    /// A gRPC workload; `authority` is used unless the config sets one
    pub fn grpc_from_config(config: &GrpcConfig, authority: &str) -> Result<Self> {
        config.validate()?;
        let authority = config.authority.as_deref().unwrap_or(authority);
        ensure_plain("authority", authority)?;
        let mut headers = Vec::new();
        hpack::encode_field(&mut headers, ":method", "POST");
        hpack::encode_field(&mut headers, ":scheme", "http");
        hpack::encode_field(&mut headers, ":path", &config.path()?);
        hpack::encode_field(&mut headers, ":authority", authority);
        hpack::encode_field(&mut headers, "content-type", "application/grpc");
        hpack::encode_field(&mut headers, "te", "trailers");
        hpack::encode_field(&mut headers, "user-agent", "grpc-xylem");
        for (name, value) in &config.metadata {
            hpack::encode_field(&mut headers, name, value);
        }
        Ok(Self::new(Exchange::Grpc, headers, config.messages()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grpc(fields: serde_json::Value) -> GrpcConfig {
        let mut config = serde_json::json!({ "call": "pkg.Echo/Say" });
        config.as_object_mut().unwrap().extend(fields.as_object().unwrap().clone());
        serde_json::from_value(config).unwrap()
    }

    #[test]
    fn decodes_hex_and_base64() {
        assert_eq!(decode_hex("0a03 4142\n43").unwrap(), b"\x0a\x03ABC");
        assert!(decode_hex("abc").is_err());
        assert!(decode_hex("zz").is_err());
        for (text, bytes) in [
            ("", &b""[..]),
            ("Zg==", b"f"),
            ("Zm8=", b"fo"),
            ("Zm9v", b"foo"),
            ("Zm9vYg", b"foob"),
            ("+/+/", b"\xfb\xff\xbf"),
        ] {
            assert_eq!(decode_base64(text).unwrap(), bytes, "{text}");
        }
        for text in ["Z", "Zh==", "Zm9v!", "-_"] {
            assert!(decode_base64(text).is_err(), "{text}");
        }
    }

    #[test]
    fn grpc_calls_need_a_full_method_name() {
        assert_eq!(grpc(serde_json::json!({})).path().unwrap(), "/pkg.Echo/Say");
        let config = grpc(serde_json::json!({ "call": "/a.B/C" }));
        assert_eq!(config.path().unwrap(), "/a.B/C");
        for call in ["Say", "pkg.Echo/", "/Say", "a/b/c", "a b/c"] {
            assert!(grpc(serde_json::json!({ "call": call })).validate().is_err(), "{call}");
        }
    }

    #[test]
    fn grpc_messages_come_from_one_source() {
        assert_eq!(grpc(serde_json::json!({})).messages().unwrap(), [vec![0, 0, 0, 0, 0]]);
        let hex = grpc(serde_json::json!({ "request_hex": "0a0141" }));
        assert_eq!(hex.messages().unwrap(), [vec![0, 0, 0, 0, 3, 0x0a, 1, b'A']]);
        let base64 = grpc(serde_json::json!({ "request_base64": "CgFB" }));
        assert_eq!(base64.messages().unwrap(), hex.messages().unwrap());
        let both = grpc(serde_json::json!({ "request_hex": "00", "request_base64": "AA==" }));
        assert!(both.validate().unwrap_err().to_string().contains("more than one"));

        let dir = std::env::temp_dir().join(format!("xylem-grpc-messages-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let raw = dir.join("request.bin");
        std::fs::write(&raw, b"\x0a\x01A").unwrap();
        let file = grpc(serde_json::json!({ "request_file": raw }));
        assert_eq!(file.messages().unwrap(), hex.messages().unwrap());
        let framed = dir.join("messages.bin");
        std::fs::write(&framed, b"\0\0\0\0\x01x\0\0\0\0\0").unwrap();
        let file = grpc(serde_json::json!({ "messages_file": framed }));
        assert_eq!(file.messages().unwrap(), [b"\0\0\0\0\x01x".to_vec(), vec![0; 5]]);
        for bad in [&b"\0\0\0\0\x02x"[..], b"\x01\0\0\0\0", b"\0\0", b""] {
            std::fs::write(&framed, bad).unwrap();
            assert!(file.messages().is_err(), "{bad:?}");
        }
        let missing = grpc(serde_json::json!({ "request_file": dir.join("absent") }));
        assert!(missing.validate().unwrap_err().to_string().contains("Cannot read"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn grpc_metadata_names_are_checked() {
        let ok = grpc(serde_json::json!({ "metadata": { "x-tenant": "a b", "auth_2": "t" } }));
        ok.validate().unwrap();
        for (name, value) in [
            ("X-Upper", "v"),
            ("te", "trailers"),
            ("grpc-timeout", "1S"),
            (":path", "/"),
            ("", "v"),
            ("x-crlf", "a\r\nb: c"),
        ] {
            let config = grpc(serde_json::json!({ "metadata": { name: value } }));
            assert!(config.validate().is_err(), "{name}: {value:?}");
        }
    }

    #[test]
    fn http2_config_is_checked() {
        Http2Config::default().validate().unwrap();
        for (method, path, authority) in [
            ("DELETE", "/", None),
            ("GET", "x", None),
            ("GET", "/a b", None),
            ("GET", "/", Some("a b")),
        ] {
            let config = Http2Config {
                method: method.to_string(),
                path: path.to_string(),
                authority: authority.map(str::to_string),
                body_size: 0,
            };
            assert!(config.validate().is_err(), "{method} {path} {authority:?}");
        }
    }
}
