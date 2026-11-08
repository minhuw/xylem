//! RESP encoding and framing
//!
//! Requests are written by [`encode_command`], the one RESP writer used by
//! command encoding and command templates.
//! Replies are framed by [`find_resp_message_end`] and classified by
//! [`classify_reply`].

use anyhow::{anyhow, Result};
use std::io::Write;

/// Room reserved for the framing of one element: a `*` or `$` marker, up to
/// 20 length digits and CRLF, plus a bulk string's closing CRLF.
const FRAMING_RESERVE: usize = 25;

/// Encode a command as a RESP array with one bulk string per argument,
/// written into one buffer sized for the whole request.
pub(crate) fn encode_command(args: &[&str]) -> Vec<u8> {
    let capacity =
        FRAMING_RESERVE + args.iter().map(|arg| FRAMING_RESERVE + arg.len()).sum::<usize>();
    let mut request = Vec::with_capacity(capacity);
    // Array header: *<count>\r\n
    write!(request, "*{}\r\n", args.len()).expect("writing to a Vec cannot fail");
    for arg in args {
        // Bulk string: $<length>\r\n<bytes>\r\n
        write!(request, "${}\r\n", arg.len()).expect("writing to a Vec cannot fail");
        request.extend_from_slice(arg.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    request
}

pub(crate) enum ReplyKind<'a> {
    Success,
    /// A top-level error: the text of a simple error, or the content of a
    /// RESP3 blob error
    Error(&'a [u8]),
    /// RESP3 out-of-band push message, which answers no request
    Push,
}

/// Classify one complete reply, as measured by [`find_resp_message_end`],
/// looking past any attributes to the reply they are attached to.
pub(crate) fn classify_reply(frame: &[u8]) -> Result<ReplyKind<'_>> {
    let mut payload = frame;
    while payload[0] == b'|' {
        let offset = attribute_payload_offset(payload, 0)?;
        payload = &payload[offset..];
    }
    let kind = match payload[0] {
        b'-' => ReplyKind::Error(&payload[1..payload.len() - 2]),
        b'!' => {
            // !<length>\r\n<content>\r\n; a null one has no content
            let start = payload.windows(2).position(|w| w == b"\r\n").map_or(0, |pos| pos + 2);
            ReplyKind::Error(payload.get(start..payload.len() - 2).unwrap_or_default())
        }
        b'>' => ReplyKind::Push,
        _ => ReplyKind::Success,
    };
    Ok(kind)
}

/// Locate the attached reply after an attribute's key/value pairs.
/// Zero means the attribute itself is incomplete.
fn attribute_payload_offset(data: &[u8], depth: usize) -> Result<usize> {
    let Some(end) = data.windows(2).position(|w| w == b"\r\n") else {
        return Ok(0);
    };
    let pairs: usize = std::str::from_utf8(&data[1..end])?.parse()?;
    anyhow::ensure!(pairs <= MAX_ARRAY_SIZE as usize, "Attribute too large");
    let count = pairs.checked_mul(2).ok_or_else(|| anyhow!("Attribute count overflow"))?;
    let mut pos = end + 2;
    for _ in 0..count {
        let size = find_resp_message_end_with_depth(&data[pos..], depth + 1)?;
        if size == 0 {
            return Ok(0);
        }
        pos += size;
    }
    Ok(pos)
}

/// Maximum recursion depth for nested RESP structures
const MAX_RESP_DEPTH: usize = 32;

/// Maximum array/map size to prevent DoS
const MAX_ARRAY_SIZE: i64 = 10_000_000;

/// Maximum bulk string size (1GB)
const MAX_BULK_STRING_SIZE: i64 = 1_073_741_824;

/// Maximum total message size (10MB) to prevent scanning huge buffers
const MAX_MESSAGE_SIZE: usize = 10_485_760;

/// Find the end of a complete RESP message, returns bytes consumed
/// Supports both RESP2 and RESP3 types
pub(crate) fn find_resp_message_end(data: &[u8]) -> Result<usize> {
    // Limit a single frame, not the entire receive buffer (which may contain
    // many complete pipelined responses).
    let bounded = &data[..data.len().min(MAX_MESSAGE_SIZE + 1)];
    let end = find_resp_message_end_with_depth(bounded, 0)?;
    anyhow::ensure!(
        end <= MAX_MESSAGE_SIZE && !(end == 0 && data.len() > MAX_MESSAGE_SIZE),
        "RESP message too large"
    );
    Ok(end)
}

/// Internal implementation with depth tracking
fn find_resp_message_end_with_depth(data: &[u8], depth: usize) -> Result<usize> {
    if data.is_empty() {
        return Ok(0);
    }

    // Check recursion depth to prevent stack overflow
    if depth > MAX_RESP_DEPTH {
        return Err(anyhow!("RESP message nesting too deep (max: {})", MAX_RESP_DEPTH));
    }

    match data[0] {
        b'+' | b'-' | b':' => {
            // RESP2: Simple strings, errors, integers end with \r\n
            if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
                Ok(pos + 2)
            } else {
                Ok(0) // Incomplete
            }
        }
        b'$' | b'!' | b'=' => {
            // RESP2: Bulk string ($)
            // RESP3: Blob error (!), Verbatim string (=)
            if let Some(first_crlf) = data.windows(2).position(|w| w == b"\r\n") {
                let length_str = std::str::from_utf8(&data[1..first_crlf])
                    .map_err(|e| anyhow!("Invalid bulk string length: {e}"))?;
                let length: i64 = length_str
                    .parse()
                    .map_err(|e| anyhow!("Failed to parse bulk string length: {e}"))?;

                if length == -1 {
                    // Null bulk string
                    return Ok(first_crlf + 2);
                }

                // Validate length is non-negative and within limits
                if length < 0 {
                    return Err(anyhow!("Invalid negative bulk string length: {}", length));
                }
                if length > MAX_BULK_STRING_SIZE {
                    return Err(anyhow!(
                        "Bulk string too large: {} (max: {})",
                        length,
                        MAX_BULK_STRING_SIZE
                    ));
                }

                // Use checked arithmetic to prevent overflow
                let expected_total = first_crlf
                    .checked_add(2)
                    .and_then(|v| v.checked_add(length as usize))
                    .and_then(|v| v.checked_add(2))
                    .ok_or_else(|| anyhow!("Integer overflow in bulk string size calculation"))?;

                if data.len() >= expected_total {
                    anyhow::ensure!(
                        &data[expected_total - 2..expected_total] == b"\r\n",
                        "Invalid bulk string terminator"
                    );
                    Ok(expected_total)
                } else {
                    Ok(0) // Incomplete
                }
            } else {
                Ok(0) // Incomplete
            }
        }
        b'|' => {
            let offset = attribute_payload_offset(data, depth)?;
            if offset == 0 {
                return Ok(0);
            }
            let size = find_resp_message_end_with_depth(&data[offset..], depth + 1)?;
            if size == 0 {
                Ok(0)
            } else {
                Ok(offset + size)
            }
        }
        b'*' | b'%' | b'~' | b'>' => {
            // RESP2: Array (*)
            // RESP3: Map (%), Set (~), Attribute (|), Push (>)
            // Need to recursively parse all elements
            let first_crlf = match data.windows(2).position(|w| w == b"\r\n") {
                Some(pos) => pos,
                None => return Ok(0), // Incomplete
            };

            let count_str = std::str::from_utf8(&data[1..first_crlf])
                .map_err(|e| anyhow!("Invalid array count: {e}"))?;
            let count: i64 =
                count_str.parse().map_err(|e| anyhow!("Failed to parse array count: {e}"))?;

            if count == -1 {
                anyhow::ensure!(data[0] == b'*', "Only arrays support null aggregate counts");
                // Null array
                return Ok(first_crlf + 2);
            }

            // Validate count is non-negative and within limits
            if count < -1 {
                return Err(anyhow!("Invalid array count: {} (must be >= -1)", count));
            }
            if count > MAX_ARRAY_SIZE {
                return Err(anyhow!("Array too large: {} (max: {})", count, MAX_ARRAY_SIZE));
            }

            let count = if data[0] == b'%' {
                count.checked_mul(2).ok_or_else(|| anyhow!("Map count overflow"))?
            } else {
                count
            };
            let mut pos = first_crlf + 2;
            // Parse each element in the array with incremented depth
            for _ in 0..count {
                if pos >= data.len() {
                    return Ok(0); // Incomplete
                }
                let elem_size = find_resp_message_end_with_depth(&data[pos..], depth + 1)?;
                if elem_size == 0 {
                    return Ok(0); // Incomplete element
                }
                pos += elem_size;
            }
            Ok(pos)
        }
        b'_' => {
            // RESP3: Null (_\r\n)
            if data.len() >= 3 && &data[0..3] == b"_\r\n" {
                Ok(3)
            } else {
                Ok(0) // Incomplete
            }
        }
        b',' | b'(' => {
            // RESP3: Double (,<floating-point-number>\r\n)
            // RESP3: Big number ((<big-number>\r\n)
            if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
                Ok(pos + 2)
            } else {
                Ok(0) // Incomplete
            }
        }
        b'#' => {
            // RESP3: Boolean (#t\r\n or #f\r\n)
            if data.len() >= 4 {
                // Validate actual boolean value
                if data[1] != b't' && data[1] != b'f' {
                    return Err(anyhow!(
                        "Invalid RESP3 boolean value: must be 't' or 'f', got '{}'",
                        data[1] as char
                    ));
                }
                if &data[2..4] == b"\r\n" {
                    Ok(4)
                } else {
                    Ok(0) // Incomplete
                }
            } else {
                Ok(0) // Incomplete
            }
        }
        _ => Err(anyhow!("Invalid RESP message type")),
    }
}
