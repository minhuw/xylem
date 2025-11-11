//! Redis Cluster redirect handling
//!
//! This module handles MOVED and ASK redirects, which are used by Redis Cluster
//! to inform clients about slot ownership and migration.

use anyhow::{anyhow, Result};
use std::net::SocketAddr;

/// Type of redirect received from Redis Cluster
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedirectType {
    /// MOVED: Permanent slot reassignment
    ///
    /// The client should update its slot map and retry the command
    /// on the new node.
    Moved { slot: u16, addr: SocketAddr },

    /// ASK: Temporary migration state
    ///
    /// The client should send ASKING before retry, but NOT update
    /// its slot map. This is used during slot migration.
    Ask { slot: u16, addr: SocketAddr },
}

/// Parse a Redis error response to detect redirects
///
/// # Arguments
///
/// * `error_msg` - The error message bytes (including `-` prefix)
///
/// # Returns
///
/// - `Ok(Some(RedirectType))` - Successfully parsed a redirect
/// - `Ok(None)` - Not a redirect error
/// - `Err(_)` - Invalid redirect format
///
/// # Example
///
/// ```
/// use xylem_protocols::redis::cluster::{parse_redirect, RedirectType};
///
/// let error = b"-MOVED 3999 127.0.0.1:6381\r\n";
/// let redirect = parse_redirect(error).unwrap();
///
/// match redirect {
///     Some(RedirectType::Moved { slot, addr }) => {
///         assert_eq!(slot, 3999);
///         assert_eq!(addr, "127.0.0.1:6381".parse().unwrap());
///     }
///     _ => panic!("Expected MOVED redirect"),
/// }
/// ```
pub fn parse_redirect(error_msg: &[u8]) -> Result<Option<RedirectType>> {
    let msg = std::str::from_utf8(error_msg)
        .map_err(|e| anyhow!("Invalid UTF-8 in error message: {}", e))?;

    // -MOVED 3999 127.0.0.1:6381\r\n
    if msg.starts_with("-MOVED ") {
        return parse_moved(msg);
    }

    // -ASK 3999 127.0.0.1:6381\r\n
    if msg.starts_with("-ASK ") {
        return parse_ask(msg);
    }

    // Not a redirect
    Ok(None)
}

/// Parse MOVED redirect
///
/// Format: `-MOVED <slot> <ip>:<port>\r\n`
fn parse_moved(msg: &str) -> Result<Option<RedirectType>> {
    // Strip "-MOVED " prefix and trailing \r\n
    let content = msg.trim_start_matches("-MOVED ").trim_end_matches("\r\n").trim();

    let parts: Vec<&str> = content.split_whitespace().collect();

    if parts.len() != 2 {
        return Err(anyhow!(
            "Invalid MOVED format: expected 2 parts, got {}: '{}'",
            parts.len(),
            msg
        ));
    }

    let slot: u16 = parts[0]
        .parse()
        .map_err(|e| anyhow!("Invalid slot number in MOVED '{}': {}", parts[0], e))?;

    let addr: SocketAddr = parts[1]
        .parse()
        .map_err(|e| anyhow!("Invalid address in MOVED '{}': {}", parts[1], e))?;

    Ok(Some(RedirectType::Moved { slot, addr }))
}

/// Parse ASK redirect
///
/// Format: `-ASK <slot> <ip>:<port>\r\n`
fn parse_ask(msg: &str) -> Result<Option<RedirectType>> {
    // Strip "-ASK " prefix and trailing \r\n
    let content = msg.trim_start_matches("-ASK ").trim_end_matches("\r\n").trim();

    let parts: Vec<&str> = content.split_whitespace().collect();

    if parts.len() != 2 {
        return Err(anyhow!(
            "Invalid ASK format: expected 2 parts, got {}: '{}'",
            parts.len(),
            msg
        ));
    }

    let slot: u16 = parts[0]
        .parse()
        .map_err(|e| anyhow!("Invalid slot number in ASK '{}': {}", parts[0], e))?;

    let addr: SocketAddr = parts[1]
        .parse()
        .map_err(|e| anyhow!("Invalid address in ASK '{}': {}", parts[1], e))?;

    Ok(Some(RedirectType::Ask { slot, addr }))
}

/// Generate ASKING command (sent before retrying ASK redirect)
///
/// The ASKING command sets a one-time flag on the connection that allows
/// the next command to be executed even if the slot is in IMPORTING state.
///
/// # Returns
///
/// RESP-encoded ASKING command: `*1\r\n$6\r\nASKING\r\n`
///
/// # Example
///
/// ```
/// use xylem_protocols::redis::cluster::generate_asking_command;
///
/// let cmd = generate_asking_command();
/// assert_eq!(cmd, b"*1\r\n$6\r\nASKING\r\n");
/// ```
pub fn generate_asking_command() -> Vec<u8> {
    // *1\r\n$6\r\nASKING\r\n
    b"*1\r\n$6\r\nASKING\r\n".to_vec()
}

/// Generate CLUSTER SLOTS command for topology discovery
///
/// # Returns
///
/// RESP-encoded CLUSTER SLOTS command: `*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n`
pub fn generate_cluster_slots_command() -> Vec<u8> {
    // *2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n
    b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n".to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_moved() {
        let error = b"-MOVED 3999 127.0.0.1:6381\r\n";
        let redirect = parse_redirect(error).unwrap();

        match redirect {
            Some(RedirectType::Moved { slot, addr }) => {
                assert_eq!(slot, 3999);
                assert_eq!(addr, "127.0.0.1:6381".parse::<SocketAddr>().unwrap());
            }
            _ => panic!("Expected MOVED redirect"),
        }
    }

    #[test]
    fn test_parse_moved_ipv6() {
        let error = b"-MOVED 12345 [::1]:7000\r\n";
        let redirect = parse_redirect(error).unwrap();

        match redirect {
            Some(RedirectType::Moved { slot, addr }) => {
                assert_eq!(slot, 12345);
                assert_eq!(addr, "[::1]:7000".parse::<SocketAddr>().unwrap());
            }
            _ => panic!("Expected MOVED redirect"),
        }
    }

    #[test]
    fn test_parse_ask() {
        let error = b"-ASK 3999 127.0.0.1:6381\r\n";
        let redirect = parse_redirect(error).unwrap();

        match redirect {
            Some(RedirectType::Ask { slot, addr }) => {
                assert_eq!(slot, 3999);
                assert_eq!(addr, "127.0.0.1:6381".parse::<SocketAddr>().unwrap());
            }
            _ => panic!("Expected ASK redirect"),
        }
    }

    #[test]
    fn test_parse_ask_different_slot() {
        let error = b"-ASK 10000 192.168.1.100:6379\r\n";
        let redirect = parse_redirect(error).unwrap();

        match redirect {
            Some(RedirectType::Ask { slot, addr }) => {
                assert_eq!(slot, 10000);
                assert_eq!(addr, "192.168.1.100:6379".parse::<SocketAddr>().unwrap());
            }
            _ => panic!("Expected ASK redirect"),
        }
    }

    #[test]
    fn test_parse_non_redirect_error() {
        let error = b"-ERR unknown command\r\n";
        let redirect = parse_redirect(error).unwrap();
        assert!(redirect.is_none());
    }

    #[test]
    fn test_parse_other_errors() {
        let errors: &[&[u8]] = &[
            b"-ERR syntax error\r\n",
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            b"-NOAUTH Authentication required\r\n",
        ];

        for error in errors {
            let redirect = parse_redirect(error).unwrap();
            assert!(
                redirect.is_none(),
                "Should not parse as redirect: {:?}",
                String::from_utf8_lossy(error)
            );
        }
    }

    #[test]
    fn test_parse_moved_invalid_slot() {
        let error = b"-MOVED invalid 127.0.0.1:6381\r\n";
        let result = parse_redirect(error);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_moved_invalid_address() {
        let error = b"-MOVED 3999 invalid_address\r\n";
        let result = parse_redirect(error);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_moved_missing_parts() {
        let error = b"-MOVED 3999\r\n";
        let result = parse_redirect(error);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_ask_invalid_format() {
        let error = b"-ASK\r\n";
        let result = parse_redirect(error);
        // ASK without arguments should return error or None, but not panic
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn test_generate_asking() {
        let cmd = generate_asking_command();
        let cmd_str = String::from_utf8_lossy(&cmd);
        assert!(cmd_str.contains("ASKING"));
        assert_eq!(cmd, b"*1\r\n$6\r\nASKING\r\n");
    }

    #[test]
    fn test_generate_cluster_slots() {
        let cmd = generate_cluster_slots_command();
        let cmd_str = String::from_utf8_lossy(&cmd);
        assert!(cmd_str.contains("CLUSTER"));
        assert!(cmd_str.contains("SLOTS"));
        assert_eq!(cmd, b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n");
    }

    #[test]
    fn test_redirect_type_equality() {
        let addr1: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:7001".parse().unwrap();

        let moved1 = RedirectType::Moved { slot: 100, addr: addr1 };
        let moved2 = RedirectType::Moved { slot: 100, addr: addr1 };
        let moved3 = RedirectType::Moved { slot: 100, addr: addr2 };

        assert_eq!(moved1, moved2);
        assert_ne!(moved1, moved3);

        let ask1 = RedirectType::Ask { slot: 100, addr: addr1 };
        assert_ne!(moved1, ask1); // Different variants
    }

    #[test]
    fn test_moved_without_crlf() {
        // Some tests might not include \r\n
        let error = b"-MOVED 3999 127.0.0.1:6381";
        let redirect = parse_redirect(error).unwrap();

        match redirect {
            Some(RedirectType::Moved { slot, addr }) => {
                assert_eq!(slot, 3999);
                assert_eq!(addr, "127.0.0.1:6381".parse::<SocketAddr>().unwrap());
            }
            _ => panic!("Expected MOVED redirect"),
        }
    }

    #[test]
    fn test_slot_boundary_values() {
        // Test min and max slot values
        let error_min = b"-MOVED 0 127.0.0.1:6381\r\n";
        let redirect_min = parse_redirect(error_min).unwrap();
        match redirect_min {
            Some(RedirectType::Moved { slot, .. }) => assert_eq!(slot, 0),
            _ => panic!("Expected MOVED redirect"),
        }

        let error_max = b"-MOVED 16383 127.0.0.1:6381\r\n";
        let redirect_max = parse_redirect(error_max).unwrap();
        match redirect_max {
            Some(RedirectType::Moved { slot, .. }) => assert_eq!(slot, 16383),
            _ => panic!("Expected MOVED redirect"),
        }
    }

    #[test]
    fn test_parse_with_extra_whitespace() {
        let error = b"-MOVED   3999   127.0.0.1:6381  \r\n";
        let redirect = parse_redirect(error).unwrap();

        match redirect {
            Some(RedirectType::Moved { slot, addr }) => {
                assert_eq!(slot, 3999);
                assert_eq!(addr, "127.0.0.1:6381".parse::<SocketAddr>().unwrap());
            }
            _ => panic!("Expected MOVED redirect"),
        }
    }

    #[test]
    fn test_parse_moved_out_of_range_slot() {
        // Slot number too large (valid range is 0-16383)
        // The parser doesn't validate slot range, just parses the number
        let error = b"-MOVED 16384 127.0.0.1:6381\r\n";
        let result = parse_redirect(error);
        assert!(result.is_ok());
        match result.unwrap() {
            Some(RedirectType::Moved { slot, .. }) => assert_eq!(slot, 16384),
            _ => panic!("Expected MOVED redirect"),
        }

        // Slot number exceeds u16::MAX should fail
        let error = b"-MOVED 99999 127.0.0.1:6381\r\n";
        let result = parse_redirect(error);
        // Should fail to parse because 99999 > u16::MAX (65535)
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_moved_negative_slot() {
        // Negative slot number - should fail parsing
        let error = b"-MOVED -1 127.0.0.1:6381\r\n";
        let result = parse_redirect(error);
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn test_parse_redirect_invalid_address() {
        // Invalid IP address
        let error = b"-MOVED 3999 999.999.999.999:6381\r\n";
        let result = parse_redirect(error);
        assert!(result.is_err());

        // Invalid port
        let error = b"-MOVED 3999 127.0.0.1:99999\r\n";
        let result = parse_redirect(error);
        assert!(result.is_err());

        // Missing port
        let error = b"-MOVED 3999 127.0.0.1\r\n";
        let result = parse_redirect(error);
        assert!(result.is_err());

        // Empty address
        let error = b"-MOVED 3999 \r\n";
        let result = parse_redirect(error);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_redirect_case_sensitivity() {
        // Redis is case-sensitive for error types
        let error_lower = b"-moved 3999 127.0.0.1:6381\r\n";
        let redirect_lower = parse_redirect(error_lower).unwrap();
        assert!(redirect_lower.is_none(), "Should not match lowercase 'moved'");

        let error_mixed = b"-Moved 3999 127.0.0.1:6381\r\n";
        let redirect_mixed = parse_redirect(error_mixed).unwrap();
        assert!(redirect_mixed.is_none(), "Should not match mixed case 'Moved'");
    }

    #[test]
    fn test_parse_redirect_with_hostname() {
        // Hostname instead of IP (should fail - we expect SocketAddr)
        let error = b"-MOVED 3999 redis-node-1:6381\r\n";
        let result = parse_redirect(error);
        assert!(result.is_err(), "Hostnames should fail to parse");
    }

    #[test]
    fn test_generate_asking_command_format() {
        let cmd = generate_asking_command();

        // Verify RESP format
        assert_eq!(cmd, b"*1\r\n$6\r\nASKING\r\n");

        // Verify it's a valid RESP array with 1 element
        assert!(cmd.starts_with(b"*1\r\n"));

        // Verify bulk string length is correct (ASKING = 6 chars)
        let cmd_str = std::str::from_utf8(&cmd).unwrap();
        assert!(cmd_str.contains("$6\r\n"));
    }

    #[test]
    fn test_generate_cluster_slots_format() {
        let cmd = generate_cluster_slots_command();

        // Verify RESP format
        assert_eq!(cmd, b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n");

        // Verify it's a valid RESP array with 2 elements
        assert!(cmd.starts_with(b"*2\r\n"));
    }

    #[test]
    fn test_parse_redirect_empty_input() {
        let error = b"";
        let result = parse_redirect(error);
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn test_parse_redirect_only_crlf() {
        let error = b"\r\n";
        let result = parse_redirect(error);
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn test_parse_redirect_truncated() {
        // Incomplete MOVED message - missing address
        let error = b"-MOVED 3999";
        let result = parse_redirect(error);
        // Should return error because there's no address part
        assert!(result.is_err(), "Truncated MOVED should fail parsing");

        // Only the prefix without space - returns Ok(None) because it doesn't match "-MOVED "
        let error = b"-MOVED";
        let result = parse_redirect(error);
        // This doesn't match "-MOVED " prefix, so returns Ok(None)
        assert!(result.unwrap().is_none(), "MOVED without space is not recognized");

        // Prefix with space but no data
        let error = b"-MOVED ";
        let result = parse_redirect(error);
        // This matches "-MOVED " but has no parts, so should error
        assert!(result.is_err(), "MOVED with space but no data should fail");
    }

    #[test]
    fn test_ipv6_redirect_formats() {
        // IPv6 with brackets (correct format)
        let error = b"-MOVED 3999 [::1]:6381\r\n";
        let redirect = parse_redirect(error).unwrap();
        match redirect {
            Some(RedirectType::Moved { slot, addr }) => {
                assert_eq!(slot, 3999);
                assert_eq!(addr.ip(), std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));
                assert_eq!(addr.port(), 6381);
            }
            _ => panic!("Expected MOVED redirect with IPv6"),
        }

        // Full IPv6 address
        let error = b"-ASK 100 [2001:db8::1]:7000\r\n";
        let redirect = parse_redirect(error).unwrap();
        match redirect {
            Some(RedirectType::Ask { slot, .. }) => {
                assert_eq!(slot, 100);
            }
            _ => panic!("Expected ASK redirect with IPv6"),
        }
    }
}
