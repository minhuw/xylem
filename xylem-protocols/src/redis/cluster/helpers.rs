//! Helper functions for Redis Cluster operations
//!
//! Utilities for working with redirects, topology, and cluster management.

use super::protocol::RedisClusterProtocol;
use super::redirect::RedirectType;
use anyhow::anyhow;
use std::net::SocketAddr;
use std::time::Duration;

/// Check if an error is a Redis Cluster redirect
///
/// Examines error messages to detect MOVED or ASK redirects.
///
/// # Arguments
///
/// * `error` - The error to check
///
/// # Returns
///
/// `Some(RedirectType)` if this is a redirect, `None` otherwise
///
/// # Example
///
/// ```ignore
/// match protocol.parse_response(conn_id, data) {
///     Err(e) => {
///         if let Some(redirect) = is_redirect_error(&e) {
///             // Handle redirect
///         }
///     }
///     _ => {}
/// }
/// ```
pub fn is_redirect_error(error: &anyhow::Error) -> Option<RedirectType> {
    let msg = error.to_string();

    // Check for MOVED redirect
    if msg.contains("MOVED redirect") {
        // Parse: "MOVED redirect: slot 3999 moved to 127.0.0.1:6381"
        if let Some(slot_start) = msg.find("slot ") {
            if let Some(moved_to) = msg.find(" moved to ") {
                let slot_str = &msg[slot_start + 5..moved_to];
                let addr_start = moved_to + 10;
                let addr_end =
                    msg[addr_start..].find(" ").map(|i| addr_start + i).unwrap_or(msg.len());
                let addr_str = &msg[addr_start..addr_end];

                if let (Ok(slot), Ok(addr)) =
                    (slot_str.parse::<u16>(), addr_str.parse::<SocketAddr>())
                {
                    return Some(RedirectType::Moved { slot, addr });
                }
            }
        }
    }

    // Check for ASK redirect
    if msg.contains("ASK redirect") {
        // Parse: "ASK redirect: slot 100 temporarily at 127.0.0.1:7001"
        if let Some(slot_start) = msg.find("slot ") {
            if let Some(temp_at) = msg.find(" temporarily at ") {
                let slot_str = &msg[slot_start + 5..temp_at];
                let addr_start = temp_at + 16;
                let addr_end =
                    msg[addr_start..].find(" ").map(|i| addr_start + i).unwrap_or(msg.len());
                let addr_str = &msg[addr_start..addr_end];

                if let (Ok(slot), Ok(addr)) =
                    (slot_str.parse::<u16>(), addr_str.parse::<SocketAddr>())
                {
                    return Some(RedirectType::Ask { slot, addr });
                }
            }
        }
    }

    None
}

/// Extract slot and address from a redirect error
///
/// Convenience function to get redirect details without matching on type.
///
/// # Returns
///
/// `Some((slot, address))` if this is a redirect, `None` otherwise
pub fn extract_redirect(error: &anyhow::Error) -> Option<(u16, SocketAddr)> {
    is_redirect_error(error).map(|redirect| match redirect {
        RedirectType::Moved { slot, addr } => (slot, addr),
        RedirectType::Ask { slot, addr } => (slot, addr),
    })
}

/// Check if cluster topology is stale
///
/// Returns true if topology hasn't been updated recently or never updated.
///
/// # Arguments
///
/// * `protocol` - The cluster protocol instance
/// * `threshold` - Maximum age before topology is considered stale
///
/// # Example
///
/// ```ignore
/// use std::time::Duration;
///
/// if is_topology_stale(&protocol, Duration::from_secs(60)) {
///     // Refresh topology
///     protocol.discover_topology()?;
/// }
/// ```
pub fn is_topology_stale(protocol: &RedisClusterProtocol, threshold: Duration) -> bool {
    protocol.topology_age().map(|age| age > threshold).unwrap_or(true) // Never updated = stale
}

/// Build a retry strategy recommendation based on redirect type
///
/// Returns a tuple of (should_send_asking, should_update_connection, description)
pub fn redirect_strategy(redirect: &RedirectType) -> (bool, bool, &'static str) {
    match redirect {
        RedirectType::Moved { .. } => {
            (false, true, "MOVED: Update topology and connect to new node")
        }
        RedirectType::Ask { .. } => {
            (true, false, "ASK: Send ASKING command and retry on temp node")
        }
    }
}

/// Format a redirect for logging/debugging
pub fn format_redirect(redirect: &RedirectType) -> String {
    match redirect {
        RedirectType::Moved { slot, addr } => {
            format!("MOVED: slot {} → {} (permanent)", slot, addr)
        }
        RedirectType::Ask { slot, addr } => {
            format!("ASK: slot {} → {} (temporary)", slot, addr)
        }
    }
}

/// Validate that a slot number is within valid range
pub fn validate_slot(slot: u16) -> anyhow::Result<()> {
    if slot >= 16384 {
        Err(anyhow!("Invalid slot {}: must be < 16384", slot))
    } else {
        Ok(())
    }
}

/// Calculate the ideal number of nodes for even slot distribution
///
/// Redis Cluster has 16,384 slots. This function suggests node counts
/// that divide evenly for balanced distribution.
pub fn recommended_node_counts() -> Vec<usize> {
    // Common factors of 16384 that make sense for cluster sizes
    vec![1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 256, 512, 1024]
}

/// Calculate slot range for a node in an evenly-distributed cluster
///
/// Uses simple division: node 0 gets slots 0 to (16384/n)-1, etc.
/// Last node gets any remainder slots.
///
/// # Arguments
///
/// * `node_index` - Zero-based index of the node (0 to node_count-1)
/// * `node_count` - Total number of nodes in the cluster
///
/// # Returns
///
/// `Some((start, end))` if valid, `None` if invalid parameters
///
/// # Example
///
/// ```ignore
/// // 3-node cluster: 16384 / 3 = 5461 slots per node (+ 1 remainder to last node)
/// assert_eq!(calculate_slot_range(0, 3), Some((0, 5460)));
/// assert_eq!(calculate_slot_range(1, 3), Some((5461, 10921)));
/// assert_eq!(calculate_slot_range(2, 3), Some((10922, 16383)));
/// ```
pub fn calculate_slot_range(node_index: usize, node_count: usize) -> Option<(u16, u16)> {
    if node_count == 0 || node_index >= node_count {
        return None;
    }

    let slots_per_node = 16384 / node_count;
    let start = (node_index * slots_per_node) as u16;

    let end = if node_index == node_count - 1 {
        // Last node gets remainder
        16383
    } else {
        start + slots_per_node as u16 - 1
    };

    Some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_redirect_error_moved() {
        let error = anyhow!("MOVED redirect: slot 3999 moved to 127.0.0.1:6381 (topology updated)");
        let redirect = is_redirect_error(&error);

        assert!(redirect.is_some());
        match redirect.unwrap() {
            RedirectType::Moved { slot, addr } => {
                assert_eq!(slot, 3999);
                assert_eq!(addr, "127.0.0.1:6381".parse().unwrap());
            }
            _ => panic!("Expected MOVED redirect"),
        }
    }

    #[test]
    fn test_is_redirect_error_ask() {
        let error =
            anyhow!("ASK redirect: slot 100 temporarily at 127.0.0.1:7001 (no topology update)");
        let redirect = is_redirect_error(&error);

        assert!(redirect.is_some());
        match redirect.unwrap() {
            RedirectType::Ask { slot, addr } => {
                assert_eq!(slot, 100);
                assert_eq!(addr, "127.0.0.1:7001".parse().unwrap());
            }
            _ => panic!("Expected ASK redirect"),
        }
    }

    #[test]
    fn test_is_redirect_error_not_redirect() {
        let error = anyhow!("Some other error");
        let redirect = is_redirect_error(&error);
        assert!(redirect.is_none());
    }

    #[test]
    fn test_extract_redirect() {
        let error = anyhow!("MOVED redirect: slot 3999 moved to 127.0.0.1:6381 (topology updated)");
        let extracted = extract_redirect(&error);

        assert!(extracted.is_some());
        let (slot, addr) = extracted.unwrap();
        assert_eq!(slot, 3999);
        assert_eq!(addr, "127.0.0.1:6381".parse().unwrap());
    }

    #[test]
    fn test_redirect_strategy() {
        let moved = RedirectType::Moved {
            slot: 100,
            addr: "127.0.0.1:7000".parse().unwrap(),
        };
        let (asking, update, _) = redirect_strategy(&moved);
        assert!(!asking);
        assert!(update);

        let ask = RedirectType::Ask {
            slot: 100,
            addr: "127.0.0.1:7000".parse().unwrap(),
        };
        let (asking, update, _) = redirect_strategy(&ask);
        assert!(asking);
        assert!(!update);
    }

    #[test]
    fn test_format_redirect() {
        let moved = RedirectType::Moved {
            slot: 100,
            addr: "127.0.0.1:7000".parse().unwrap(),
        };
        let formatted = format_redirect(&moved);
        assert!(formatted.contains("MOVED"));
        assert!(formatted.contains("100"));
        assert!(formatted.contains("permanent"));
    }

    #[test]
    fn test_validate_slot() {
        assert!(validate_slot(0).is_ok());
        assert!(validate_slot(16383).is_ok());
        assert!(validate_slot(16384).is_err());
        assert!(validate_slot(20000).is_err());
    }

    #[test]
    fn test_calculate_slot_range_3_nodes() {
        // 16384 / 3 = 5461 slots per node (+1 remainder to last node)
        assert_eq!(calculate_slot_range(0, 3), Some((0, 5460)));
        assert_eq!(calculate_slot_range(1, 3), Some((5461, 10921)));
        assert_eq!(calculate_slot_range(2, 3), Some((10922, 16383)));
    }

    #[test]
    fn test_calculate_slot_range_invalid() {
        assert_eq!(calculate_slot_range(3, 3), None); // node_index >= node_count
        assert_eq!(calculate_slot_range(0, 0), None); // node_count == 0
    }

    #[test]
    fn test_recommended_node_counts() {
        let counts = recommended_node_counts();
        assert!(counts.contains(&3)); // Most common
        assert!(counts.contains(&6));
        assert!(counts.contains(&8));
    }
}
