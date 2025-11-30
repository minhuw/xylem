//! Protocol and transport metadata collection
//!
//! This module provides a trait-based interface for protocols and transports
//! to inject custom statistics and metadata into experiment results.

use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Trait for collecting protocol/transport-specific metadata
///
/// Protocols and transports can optionally implement this trait to provide
/// rich diagnostic information that goes beyond basic latency/throughput metrics.
///
/// Examples of useful metadata:
/// - Protocol-specific: Redis redirects, command distribution, key distribution
/// - Transport-specific: Connection errors, retries, TCP stats
pub trait StatsMetadata: Send {
    /// Collect metadata as a JSON value
    ///
    /// Returns arbitrary JSON that will be included in the experiment output.
    /// This allows protocols to report any structured data they track.
    fn collect_metadata(&self) -> JsonValue;

    /// Get list of metadata keys this implementation provides
    ///
    /// Useful for documentation and validation purposes.
    fn metadata_keys(&self) -> Vec<String> {
        if let JsonValue::Object(map) = self.collect_metadata() {
            map.keys().map(|k| k.to_string()).collect()
        } else {
            Vec::new()
        }
    }
}

/// Helper to merge metadata from multiple sources
///
/// When multiple workers/threads collect metadata, this function merges them
/// intelligently. Numeric values are summed, arrays are concatenated, etc.
pub fn merge_metadata(metadata_list: Vec<JsonValue>) -> JsonValue {
    if metadata_list.is_empty() {
        return JsonValue::Object(serde_json::Map::new());
    }

    if metadata_list.len() == 1 {
        return metadata_list.into_iter().next().unwrap();
    }

    // Start with the first metadata
    let mut merged = match metadata_list[0].clone() {
        JsonValue::Object(map) => map,
        _ => return metadata_list[0].clone(),
    };

    // Merge remaining metadata
    for metadata in metadata_list.into_iter().skip(1) {
        if let JsonValue::Object(map) = metadata {
            for (key, value) in map {
                merged
                    .entry(key.clone())
                    .and_modify(|existing| {
                        *existing = merge_json_values(existing.clone(), value.clone());
                    })
                    .or_insert(value);
            }
        }
    }

    JsonValue::Object(merged)
}

/// Merge two JSON values intelligently
fn merge_json_values(a: JsonValue, b: JsonValue) -> JsonValue {
    match (a, b) {
        // Merge numbers by addition
        (JsonValue::Number(n1), JsonValue::Number(n2)) => {
            if let (Some(a), Some(b)) = (n1.as_u64(), n2.as_u64()) {
                JsonValue::Number(serde_json::Number::from(a + b))
            } else if let (Some(a), Some(b)) = (n1.as_i64(), n2.as_i64()) {
                JsonValue::Number(serde_json::Number::from(a + b))
            } else if let (Some(a), Some(b)) = (n1.as_f64(), n2.as_f64()) {
                serde_json::Number::from_f64(a + b)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Number(n1))
            } else {
                JsonValue::Number(n1)
            }
        }
        // Merge arrays by concatenation
        (JsonValue::Array(mut a), JsonValue::Array(b)) => {
            a.extend(b);
            JsonValue::Array(a)
        }
        // Merge objects recursively
        (JsonValue::Object(mut a), JsonValue::Object(b)) => {
            for (key, value) in b {
                a.entry(key.clone())
                    .and_modify(|existing| {
                        *existing = merge_json_values(existing.clone(), value.clone());
                    })
                    .or_insert(value);
            }
            JsonValue::Object(a)
        }
        // For non-matching types, prefer the second value (latest)
        (_, b) => b,
    }
}

/// Container for per-group metadata
#[derive(Debug, Clone, Default)]
pub struct GroupMetadataCollector {
    /// Per-group metadata: group_id -> metadata
    metadata: HashMap<usize, JsonValue>,
}

impl GroupMetadataCollector {
    /// Create a new metadata collector
    pub fn new() -> Self {
        Self { metadata: HashMap::new() }
    }

    /// Store metadata for a specific group
    pub fn set_group_metadata(&mut self, group_id: usize, metadata: JsonValue) {
        self.metadata.insert(group_id, metadata);
    }

    /// Get metadata for a specific group
    pub fn get_group_metadata(&self, group_id: usize) -> Option<&JsonValue> {
        self.metadata.get(&group_id)
    }

    /// Get all group IDs that have metadata
    pub fn group_ids(&self) -> Vec<usize> {
        let mut ids: Vec<_> = self.metadata.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Merge multiple metadata collectors
    pub fn merge(collectors: Vec<GroupMetadataCollector>) -> Self {
        let mut merged = GroupMetadataCollector::new();

        // Collect all group IDs
        let mut all_group_ids = std::collections::HashSet::new();
        for collector in &collectors {
            all_group_ids.extend(collector.group_ids());
        }

        // Merge metadata for each group
        for group_id in all_group_ids {
            let group_metadata: Vec<JsonValue> = collectors
                .iter()
                .filter_map(|c| c.get_group_metadata(group_id).cloned())
                .collect();

            if !group_metadata.is_empty() {
                let merged_metadata = merge_metadata(group_metadata);
                merged.set_group_metadata(group_id, merged_metadata);
            }
        }

        merged
    }

    /// Convert to a HashMap for serialization
    pub fn into_map(self) -> HashMap<usize, JsonValue> {
        self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_merge_numeric_metadata() {
        let m1 = json!({"count": 100, "errors": 5});
        let m2 = json!({"count": 200, "errors": 3});

        let merged = merge_metadata(vec![m1, m2]);

        assert_eq!(merged["count"], 300);
        assert_eq!(merged["errors"], 8);
    }

    #[test]
    fn test_merge_nested_metadata() {
        let m1 = json!({
            "redirects": {
                "moved": 10,
                "ask": 5
            }
        });
        let m2 = json!({
            "redirects": {
                "moved": 20,
                "ask": 8
            }
        });

        let merged = merge_metadata(vec![m1, m2]);

        assert_eq!(merged["redirects"]["moved"], 30);
        assert_eq!(merged["redirects"]["ask"], 13);
    }

    #[test]
    fn test_group_metadata_collector() {
        let mut collector = GroupMetadataCollector::new();

        collector.set_group_metadata(0, json!({"requests": 1000}));
        collector.set_group_metadata(1, json!({"requests": 2000}));

        assert_eq!(collector.get_group_metadata(0).unwrap()["requests"], 1000);
        assert_eq!(collector.group_ids(), vec![0, 1]);
    }

    #[test]
    fn test_merge_group_collectors() {
        let mut c1 = GroupMetadataCollector::new();
        c1.set_group_metadata(0, json!({"count": 100}));

        let mut c2 = GroupMetadataCollector::new();
        c2.set_group_metadata(0, json!({"count": 200}));

        let merged = GroupMetadataCollector::merge(vec![c1, c2]);

        assert_eq!(merged.get_group_metadata(0).unwrap()["count"], 300);
    }

    #[test]
    fn test_merge_multiple_groups() {
        let mut c1 = GroupMetadataCollector::new();
        c1.set_group_metadata(0, json!({"requests": 100, "errors": 5}));
        c1.set_group_metadata(1, json!({"requests": 200}));

        let mut c2 = GroupMetadataCollector::new();
        c2.set_group_metadata(0, json!({"requests": 150, "errors": 3}));
        c2.set_group_metadata(2, json!({"requests": 50}));

        let merged = GroupMetadataCollector::merge(vec![c1, c2]);

        // Group 0: should have merged data from both
        assert_eq!(merged.get_group_metadata(0).unwrap()["requests"], 250);
        assert_eq!(merged.get_group_metadata(0).unwrap()["errors"], 8);

        // Group 1: only in c1
        assert_eq!(merged.get_group_metadata(1).unwrap()["requests"], 200);

        // Group 2: only in c2
        assert_eq!(merged.get_group_metadata(2).unwrap()["requests"], 50);

        // Check all groups are present
        let ids = merged.group_ids();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[test]
    fn test_merge_empty_collectors() {
        let merged = GroupMetadataCollector::merge(vec![]);
        assert_eq!(merged.group_ids().len(), 0);
    }

    #[test]
    fn test_merge_complex_nested_structures() {
        let m1 = json!({
            "commands": {
                "GET": 100,
                "SET": 50
            },
            "redirects": {
                "moved": 10,
                "ask": 5
            }
        });

        let m2 = json!({
            "commands": {
                "GET": 200,
                "SET": 75,
                "DEL": 25
            },
            "redirects": {
                "moved": 15,
                "ask": 8
            },
            "cache": {
                "hits": 500,
                "misses": 100
            }
        });

        let merged = merge_metadata(vec![m1, m2]);

        // Commands should be summed
        assert_eq!(merged["commands"]["GET"], 300);
        assert_eq!(merged["commands"]["SET"], 125);
        assert_eq!(merged["commands"]["DEL"], 25);

        // Redirects should be summed
        assert_eq!(merged["redirects"]["moved"], 25);
        assert_eq!(merged["redirects"]["ask"], 13);

        // Cache should be present (only in m2)
        assert_eq!(merged["cache"]["hits"], 500);
        assert_eq!(merged["cache"]["misses"], 100);
    }

    #[test]
    fn test_merge_arrays() {
        let m1 = json!({"errors": ["timeout", "connection_reset"]});
        let m2 = json!({"errors": ["dns_failure"]});

        let merged = merge_metadata(vec![m1, m2]);

        assert_eq!(merged["errors"], json!(["timeout", "connection_reset", "dns_failure"]));
    }

    #[test]
    fn test_merge_mixed_types() {
        let m1 = json!({"value": 100});
        let m2 = json!({"value": "text"});

        let merged = merge_metadata(vec![m1, m2]);

        // When types conflict, later value wins
        assert_eq!(merged["value"], "text");
    }

    #[test]
    fn test_into_map() {
        let mut collector = GroupMetadataCollector::new();
        collector.set_group_metadata(0, json!({"count": 100}));
        collector.set_group_metadata(1, json!({"count": 200}));

        let map = collector.into_map();
        assert_eq!(map.len(), 2);
        assert_eq!(map[&0]["count"], 100);
        assert_eq!(map[&1]["count"], 200);
    }

    #[test]
    fn test_merge_floating_point_numbers() {
        let m1 = json!({"latency_avg": 10.5, "throughput": 100.25});
        let m2 = json!({"latency_avg": 15.3, "throughput": 150.75});

        let merged = merge_metadata(vec![m1, m2]);

        // Floating point addition
        assert!((merged["latency_avg"].as_f64().unwrap() - 25.8).abs() < 0.01);
        assert!((merged["throughput"].as_f64().unwrap() - 251.0).abs() < 0.01);
    }
}
