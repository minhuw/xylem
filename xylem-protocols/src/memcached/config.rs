//! Config-aware Memcached construction.

use crate::configs::{MemcachedCommandParams, MemcachedConfig, OperationsConfig};
use crate::memcached::{MemcachedAsciiProtocol, MemcachedBinaryProtocol, MemcachedOp};
use crate::workload::{CommandSelector, ValueSizeGenerator};
use anyhow::Result;

/// Supported operation names, for errors
const OPERATIONS: &str = "GET, SET, ADD, REPLACE, DELETE, INCR, DECR, TOUCH, MGET";

/// Parse a Memcached operation and its parameters; a command takes only its
/// own parameters
fn parse_memcached_op(op: &str, params: Option<&MemcachedCommandParams>) -> Result<MemcachedOp> {
    let default = MemcachedCommandParams::default();
    let params = params.unwrap_or(&default);
    let name = op.to_uppercase();
    let allowed: &[&str] = match name.as_str() {
        "MGET" => &["count"],
        "INCR" | "DECR" => &["delta"],
        "TOUCH" => &["exptime"],
        _ => &[],
    };
    for (key, set) in [
        ("count", params.count.is_some()),
        ("delta", params.delta.is_some()),
        ("exptime", params.exptime.is_some()),
    ] {
        anyhow::ensure!(
            !set || allowed.contains(&key),
            "Memcached command {op} takes no parameter {key}"
        );
    }
    Ok(match name.as_str() {
        "GET" => MemcachedOp::Get,
        "SET" => MemcachedOp::Set,
        "ADD" => MemcachedOp::Add,
        "REPLACE" => MemcachedOp::Replace,
        "DELETE" => MemcachedOp::Delete,
        "INCR" => MemcachedOp::Incr { delta: params.delta.unwrap_or(1) },
        "DECR" => MemcachedOp::Decr { delta: params.delta.unwrap_or(1) },
        "TOUCH" => MemcachedOp::Touch { exptime: params.exptime.unwrap_or(0) },
        "MGET" => {
            let count = params.count.unwrap_or(10);
            anyhow::ensure!(count > 0, "Memcached mget count must be > 0");
            MemcachedOp::MultiGet { count }
        }
        _ => anyhow::bail!("Unknown Memcached operation: {op}. Supported: {OPERATIONS}"),
    })
}

fn validate_keys(config: &MemcachedConfig) -> Result<()> {
    config.keys.validate()?;
    let prefix = config.keys.prefix();
    anyhow::ensure!(
        !prefix.bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control()) && prefix.len() <= 230,
        "Memcached key prefix must contain no whitespace/control bytes and leave room for a u64 key within 250 bytes"
    );
    Ok(())
}

impl MemcachedConfig {
    /// The selector of the measurement operations: `operation`, or the
    /// `operations` mix (default GET)
    pub fn operation_selector(
        &self,
        seed: Option<u64>,
    ) -> Result<Box<dyn CommandSelector<MemcachedOp>>> {
        let operations =
            OperationsConfig::resolve(self.operation.as_deref(), self.operations.as_ref(), "GET")?;
        operations.selector(&self.keys, seed, parse_memcached_op)
    }

    /// The operations every request may send, for validation
    pub fn operations_used(&self) -> Result<Vec<MemcachedOp>> {
        let operations =
            OperationsConfig::resolve(self.operation.as_deref(), self.operations.as_ref(), "GET")?;
        match operations.as_ref() {
            OperationsConfig::Fixed { operation } => Ok(vec![parse_memcached_op(operation, None)?]),
            OperationsConfig::Weighted { commands } => commands
                .iter()
                .map(|cmd| parse_memcached_op(&cmd.name, cmd.params.as_ref()))
                .collect(),
        }
    }

    fn value_size_generator(
        &self,
        seed: Option<u64>,
    ) -> Result<Option<Box<dyn ValueSizeGenerator>>> {
        self.value_size.as_ref().map(|sizes| sizes.to_generator(seed)).transpose()
    }
}

/// Apply every config field shared by both codecs
macro_rules! from_config {
    ($protocol:ty, $config:expr, $seed:expr) => {{
        let (config, seed) = ($config, $seed);
        validate_keys(config)?;
        let mut protocol = <$protocol>::with_workload(
            MemcachedOp::Get,
            config.keys.to_key_gen(seed)?,
            config.keys.value_size(),
        )
        .with_operations(config.operation_selector(seed)?)
        .with_key_prefix(config.keys.prefix().to_string());
        if let Some(sizes) = config.value_size_generator(seed)? {
            protocol = protocol.with_value_size_generator(sizes);
        }
        if let Some(insert) = &config.insert_phase {
            let (count, sizes) =
                insert.resolve_value_sizes(&config.keys, config.value_size.as_ref(), seed)?;
            protocol = protocol.with_insert_phase_sizes(count, sizes);
        }
        Ok(protocol)
    }};
}

impl MemcachedBinaryProtocol {
    /// Construct a workload with configured keys, prefix, operations, value
    /// sizes and insertion phase.
    pub fn from_config(config: &MemcachedConfig, seed: Option<u64>) -> Result<Self> {
        from_config!(Self, config, seed)
    }
}

impl MemcachedAsciiProtocol {
    /// Construct a workload with configured keys, prefix, operations, value
    /// sizes and insertion phase.
    pub fn from_config(config: &MemcachedConfig, seed: Option<u64>) -> Result<Self> {
        from_config!(Self, config, seed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Protocol;

    #[test]
    fn test_binary_from_config_default() {
        let protocol =
            MemcachedBinaryProtocol::from_config(&MemcachedConfig::default(), None).unwrap();
        assert_eq!(Protocol::name(&protocol), "memcached-binary");
    }

    #[test]
    fn test_ascii_from_config_default() {
        let protocol =
            MemcachedAsciiProtocol::from_config(&MemcachedConfig::default(), None).unwrap();
        assert_eq!(Protocol::name(&protocol), "memcached-ascii");
    }

    #[test]
    fn test_binary_from_config_with_set() {
        let config = MemcachedConfig {
            operation: Some("SET".to_string()),
            ..Default::default()
        };
        let protocol = MemcachedBinaryProtocol::from_config(&config, None).unwrap();
        assert_eq!(Protocol::name(&protocol), "memcached-binary");
    }

    #[test]
    fn test_invalid_operation() {
        let config = MemcachedConfig {
            operation: Some("INVALID".to_string()),
            ..Default::default()
        };
        assert!(MemcachedBinaryProtocol::from_config(&config, None).is_err());
    }

    fn config(json: serde_json::Value) -> MemcachedConfig {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn every_operation_and_its_params_parse() {
        use serde_json::json;
        for (command, expected) in [
            (json!({ "name": "get" }), MemcachedOp::Get),
            (json!({ "name": "SET" }), MemcachedOp::Set),
            (json!({ "name": "add" }), MemcachedOp::Add),
            (json!({ "name": "replace" }), MemcachedOp::Replace),
            (json!({ "name": "delete" }), MemcachedOp::Delete),
            (json!({ "name": "incr" }), MemcachedOp::Incr { delta: 1 }),
            (
                json!({ "name": "incr", "params": { "delta": 7 } }),
                MemcachedOp::Incr { delta: 7 },
            ),
            (
                json!({ "name": "decr", "params": { "delta": 2 } }),
                MemcachedOp::Decr { delta: 2 },
            ),
            (json!({ "name": "touch" }), MemcachedOp::Touch { exptime: 0 }),
            (
                json!({ "name": "touch", "params": { "exptime": 30 } }),
                MemcachedOp::Touch { exptime: 30 },
            ),
            (json!({ "name": "mget" }), MemcachedOp::MultiGet { count: 10 }),
            (
                json!({ "name": "mget", "params": { "count": 4 } }),
                MemcachedOp::MultiGet { count: 4 },
            ),
        ] {
            let mut command = command;
            command["weight"] = json!(1.0);
            let config =
                config(json!({ "operations": { "strategy": "weighted", "commands": [command] } }));
            assert_eq!(config.operations_used().unwrap(), [expected], "{command}");
            MemcachedAsciiProtocol::from_config(&config, None).unwrap();
        }
    }

    #[test]
    fn invalid_operations_are_rejected() {
        use serde_json::json;
        for (json, error) in [
            (
                json!({ "operation": "get", "operations": { "strategy": "fixed", "operation": "set" } }),
                "Set either operation or operations, not both",
            ),
            (json!({ "operation": "cas" }), "Unknown Memcached operation: cas"),
            (
                json!({ "operations": { "strategy": "weighted", "commands": [
                { "name": "get", "weight": 1.0, "params": { "count": 2 } }] } }),
                "Memcached command get takes no parameter count",
            ),
            (
                json!({ "operations": { "strategy": "weighted", "commands": [
                { "name": "incr", "weight": 1.0, "params": { "exptime": 2 } }] } }),
                "Memcached command incr takes no parameter exptime",
            ),
            (
                json!({ "operations": { "strategy": "weighted", "commands": [
                { "name": "mget", "weight": 1.0, "params": { "count": 0 } }] } }),
                "Memcached mget count must be > 0",
            ),
            (
                json!({ "operations": { "strategy": "weighted", "commands": [] } }),
                "requires at least one command",
            ),
            (
                json!({ "operations": { "strategy": "weighted", "commands": [
                { "name": "get", "weight": 0.0 }] } }),
                "All weights are zero",
            ),
        ] {
            let error_text = MemcachedBinaryProtocol::from_config(&config(json.clone()), None)
                .err()
                .unwrap_or_else(|| panic!("{json} is accepted"))
                .to_string();
            assert!(error_text.contains(error), "{json}: {error_text}");
        }
        let unknown = json!({ "operations": { "strategy": "weighted", "commands": [
            { "name": "get", "weight": 1.0, "params": { "bogus": 1 } }] } });
        assert!(serde_json::from_value::<MemcachedConfig>(unknown).is_err());
    }

    #[test]
    fn a_weighted_mix_follows_its_weights_with_per_command_sizes() {
        use crate::Protocol;
        use serde_json::json;
        let config = config(json!({
            "keys": { "strategy": "random", "max": 100, "value_size": 8 },
            "operations": { "strategy": "weighted", "commands": [
                { "name": "get", "weight": 0.6 },
                { "name": "set", "weight": 0.3 },
                { "name": "delete", "weight": 0.1 },
            ] },
            "value_size": { "strategy": "per_command",
                "commands": { "set": { "distribution": "weighted",
                    "sizes": [{ "size": 3, "weight": 1.0 }, { "size": 5, "weight": 1.0 }] } },
                "default": { "strategy": "fixed", "size": 1 } },
        }));
        let mut protocol = MemcachedAsciiProtocol::from_config(&config, Some(1)).unwrap();
        let mut counts = std::collections::HashMap::new();
        let mut sizes = std::collections::HashSet::new();
        for _ in 0..10_000 {
            let request = String::from_utf8(protocol.next_request(0).data).unwrap();
            let command = request.split(' ').next().unwrap().to_string();
            if command == "set" {
                sizes.insert(
                    request.split("\r\n").next().unwrap().rsplit(' ').next().unwrap().to_string(),
                );
            }
            *counts.entry(command).or_insert(0) += 1;
        }
        for (command, weight) in [("get", 0.6), ("set", 0.3), ("delete", 0.1)] {
            let share = counts[command] as f64 / 10_000.0;
            assert!((share - weight).abs() < 0.03, "{command}: {share}");
        }
        assert_eq!(sizes, ["3", "5"].map(String::from).into());
    }

    fn ascii_set_value_len(data: &[u8]) -> usize {
        let s = std::str::from_utf8(data).unwrap();
        let (header, rest) = s.split_once("\r\n").unwrap();
        assert!(header.starts_with("set "), "expected ASCII set, got {header:?}");
        let len: usize = header.rsplit(' ').next().unwrap().parse().unwrap();
        assert_eq!(rest.strip_suffix("\r\n").unwrap().len(), len);
        len
    }

    fn binary_set_value_len(data: &[u8]) -> usize {
        assert!(data.len() >= 24, "binary request shorter than header");
        assert_eq!(data[0], 0x80, "expected request magic");
        assert_eq!(data[1], 0x01, "expected binary SET opcode");
        let key_len = u16::from_be_bytes([data[2], data[3]]) as usize;
        let extra_len = usize::from(data[4]);
        let body_len = u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;
        assert_eq!(extra_len, 8);
        assert_eq!(data.len(), 24 + body_len);
        body_len - extra_len - key_len
    }

    fn collect_insert_sizes(cfg: &MemcachedConfig, seed: Option<u64>, count: usize) -> Vec<usize> {
        use crate::Protocol;
        let mut ascii = MemcachedAsciiProtocol::from_config(cfg, seed).unwrap();
        let mut binary = MemcachedBinaryProtocol::from_config(cfg, seed).unwrap();
        let mut ascii_sizes = Vec::with_capacity(count);
        let mut binary_sizes = Vec::with_capacity(count);
        for _ in 0..count {
            let a = ascii.next_request(0);
            let b = binary.next_request(0);
            assert!(a.metadata.is_warmup);
            assert!(b.metadata.is_warmup);
            ascii_sizes.push(ascii_set_value_len(&a.data));
            binary_sizes.push(binary_set_value_len(&b.data));
        }
        assert!(!ascii.next_request(0).metadata.is_warmup);
        assert!(!binary.next_request(0).metadata.is_warmup);
        assert_eq!(ascii_sizes, binary_sizes);
        ascii_sizes
    }

    #[test]
    fn insert_phase_inherits_workload_value_size_distributions() {
        use serde_json::json;

        // Weighted distribution inherited when insert_phase = {}
        let weighted_cfg = config(json!({
            "keys": { "strategy": "round-robin", "max": 60, "value_size": 8 },
            "value_size": {
                "strategy": "weighted",
                "sizes": [{ "size": 16, "weight": 1.0 }, { "size": 256, "weight": 1.0 }]
            },
            "insert_phase": {},
        }));
        let sizes = collect_insert_sizes(&weighted_cfg, Some(7), 60);
        let unique: std::collections::HashSet<_> = sizes.into_iter().collect();
        assert_eq!(unique, [16, 256].into());

        // Uniform distribution inherited when insert_phase = {}
        let uniform_cfg = config(json!({
            "keys": { "strategy": "round-robin", "max": 80, "value_size": 8 },
            "value_size": { "strategy": "uniform", "min": 10, "max": 20 },
            "insert_phase": {},
        }));
        let sizes = collect_insert_sizes(&uniform_cfg, Some(9), 80);
        assert!(sizes.iter().all(|&s| (10..=20).contains(&s)));
        assert!(sizes.iter().collect::<std::collections::HashSet<_>>().len() > 1);

        // PerCommand selecting "SET"
        let per_cmd_set_cfg = config(json!({
            "keys": { "strategy": "round-robin", "max": 50, "value_size": 8 },
            "value_size": {
                "strategy": "per_command",
                "commands": {
                    "set": {
                        "distribution": "weighted",
                        "sizes": [{ "size": 32, "weight": 1.0 }, { "size": 64, "weight": 1.0 }]
                    }
                },
                "default": { "strategy": "fixed", "size": 99 }
            },
            "insert_phase": {},
        }));
        let sizes = collect_insert_sizes(&per_cmd_set_cfg, Some(11), 50);
        let unique: std::collections::HashSet<_> = sizes.into_iter().collect();
        assert_eq!(unique, [32, 64].into());

        // PerCommand falling back to default when "set" is not specified
        let per_cmd_default_cfg = config(json!({
            "keys": { "strategy": "round-robin", "max": 40, "value_size": 8 },
            "value_size": {
                "strategy": "per_command",
                "commands": {
                    "add": { "distribution": "fixed", "size": 500 }
                },
                "default": { "strategy": "uniform", "min": 40, "max": 45 }
            },
            "insert_phase": {},
        }));
        let sizes = collect_insert_sizes(&per_cmd_default_cfg, Some(13), 40);
        assert!(sizes.iter().all(|&s| (40..=45).contains(&s)));
        assert!(sizes.iter().collect::<std::collections::HashSet<_>>().len() > 1);
    }

    #[test]
    fn insert_phase_value_size_fixed_and_explicit_distribution_override_workload() {
        use serde_json::json;

        // Fixed scalar override
        let fixed_override = config(json!({
            "keys": { "strategy": "round-robin", "max": 10, "value_size": 8 },
            "value_size": { "strategy": "uniform", "min": 16, "max": 64 },
            "insert_phase": { "value_size": 128 },
        }));
        let sizes = collect_insert_sizes(&fixed_override, Some(42), 10);
        assert_eq!(sizes, vec![128; 10]);

        // Explicit distribution override independent of config.value_size
        let dist_override = config(json!({
            "keys": { "strategy": "round-robin", "max": 50, "value_size": 8 },
            "value_size": { "strategy": "fixed", "size": 999 },
            "insert_phase": {
                "value_size": {
                    "strategy": "weighted",
                    "sizes": [{ "size": 12, "weight": 1.0 }, { "size": 48, "weight": 1.0 }]
                }
            },
        }));
        let sizes = collect_insert_sizes(&dist_override, Some(42), 50);
        let unique: std::collections::HashSet<_> = sizes.into_iter().collect();
        assert_eq!(unique, [12, 48].into());
    }

    #[test]
    fn insert_phase_seed_determinism_and_stream_independence() {
        use crate::Protocol;
        use serde_json::json;

        let cfg = config(json!({
            "keys": { "strategy": "round-robin", "max": 32, "value_size": 8 },
            "operation": "SET",
            "value_size": { "strategy": "uniform", "min": 10, "max": 1000 },
            "insert_phase": {},
        }));

        // Same seed yields identical insert sizes
        let first = collect_insert_sizes(&cfg, Some(12345), 32);
        let second = collect_insert_sizes(&cfg, Some(12345), 32);
        assert_eq!(first, second);

        // Insert stream is independent of measurement value_size stream
        let mut ascii = MemcachedAsciiProtocol::from_config(&cfg, Some(12345)).unwrap();
        let mut binary = MemcachedBinaryProtocol::from_config(&cfg, Some(12345)).unwrap();
        let insert_ascii: Vec<_> = (0..32)
            .map(|_| {
                let req = ascii.next_request(0);
                assert!(req.metadata.is_warmup);
                ascii_set_value_len(&req.data)
            })
            .collect();
        let measure_ascii: Vec<_> = (0..32)
            .map(|_| {
                let req = ascii.next_request(0);
                assert!(!req.metadata.is_warmup);
                ascii_set_value_len(&req.data)
            })
            .collect();
        let insert_binary: Vec<_> = (0..32)
            .map(|_| {
                let req = binary.next_request(0);
                assert!(req.metadata.is_warmup);
                binary_set_value_len(&req.data)
            })
            .collect();
        let measure_binary: Vec<_> = (0..32)
            .map(|_| {
                let req = binary.next_request(0);
                assert!(!req.metadata.is_warmup);
                binary_set_value_len(&req.data)
            })
            .collect();
        assert_eq!(insert_ascii, insert_binary);
        assert_eq!(measure_ascii, measure_binary);
        assert_ne!(insert_ascii, measure_ascii);
    }

    #[test]
    fn non_memcached_protocols_reject_distribution_insert_phase_value_size() {
        use crate::configs::{MasstreeConfig, RedisConfig};
        use crate::{MasstreeProtocol, RedisProtocol};
        use serde_json::json;

        let redis_cfg: RedisConfig = serde_json::from_value(json!({
            "keys": { "strategy": "round-robin", "max": 10, "value_size": 8 },
            "insert_phase": {
                "value_size": { "strategy": "uniform", "min": 8, "max": 64 }
            }
        }))
        .unwrap();
        let redis_err = RedisProtocol::from_config(&redis_cfg, Some(1)).err().unwrap().to_string();
        assert!(redis_err.contains("Memcached"), "unexpected Redis error: {redis_err}");

        let masstree_cfg: MasstreeConfig = serde_json::from_value(json!({
            "keys": { "strategy": "round-robin", "max": 10, "value_size": 8 },
            "insert_phase": {
                "value_size": {
                    "strategy": "weighted",
                    "sizes": [{ "size": 16, "weight": 1.0 }]
                }
            }
        }))
        .unwrap();
        let masstree_err =
            MasstreeProtocol::from_config(&masstree_cfg, Some(1)).err().unwrap().to_string();
        assert!(masstree_err.contains("Memcached"), "unexpected Masstree error: {masstree_err}");
    }
}
