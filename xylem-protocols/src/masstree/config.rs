//! Config-aware Masstree construction.
use crate::configs::{MasstreeCommandParams, MasstreeConfig, OperationsConfig};
use crate::masstree::{MasstreeOp, MasstreeProtocol};
use crate::workload::CommandSelector;
use anyhow::Result;

impl MasstreeProtocol {
    /// Construct a workload with the configured operations, keys, prefix,
    /// value sizes, random data and insertion phase.
    pub fn from_config(config: &MasstreeConfig, seed: Option<u64>) -> Result<Self> {
        let mut protocol = Self::with_workload_and_options(
            MasstreeOp::Get,
            config.keys.to_key_gen(seed)?,
            config.keys.value_size(),
            config.keys.prefix().to_string(),
            config.random_data,
            seed,
            None, // insert_phase handled below
        )
        .with_operations(config.operation_selector(seed)?);
        if let Some(sizes) = &config.value_size {
            protocol = protocol.with_value_size_generator(sizes.to_generator(seed)?);
        }

        // Apply insert phase if configured
        if let Some(insert) = &config.insert_phase {
            let (count, size) = insert.resolve(&config.keys)?;
            protocol = protocol.with_insert_phase(count, size);
        }

        Ok(protocol)
    }
}

impl MasstreeConfig {
    /// The selector of the measurement operations: `operation`, or the
    /// `operations` mix (default get)
    pub fn operation_selector(
        &self,
        seed: Option<u64>,
    ) -> Result<Box<dyn CommandSelector<MasstreeOp>>> {
        self.keys.validate()?;
        let operations =
            OperationsConfig::resolve(self.operation.as_deref(), self.operations.as_ref(), "get")?;
        operations.selector(&self.keys, seed, |name, params| self.parse_op(name, params))
    }

    /// Parse a Masstree operation; scan and put parameters default to the
    /// top-level `scan` and `put_columns`
    fn parse_op(&self, op: &str, params: Option<&MasstreeCommandParams>) -> Result<MasstreeOp> {
        let default = MasstreeCommandParams::default();
        let params = params.unwrap_or(&default);
        let name = op.to_lowercase();
        let allowed: &[&str] = match name.as_str() {
            "scan" => &["count", "fields"],
            "put" => &["columns"],
            _ => &[],
        };
        for (key, set) in [
            ("count", params.count.is_some()),
            ("fields", params.fields.is_some()),
            ("columns", params.columns.is_some()),
        ] {
            anyhow::ensure!(
                !set || allowed.contains(&key),
                "Masstree command {op} takes no parameter {key}"
            );
        }
        Ok(match name.as_str() {
            "get" => MasstreeOp::Get,
            "set" => MasstreeOp::Set,
            "put" => {
                let columns = params.columns.or(self.put_columns).unwrap_or(1);
                anyhow::ensure!(
                    (1..=u16::MAX as usize).contains(&columns),
                    "Masstree put columns must be in 1..=65535, got {columns}"
                );
                MasstreeOp::Put { columns: columns as u32 }
            }
            "remove" => MasstreeOp::Remove,
            "scan" => {
                let scan = self.scan.as_ref();
                let count = params.count.or(scan.map(|s| s.count)).unwrap_or(10);
                anyhow::ensure!(
                    (1..=i32::MAX as usize).contains(&count),
                    "Masstree scan count must be in 1..={}, got {count}",
                    i32::MAX
                );
                let fields = match (&params.fields, scan) {
                    (Some(fields), _) => fields.clone(),
                    (None, Some(scan)) => column_indices(&scan.fields)?,
                    (None, None) => Vec::new(),
                };
                MasstreeOp::Scan { count: count as u32, fields }
            }
            "checkpoint" => MasstreeOp::Checkpoint,
            _ => anyhow::bail!(
                "Unknown Masstree operation: {op}. Supported: get, set, put, remove, scan, checkpoint"
            ),
        })
    }
}

/// The column indices of the decimal strings `fields`
fn column_indices(fields: &[String]) -> Result<Vec<u32>> {
    fields
        .iter()
        .map(|field| {
            field
                .parse()
                .map_err(|_| anyhow::anyhow!("Masstree scan field {field:?} is not a column index"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config(json: serde_json::Value) -> MasstreeConfig {
        serde_json::from_value(json).unwrap()
    }

    fn op(json: serde_json::Value) -> Result<MasstreeOp> {
        let config = config(json);
        let mut selector = config.operation_selector(None)?;
        Ok(selector.next_command())
    }

    #[test]
    fn operations_and_params_parse_with_top_level_defaults() {
        let weighted = |command: serde_json::Value, top: serde_json::Value| {
            let mut config =
                json!({ "operations": { "strategy": "weighted", "commands": [command] } });
            config.as_object_mut().unwrap().extend(top.as_object().unwrap().clone());
            config
        };
        for (config, expected) in [
            (json!({}), MasstreeOp::Get),
            (json!({ "operation": "SET" }), MasstreeOp::Set),
            (json!({ "operation": "put" }), MasstreeOp::Put { columns: 1 }),
            (json!({ "operation": "put", "put_columns": 3 }), MasstreeOp::Put { columns: 3 }),
            (json!({ "operation": "remove" }), MasstreeOp::Remove),
            (json!({ "operation": "checkpoint" }), MasstreeOp::Checkpoint),
            (json!({ "operation": "scan" }), MasstreeOp::Scan { count: 10, fields: vec![] }),
            (
                json!({ "operation": "scan", "scan": { "count": 4, "fields": ["0", "2"] } }),
                MasstreeOp::Scan { count: 4, fields: vec![0, 2] },
            ),
            (
                weighted(
                    json!({ "name": "scan", "weight": 1.0, "params": { "count": 2, "fields": [1] } }),
                    json!({ "scan": { "count": 4 } }),
                ),
                MasstreeOp::Scan { count: 2, fields: vec![1] },
            ),
            (
                weighted(
                    json!({ "name": "put", "weight": 1.0, "params": { "columns": 2 } }),
                    json!({ "put_columns": 5 }),
                ),
                MasstreeOp::Put { columns: 2 },
            ),
        ] {
            assert_eq!(op(config.clone()).unwrap(), expected, "{config}");
        }
    }

    #[test]
    fn invalid_operations_are_rejected() {
        for (config, error) in [
            (json!({ "operation": "cas" }), "Unknown Masstree operation: cas"),
            (
                json!({ "operation": "get", "operations": { "strategy": "fixed", "operation": "set" } }),
                "Set either operation or operations, not both",
            ),
            (
                json!({ "operations": { "strategy": "weighted", "commands": [
                    { "name": "get", "weight": 1.0, "params": { "count": 2 } }] } }),
                "Masstree command get takes no parameter count",
            ),
            (json!({ "operation": "scan", "scan": { "count": 0 } }), "scan count must be in"),
            (json!({ "operation": "put", "put_columns": 0 }), "put columns must be in"),
            (
                json!({ "operation": "scan", "scan": { "fields": ["name"] } }),
                "Masstree scan field \"name\" is not a column index",
            ),
        ] {
            let message =
                op(config.clone()).err().unwrap_or_else(|| panic!("{config}")).to_string();
            assert!(message.contains(error), "{config}: {message}");
        }
    }
}
