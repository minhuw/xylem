//! Config-aware Masstree construction.
use crate::configs::MasstreeConfig;
use crate::masstree::{MasstreeOp, MasstreeProtocol};
use anyhow::Result;

impl MasstreeProtocol {
    /// Construct a workload with the configured operation, keys, prefix, value size,
    /// random data and insertion phase. A configured `value_size` is sent at its
    /// representative fixed size; otherwise `keys.value_size` is used.
    pub fn from_config(config: &MasstreeConfig, seed: Option<u64>) -> Result<Self> {
        let value_size = config
            .value_size
            .as_ref()
            .map(|vs| vs.fixed_size())
            .unwrap_or_else(|| config.keys.value_size());

        let mut protocol = Self::with_workload_and_options(
            parse_masstree_op(config)?,
            config.keys.to_key_gen(seed)?,
            value_size,
            config.keys.prefix().to_string(),
            config.random_data,
            seed,
            None, // insert_phase handled below
        );

        // Apply insert phase if configured
        if let Some(insert) = &config.insert_phase {
            let (count, size) = insert.resolve(&config.keys)?;
            protocol = protocol.with_insert_phase(count, size);
        }

        Ok(protocol)
    }
}

/// Parse Masstree operation from config
fn parse_masstree_op(config: &MasstreeConfig) -> Result<MasstreeOp> {
    match config.operation.to_lowercase().as_str() {
        "get" => Ok(MasstreeOp::Get),
        "set" => Ok(MasstreeOp::Set),
        "put" => {
            // For PUT, create column updates. Default to single column (0) with empty placeholder.
            // The actual value will be filled in during request generation.
            let num_columns = config.put_columns.unwrap_or(1);
            let columns: Vec<(u32, Vec<u8>)> =
                (0..num_columns as u32).map(|i| (i, Vec::new())).collect();
            Ok(MasstreeOp::Put { columns })
        }
        "remove" => Ok(MasstreeOp::Remove),
        "scan" => {
            let scan_config = config.scan.as_ref();
            let count = scan_config.map(|s| s.count as u32).unwrap_or(10);
            // Convert string field names to u32 indices (assume numeric field names or use index)
            let fields: Vec<u32> = scan_config
                .map(|s| s.fields.iter().filter_map(|f| f.parse().ok()).collect())
                .unwrap_or_default();
            Ok(MasstreeOp::Scan {
                firstkey: String::new(), // Will be set during request generation
                count,
                fields,
            })
        }
        "checkpoint" => Ok(MasstreeOp::Checkpoint),
        other => anyhow::bail!(
            "Unknown Masstree operation: {}. Supported: get, set, put, remove, scan, checkpoint",
            other
        ),
    }
}
