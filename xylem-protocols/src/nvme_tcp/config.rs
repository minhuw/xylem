//! NVMe/TCP protocol configuration

use serde::{Deserialize, Serialize};

/// Longest NQN the NVMe base specification allows, in bytes
pub const MAX_NQN_LEN: usize = 223;

/// Host NQN used unless `hostnqn` is set
pub const DEFAULT_HOSTNQN: &str =
    "nqn.2014-08.org.nvmexpress:uuid:78796c65-6d00-4e56-4d65-2f5443500001";

/// NVMe/TCP protocol configuration
///
/// Each thread of the group connects one controller per
/// `io_queues_per_controller` of its connections: an admin queue on a
/// connection of its own, then one I/O queue per connection. The group's
/// `connections_per_thread` counts I/O queues; the admin queues are extra.
/// Only I/O queue commands are measured.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct NvmeTcpConfig {
    /// NQN of the NVM subsystem to connect to
    pub subnqn: String,
    /// Host NQN to connect as
    #[serde(default = "default_hostnqn")]
    pub hostnqn: String,
    /// Namespace to read and write (default: 1)
    #[serde(default = "default_nsid")]
    pub nsid: u32,
    /// Bytes per read or write, a multiple of the namespace's LBA size
    /// (default: 4096). Writes up to the target's in-capsule data size go
    /// in the command capsule, larger ones after an R2T.
    #[serde(default = "default_block_size")]
    pub block_size: u32,
    /// Percentage of commands that are reads, the rest writes (default: 100)
    #[serde(default = "default_read_percent")]
    pub read_percent: u8,
    /// Order of the blocks commands address (default: "random")
    #[serde(default)]
    pub access: NvmeAccess,
    /// First LBA of the range commands address (default: 0)
    #[serde(default)]
    pub lba_start: u64,
    /// LBAs in the range (default: up to the end of the namespace)
    #[serde(default)]
    pub lba_count: Option<u64>,
    /// I/O queues per controller (default: all of a thread's connections of
    /// the group share one controller)
    #[serde(default)]
    pub io_queues_per_controller: Option<usize>,
    /// Readback-compare mode: every write carries a pattern fixed by its
    /// LBA, each thread first writes the whole range, and every read must
    /// return the pattern of its LBAs or the run fails (default: false)
    #[serde(default)]
    pub verify: bool,
}

/// Order of the blocks of the range that commands address
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum NvmeAccess {
    /// Blocks in turn from the start of the range, wrapping at its end
    Sequential,
    /// Uniformly random blocks of the range
    #[default]
    Random,
}

fn default_hostnqn() -> String {
    DEFAULT_HOSTNQN.to_string()
}

fn default_nsid() -> u32 {
    1
}

fn default_block_size() -> u32 {
    4096
}

fn default_read_percent() -> u8 {
    100
}

impl NvmeTcpConfig {
    /// Check what can be checked without the target
    pub fn validate(&self) -> anyhow::Result<()> {
        for (name, nqn) in [("subnqn", &self.subnqn), ("hostnqn", &self.hostnqn)] {
            if nqn.is_empty() || nqn.len() > MAX_NQN_LEN || nqn.contains('\0') {
                anyhow::bail!("{name} must be 1 to {MAX_NQN_LEN} bytes without NUL");
            }
        }
        if self.nsid == 0 || self.nsid == u32::MAX {
            anyhow::bail!("nsid must be from 1 to {}", u32::MAX - 1);
        }
        if self.block_size == 0 {
            anyhow::bail!("block_size must be > 0");
        }
        if self.read_percent > 100 {
            anyhow::bail!("read_percent must be from 0 to 100");
        }
        if self.lba_count == Some(0) {
            anyhow::bail!("lba_count must be > 0");
        }
        if self.io_queues_per_controller == Some(0) {
            anyhow::bail!("io_queues_per_controller must be > 0");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config(value: serde_json::Value) -> NvmeTcpConfig {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn defaults_read_random_4k_blocks_of_namespace_1() {
        let config = config(json!({ "subnqn": "nqn.test" }));
        assert_eq!((config.nsid, config.block_size, config.read_percent), (1, 4096, 100));
        assert_eq!(config.access, NvmeAccess::Random);
        assert_eq!(config.hostnqn, DEFAULT_HOSTNQN);
        assert!(!config.verify);
        config.validate().unwrap();
    }

    #[test]
    fn invalid_values_are_rejected() {
        for (field, value, message) in [
            ("subnqn", json!(""), "subnqn"),
            ("subnqn", json!("n".repeat(MAX_NQN_LEN + 1)), "subnqn"),
            ("nsid", json!(0), "nsid"),
            ("block_size", json!(0), "block_size"),
            ("read_percent", json!(101), "read_percent"),
            ("lba_count", json!(0), "lba_count"),
            ("io_queues_per_controller", json!(0), "io_queues_per_controller"),
        ] {
            let mut value_map = json!({ "subnqn": "nqn.test" });
            value_map[field] = value;
            let error = config(value_map).validate().unwrap_err().to_string();
            assert!(error.contains(message), "{field}: {error}");
        }
    }
}
