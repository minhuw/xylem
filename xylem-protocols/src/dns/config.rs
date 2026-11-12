//! DNS protocol configuration

use crate::configs::KeysConfig;
use serde::{Deserialize, Serialize};

/// DNS protocol configuration
///
/// Each query asks one question in class IN: a name drawn from `names` and a
/// type drawn from `qtypes`, independently and each by weight, or a (name,
/// type) pair drawn from `queries` by weight.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DnsConfig {
    /// Query names, each drawn with probability weight / total weight
    #[serde(default)]
    pub names: Vec<DnsNameConfig>,
    /// Query types, each drawn with probability weight / total weight
    /// (default: A only)
    #[serde(default)]
    pub qtypes: Option<Vec<DnsQtypeConfig>>,
    /// Questions as (name, type) pairs, each drawn with probability
    /// weight / total weight; instead of `names` and `qtypes`
    #[serde(default)]
    pub queries: Vec<DnsQueryConfig>,
    /// Set the RD (recursion desired) flag (default: true, as dig and
    /// dnspython do)
    #[serde(default = "default_recursion_desired")]
    pub recursion_desired: bool,
    /// Add an EDNS(0) OPT record to every query (default: none)
    #[serde(default)]
    pub edns: Option<DnsEdnsConfig>,
    /// What an NXDOMAIN response counts as: "error" (default), an error reply
    /// of kind NXDOMAIN, or "success"
    #[serde(default)]
    pub nxdomain: DnsNxdomain,
}

/// One entry of the query name mix: either a fixed `name`, or a `pattern`
/// whose `{key}` is replaced by a key drawn from `keys` for each query
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DnsNameConfig {
    /// A fixed name, such as "www.example.test." (the final dot is optional)
    #[serde(default)]
    pub name: Option<String>,
    /// A name with one "{key}", such as "host{key}.example.test."
    #[serde(default)]
    pub pattern: Option<String>,
    /// Key generator for `pattern`
    #[serde(default)]
    pub keys: Option<DnsKeysConfig>,
    /// Relative weight (default: 1)
    #[serde(default = "default_weight")]
    pub weight: f64,
}

/// One entry of the question mix: a fixed `name` or a `pattern`, as in
/// [`DnsNameConfig`], and its `qtype`
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DnsQueryConfig {
    /// A fixed name, such as "www.example.test." (the final dot is optional)
    #[serde(default)]
    pub name: Option<String>,
    /// A name with one "{key}", such as "host{key}.example.test."
    #[serde(default)]
    pub pattern: Option<String>,
    /// Key generator for `pattern`
    #[serde(default)]
    pub keys: Option<DnsKeysConfig>,
    /// Type mnemonic or "TYPE<n>", as in [`DnsQtypeConfig`]
    pub qtype: String,
    /// Relative weight (default: 1)
    #[serde(default = "default_weight")]
    pub weight: f64,
}

/// Key generator of a name pattern; the key is a decimal number
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "strategy", rename_all = "lowercase", deny_unknown_fields)]
pub enum DnsKeysConfig {
    /// Sequential keys from `start`
    Sequential {
        #[serde(default)]
        start: u64,
    },
    /// Uniformly random keys in [0, max)
    Random { max: u64 },
    /// Keys 0, 1, ..., max - 1, then again from 0
    #[serde(rename = "round-robin")]
    RoundRobin { max: u64 },
    /// Zipfian keys in [0, n) with exponent `theta`
    Zipfian { n: u64, theta: f64 },
    /// Normally distributed keys in [0, max)
    Gaussian {
        mean_pct: f64,
        std_dev_pct: f64,
        max: u64,
    },
}

impl DnsKeysConfig {
    /// The same strategy as a key-value [`KeysConfig`], whose value size and
    /// prefix DNS does not use
    pub fn to_keys_config(&self) -> KeysConfig {
        let (value_size, prefix) = (1, String::new());
        match *self {
            Self::Sequential { start } => KeysConfig::Sequential { start, value_size, prefix },
            Self::Random { max } => KeysConfig::Random { max, value_size, prefix },
            Self::RoundRobin { max } => KeysConfig::RoundRobin { max, value_size, prefix },
            Self::Zipfian { n, theta } => KeysConfig::Zipfian { n, theta, value_size, prefix },
            Self::Gaussian { mean_pct, std_dev_pct, max } => KeysConfig::Gaussian {
                mean_pct,
                std_dev_pct,
                max,
                value_size,
                prefix,
            },
        }
    }
}

/// One entry of the query type mix
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DnsQtypeConfig {
    /// Type mnemonic (A, NS, CNAME, SOA, PTR, MX, TXT, AAAA, SRV, NAPTR, DS,
    /// RRSIG, NSEC, DNSKEY, SVCB, HTTPS, CAA, ANY) or "TYPE<n>"
    pub qtype: String,
    /// Relative weight (default: 1)
    #[serde(default = "default_weight")]
    pub weight: f64,
}

/// EDNS(0) OPT record settings (RFC 6891)
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DnsEdnsConfig {
    /// Advertised UDP payload size, 512 to 65535 (default: 1232)
    #[serde(default = "default_udp_size")]
    pub udp_size: u16,
    /// Set the DO (DNSSEC OK) bit (default: false)
    #[serde(default)]
    pub dnssec_ok: bool,
}

/// What an NXDOMAIN response counts as
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum DnsNxdomain {
    /// An error reply of kind "NXDOMAIN"
    #[default]
    Error,
    /// A successful response
    Success,
}

impl Default for DnsConfig {
    fn default() -> Self {
        Self {
            names: Vec::new(),
            qtypes: None,
            queries: Vec::new(),
            recursion_desired: default_recursion_desired(),
            edns: None,
            nxdomain: DnsNxdomain::default(),
        }
    }
}

/// The type mix without `qtypes`: A only
pub(super) fn default_qtypes() -> Vec<DnsQtypeConfig> {
    vec![DnsQtypeConfig {
        qtype: "A".to_string(),
        weight: default_weight(),
    }]
}

fn default_recursion_desired() -> bool {
    true
}

fn default_weight() -> f64 {
    1.0
}

fn default_udp_size() -> u16 {
    1232
}
