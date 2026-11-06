//! Xylem Protocol Implementations
//!
//! This crate provides implementations of various application-level protocols
//! for latency measurement.

pub use xylem_common::{ParseResult, Protocol, Request, RequestMeta, RetryRequest};

pub mod configs;
pub mod factories;
pub mod factory;
pub mod http;
pub mod masstree;
pub mod memcached;
pub mod protocol;
pub mod redis;
pub mod workload;
pub mod xylem_echo;

// Re-export commonly used types
pub use configs::{
    CommandValueSizeConfig, DataImportConfig, HttpConfig, InsertPhaseConfig, KeysConfig,
    MasstreeConfig, MasstreeScanConfig, MemcachedConfig, RedisCommandParams, RedisCommandWeight,
    RedisConfig, RedisOperationsConfig, ValueSizeConfig, VerificationConfig, XylemEchoConfig,
};
pub use factories::{
    HttpFactory, MemcachedAsciiFactory, MemcachedBinaryFactory, RedisFactory, XylemEchoFactory,
};
pub use factory::ProtocolFactory;
pub use http::HttpMethod;
pub use http::HttpProtocol;
pub use masstree::{MasstreeOp, MasstreeProtocol, ResultCode};
pub use memcached::{MemcachedAsciiProtocol, MemcachedBinaryProtocol};
pub use redis::cluster::{
    generate_asking_command, generate_cluster_slots_command, parse_redirect, ClusterNode,
    ClusterRequestId, ClusterTopology, RedirectStats, RedirectType, RedisClusterProtocol,
    SlotRange,
};
pub use redis::command_selector::{
    CommandSelector, FixedCommandSelector, KeyGenerator, WeightedCommandSelector,
};
pub use redis::command_template::CommandTemplate;
pub use redis::crc16::crc16;
pub use redis::slot::{
    calculate_slot, calculate_slot_str, extract_hash_tag, has_hash_tag, CLUSTER_SLOTS,
};
pub use redis::{RedisOp, RedisProtocol};
pub use xylem_echo::XylemEchoProtocol;
