//! Xylem Protocol Implementations
//!
//! This crate provides implementations of various application-level protocols
//! for latency measurement.

pub use xylem_common::{Protocol, Request, RequestMeta};

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
pub use redis::command_selector::{
    CommandSelector, FixedCommandSelector, KeyGenerator, WeightedCommandSelector,
};
pub use redis::command_template::CommandTemplate;
pub use redis::{RedisOp, RedisProtocol};
pub use xylem_echo::XylemEchoProtocol;
