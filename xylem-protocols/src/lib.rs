//! Xylem Protocol Implementations
//!
//! This crate provides implementations of various application-level protocols
//! for latency measurement.

pub use xylem_common::{
    ConnectionRole, ErrorReply, Outbox, Parsed, Protocol, Request, RequestMeta,
};

pub mod configs;
pub mod dns;
pub mod http;
pub mod masstree;
pub mod memcached;
pub mod redis;
pub mod workload;
pub mod xylem_echo;

// Re-export commonly used types
pub use configs::{
    CommandValueSizeConfig, HttpConfig, InsertPhaseConfig, KeysConfig, MasstreeCommandParams,
    MasstreeConfig, MasstreeOperationsConfig, MasstreeScanConfig, MemcachedCommandParams,
    MemcachedConfig, MemcachedOperationsConfig, OperationWeight, OperationsConfig, RedisAuthConfig,
    RedisCommandParams, RedisCommandWeight, RedisConfig, RedisOperationsConfig, SizeWeight,
    ValueSizeConfig, XylemEchoConfig,
};
pub use dns::{DnsConfig, DnsFraming, DnsProtocol};
pub use http::HttpMethod;
pub use http::HttpProtocol;
pub use masstree::{MasstreeOp, MasstreeProtocol, ResultCode};
pub use memcached::{MemcachedAsciiProtocol, MemcachedBinaryProtocol};
pub use redis::command_template::CommandTemplate;
pub use redis::{RedisOp, RedisProtocol};
pub use workload::selector::{
    CommandSelector, FixedCommandSelector, KeyGenerator, WeightedCommandSelector,
};
pub use xylem_echo::XylemEchoProtocol;
