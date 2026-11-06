//! Protocol factory implementations for built-in protocols
//!
//! Each factory creates protocol instances from their respective config types.

mod http;
mod masstree;
mod memcached;
mod redis;
mod xylem_echo;

pub use http::HttpFactory;
pub use memcached::{MemcachedAsciiFactory, MemcachedBinaryFactory};
pub use redis::{parse_redis_operation, RedisFactory};
pub use xylem_echo::XylemEchoFactory;
