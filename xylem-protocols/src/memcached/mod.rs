//! Memcached protocol implementations

pub mod ascii;
pub mod binary;

pub use ascii::MemcachedAsciiProtocol;
pub use binary::MemcachedBinaryProtocol;

// Re-export MemcachedOp from binary (both ascii and binary define the same enum)
pub use binary::MemcachedOp;
