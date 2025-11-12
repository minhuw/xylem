//! Test lifecycle of Docker-based service guards
//!
//! This file contains all guard lifecycle tests to avoid duplicate container
//! starts when multiple test binaries run in parallel.

mod common;

#[test]
fn test_redis_guard_lifecycle() {
    use common::redis::RedisGuard;

    eprintln!("Testing RedisGuard lifecycle...");
    let _guard = RedisGuard::new().expect("Failed to start Redis");
    eprintln!("✓ RedisGuard started successfully");

    // Guard will be dropped here and container cleaned up
}

#[test]
fn test_memcached_guard_lifecycle() {
    use common::memcached::MemcachedGuard;

    eprintln!("Testing MemcachedGuard lifecycle...");
    let _guard = MemcachedGuard::new().expect("Failed to start Memcached");
    eprintln!("✓ MemcachedGuard started successfully");
}

#[test]
fn test_nginx_guard_lifecycle() {
    use common::nginx::NginxGuard;

    eprintln!("Testing NginxGuard lifecycle...");
    let _guard = NginxGuard::new().expect("Failed to start Nginx");
    eprintln!("✓ NginxGuard started successfully");
}

#[test]
fn test_redis_cluster_guard_lifecycle() {
    use common::redis_cluster::RedisClusterGuard;

    eprintln!("Testing RedisClusterGuard lifecycle...");
    let _guard = RedisClusterGuard::new().expect("Failed to start cluster");
    eprintln!("✓ RedisClusterGuard started successfully");
}

#[test]
fn test_multi_protocol_guard_lifecycle() {
    use common::multi_protocol::MultiProtocolGuard;

    eprintln!("Testing MultiProtocolGuard lifecycle...");
    let _guard = MultiProtocolGuard::new().expect("Failed to start multi-protocol services");
    eprintln!("✓ MultiProtocolGuard started successfully");
}
