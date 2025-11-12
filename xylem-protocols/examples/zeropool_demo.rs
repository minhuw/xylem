//! Example: Using zeropool for Protocol Request Generation
//!
//! This example demonstrates how to use zeropool for high-performance
//! buffer pooling in protocol implementations.
//!
//! Run with: cargo run --example zeropool_demo --release

use std::time::Instant;
use zeropool::BufferPool;

/// Traditional approach: allocate Vec for each request
fn generate_request_traditional(key: u64, value_size: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128 + value_size);

    // Simulate protocol encoding
    buf.extend_from_slice(b"GET ");
    buf.extend_from_slice(format!("key:{}", key).as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(&vec![b'x'; value_size]);
    buf.extend_from_slice(b"\r\n");

    buf
}

/// Modern approach: reuse buffers from pool
fn generate_request_pooled(pool: &BufferPool, key: u64, value_size: usize) -> Vec<u8> {
    let mut buf = pool.get(128 + value_size);

    // Simulate protocol encoding
    buf.extend_from_slice(b"GET ");
    buf.extend_from_slice(format!("key:{}", key).as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(&vec![b'x'; value_size]);
    buf.extend_from_slice(b"\r\n");

    buf.to_vec()
}

fn benchmark_traditional(num_requests: usize, value_size: usize) -> std::time::Duration {
    let start = Instant::now();

    for i in 0..num_requests {
        let _req = generate_request_traditional(i as u64, value_size);
        // Request would be sent here
    }

    start.elapsed()
}

fn benchmark_pooled(num_requests: usize, value_size: usize) -> std::time::Duration {
    let pool = BufferPool::new();
    let start = Instant::now();

    for i in 0..num_requests {
        let _req = generate_request_pooled(&pool, i as u64, value_size);
        // Request would be sent here
    }

    start.elapsed()
}

fn main() {
    println!("=== ZeroPool Buffer Pooling Demo ===\n");

    println!("This example shows zeropool integration.");
    println!("Note: Performance benefits require zero-copy integration.\n");

    let test_cases = vec![
        (10_000, 64, "Small requests (64 bytes)"),
        (10_000, 1024, "Medium requests (1KB)"),
        (1_000, 4096, "Large requests (4KB)"),
    ];

    for (num_requests, value_size, description) in test_cases {
        println!("{}", description);
        println!("  Requests: {}", num_requests);

        // Warm up
        let _ = benchmark_traditional(100, value_size);
        let _ = benchmark_pooled(100, value_size);

        // Benchmark traditional approach
        let trad_time = benchmark_traditional(num_requests, value_size);
        let trad_us = trad_time.as_micros() as f64;
        let trad_req_per_sec = (num_requests as f64) / trad_time.as_secs_f64();

        println!("  Traditional: {:.2}ms ({:.0} req/s)", trad_us / 1000.0, trad_req_per_sec);

        // Benchmark pooled approach
        let pool_time = benchmark_pooled(num_requests, value_size);
        let pool_us = pool_time.as_micros() as f64;
        let pool_req_per_sec = (num_requests as f64) / pool_time.as_secs_f64();

        println!("  With Pool:   {:.2}ms ({:.0} req/s)", pool_us / 1000.0, pool_req_per_sec);

        // Calculate improvement
        let speedup = trad_us / pool_us;
        let improvement = ((trad_us - pool_us) / trad_us) * 100.0;

        println!("  Speedup:     {:.2}x ({:.1}% change)\n", speedup, improvement);
    }

    println!("=== Pool Features ===\n");

    let pool = BufferPool::new();

    println!("✓ Thread-safe buffer pool");
    println!("✓ System-aware defaults (adapts to CPU count)");
    println!("✓ Thread-local caching for fast path");
    println!("✓ CPU-scaled sharding to reduce contention");
    println!("✓ Smart buffer reuse with first-fit allocation\n");

    println!("Example usage:");
    let mut buf = pool.get(1024);
    println!("  let mut buf = pool.get(1024);  // Get 1KB buffer");
    buf.extend_from_slice(b"Hello, ZeroPool!");
    println!("  buf.extend_from_slice(data);    // Use buffer");
    pool.put(buf);
    println!("  pool.put(buf);                  // Return to pool\n");

    println!("For real benefits, integrate with zero-copy:");
    println!("  1. Remove to_vec() copies from protocol builders");
    println!("  2. Return buffer directly from generate_request()");
    println!("  3. Expected: 2-10x speedup in request generation\n");
}
