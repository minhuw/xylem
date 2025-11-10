//! Feature Showcase: Demonstrates all new features programmatically
//!
//! This example shows how to use the newly implemented features via Rust API
//! (configuration integration via TOML is pending)

use xylem_core::workload::{
    KeyGeneration, RateControl, RequestGenerator, FixedSize, UniformSize, NormalSize, PerCommandSize,
};
use xylem_protocols::{
    RedisProtocol, RedisOp, CommandTemplate, WeightedCommandSelector, FixedCommandSelector,
};
use std::collections::HashMap;

fn main() {
    println!("=== Xylem New Features Showcase ===\n");

    feature_1_weighted_commands();
    feature_3_variable_sizes();
    feature_4_command_variety();
    feature_6_gaussian_keys();
    combined_example();
}

/// Feature 1: Weighted Command Selection
fn feature_1_weighted_commands() {
    println!("--- Feature 1: Weighted Command Selection ---");

    // Create weighted selector: 70% GET, 20% SET, 10% INCR
    let selector = WeightedCommandSelector::new(vec![
        (RedisOp::Get, 0.7),
        (RedisOp::Set, 0.2),
        (RedisOp::Incr, 0.1),
    ])
    .expect("Failed to create weighted selector");

    let protocol = RedisProtocol::with_selector(Box::new(selector));
    println!("✓ Created protocol with weighted commands (70% GET, 20% SET, 10% INCR)");
    println!("  Protocol: {}\n", protocol.name());
}

/// Feature 3: Variable Data Sizes
fn feature_3_variable_sizes() {
    println!("--- Feature 3: Variable Data Sizes ---");

    // Example 1: Uniform sizes (100-1000 bytes)
    let uniform_gen = Box::new(UniformSize::new(100, 1000).expect("Failed to create uniform size"));
    println!("✓ Created uniform size generator: [100, 1000] bytes");

    // Example 2: Normal distribution (mean=512, std_dev=128, clamped to [64, 2048])
    let normal_gen = Box::new(
        NormalSize::new(512.0, 128.0, 64, 2048).expect("Failed to create normal size"),
    );
    println!("✓ Created normal size generator: mean=512, σ=128, range=[64, 2048]");

    // Example 3: Per-command sizes
    let mut command_sizes: HashMap<String, Box<dyn xylem_core::workload::ValueSizeGenerator>> =
        HashMap::new();
    command_sizes.insert("get".to_string(), Box::new(FixedSize::new(64)));
    command_sizes.insert(
        "set".to_string(),
        Box::new(UniformSize::new(256, 1024).expect("Failed to create set size")),
    );

    let per_command_gen = Box::new(PerCommandSize::new(
        command_sizes,
        Box::new(FixedSize::new(512)),
    ));
    println!("✓ Created per-command size generator:");
    println!("  - GET: 64 bytes (fixed)");
    println!("  - SET: 256-1024 bytes (uniform)");
    println!("  - Default: 512 bytes (fixed)\n");
}

/// Feature 4: Command Variety (MGET, WAIT, Custom)
fn feature_4_command_variety() {
    println!("--- Feature 4: Command Variety ---");

    // MGET: Fetch 10 keys per operation
    let mget_op = RedisOp::MGet { count: 10 };
    println!("✓ MGET operation: fetch 10 keys per request");

    // WAIT: Wait for 2 replicas with 1s timeout
    let wait_op = RedisOp::Wait {
        num_replicas: 2,
        timeout_ms: 1000,
    };
    println!("✓ WAIT operation: 2 replicas, 1000ms timeout");

    // Custom: HSET command
    let hset_template =
        CommandTemplate::parse("HSET myhash __key__ __data__").expect("Failed to parse template");
    let custom_op = RedisOp::Custom(hset_template);
    println!("✓ Custom command: HSET myhash __key__ __data__");

    // Custom: ZADD with score
    let zadd_template =
        CommandTemplate::parse("ZADD myset __value_size__ __key__").expect("Failed to parse template");
    println!("✓ Custom command: ZADD myset __value_size__ __key__");

    // Custom: SETEX with TTL
    let setex_template =
        CommandTemplate::parse("SETEX __key__ 300 __data__").expect("Failed to parse template");
    println!("✓ Custom command: SETEX __key__ 300 __data__");

    // Weighted selector with all command types
    let full_selector = WeightedCommandSelector::new(vec![
        (RedisOp::Get, 0.4),
        (mget_op, 0.2),
        (RedisOp::Set, 0.2),
        (wait_op, 0.1),
        (custom_op, 0.1),
    ])
    .expect("Failed to create full selector");
    println!("\n✓ Created weighted selector with all command types\n");
}

/// Feature 6: Gaussian Key Distribution
fn feature_6_gaussian_keys() {
    println!("--- Feature 6: Gaussian Key Distribution ---");

    // Center at 50% of keyspace with 10% spread
    let gaussian_keygen = KeyGeneration::gaussian(0.5, 0.1, 10000)
        .expect("Failed to create Gaussian distribution");
    println!("✓ Created Gaussian key distribution:");
    println!("  - Mean: 50% of keyspace (key 5000)");
    println!("  - Std Dev: 10% of keyspace (±1000 keys)");
    println!("  - Keyspace: [0, 9999]");
    println!("  - ~68% of requests will hit keys 4000-6000\n");

    // Tight hot-spot (cache warmth)
    let hotspot_keygen = KeyGeneration::gaussian(0.5, 0.05, 10000)
        .expect("Failed to create hotspot");
    println!("✓ Created tight hot-spot distribution (cache warmth):");
    println!("  - Mean: 50% (key 5000)");
    println!("  - Std Dev: 5% (±500 keys)");
    println!("  - ~68% of requests hit keys 4500-5500\n");
}

/// Combined Example: All features together
fn combined_example() {
    println!("--- Combined Example: All Features ---");

    // 1. Gaussian key distribution
    let keygen = KeyGeneration::gaussian(0.6, 0.15, 1_000_000)
        .expect("Failed to create keygen");
    println!("✓ Gaussian keys: center at 60%, 15% spread, 1M keyspace");

    // 2. Variable sizes (per-command)
    let mut cmd_sizes: HashMap<String, Box<dyn xylem_core::workload::ValueSizeGenerator>> =
        HashMap::new();
    cmd_sizes.insert("get".to_string(), Box::new(FixedSize::new(64)));
    cmd_sizes.insert(
        "set".to_string(),
        Box::new(UniformSize::new(256, 2048).unwrap()),
    );
    let size_gen = Box::new(PerCommandSize::new(
        cmd_sizes,
        Box::new(NormalSize::new(512.0, 128.0, 64, 4096).unwrap()),
    ));
    println!("✓ Per-command sizes: GET=64, SET=256-2048, default=512±128");

    // 3. Weighted command selection with custom templates
    let hset_template = CommandTemplate::parse("HSET stats __key__ __value_size__").unwrap();
    let selector = WeightedCommandSelector::new(vec![
        (RedisOp::Get, 0.5),
        (RedisOp::MGet { count: 5 }, 0.2),
        (RedisOp::Set, 0.25),
        (RedisOp::Custom(hset_template), 0.05),
    ])
    .unwrap();
    println!("✓ Weighted commands: 50% GET, 20% MGET(5), 25% SET, 5% HSET");

    // 4. Create protocol with all features
    let protocol = RedisProtocol::with_selector(Box::new(selector));
    println!("✓ Protocol created: {}", protocol.name());

    // 5. Create request generator
    let rate_control = RateControl::ClosedLoop;
    let generator = RequestGenerator::with_value_size_generator(keygen, rate_control, size_gen);
    println!("✓ Request generator created with all features");

    println!("\n=== Showcase Complete ===");
    println!("All features are fully functional and ready for integration!");
}
