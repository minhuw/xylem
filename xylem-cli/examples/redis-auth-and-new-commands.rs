//! Example: Redis connection setup and newer commands
//!
//! This example shows:
//! - Connection setup from config: AUTH, SELECT and HELLO, sent once per
//!   connection before the workload, never as part of the command mix
//! - SETEX (set with expiry)
//! - SETRANGE/GETRANGE (offset-based operations)
//! - CLUSTER SLOTS (topology discovery)
//! - RESP3 reply framing, including skipped push messages

use xylem_protocols::redis::command_selector::FixedCommandSelector;
use xylem_protocols::redis::{RedisOp, RedisProtocol};
use xylem_protocols::{Protocol, RedisAuthConfig, RedisConfig};

fn main() {
    println!("=== Redis Connection Setup ===\n");

    // The same fields a profile sets in [traffic_groups.protocol_config]:
    //   auth = { username = "bench", password = "..." }
    //   db = 3
    //   resp = 3
    let config = RedisConfig {
        auth: Some(RedisAuthConfig {
            username: Some("bench".to_string()),
            password: Some("example-password".to_string()),
            password_env: None,
        }),
        db: Some(3),
        resp: Some(3),
        ..Default::default()
    };
    let setup = config.connection_setup().expect("valid connection setup");
    println!("Every connection sends, in order: {}", setup.step_names().join(", "));

    // Each setup command waits for its reply before the next one goes out.
    // Setup requests are warmup and never dumped; AUTH carries the password,
    // so this example prints only its command name.
    let mut protocol = RedisProtocol::from_config(&config, None).expect("valid config");
    let replies: [&[u8]; 3] = [b"+OK\r\n", b"+OK\r\n", b"%1\r\n$5\r\nproto\r\n:3\r\n"];
    for (step, reply) in setup.step_names().into_iter().zip(replies) {
        let request = protocol.next_request(0);
        assert!(request.metadata.is_setup && !protocol.can_send(0));
        if step == "AUTH" {
            println!("   {step}: <credentials not shown>");
        } else {
            println!("   {step}: {}", String::from_utf8_lossy(&request.data).escape_debug());
        }
        protocol.parse_response(0, reply).expect("setup reply accepted");
    }
    let first = protocol.next_request(0);
    println!(
        "   First measured request: {}",
        String::from_utf8_lossy(&first.data).escape_debug()
    );

    // An error reply during setup fails the run, without the password.
    let mut rejected = RedisProtocol::from_config(&config, None).expect("valid config");
    rejected.next_request(0);
    let error = rejected
        .parse_response(0, b"-WRONGPASS invalid username-password pair\r\n")
        .unwrap_err();
    println!("   Rejected AUTH: {error}\n");

    println!("=== Commands ===\n");

    // 1. SETEX - Set with expiry (session storage)
    println!("1. SETEX (TTL=300 seconds):");
    let setex_selector = Box::new(FixedCommandSelector::new(RedisOp::SetEx { ttl_seconds: 300 }));
    let mut setex_protocol = RedisProtocol::new(setex_selector);
    let setex_request = setex_protocol.generate_request_with_key(0, 42, 20);
    println!("   Request: {}", String::from_utf8_lossy(&setex_request.data));

    // 2. SETRANGE - Offset-based write
    println!("2. SETRANGE (Offset=10):");
    let setrange_selector = Box::new(FixedCommandSelector::new(RedisOp::SetRange { offset: 10 }));
    let mut setrange_protocol = RedisProtocol::new(setrange_selector);
    let setrange_request = setrange_protocol.generate_request_with_key(0, 100, 8);
    println!("   Request: {}", String::from_utf8_lossy(&setrange_request.data));

    // 3. GETRANGE - Offset-based read
    println!("3. GETRANGE (Offset=5 to end):");
    let getrange_selector =
        Box::new(FixedCommandSelector::new(RedisOp::GetRange { offset: 5, end: -1 }));
    let mut getrange_protocol = RedisProtocol::new(getrange_selector);
    let getrange_request = getrange_protocol.generate_request_with_key(0, 200, 64);
    println!("   Request: {}", String::from_utf8_lossy(&getrange_request.data));

    // 4. CLUSTER SLOTS - Topology discovery
    println!("4. CLUSTER SLOTS:");
    let slots_selector = Box::new(FixedCommandSelector::new(RedisOp::ClusterSlots));
    let mut slots_protocol = RedisProtocol::new(slots_selector);
    let slots_request = slots_protocol.generate_request_with_key(0, 1, 64);
    println!("   Request: {}", String::from_utf8_lossy(&slots_request.data));

    println!("\n=== RESP3 Reply Framing ===\n");

    let mut resp3_protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Get)));
    let replies: [(&str, &[u8]); 7] = [
        ("Null", b"_\r\n"),
        ("Boolean", b"#t\r\n"),
        ("Double", b",3.14159\r\n"),
        ("Map", b"%1\r\n+field\r\n:1\r\n"),
        ("Set", b"~2\r\n+a\r\n+b\r\n"),
        ("Attribute + reply", b"|1\r\n+ttl\r\n:60\r\n$1\r\nv\r\n"),
        // A push message answers no request: it is skipped with the reply after it
        ("Push + reply", b">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n+OK\r\n"),
    ];
    for (name, reply) in replies {
        match resp3_protocol.parse_response(0, reply) {
            Ok((consumed, Some(_))) => println!("✓ RESP3 {name} parsed ({consumed} bytes)"),
            _ => println!("✗ Failed to parse RESP3 {name}"),
        }
    }
    println!("Push messages skipped: {}", resp3_protocol.push_messages());

    println!("\n=== All Examples Complete ===");
}
