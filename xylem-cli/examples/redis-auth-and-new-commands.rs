//! Example demonstrating new Redis commands
//!
//! This example shows how to use:
//! - AUTH (simple and ACL authentication)
//! - SELECT (database selection)
//! - HELLO (protocol negotiation)
//! - SETEX (set with expiry)
//! - SETRANGE/GETRANGE (offset-based operations)
//! - CLUSTER SLOTS (topology discovery)

use xylem_protocols::redis::command_selector::FixedCommandSelector;
use xylem_protocols::redis::{RedisOp, RedisProtocol};
use xylem_protocols::Protocol;

fn main() {
    println!("=== Redis New Commands Examples ===\n");

    // 1. AUTH - Simple password authentication
    println!("1. AUTH (Simple Password):");
    let auth_selector = Box::new(FixedCommandSelector::new(RedisOp::Auth {
        username: None,
        password: "mySecretPassword".to_string(),
    }));
    let mut auth_protocol = RedisProtocol::new(auth_selector);
    let (auth_request, _) = auth_protocol.generate_request(0, 1, 64);
    println!("   Request: {}", String::from_utf8_lossy(&auth_request));

    // 2. AUTH - ACL with username (Redis 6.0+)
    println!("2. AUTH (ACL - Redis 6.0+):");
    let acl_selector = Box::new(FixedCommandSelector::new(RedisOp::Auth {
        username: Some("admin".to_string()),
        password: "adminPassword".to_string(),
    }));
    let mut acl_protocol = RedisProtocol::new(acl_selector);
    let (acl_request, _) = acl_protocol.generate_request(0, 1, 64);
    println!("   Request: {}", String::from_utf8_lossy(&acl_request));

    // 3. SELECT - Database selection
    println!("3. SELECT (Database 5):");
    let select_selector = Box::new(FixedCommandSelector::new(RedisOp::SelectDb { db: 5 }));
    let mut select_protocol = RedisProtocol::new(select_selector);
    let (select_request, _) = select_protocol.generate_request(0, 1, 64);
    println!("   Request: {}", String::from_utf8_lossy(&select_request));

    // 4. HELLO - RESP2 protocol negotiation
    println!("4. HELLO (RESP2):");
    let hello2_selector = Box::new(FixedCommandSelector::new(RedisOp::Hello { version: 2 }));
    let mut hello2_protocol = RedisProtocol::new(hello2_selector);
    let (hello2_request, _) = hello2_protocol.generate_request(0, 1, 64);
    println!("   Request: {}", String::from_utf8_lossy(&hello2_request));

    // 5. HELLO - RESP3 protocol negotiation
    println!("5. HELLO (RESP3):");
    let hello3_selector = Box::new(FixedCommandSelector::new(RedisOp::Hello { version: 3 }));
    let mut hello3_protocol = RedisProtocol::new(hello3_selector);
    let (hello3_request, _) = hello3_protocol.generate_request(0, 1, 64);
    println!("   Request: {}", String::from_utf8_lossy(&hello3_request));

    // 6. SETEX - Set with expiry (session storage)
    println!("6. SETEX (TTL=300 seconds):");
    let setex_selector = Box::new(FixedCommandSelector::new(RedisOp::SetEx { ttl_seconds: 300 }));
    let mut setex_protocol = RedisProtocol::new(setex_selector);
    let (setex_request, _) = setex_protocol.generate_request(0, 42, 20);
    println!("   Request: {}", String::from_utf8_lossy(&setex_request));

    // 7. SETRANGE - Offset-based write
    println!("7. SETRANGE (Offset=10):");
    let setrange_selector = Box::new(FixedCommandSelector::new(RedisOp::SetRange { offset: 10 }));
    let mut setrange_protocol = RedisProtocol::new(setrange_selector);
    let (setrange_request, _) = setrange_protocol.generate_request(0, 100, 8);
    println!("   Request: {}", String::from_utf8_lossy(&setrange_request));

    // 8. GETRANGE - Offset-based read
    println!("8. GETRANGE (Offset=5 to end):");
    let getrange_selector =
        Box::new(FixedCommandSelector::new(RedisOp::GetRange { offset: 5, end: -1 }));
    let mut getrange_protocol = RedisProtocol::new(getrange_selector);
    let (getrange_request, _) = getrange_protocol.generate_request(0, 200, 64);
    println!("   Request: {}", String::from_utf8_lossy(&getrange_request));

    // 9. CLUSTER SLOTS - Topology discovery
    println!("9. CLUSTER SLOTS:");
    let slots_selector = Box::new(FixedCommandSelector::new(RedisOp::ClusterSlots));
    let mut slots_protocol = RedisProtocol::new(slots_selector);
    let (slots_request, _) = slots_protocol.generate_request(0, 1, 64);
    println!("   Request: {}", String::from_utf8_lossy(&slots_request));

    println!("\n=== RESP3 Response Parsing ===\n");

    // Demonstrate RESP3 response parsing
    let resp3_selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
    let mut resp3_protocol = RedisProtocol::new(resp3_selector);

    // RESP3 Null
    let null_response = b"_\r\n";
    match resp3_protocol.parse_response(0, null_response) {
        Ok((consumed, Some(_))) => println!("✓ RESP3 Null parsed ({} bytes)", consumed),
        _ => println!("✗ Failed to parse RESP3 Null"),
    }

    // RESP3 Boolean
    let bool_response = b"#t\r\n";
    match resp3_protocol.parse_response(0, bool_response) {
        Ok((consumed, Some(_))) => println!("✓ RESP3 Boolean parsed ({} bytes)", consumed),
        _ => println!("✗ Failed to parse RESP3 Boolean"),
    }

    // RESP3 Double
    let double_response = b",3.14159\r\n";
    match resp3_protocol.parse_response(0, double_response) {
        Ok((consumed, Some(_))) => println!("✓ RESP3 Double parsed ({} bytes)", consumed),
        _ => println!("✗ Failed to parse RESP3 Double"),
    }

    // RESP3 Map
    let map_response = b"%2\r\n";
    match resp3_protocol.parse_response(0, map_response) {
        Ok((consumed, Some(_))) => println!("✓ RESP3 Map parsed ({} bytes)", consumed),
        _ => println!("✗ Failed to parse RESP3 Map"),
    }

    // RESP3 Set
    let set_response = b"~3\r\n";
    match resp3_protocol.parse_response(0, set_response) {
        Ok((consumed, Some(_))) => println!("✓ RESP3 Set parsed ({} bytes)", consumed),
        _ => println!("✗ Failed to parse RESP3 Set"),
    }

    println!("\n=== All Examples Complete ===");
}
