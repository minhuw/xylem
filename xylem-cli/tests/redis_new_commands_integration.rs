//! Integration tests for new Redis commands
//!
//! Tests: AUTH, SELECT, HELLO, SETEX, SETRANGE, GETRANGE, CLUSTER SLOTS
//!
//! These tests use Docker Compose to manage Redis servers with various configurations:
//! - Standard Redis (port 6379)
//! - Password-protected Redis (port 6380)
//! - ACL-enabled Redis (port 6381)

use std::io::{Read, Write};
use std::net::TcpStream;
use xylem_protocols::redis::{RedisOp, RedisProtocol};
use xylem_protocols::{FixedCommandSelector, Protocol};

mod common;

/// Helper to send command and read response
fn send_command_and_read_response(
    protocol: &mut RedisProtocol,
    addr: &str,
    conn_id: usize,
    key: u64,
    value_size: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let (request, _) = protocol.generate_request(conn_id, key, value_size);

    let mut stream = TcpStream::connect(addr)?;
    stream.write_all(&request)?;

    let mut buffer = vec![0u8; 8192];
    let n = stream.read(&mut buffer)?;
    buffer.truncate(n);

    Ok(buffer)
}

#[test]
fn test_auth_simple_password() {
    let _redis =
        common::redis_auth::RedisAuthGuard::new().expect("Failed to start Redis with auth");

    println!("Testing simple password authentication...");

    // Try without AUTH - should fail
    let mut get_protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Get)));
    let mut stream = TcpStream::connect("127.0.0.1:6380").unwrap();
    let (request, _) = get_protocol.generate_request(0, 1, 64);
    stream.write_all(&request).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(
        response.contains("NOAUTH") || response.contains("authentication"),
        "Should get auth error without AUTH"
    );

    println!("✓ Verified NOAUTH error without authentication");

    // Now authenticate
    let mut auth_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Auth {
            username: None,
            password: "testpassword".to_string(),
        })));

    let mut stream = TcpStream::connect("127.0.0.1:6380").unwrap();
    let (auth_request, _) = auth_protocol.generate_request(0, 1, 64);
    stream.write_all(&auth_request).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(response.contains("+OK"), "AUTH should succeed with correct password");

    println!("✓ Simple password authentication successful");

    // Now GET should work
    let (get_request, _) = get_protocol.generate_request(0, 1, 64);
    stream.write_all(&get_request).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    // Should get a valid response (not auth error)
    assert!(
        !String::from_utf8_lossy(&buffer[..n]).contains("NOAUTH"),
        "Should be able to execute commands after AUTH"
    );

    println!("✓ Commands work after authentication");
}

#[test]
fn test_auth_acl_username_password() {
    let _redis = common::redis_acl::RedisAclGuard::new().expect("Failed to start Redis with ACL");

    println!("Testing ACL authentication (username + password)...");

    // Authenticate with username and password
    let mut auth_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Auth {
            username: Some("testuser".to_string()),
            password: "testpassword".to_string(),
        })));

    let mut stream = TcpStream::connect("127.0.0.1:6381").unwrap();
    let (auth_request, _) = auth_protocol.generate_request(0, 1, 64);
    stream.write_all(&auth_request).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(
        response.contains("+OK"),
        "ACL AUTH should succeed with correct username and password"
    );

    println!("✓ ACL authentication successful");

    // Verify we can execute commands
    let mut get_protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Get)));
    let (get_request, _) = get_protocol.generate_request(0, 1, 64);
    stream.write_all(&get_request).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    // Should get valid response
    assert!(
        !String::from_utf8_lossy(&buffer[..n]).contains("NOAUTH"),
        "Should be able to execute commands after ACL AUTH"
    );

    println!("✓ Commands work after ACL authentication");
}

#[test]
fn test_select_database() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Testing SELECT database command...");

    // SELECT database 5
    let mut select_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::SelectDb { db: 5 })));

    let response = send_command_and_read_response(&mut select_protocol, "127.0.0.1:6379", 0, 1, 64)
        .expect("SELECT failed");
    let response_str = String::from_utf8_lossy(&response);
    assert!(response_str.contains("+OK"), "SELECT should return +OK");

    println!("✓ SELECT database 5 successful");

    // Verify we're in DB 5 by setting/getting a key
    let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();

    // SELECT 5 again
    let (select_req, _) = select_protocol.generate_request(0, 1, 64);
    stream.write_all(&select_req).unwrap();
    let _ = stream.read(&mut vec![0u8; 1024]).unwrap();

    // SET key in DB 5
    let mut set_protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Set)));
    let (set_req, _) = set_protocol.generate_request(0, 999, 10);
    stream.write_all(&set_req).unwrap();
    let _ = stream.read(&mut vec![0u8; 1024]).unwrap();

    println!("✓ SET key in database 5");

    // SELECT 0 (default)
    let mut select0_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::SelectDb { db: 0 })));
    let (select0_req, _) = select0_protocol.generate_request(0, 1, 64);
    stream.write_all(&select0_req).unwrap();
    let _ = stream.read(&mut vec![0u8; 1024]).unwrap();

    // GET key:999 in DB 0 should return nil
    let mut get_protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Get)));
    let (get_req, _) = get_protocol.generate_request(0, 999, 64);
    stream.write_all(&get_req).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(response.contains("$-1"), "Key should not exist in DB 0 (it's in DB 5)");

    println!("✓ Database isolation verified");
}

#[test]
fn test_hello_resp2() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Testing HELLO command for RESP2...");

    let mut hello_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Hello { version: 2 })));

    let response = send_command_and_read_response(&mut hello_protocol, "127.0.0.1:6379", 0, 1, 64)
        .expect("HELLO failed");

    // HELLO 2 returns a map/array with server info
    // In RESP2 it returns an array
    let response_str = String::from_utf8_lossy(&response);
    assert!(
        response_str.starts_with('*') || response_str.starts_with('%'),
        "HELLO should return array or map, got: {}",
        response_str.chars().next().unwrap_or('?')
    );

    println!("✓ HELLO 2 (RESP2) successful");
}

#[test]
fn test_hello_resp3() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Testing HELLO command for RESP3...");

    let mut hello_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Hello { version: 3 })));

    let response = send_command_and_read_response(&mut hello_protocol, "127.0.0.1:6379", 0, 1, 64)
        .expect("HELLO failed");

    // HELLO 3 returns a map in RESP3 format
    let response_str = String::from_utf8_lossy(&response);
    assert!(
        response_str.starts_with('%') || response_str.starts_with('*'),
        "HELLO 3 should return map or array"
    );

    println!("✓ HELLO 3 (RESP3) successful");
}

#[test]
fn test_setex_with_expiry() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Testing SETEX command with expiry...");

    // SETEX with 1 second TTL
    let mut setex_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::SetEx { ttl_seconds: 1 })));

    let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
    let (setex_req, _) = setex_protocol.generate_request(0, 888, 20);
    stream.write_all(&setex_req).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(response.contains("+OK"), "SETEX should succeed");

    println!("✓ SETEX command executed");

    // Immediately GET - should exist
    let mut get_protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Get)));
    let (get_req, _) = get_protocol.generate_request(0, 888, 64);
    stream.write_all(&get_req).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(response.starts_with('$'), "Key should exist immediately after SETEX");

    println!("✓ Key exists immediately after SETEX");

    // Wait for expiry (keep stream alive with TTL command)
    println!("  Waiting 2 seconds for key to expire...");
    std::thread::sleep(std::time::Duration::from_secs(2));

    // GET again on same stream - should be expired
    let (get_req, _) = get_protocol.generate_request(0, 888, 64);
    stream.write_all(&get_req).expect("Failed to write GET request");

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).expect("Failed to read response");
    let response = String::from_utf8_lossy(&buffer[..n]);

    // Check if key is expired - could be $-1 (RESP2) or _ (RESP3 null)
    assert!(
        response.contains("$-1") || response.starts_with('_'),
        "Key should be expired after TTL, got: {}",
        response
    );

    println!("✓ Key expired after TTL");
}

#[test]
fn test_setrange_getrange() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Testing SETRANGE and GETRANGE commands...");

    let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();

    // First SET a base value
    let mut set_protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Set)));
    let (set_req, _) = set_protocol.generate_request(0, 777, 20);
    stream.write_all(&set_req).unwrap();
    let _ = stream.read(&mut vec![0u8; 1024]).unwrap();

    println!("✓ Base value set");

    // SETRANGE at offset 5
    let mut setrange_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::SetRange { offset: 5 })));
    let (setrange_req, _) = setrange_protocol.generate_request(0, 777, 8);
    stream.write_all(&setrange_req).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(response.starts_with(':'), "SETRANGE should return integer (string length)");

    println!("✓ SETRANGE at offset 5 successful");

    // GETRANGE to read back
    let mut getrange_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::GetRange {
            offset: 5,
            end: 12,
        })));
    let (getrange_req, _) = getrange_protocol.generate_request(0, 777, 64);
    stream.write_all(&getrange_req).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(response.starts_with('$'), "GETRANGE should return bulk string");

    println!("✓ GETRANGE retrieved modified range");
}

#[test]
fn test_cluster_slots_non_cluster() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Testing CLUSTER SLOTS on non-cluster Redis...");

    // CLUSTER SLOTS on non-cluster Redis should return error
    let mut slots_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::ClusterSlots)));

    let response = send_command_and_read_response(&mut slots_protocol, "127.0.0.1:6379", 0, 1, 64)
        .expect("CLUSTER SLOTS command failed to send");

    let response_str = String::from_utf8_lossy(&response);
    assert!(
        response_str.starts_with('-'),
        "CLUSTER SLOTS should return error on non-cluster Redis"
    );

    println!("✓ CLUSTER SLOTS correctly returns error on non-cluster instance");
}

#[test]
fn test_resp3_parsing_integration() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Testing RESP3 protocol parsing...");

    // Enable RESP3 with HELLO 3
    let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();

    let mut hello_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Hello { version: 3 })));
    let (hello_req, _) = hello_protocol.generate_request(0, 1, 64);
    stream.write_all(&hello_req).unwrap();
    let _ = stream.read(&mut vec![0u8; 8192]).unwrap();

    println!("✓ RESP3 protocol enabled");

    // Now responses should be in RESP3 format
    // Test GET with null response (RESP3 null is _\r\n)
    let mut get_protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Get)));
    let (get_req, _) = get_protocol.generate_request(0, 99999, 64);
    stream.write_all(&get_req).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();

    // Parse with protocol
    let (consumed, id) = get_protocol
        .parse_response(0, &buffer[..n])
        .expect("Should parse RESP3 response");

    assert!(consumed > 0, "Should consume bytes");
    assert!(id.is_some(), "Should return request ID");

    println!("✓ RESP3 response parsed successfully");
}
