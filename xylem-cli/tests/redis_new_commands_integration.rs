//! Integration tests for Redis connection setup and newer commands
//!
//! Tests: connection setup from the profile (auth, db, resp), SETEX,
//! SETRANGE, GETRANGE, CLUSTER SLOTS
//!
//! These tests use Docker Compose to manage Redis servers with various configurations:
//! - Standard Redis (port 6379)
//! - Password-protected Redis (port 6380)
//! - ACL-enabled Redis (port 6381)

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use xylem_protocols::redis::{RedisOp, RedisProtocol};
use xylem_protocols::{CommandTemplate, FixedCommandSelector};

mod common;

/// Helper to send command and read response
fn send_command_and_read_response(
    protocol: &mut RedisProtocol,
    addr: &str,
    conn_id: usize,
    key: u64,
    value_size: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let request = protocol.generate_request_with_key(conn_id, key, value_size);

    let mut stream = TcpStream::connect(addr)?;
    stream.write_all(&request.data)?;

    let mut buffer = vec![0u8; 8192];
    let n = stream.read(&mut buffer)?;
    buffer.truncate(n);

    Ok(buffer)
}

/// Result of one run of the xylem CLI
struct CliRun {
    success: bool,
    /// stdout and stderr together
    output: String,
    /// The JSON results, when the run succeeded
    results: Option<serde_json::Value>,
}

impl CliRun {
    /// Completed measurement requests
    fn completed(&self) -> u64 {
        let results = self.results.as_ref().expect("successful run has results");
        results["global"]["requests"]["completed"].as_u64().unwrap()
    }
}

/// Run the xylem CLI for one second against `port` with a GET/SET mix on
/// two threads of two pipelined connections each. `protocol_config` is
/// TOML for `[traffic_groups.protocol_config]`; `envs` are set for the run.
fn run_xylem(port: u16, protocol_config: &str, envs: &[(&str, &str)]) -> CliRun {
    run_xylem_group(port, "", protocol_config, envs)
}

/// [`run_xylem`] with `group`, TOML keys of the traffic group itself
fn run_xylem_group(port: u16, group: &str, protocol_config: &str, envs: &[(&str, &str)]) -> CliRun {
    let dir = tempfile::tempdir().unwrap();
    let results_path = dir.path().join("results.json");
    let profile = format!(
        r#"
[experiment]
name = "redis-setup"
duration = "1s"
seed = 1

[[traffic_groups]]
name = "setup"
protocol = "redis"
target = "127.0.0.1:{port}"
threads = [0, 1]
connections_per_thread = 2
max_pending_per_connection = 4
{group}

[traffic_groups.protocol_config]
{protocol_config}

[traffic_groups.protocol_config.operations]
strategy = "weighted"
commands = [{{ name = "get", weight = 0.5 }}, {{ name = "set", weight = 0.5 }}]

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "{}"
"#,
        results_path.display()
    );
    let profile_path = dir.path().join("profile.toml");
    std::fs::write(&profile_path, profile).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile_path)
        .envs(envs.iter().copied())
        .output()
        .expect("run xylem");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let results = output
        .status
        .success()
        .then(|| serde_json::from_str(&std::fs::read_to_string(&results_path).unwrap()).unwrap());
    CliRun {
        success: output.status.success(),
        output: text,
        results,
    }
}

/// The keys config of the CLI runs: `prefix:0` .. `prefix:99`
fn keys_config(prefix: &str) -> String {
    format!(
        "[traffic_groups.protocol_config.keys]\nstrategy = \"random\"\nmax = 100\nvalue_size = 16\nprefix = \"{prefix}\"\n"
    )
}

/// Number of the keys `prefix:0` .. `prefix:99` that exist in database `db`
fn existing_keys(redis_cli_args: &[&str], db: u32, prefix: &str) -> u64 {
    let mut args: Vec<String> = redis_cli_args.iter().map(|arg| arg.to_string()).collect();
    args.extend(["--no-auth-warning".into(), "-n".into(), db.to_string(), "EXISTS".into()]);
    args.extend((0..100).map(|key| format!("{prefix}{key}")));
    let output = Command::new("redis-cli").args(&args).output().expect("run redis-cli");
    String::from_utf8_lossy(&output.stdout).trim().parse().expect("EXISTS count")
}

#[test]
fn test_auth_simple_password() {
    let redis = common::redis_auth::RedisAuthGuard::new().expect("Failed to start Redis with auth");
    let password = redis.get_password();
    let keys = keys_config("auth-test:");

    // Without auth, every request gets a NOAUTH error reply: by default the
    // run counts them, none in the latency, and with on_error = "abort" the
    // first one fails the run
    let run = run_xylem(redis.get_port(), &keys, &[]);
    assert!(run.success, "{}", run.output);
    let global = &run.results.as_ref().unwrap()["global"];
    let requests = &global["requests"];
    assert!(requests["errors"].as_u64().unwrap() > 0, "{requests}");
    assert_eq!(requests["errors"], requests["completed"], "{requests}");
    assert_eq!(requests["errors_by_kind"]["NOAUTH"], requests["errors"], "{requests}");
    assert_eq!(global["latency"]["sample_count"], 0);
    assert!(run.output.contains("NOAUTH"), "{}", run.output);
    let run = run_xylem_group(redis.get_port(), r#"on_error = "abort""#, &keys, &[]);
    assert!(!run.success, "{}", run.output);
    assert!(run.output.contains("NOAUTH"), "{}", run.output);

    // A wrong password fails setup, naming the step but not the password
    let config = format!("auth = {{ password = \"wrong-{password}-x\" }}\n{keys}");
    let run = run_xylem(redis.get_port(), &config, &[]);
    assert!(!run.success, "{}", run.output);
    assert!(
        run.output.contains("Redis connection setup failed: AUTH on connection"),
        "{}",
        run.output
    );
    assert!(run.output.contains("WRONGPASS"), "{}", run.output);
    assert!(!run.output.contains(password), "{}", run.output);

    // The right password authenticates every connection before the workload
    let config = format!("auth = {{ password = \"{password}\" }}\n{keys}");
    let run = run_xylem(redis.get_port(), &config, &[]);
    assert!(run.success, "{}", run.output);
    assert!(run.completed() > 0);
    assert!(!run.output.contains(password), "{}", run.output);
    println!("✓ requirepass authentication: {} requests", run.completed());
}

#[test]
fn test_auth_acl_username_password() {
    let redis = common::redis_acl::RedisAclGuard::new().expect("Failed to start Redis with ACL");
    let (username, password) = (redis.get_username(), redis.get_password());
    let keys = keys_config("acl-test:");

    // The password comes from the environment
    let config = format!(
        "auth = {{ username = \"{username}\", password_env = \"XYLEM_TEST_REDIS_PASSWORD\" }}\n{keys}"
    );
    let run = run_xylem(redis.get_port(), &config, &[("XYLEM_TEST_REDIS_PASSWORD", password)]);
    assert!(run.success, "{}", run.output);
    assert!(run.completed() > 0);
    assert!(!run.output.contains(password), "{}", run.output);

    // An unset variable fails validation, naming the variable
    let run = run_xylem(redis.get_port(), &config, &[]);
    assert!(!run.success);
    assert!(
        run.output.contains("environment variable XYLEM_TEST_REDIS_PASSWORD is not set"),
        "{}",
        run.output
    );

    // An unknown user fails setup
    let config = format!("auth = {{ username = \"nobody\", password = \"{password}\" }}\n{keys}");
    let run = run_xylem(redis.get_port(), &config, &[]);
    assert!(!run.success);
    assert!(run.output.contains("AUTH on connection"), "{}", run.output);
    assert!(run.output.contains("WRONGPASS"), "{}", run.output);
    assert!(!run.output.contains(password), "{}", run.output);
    println!("✓ ACL authentication with password_env");
}

#[test]
fn test_select_database() {
    let redis = common::redis_auth::RedisAuthGuard::new().expect("Failed to start Redis with auth");
    let password = redis.get_password();
    let cli: &[&str] = &["-p", "6380", "-a", password];
    let prefix = "select-test:";

    // AUTH then SELECT: the workload's SETs land in database 5 only
    let config = format!("auth = {{ password = \"{password}\" }}\ndb = 5\n{}", keys_config(prefix));
    let run = run_xylem(redis.get_port(), &config, &[]);
    assert!(run.success, "{}", run.output);
    assert!(existing_keys(cli, 5, prefix) > 0, "SETs land in database 5");
    assert_eq!(existing_keys(cli, 0, prefix), 0, "database 0 is untouched");

    // An out-of-range database fails setup at SELECT
    let config =
        format!("auth = {{ password = \"{password}\" }}\ndb = 99\n{}", keys_config(prefix));
    let run = run_xylem(redis.get_port(), &config, &[]);
    assert!(!run.success);
    assert!(run.output.contains("SELECT on connection"), "{}", run.output);
    assert!(run.output.contains("DB index is out of range"), "{}", run.output);
    println!("✓ SELECT from the profile");
}

#[test]
fn test_hello_resp2() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");
    // resp = 2 is the default protocol and sends no HELLO
    let run = run_xylem(6379, &format!("resp = 2\n{}", keys_config("resp2-test:")), &[]);
    assert!(run.success, "{}", run.output);
    assert!(run.completed() > 0);
    println!("✓ RESP2 run: {} requests", run.completed());
}

#[test]
fn test_hello_resp3() {
    let redis = common::redis_acl::RedisAclGuard::new().expect("Failed to start Redis with ACL");
    let (username, password) = (redis.get_username(), redis.get_password());
    // AUTH, SELECT, HELLO 3, then a pipelined GET/SET mix after an insert
    // phase on four connections over two threads, all with RESP3 replies
    // (GET misses reply with the RESP3 null `_`)
    let config = format!(
        "auth = {{ username = \"{username}\", password = \"{password}\" }}\ndb = 2\nresp = 3\n\
         insert_phase = {{ key_count = 50 }}\n{}",
        keys_config("resp3-test:")
    );
    let run = run_xylem(redis.get_port(), &config, &[]);
    assert!(run.success, "{}", run.output);
    assert!(run.completed() > 0);
    let cli: &[&str] = &["-p", "6381", "--user", username, "-a", password];
    assert!(existing_keys(cli, 2, "resp3-test:") > 0, "keys land in database 2");
    println!("✓ RESP3 run: {} requests", run.completed());
}

#[test]
fn test_setex_with_expiry() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Testing SETEX command with expiry...");

    // SETEX with 1 second TTL
    let mut setex_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::SetEx { ttl_seconds: 1 })));

    let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
    let setex_req = setex_protocol.generate_request_with_key(0, 888, 20);
    stream.write_all(&setex_req.data).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(response.contains("+OK"), "SETEX should succeed");

    println!("✓ SETEX command executed");

    // Immediately GET - should exist
    let mut get_protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Get)));
    let get_req = get_protocol.generate_request_with_key(0, 888, 64);
    stream.write_all(&get_req.data).unwrap();

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(response.starts_with('$'), "Key should exist immediately after SETEX");

    println!("✓ Key exists immediately after SETEX");

    // Wait for expiry (keep stream alive with TTL command)
    println!("  Waiting 2 seconds for key to expire...");
    std::thread::sleep(std::time::Duration::from_secs(2));

    // GET again on same stream - should be expired
    let get_req = get_protocol.generate_request_with_key(0, 888, 64);
    stream.write_all(&get_req.data).expect("Failed to write GET request");

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
    let set_req = set_protocol.generate_request_with_key(0, 777, 20);
    stream.write_all(&set_req.data).unwrap();
    let _ = stream.read(&mut vec![0u8; 1024]).unwrap();

    println!("✓ Base value set");

    // SETRANGE at offset 5
    let mut setrange_protocol =
        RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::SetRange { offset: 5 })));
    let setrange_req = setrange_protocol.generate_request_with_key(0, 777, 8);
    stream.write_all(&setrange_req.data).unwrap();

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
    let getrange_req = getrange_protocol.generate_request_with_key(0, 777, 64);
    stream.write_all(&getrange_req.data).unwrap();

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

    // CLUSTER SLOTS, sent through a custom command template, should return an
    // error on non-cluster Redis
    let slots = RedisOp::Custom(CommandTemplate::parse("CLUSTER SLOTS").unwrap());
    let mut slots_protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(slots)));

    let response = send_command_and_read_response(&mut slots_protocol, "127.0.0.1:6379", 0, 1, 64)
        .expect("CLUSTER SLOTS command failed to send");

    let response_str = String::from_utf8_lossy(&response);
    assert!(
        response_str.starts_with('-'),
        "CLUSTER SLOTS should return error on non-cluster Redis"
    );

    println!("✓ CLUSTER SLOTS correctly returns error on non-cluster instance");
}
