//! Compatibility adapters must preserve the same bytes as shared constructors.
use xylem_cli::multi_protocol::*;
use xylem_protocols::configs::*;
use xylem_protocols::factories::{MemcachedAsciiFactory, MemcachedBinaryFactory, RedisFactory};
use xylem_protocols::factory::ProtocolFactory;
use xylem_protocols::{Protocol, RedisClusterProtocol};

#[test]
fn redis_cli_adapters_preserve_seeded_sizes_commands_prefix_and_insert_defaults() {
    for per_command_keys in [false, true] {
        let keys = KeysConfig::RoundRobin {
            max: 5,
            value_size: 13,
            prefix: "custom:".into(),
        };
        let config = RedisConfig {
            keys: keys.clone(),
            operations: Some(RedisOperationsConfig::Weighted {
                commands: vec![RedisCommandWeight {
                    name: "SET".into(),
                    weight: 1.0,
                    params: None,
                    keys: per_command_keys.then_some(keys),
                }],
            }),
            value_size: Some(ValueSizeConfig::Uniform { min: 3, max: 7 }),
            insert_phase: Some(InsertPhaseConfig { key_count: Some(1), value_size: None }),
            random_data: true,
            ..Default::default()
        };
        let seed = Some(42);
        let mut factory = RedisFactory.create(config.clone(), seed).unwrap();
        let mut cli = create_redis_protocol_from_config(
            &config,
            config.command_selector(seed).unwrap(),
            config.keys.to_key_gen(seed).unwrap(),
            seed,
        )
        .unwrap();
        let cluster_config = xylem_cli::multi_protocol::RedisClusterConfig {
            nodes: vec![RedisClusterNode {
                address: "127.0.0.1:7000".into(),
                slot_start: 0,
                slot_end: 16383,
            }],
        };
        let mut cluster_cli = create_redis_cluster_protocol_from_config(
            &config,
            config.command_selector(seed).unwrap(),
            cluster_config,
            config.keys.to_key_gen(seed).unwrap(),
            seed,
        )
        .unwrap();
        let mut cluster_shared = RedisClusterProtocol::from_config(&config, seed).unwrap();
        cluster_shared.register_connection("127.0.0.1:7000".parse().unwrap(), 0);
        let mut lengths = std::collections::HashSet::new();
        for i in 0..40 {
            let expected = factory.next_request(0);
            assert_eq!(cli.next_request(0), expected);
            let cluster_request = cluster_cli.next_request(0);
            assert_eq!(cluster_request, expected);
            assert_eq!(cluster_shared.next_request(0).data, expected.data);
            assert_eq!(expected.metadata.is_warmup, i == 0);
            if i > 0 {
                lengths.insert(expected.data.len());
            }
        }
        assert!(lengths.len() > 1, "uniform value sizes must not collapse to a fixed size");
    }
}

#[test]
fn memcached_cli_adapters_match_factories_in_both_formats() {
    let config = MemcachedConfig {
        keys: KeysConfig::RoundRobin {
            max: 3,
            value_size: 7,
            prefix: "custom:".into(),
        },
        operation: "SET".into(),
        insert_phase: Some(InsertPhaseConfig { key_count: Some(1), value_size: Some(11) }),
    };
    let seed = Some(42);
    let mut ascii = MemcachedAsciiFactory.create(config.clone(), seed).unwrap();
    let mut ascii_cli = create_memcached_ascii_protocol_from_config(
        &config,
        xylem_protocols::memcached::ascii::MemcachedOp::Set,
        config.keys.to_key_gen(seed).unwrap(),
    )
    .unwrap();
    let mut binary = MemcachedBinaryFactory.create(config.clone(), seed).unwrap();
    let mut binary_cli = create_memcached_binary_protocol_from_config(
        &config,
        xylem_protocols::memcached::MemcachedOp::Set,
        config.keys.to_key_gen(seed).unwrap(),
    )
    .unwrap();
    for _ in 0..10 {
        assert_eq!(ascii_cli.next_request(7), ascii.next_request(7));
        assert_eq!(binary_cli.next_request(7), binary.next_request(7));
    }
}

/// Capture complete requests from the real executable and reply without an
/// external service. Both protocols are ordered, so one response releases the
/// next request in the closed-loop profile.
fn cli_requests(protocol: &str, config: serde_json::Value) -> (String, Vec<Vec<u8>>) {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::process::Command;
    use std::time::Duration;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap().to_string();
    let is_redis = protocol == "redis";
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline, "CLI did not connect");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept: {error}"),
            }
        };
        socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut parser =
            xylem_protocols::RedisProtocol::from_config(&RedisConfig::default(), None).unwrap();
        let mut pending = Vec::new();
        let mut requests = Vec::new();
        let mut buf = [0; 4096];
        loop {
            let n = match socket.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
                Err(e) => panic!("read request: {e}"),
            };
            pending.extend_from_slice(&buf[..n]);
            let len = if is_redis {
                parser.parse_response(0, &pending).unwrap().0
            } else if let Some(end) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&pending[..end]).unwrap();
                let body_size = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("Content-Length: ")
                            .map(|size| size.parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                let total = end + 4 + body_size;
                if pending.len() >= total {
                    total
                } else {
                    0
                }
            } else {
                0
            };
            if len == 0 {
                continue;
            }
            requests.push(pending.drain(..len).collect());
            let reply: &[u8] = if is_redis {
                b"+OK\r\n"
            } else {
                b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
            };
            if socket.write_all(reply).is_err() {
                break;
            }
        }
        requests
    });
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profile.toml");
    let mut profile_value = serde_json::json!({
        "experiment": { "name": "wire-parity", "duration": "1s", "seed": 42 },
        "traffic_groups": [{
            "name": "test", "threads": [0], "connections_per_thread": 1,
            "max_pending_per_connection": 1, "target": target, "protocol": protocol,
            "traffic_policy": { "type": "closed-loop" }, "protocol_config": config
        }],
        "output": { "format": "json", "file": dir.path().join("result.json") }
    });
    fn omit_nulls(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                map.retain(|_, value| !value.is_null());
                for value in map.values_mut() {
                    omit_nulls(value);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    omit_nulls(value);
                }
            }
            _ => {}
        }
    }
    omit_nulls(&mut profile_value);
    std::fs::write(&profile, toml::to_string(&profile_value).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.join().unwrap();
    assert!(requests.len() > 2, "expected multiple completed CLI requests");
    (target, requests)
}

#[test]
fn real_cli_redis_requests_match_factory_variable_sizes() {
    let config: RedisConfig = serde_json::from_value(serde_json::json!({
        "keys": { "strategy": "round-robin", "max": 5, "value_size": 13, "prefix": "custom:" },
        "operations": { "strategy": "fixed", "operation": "SET" },
        "value_size": { "strategy": "uniform", "min": 3, "max": 7 },
        "random_data": true,
        "insert_phase": { "key_count": 1 }
    }))
    .unwrap();
    let (_, requests) = cli_requests("redis", serde_json::to_value(&config).unwrap());
    let mut factory = RedisFactory.create(config, Some(42)).unwrap();
    let mut lengths = std::collections::HashSet::new();
    for request in requests {
        assert_eq!(request, factory.next_request(0).data);
        lengths.insert(request.len());
    }
    assert!(lengths.len() > 2);
}

#[test]
fn real_cli_http_honors_body_size_and_host_fallback() {
    use xylem_protocols::factories::HttpFactory;
    for host in [None, Some("example.test".to_string())] {
        let mut config = HttpConfig {
            method: "POST".into(),
            path: "/payload".into(),
            host,
            body_size: 17,
        };
        let (target, requests) = cli_requests("http", serde_json::to_value(&config).unwrap());
        config.host.get_or_insert(target);
        let mut factory = HttpFactory.create(config, Some(42)).unwrap();
        for request in requests {
            assert_eq!(request, factory.next_request(0).data);
        }
    }
}

#[test]
fn cluster_cli_adapter_rejects_overlapping_and_invalid_ranges() {
    for ranges in [[(0, 100), (100, 200)], [(100, 99), (101, 200)], [(0, 100), (101, 16384)]] {
        let config = xylem_cli::multi_protocol::RedisClusterConfig {
            nodes: ranges
                .into_iter()
                .enumerate()
                .map(|(index, (slot_start, slot_end))| RedisClusterNode {
                    address: format!("127.0.0.1:{}", 7000 + index),
                    slot_start,
                    slot_end,
                })
                .collect(),
        };
        assert!(create_redis_cluster_protocol(
            Box::new(xylem_protocols::FixedCommandSelector::new(xylem_protocols::RedisOp::Get)),
            config,
        )
        .is_err());
    }
}

fn serve_http_requests(listener: std::net::TcpListener) -> std::thread::JoinHandle<usize> {
    use std::io::{Read, Write};
    use std::time::{Duration, Instant};
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "CLI did not connect");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept: {error}"),
            }
        };
        socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut pending = Vec::new();
        let mut buf = [0; 4096];
        let mut replies = 0;
        loop {
            match socket.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => pending.extend_from_slice(&buf[..n]),
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => break,
                Err(error) => panic!("read: {error}"),
            }
            while let Some(end) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                pending.drain(..end + 4);
                if socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").is_err() {
                    return replies;
                }
                replies += 1;
            }
        }
        replies
    })
}

#[test]
fn real_cli_heterogeneous_worker_sampling_uses_experiment_wide_fallback() {
    use std::net::TcpListener;
    use std::process::Command;

    // Swapping the policies checks both disabled and enabled global fallback,
    // while each group must retain its own policy on its separate worker.
    for policies in [["none", "unlimited"], ["unlimited", "none"]] {
        let mut servers = Vec::new();
        let mut groups = Vec::new();
        for (index, policy) in policies.iter().enumerate() {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let target = listener.local_addr().unwrap().to_string();
            listener.set_nonblocking(true).unwrap();
            servers.push(serve_http_requests(listener));
            groups.push(serde_json::json!({
                "name": format!("group-{index}"), "threads": [index],
                "connections_per_thread": 1, "max_pending_per_connection": 1,
                "target": target, "protocol": "http",
                "traffic_policy": { "type": "closed-loop" },
                "sampling_policy": { "type": policy }
            }));
        }
        let dir = tempfile::tempdir().unwrap();
        let profile = dir.path().join("profile.toml");
        let results = dir.path().join("results.json");
        let config = serde_json::json!({
            "experiment": { "name": "heterogeneous-sampling", "duration": "1s" },
            "traffic_groups": groups,
            "output": { "format": "json", "file": results }
        });
        std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
            .arg("-P")
            .arg(profile)
            .output()
            .unwrap();
        for server in servers {
            assert!(server.join().unwrap() > 0, "each worker must complete requests");
        }
        assert!(
            output.status.success(),
            "CLI failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: serde_json::Value =
            serde_json::from_slice(&std::fs::read(results).unwrap()).unwrap();
        let groups = result["traffic_groups"].as_array().unwrap();
        assert_eq!(groups.len(), 2);
        let mut total = 0;
        for (index, policy) in policies.iter().enumerate() {
            let group = groups.iter().find(|group| group["id"] == index).unwrap();
            assert_eq!(group["threads"], serde_json::json!([index]));
            let stats = &group["stats"];
            let requests = stats["total_requests"].as_u64().unwrap();
            assert!(requests > 0);
            total += requests;
            assert_eq!(stats["latency"]["mean_us"].as_f64().unwrap() > 0.0, *policy != "none");
        }
        assert_eq!(result["global"]["total_requests"].as_u64().unwrap(), total);
        assert_eq!(
            result["global"]["latency"]["mean_us"].as_f64().unwrap() > 0.0,
            policies[0] != "none"
        );
    }
}
