use xylem_protocols::configs::*;
use xylem_protocols::factories::{
    HttpFactory, MemcachedAsciiFactory, MemcachedBinaryFactory, RedisFactory,
};
use xylem_protocols::factory::ProtocolFactory;
use xylem_protocols::http::HttpProtocol;
use xylem_protocols::memcached::MemcachedAsciiProtocol;
use xylem_protocols::redis::cluster::RedisClusterProtocol;
use xylem_protocols::{ParseResult, Protocol, RedisProtocol};

fn redis() -> RedisProtocol {
    RedisProtocol::from_config(&RedisConfig::default(), Some(42)).unwrap()
}

#[test]
fn redis_maps_attributes_and_pipelined_ids() {
    let frames: &[&[u8]] = &[
        // A representative HELLO map, including an array and nested map.
        b"%3\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n+modules\r\n*1\r\n%1\r\n+name\r\n+test\r\n",
        // Nested attributes on an element; outer attributes attach to the map.
        b"|1\r\n+meta\r\n%1\r\n+k\r\n+v\r\n%1\r\n+key\r\n|1\r\n+ttl\r\n:10\r\n+value\r\n",
        b"|0\r\n|1\r\n+ttl\r\n:1\r\n+OK\r\n",
    ];
    let mut protocol = redis();
    for (seq, frame) in frames.iter().enumerate() {
        for end in 0..frame.len() {
            assert_eq!(protocol.parse_response(7, &frame[..end]).unwrap(), (0, None));
        }
        let data = [*frame, b"+NEXT\r\n"].concat();
        assert_eq!(
            protocol.parse_response(7, &data).unwrap(),
            (frame.len(), Some((7, seq as u64)))
        );
    }
    assert_eq!(protocol.parse_response(7, b"+NEXT\r\n").unwrap(), (7, Some((7, 3))));
    for bad in [b"%999999999999999999999999\r\n".as_slice(), b"|10000001\r\n", b"%-1\r\n"] {
        assert!(protocol.parse_response(7, bad).is_err());
    }
}

#[test]
fn redis_limits_frames_instead_of_coalesced_buffer() {
    let mut data = b"+OK\r\n".to_vec();
    data.resize(10_485_762, b'x');
    assert_eq!(redis().parse_response(0, &data).unwrap(), (5, Some((0, 0))));
}

fn cluster() -> RedisClusterProtocol {
    let config = RedisConfig {
        keys: KeysConfig::Sequential {
            start: 42,
            value_size: 12,
            prefix: "test:".into(),
        },
        random_data: true,
        insert_phase: Some(InsertPhaseConfig { key_count: Some(1), value_size: Some(17) }),
        ..Default::default()
    };
    let mut protocol = RedisClusterProtocol::from_config(&config, Some(42)).unwrap();
    protocol.register_connection("127.0.0.1:7000".parse().unwrap(), 0);
    protocol.register_connection("127.0.0.1:7001".parse().unwrap(), 1);
    protocol
}

#[test]
fn moved_and_ask_preserve_payload_warmup_fifo_and_completion() {
    for kind in ["MOVED", "ASK"] {
        let mut protocol = cluster();
        let original = protocol.next_request(0);
        assert!(original.metadata.is_warmup);
        assert!(original.data.starts_with(b"*3\r\n$3\r\nSET\r\n"));
        let second = protocol.next_request(0);
        assert!(!second.metadata.is_warmup);
        assert!(second.data.starts_with(b"*2\r\n$3\r\nGET\r\n"));
        let reply = format!("-{kind} {} 127.0.0.1:7001\r\n", original.request_id.1);
        for end in 0..reply.len() {
            assert!(matches!(
                protocol.parse_response_extended(0, &reply.as_bytes()[..end]).unwrap(),
                ParseResult::Incomplete
            ));
        }
        let coalesced = [reply.as_bytes(), b"$-1\r\n"].concat();
        let ParseResult::Retry(retry) = protocol.parse_response_extended(0, &coalesced).unwrap()
        else {
            panic!("expected retry")
        };
        assert_eq!(retry.bytes_consumed, reply.len());
        assert_eq!(retry.original_request_id, original.request_id);
        assert_eq!(retry.target_conn_id, Some(1));
        assert!(retry.is_warmup);
        assert_eq!(retry.attempt, 0);
        assert_eq!(retry.prepare_commands.len(), usize::from(kind == "ASK"));
        assert!(
            matches!(protocol.parse_response_extended(0, &coalesced[reply.len()..]).unwrap(),
            ParseResult::Complete { request_id, bytes_consumed: 5 } if request_id == second.request_id)
        );
        // An earlier outstanding request on the retry target must complete first.
        protocol.update_topology(
            xylem_protocols::redis::cluster::ClusterTopology::from_slot_ranges(vec![
                xylem_protocols::redis::cluster::topology::SlotRange {
                    start: 0,
                    end: 16383,
                    master: "127.0.0.1:7001".parse().unwrap(),
                    replicas: vec![],
                },
            ]),
        );
        let earlier = protocol.generate_request_with_key(1, 100, 1);
        let target = earlier.request_id.2 .0;
        let retried = protocol.regenerate_request(target, retry.original_request_id);
        assert_eq!(retried.data, original.data);
        assert_eq!(retried.metadata, original.metadata);
        assert_eq!(retried.request_id.0, original.request_id.0);
        assert_eq!(retried.request_id.1, original.request_id.1);
        assert!(matches!(protocol.parse_response_extended(target, b"$-1\r\n").unwrap(),
            ParseResult::Complete { request_id, .. } if request_id == earlier.request_id));
        let completion: &[u8] = if kind == "ASK" {
            b"+OK\r\n+OK\r\n"
        } else {
            b"+OK\r\n"
        };
        for end in 0..completion.len() {
            assert!(matches!(
                protocol.parse_response_extended(target, &completion[..end]).unwrap(),
                ParseResult::Incomplete
            ));
        }
        assert!(matches!(protocol.parse_response_extended(target, completion).unwrap(),
            ParseResult::Complete { request_id, bytes_consumed } if request_id == retried.request_id && bytes_consumed == completion.len()));
        let next = protocol.next_request(0);
        assert!(
            matches!(protocol.parse_response_extended(next.request_id.2.0, b"+OK\r\n").unwrap(),
            ParseResult::Complete { request_id, .. } if request_id == next.request_id)
        );
    }
}

#[test]
fn redirect_budget_follows_request_across_connections() {
    let mut protocol = cluster();
    let mut request = protocol.next_request(0);
    for attempt in 0..5 {
        let target = 1 - request.request_id.2 .0;
        let redirect = format!("-MOVED {} 127.0.0.1:{}\r\n", request.request_id.1, 7000 + target);
        let ParseResult::Retry(retry) = protocol
            .parse_response_extended(request.request_id.2 .0, redirect.as_bytes())
            .unwrap()
        else {
            panic!("expected retry")
        };
        assert_eq!(retry.attempt, attempt);
        let next = protocol.regenerate_request(target, request.request_id);
        assert_eq!(next.data, request.data);
        assert_eq!(next.metadata, request.metadata);
        request = next;
    }
    let redirect = format!("-MOVED {} 127.0.0.1:7000\r\n", request.request_id.1);
    assert!(protocol
        .parse_response_extended(request.request_id.2 .0, redirect.as_bytes())
        .unwrap_err()
        .to_string()
        .contains("Max redirect limit"));
    assert_eq!(protocol.redirect_stats().redirect_loops_prevented, 1);
    let next = protocol.next_request(0);
    assert!(
        matches!(protocol.parse_response_extended(next.request_id.2.0, b"+OK\r\n").unwrap(), ParseResult::Complete { request_id, .. } if request_id == next.request_id)
    );
}

#[test]
fn memcached_value_lengths_are_binary_safe_and_require_end() {
    let mut protocol = MemcachedAsciiProtocol::default();
    for (seq, value) in [b"".as_slice(), b"a\nb", b"a\r\nb", b"\r\nEND\r\n", b"\0\xff"]
        .iter()
        .enumerate()
    {
        let header = format!("VALUE key:1 0 {}\r\n", value.len());
        let response = [header.as_bytes(), value, b"\r\nEND\r\n"].concat();
        for end in 0..response.len() {
            assert_eq!(protocol.parse_response(0, &response[..end]).unwrap(), (0, None));
        }
        let coalesced = [response.as_slice(), b"STORED\r\n"].concat();
        assert_eq!(
            protocol.parse_response(0, &coalesced).unwrap(),
            (response.len(), Some((0, seq as u64)))
        );
    }
    assert_eq!(protocol.parse_response(0, b"STORED\r\n").unwrap(), (8, Some((0, 5))));
    let multi = b"VALUE a 0 1\r\nx\r\nVALUE b 0 0 42\r\n\r\nEND\r\n";
    assert_eq!(protocol.parse_response(0, multi).unwrap(), (multi.len(), Some((0, 6))));
}

#[test]
fn memcached_errors_and_malformed_lengths_do_not_succeed() {
    for response in [
        b"ERROR\r\n".as_slice(),
        b"CLIENT_ERROR bad command\r\n",
        b"SERVER_ERROR unavailable\r\n",
        b"VALUE k 0 nope\r\n",
        b"VALUE k 0 1\r\nx!!END\r\n",
        b"NOT_STORED\r\n",
    ] {
        let mut protocol = MemcachedAsciiProtocol::default();
        assert!(protocol.parse_response(0, response).is_err());
        assert_eq!(protocol.parse_response(0, b"END\r\n").unwrap(), (5, Some((0, 0))));
    }
}

#[test]
fn http_explicit_framing_and_bodyless_responses() {
    let mut protocol = HttpProtocol::default();
    let responses: &[&[u8]] = &[
        b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\na\nb",
        b"HTTP/1.1 204 No Content\r\n\r\n",
        b"HTTP/1.1 304 Not Modified\r\nContent-Length: 100\r\n\r\n",
        b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
    ];
    for (seq, response) in responses.iter().enumerate() {
        for end in 0..response.len() {
            assert_eq!(protocol.parse_response(3, &response[..end]).unwrap(), (0, None));
        }
        let coalesced = [*response, responses[0]].concat();
        assert_eq!(
            protocol.parse_response(3, &coalesced).unwrap(),
            (response.len(), Some((3, seq as u64)))
        );
    }
}

#[test]
fn http_rejects_unsupported_and_ambiguous_framing_before_success() {
    let responses: &[&[u8]] = &[
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\nTrailer: yes\r\n\r\n",
        b"HTTP/1.1 200 OK\r\n\r\nbody",
        b"HTTP/1.1 200 OK\r\nContent-Length: nope\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: +1\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 1\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 18446744073709551615\r\n\r\n",
        b"HTTP/1.1 101 Switching Protocols\r\n\r\n",
    ];
    for response in responses {
        let mut protocol = HttpProtocol::default();
        assert!(protocol.parse_response(0, response).is_err());
        let good = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(protocol.parse_response(0, good).unwrap(), (good.len(), Some((0, 0))));
    }
}

#[test]
fn factories_honor_keys_values_insertion_and_http_body() {
    let config = MemcachedConfig {
        keys: KeysConfig::Sequential {
            start: 25,
            value_size: 3,
            prefix: "item:".into(),
        },
        insert_phase: Some(InsertPhaseConfig { key_count: Some(1), value_size: Some(5) }),
        ..Default::default()
    };
    let mut ascii = MemcachedAsciiFactory.create(config.clone(), Some(42)).unwrap();
    let warmup = ascii.next_request(0);
    assert!(warmup.metadata.is_warmup);
    assert_eq!(warmup.data, b"set item:25 0 0 5\r\nxxxxx\r\n");
    let get = ascii.next_request(0);
    assert!(!get.metadata.is_warmup);
    assert_eq!(get.data, b"get item:25\r\n");
    let mut binary = MemcachedBinaryFactory.create(config, Some(42)).unwrap();
    let warmup = binary.next_request(0);
    assert!(warmup.metadata.is_warmup);
    assert_eq!(warmup.data[1], 1); // SET
    assert_eq!(&warmup.data[32..], b"item:25xxxxx");
    let get = binary.next_request(0);
    assert!(!get.metadata.is_warmup);
    assert_eq!(&get.data[24..], b"item:25");
    let mut http = HttpFactory
        .create(
            HttpConfig {
                method: "POST".into(),
                body_size: 7,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    let post = http.next_request(0);
    assert!(post.data.ends_with(b"\r\n\r\nxxxxxxx"));
    assert!(String::from_utf8_lossy(&post.data).contains("Content-Length: 7\r\n"));
}

#[test]
fn redis_factory_retains_variable_sizes_random_data_and_per_command_keys() {
    let config = RedisConfig {
        keys: KeysConfig::Sequential {
            start: 12,
            value_size: 8,
            prefix: "test:".into(),
        },
        operations: Some(RedisOperationsConfig::Weighted {
            commands: vec![RedisCommandWeight {
                name: "SET".into(),
                weight: 1.0,
                params: None,
                keys: Some(KeysConfig::Sequential {
                    start: 99,
                    value_size: 8,
                    prefix: "test:".into(),
                }),
            }],
        }),
        value_size: Some(ValueSizeConfig::PerCommand {
            commands: [("set".into(), CommandValueSizeConfig::Uniform { min: 3, max: 4 })]
                .into_iter()
                .collect(),
            default: Box::new(ValueSizeConfig::Fixed { size: 100 }),
        }),
        random_data: true,
        insert_phase: Some(InsertPhaseConfig { key_count: Some(1), value_size: Some(9) }),
        ..Default::default()
    };
    let mut first = RedisFactory.create(config.clone(), Some(42)).unwrap();
    let mut second = RedisFactory.create(config, Some(42)).unwrap();
    let mut lengths = std::collections::HashSet::new();
    for i in 0..50 {
        let request = first.next_request(0);
        assert_eq!(request, second.next_request(0));
        let key = if i == 0 { 12 } else { 98 + i };
        let prefix =
            format!("*3\r\n$3\r\nSET\r\n${}\r\ntest:{key}\r\n$", format!("test:{key}").len());
        assert!(request.data.starts_with(prefix.as_bytes()));
        let remainder = &request.data[prefix.len()..];
        let split = remainder.windows(2).position(|w| w == b"\r\n").unwrap();
        let length: usize = std::str::from_utf8(&remainder[..split]).unwrap().parse().unwrap();
        let payload = &remainder[split + 2..remainder.len() - 2];
        assert_eq!(payload.len(), length);
        assert_ne!(payload, vec![b'x'; length]);
        assert_eq!(request.metadata.is_warmup, i == 0);
        if i == 0 {
            assert_eq!(length, 9);
        } else {
            lengths.insert(length);
        }
    }
    assert_eq!(lengths, [3, 4].into_iter().collect());
}

#[test]
fn redis_factory_uses_command_parameters_and_rejects_ignored_settings() {
    let mut config = RedisConfig {
        operations: Some(RedisOperationsConfig::Weighted {
            commands: vec![RedisCommandWeight {
                name: "MGET".into(),
                weight: 1.0,
                params: Some(RedisCommandParams::MGet { count: 2 }),
                keys: None,
            }],
        }),
        ..Default::default()
    };
    let mut protocol = RedisFactory.create(config.clone(), None).unwrap();
    assert_eq!(
        protocol.next_request(0).data,
        b"*3\r\n$4\r\nMGET\r\n$5\r\nkey:0\r\n$5\r\nkey:1\r\n"
    );
    config.data_import = Some(DataImportConfig {
        file: "unused.csv".into(),
        verification: None,
    });
    assert!(RedisFactory.create(config, None).is_err());
    let bad = RedisConfig {
        keys: KeysConfig::Random {
            max: 0,
            value_size: 64,
            prefix: "key:".into(),
        },
        ..Default::default()
    };
    assert!(RedisFactory.create(bad, None).is_err());
    let bad = MemcachedConfig {
        insert_phase: Some(InsertPhaseConfig::default()),
        ..Default::default()
    };
    assert!(MemcachedAsciiFactory.create(bad, None).is_err());
}

#[test]
fn normal_set_sizes_and_round_robin_keys_reach_both_memcached_formats() {
    let config = MemcachedConfig {
        keys: KeysConfig::RoundRobin {
            max: 2,
            value_size: 3,
            prefix: "p:".into(),
        },
        operation: "SET".into(),
        insert_phase: None,
    };
    let mut ascii = MemcachedAsciiFactory.create(config.clone(), None).unwrap();
    let mut binary = MemcachedBinaryFactory.create(config, None).unwrap();
    for key in [0, 1, 0] {
        assert_eq!(ascii.next_request(0).data, format!("set p:{key} 0 0 3\r\nxxx\r\n").as_bytes());
        let request = binary.next_request(0);
        assert_eq!(&request.data[32..], format!("p:{key}xxx").as_bytes());
        assert_eq!(u32::from_be_bytes(request.data[8..12].try_into().unwrap()), 14);
    }
}

#[test]
fn cluster_variable_sizes_and_static_topology_are_used() {
    for per_command in [false, true] {
        let config = RedisConfig {
            keys: KeysConfig::RoundRobin {
                max: 2,
                value_size: 1,
                prefix: "p:".into(),
            },
            operations: Some(RedisOperationsConfig::Weighted {
                commands: vec![RedisCommandWeight {
                    name: "SET".into(),
                    weight: 1.0,
                    params: None,
                    keys: per_command.then_some(KeysConfig::RoundRobin {
                        max: 2,
                        value_size: 1,
                        prefix: "p:".into(),
                    }),
                }],
            }),
            value_size: Some(ValueSizeConfig::Uniform { min: 3, max: 4 }),
            redis_cluster: Some(RedisClusterConfig {
                nodes: vec![RedisClusterNodeConfig {
                    address: "127.0.0.1:7001".into(),
                    slot_start: 0,
                    slot_end: 16383,
                }],
            }),
            ..Default::default()
        };
        let mut protocol = RedisClusterProtocol::from_config(&config, Some(42)).unwrap();
        protocol.register_connection("127.0.0.1:7000".parse().unwrap(), 0);
        protocol.register_connection("127.0.0.1:7001".parse().unwrap(), 1);
        let mut sizes = std::collections::HashSet::new();
        for i in 0..50 {
            let request = protocol.next_request(0);
            let key = i % 2;
            assert_eq!(request.request_id.2 .0, 1);
            let prefix = format!("*3\r\n$3\r\nSET\r\n$3\r\np:{key}\r\n$");
            assert!(request.data.starts_with(prefix.as_bytes()));
            let size = request.data[prefix.len()] - b'0';
            sizes.insert(size);
            assert!(request
                .data
                .ends_with(&[vec![b'x'; size as usize], b"\r\n".to_vec()].concat()));
            protocol.parse_response(1, b"+OK\r\n").unwrap();
        }
        assert_eq!(sizes, [3, 4].into_iter().collect());
    }
}

#[test]
fn retry_does_not_advance_command_or_payload_generators() {
    let config = RedisConfig {
        operations: Some(RedisOperationsConfig::Weighted {
            commands: vec![
                RedisCommandWeight {
                    name: "SET".into(),
                    weight: 1.0,
                    params: None,
                    keys: None,
                },
                RedisCommandWeight {
                    name: "GET".into(),
                    weight: 1.0,
                    params: None,
                    keys: None,
                },
            ],
        }),
        value_size: Some(ValueSizeConfig::Uniform { min: 3, max: 7 }),
        random_data: true,
        ..Default::default()
    };
    let mut retried = RedisClusterProtocol::from_config(&config, Some(42)).unwrap();
    let mut control = RedisProtocol::from_config(&config, Some(42)).unwrap();
    retried.register_connection("127.0.0.1:7000".parse().unwrap(), 0);
    for _ in 0..40 {
        let original = retried.next_request(0);
        assert_eq!(original.data, control.next_request(0).data);
        let redirect = format!("-MOVED {} 127.0.0.1:7000\r\n", original.request_id.1);
        assert!(matches!(
            retried.parse_response_extended(0, redirect.as_bytes()).unwrap(),
            ParseResult::Retry(_)
        ));
        let retry = retried.regenerate_request(0, original.request_id);
        assert_eq!(retry.data, original.data);
        assert!(
            matches!(retried.parse_response_extended(0, b"+OK\r\n").unwrap(), ParseResult::Complete { request_id, .. } if request_id == retry.request_id)
        );
    }
}

#[test]
fn asking_error_is_never_counted_as_a_retry_completion() {
    let mut protocol = cluster();
    let original = protocol.next_request(0);
    let redirect = format!("-ASK {} 127.0.0.1:7001\r\n", original.request_id.1);
    let ParseResult::Retry(retry) =
        protocol.parse_response_extended(0, redirect.as_bytes()).unwrap()
    else {
        panic!("expected ASK redirect");
    };
    protocol.regenerate_request(1, retry.original_request_id);
    let error = b"-ERR ASKING denied\r\n";
    for end in 0..error.len() {
        assert!(matches!(
            protocol.parse_response_extended(1, &error[..end]).unwrap(),
            ParseResult::Incomplete
        ));
    }
    assert!(protocol
        .parse_response_extended(1, &[error.as_slice(), b"+OK\r\n"].concat())
        .unwrap_err()
        .to_string()
        .contains("ASKING failed"));
}
