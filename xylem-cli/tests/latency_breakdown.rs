//! Through the real executable, over loopback: kernel timestamps split each
//! round trip in the latency breakdown, user-space timestamps report only
//! its service part, and NIC timestamps that are not switched on fail the
//! run before any connection opens.
//!
//! The echo servers answer like `xylem-echo-server` with no delay: every
//! TCP byte and every UDP datagram goes straight back.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::Path;
use std::process::{Command, Output};
use std::time::Duration;

/// Echo every byte of `socket` back until the peer closes it
fn echo_stream(mut socket: TcpStream) {
    let mut buf = [0; 4096];
    while let Ok(n) = socket.read(&mut buf) {
        if n == 0 || socket.write_all(&buf[..n]).is_err() {
            break;
        }
    }
}

/// TCP echo server; runs until the test process exits
fn serve_tcp_echo() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for socket in listener.incoming() {
            let Ok(socket) = socket else { break };
            std::thread::spawn(move || echo_stream(socket));
        }
    });
    addr
}

/// UDP echo server; runs until the test process exits
fn serve_udp_echo() -> SocketAddr {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    std::thread::spawn(move || {
        let mut buf = [0; 2048];
        while let Ok((n, peer)) = socket.recv_from(&mut buf) {
            let _ = socket.send_to(&buf[..n], peer);
        }
    });
    addr
}

/// Run a one-second closed-loop xylem-echo profile against `target` with
/// `timestamps` (absent for the default), writing JSON results into `dir`
fn run_cli(
    transport: &str,
    target: SocketAddr,
    timestamps: Option<serde_json::Value>,
    dir: &Path,
) -> Output {
    let file = dir.join("results.json");
    let mut group = serde_json::json!({
        "name": "echo", "threads": [0], "connections_per_thread": 2,
        "max_pending_per_connection": 1, "target": target.to_string(),
        "transport": transport, "protocol": "xylem-echo",
        "traffic_policy": { "type": "closed-loop" }
    });
    if let Some(timestamps) = timestamps {
        group["timestamps"] = timestamps;
    }
    let config = serde_json::json!({
        "experiment": { "name": "latency-breakdown", "duration": "1s", "seed": 1 },
        "traffic_groups": [group],
        "output": { "format": "json", "file": file }
    });
    let profile = dir.join("profile.toml");
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "off")
        .output()
        .unwrap()
}

/// The JSON results of a successful run
fn results(output: &Output, dir: &Path) -> serde_json::Value {
    assert_eq!(output.status.code(), Some(0), "{}", String::from_utf8_lossy(&output.stderr));
    serde_json::from_str(&std::fs::read_to_string(dir.join("results.json")).unwrap()).unwrap()
}

/// The global and group latency breakdowns with their latency sample counts
fn breakdowns(json: &serde_json::Value) -> [(&serde_json::Value, u64); 2] {
    let group = &json["traffic_groups"][0]["stats"];
    [&json["global"], group].map(|stats| {
        (&stats["latency_breakdown"], stats["latency"]["sample_count"].as_u64().unwrap())
    })
}

fn f64_at(value: &serde_json::Value, key: &str) -> f64 {
    value[key].as_f64().unwrap_or_else(|| panic!("{key}: {value}"))
}

#[cfg(target_os = "linux")]
#[test]
fn kernel_timestamps_split_the_round_trip_over_tcp_and_udp() {
    for (transport, target) in [("tcp", serve_tcp_echo()), ("udp", serve_udp_echo())] {
        let dir = tempfile::tempdir().unwrap();
        let output = run_cli(transport, target, Some("kernel".into()), dir.path());
        let json = results(&output, dir.path());
        for (breakdown, samples) in breakdowns(&json) {
            let context = format!("{transport}: {breakdown}");
            assert!(samples > 1000, "{context}");
            // Loopback stamps every send and receive in the kernel
            let split = breakdown["kernel_split_count"].as_u64().unwrap();
            assert!(split * 10 >= samples * 9, "{context}");
            let clock = breakdown["clock"].as_str().unwrap();
            assert!(clock == "kernel_software" || clock == "mixed" && split < samples, "{context}");
            let service = &breakdown["service_us"];
            assert_eq!(service["sample_count"], samples, "{context}");
            for part in ["host_stack_us", "network_server_us"] {
                let part = &breakdown[part];
                assert_eq!(part["sample_count"], split, "{context}");
                for key in ["mean_us", "p50_us", "p99_us", "p999_us", "max_us"] {
                    assert!(f64_at(part, key) >= 0.0, "{context}");
                }
                assert!(f64_at(part, "p50_us") <= f64_at(part, "max_us") * 1.001, "{context}");
            }
            let network = &breakdown["network_server_us"];
            assert!(f64_at(network, "p50_us") > 0.0, "{context}");
            // The latency uses the kernel round trip, so split requests'
            // service latency is it
            if split == samples {
                assert_eq!(service, network, "{context}");
            }
        }
    }
}

#[test]
fn user_space_timestamps_report_only_the_service_latency() {
    let dir = tempfile::tempdir().unwrap();
    let output = run_cli("udp", serve_udp_echo(), None, dir.path());
    let json = results(&output, dir.path());
    for (breakdown, samples) in breakdowns(&json) {
        assert_eq!(breakdown["clock"], "user_space", "{breakdown}");
        assert_eq!(breakdown["kernel_split_count"], 0, "{breakdown}");
        assert!(breakdown["host_stack_us"].is_null(), "{breakdown}");
        assert!(breakdown["network_server_us"].is_null(), "{breakdown}");
        assert_eq!(breakdown["service_us"]["sample_count"], samples, "{breakdown}");
        assert!(f64_at(&breakdown["service_us"], "p99_us") > 0.0, "{breakdown}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn nic_timestamps_that_are_off_fail_the_run_before_it_connects() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let target = listener.local_addr().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let hardware = serde_json::json!({ "hardware": "lo" });
    let output = run_cli("tcp", target, Some(hardware), dir.path());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    // lo has no PHC: its driver rejects the query or reports it off
    assert!(
        stderr
            .starts_with(r#"Error: Traffic group 0 'echo' sets timestamps = { hardware = "lo" }"#),
        "{stderr}"
    );
    assert!(stderr.contains("Caused by: Configuration error: "), "{stderr}");
    assert_eq!(stderr.matches("Configuration error").count(), 1, "{stderr}");
    assert!(stderr.contains("'lo'"), "{stderr}");
    assert!(!dir.path().join("results.json").exists());
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        listener.accept().map(|_| ()).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock,
        "no connection was opened"
    );
}
