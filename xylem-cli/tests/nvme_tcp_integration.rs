//! NVMe/TCP against SPDK's nvmf_tgt through the real CLI
//!
//! Each test starts its own target: a 16 MiB Malloc bdev (512-byte blocks)
//! as namespace 1 of one subsystem, listening on a free loopback port with
//! posix sockets. It runs unprivileged, without hugepages (`--no-huge`, so
//! SPDK 24.01 or later), on one core: the last CPU, or `XYLEM_NVMF_CORE`.
//!
//! The target binary is `XYLEM_NVMF_TGT`, else `nvmf_tgt` built from this
//! flake's nixpkgs (`nix build --inputs-from . nixpkgs#spdk`), which the
//! binary cache usually has.

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const SUBNQN: &str = "nqn.2016-06.io.spdk:xylem-test";
/// LBAs of the Malloc bdev
const NUM_BLOCKS: u64 = 32768;

/// The nvmf_tgt binary to run
fn nvmf_tgt() -> PathBuf {
    if let Some(path) = std::env::var_os("XYLEM_NVMF_TGT") {
        return path.into();
    }
    let flake = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let output = Command::new("nix")
        .args(["build", "--no-link", "--print-out-paths", "--inputs-from"])
        .arg(&flake)
        .arg("nixpkgs#spdk")
        .output()
        .expect("run nix to build SPDK; or set XYLEM_NVMF_TGT to an nvmf_tgt binary");
    assert!(
        output.status.success(),
        "nix build nixpkgs#spdk failed; set XYLEM_NVMF_TGT to an nvmf_tgt binary: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let out = String::from_utf8(output.stdout).unwrap();
    Path::new(out.trim()).join("bin/nvmf_tgt")
}

/// An SPDK NVMe/TCP target, killed on drop
struct SpdkTarget {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl SpdkTarget {
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let call = |method: &str, params: serde_json::Value| serde_json::json!({ "method": method, "params": params });
        let config = serde_json::json!({ "subsystems": [
            { "subsystem": "bdev", "config": [call("bdev_malloc_create", serde_json::json!({
                "name": "Malloc0", "num_blocks": NUM_BLOCKS, "block_size": 512
            }))] },
            { "subsystem": "nvmf", "config": [
                call("nvmf_create_transport", serde_json::json!({
                    "trtype": "TCP", "max_queue_depth": 128, "max_io_qpairs_per_ctrlr": 8,
                    "in_capsule_data_size": 4096, "num_shared_buffers": 511
                })),
                call("nvmf_create_subsystem", serde_json::json!({
                    "nqn": SUBNQN, "serial_number": "XYLEM", "allow_any_host": true
                })),
                call("nvmf_subsystem_add_ns", serde_json::json!({
                    "nqn": SUBNQN, "namespace": { "bdev_name": "Malloc0" }
                })),
                call("nvmf_subsystem_add_listener", serde_json::json!({
                    "nqn": SUBNQN, "listen_address": {
                        "trtype": "TCP", "adrfam": "IPv4", "traddr": "127.0.0.1",
                        "trsvcid": port.to_string()
                    }
                })),
            ] },
        ] });
        let config_path = dir.path().join("config.json");
        std::fs::write(&config_path, config.to_string()).unwrap();
        let core = std::env::var("XYLEM_NVMF_CORE").unwrap_or_else(|_| {
            let cpus = std::thread::available_parallelism().map_or(1, usize::from);
            (cpus - 1).to_string()
        });
        let log_path = dir.path().join("nvmf_tgt.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let mut child = Command::new(nvmf_tgt())
            .args(["--no-huge", "-s", "512", "-m", &format!("[{core}]")])
            .args(["--no-pci", "--disable-cpumask-locks", "--num-trace-entries", "0"])
            .arg("-r")
            .arg(dir.path().join("rpc.sock"))
            .arg("-c")
            .arg(&config_path)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .expect("start nvmf_tgt");

        // It listens once its whole configuration is applied
        let deadline = Instant::now() + Duration::from_secs(30);
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            let log = || std::fs::read_to_string(&log_path).unwrap_or_default();
            if let Some(status) = child.try_wait().unwrap() {
                panic!("nvmf_tgt exited ({status}) before listening:\n{}", log());
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("nvmf_tgt did not listen within 30 s:\n{}", log());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Self { child, port, _dir: dir }
    }
}

impl Drop for SpdkTarget {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Run a one-second NVMe/TCP profile against `target` and return the global
/// results; the run must succeed
fn run(
    target: &SpdkTarget,
    connections: usize,
    queue_depth: usize,
    protocol_config: serde_json::Value,
) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profile.toml");
    let results = dir.path().join("results.json");
    let mut protocol_config = protocol_config;
    protocol_config["subnqn"] = SUBNQN.into();
    let config = serde_json::json!({
        "experiment": { "name": "nvme-tcp", "duration": "1s", "seed": 1 },
        "traffic_groups": [{
            "name": "spdk", "threads": [0], "connections_per_thread": connections,
            "max_pending_per_connection": queue_depth,
            "target": format!("127.0.0.1:{}", target.port),
            "protocol": "nvme-tcp",
            "traffic_policy": { "type": "closed-loop" },
            "protocol_config": protocol_config
        }],
        "output": { "format": "json", "file": results }
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "warn")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "xylem failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&results).unwrap()).unwrap();
    json["global"].clone()
}

/// Check that every measured command completed without error, and return
/// the completed count
fn all_completed(global: &serde_json::Value) -> u64 {
    let requests = &global["requests"];
    let count = |key: &str| requests[key].as_u64().unwrap();
    assert!(count("completed") > 100, "{requests}");
    assert_eq!(count("errors"), 0, "{requests}");
    assert_eq!(count("failed") + count("timed_out"), 0, "{requests}");
    assert_eq!(count("sent"), count("completed"), "{requests}");
    count("completed")
}

#[test]
fn nvme_tcp_integration_4k_reads_and_in_capsule_writes() {
    let target = SpdkTarget::start();
    for (access, queue_depth) in [("random", 1), ("sequential", 32)] {
        let reads = run(&target, 2, queue_depth, serde_json::json!({ "access": access }));
        all_completed(&reads);
        // Each read sends a 72-byte capsule and receives a 4096-byte C2HData
        // PDU with its 24-byte header
        let rps = reads["throughput_rps"].as_f64().unwrap();
        let mbps = reads["throughput_mbps"].as_f64().unwrap();
        let expected = rps * (72.0 + 24.0 + 4096.0) * 8.0 / 1e6;
        assert!((mbps - expected).abs() < expected * 0.01, "{mbps} vs {expected} Mbit/s");

        let writes = run(
            &target,
            2,
            queue_depth,
            serde_json::json!({ "access": access, "read_percent": 0 }),
        );
        all_completed(&writes);
    }
}

#[test]
fn nvme_tcp_integration_large_writes_via_r2t_on_several_controllers() {
    let target = SpdkTarget::start();
    // Two controllers of two I/O queues each; 64 KiB writes exceed the
    // 4096-byte in-capsule size and go after R2Ts, 128 KiB reads arrive in
    // C2HData PDUs larger than the receive buffer
    let config = serde_json::json!({
        "block_size": 65536, "read_percent": 0, "io_queues_per_controller": 2
    });
    all_completed(&run(&target, 4, 4, config));
    let config = serde_json::json!({ "block_size": 131072, "io_queues_per_controller": 2 });
    all_completed(&run(&target, 4, 2, config));
}

#[test]
fn nvme_tcp_integration_readback_compare() {
    let target = SpdkTarget::start();
    // The whole 2048-LBA range is written first, then every read of the mix
    // must return the pattern; a mismatch fails the run
    for block_size in [4096, 65536] {
        let config = serde_json::json!({
            "block_size": block_size, "read_percent": 50, "verify": true, "lba_count": 2048
        });
        all_completed(&run(&target, 4, 8, config));
    }
}

#[test]
fn nvme_tcp_integration_rejects_a_range_beyond_the_namespace() {
    let target = SpdkTarget::start();
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profile.toml");
    let config = serde_json::json!({
        "experiment": { "name": "nvme-tcp", "duration": "1s" },
        "traffic_groups": [{
            "name": "spdk", "threads": [0], "connections_per_thread": 1,
            "target": format!("127.0.0.1:{}", target.port), "protocol": "nvme-tcp",
            "traffic_policy": { "type": "closed-loop" },
            "protocol_config": { "subnqn": SUBNQN, "lba_start": NUM_BLOCKS }
        }],
        "output": { "format": "json", "file": dir.path().join("results.json") }
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("exceeds namespace 1's 32768 LBAs"), "{stderr}");
}
