//! Integration tests for different transport types (UDP and Unix sockets)
//!
//! These tests verify that the transport layer works correctly with real servers.

use std::net::UdpSocket;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use xylem_core::connection::ConnectionPool;
use xylem_core::scheduler::UniformPolicyScheduler;
use xylem_protocols::xylem_echo::XylemEchoProtocol;
use xylem_transport::{Transport, UdpTransport};

#[cfg(unix)]
use std::os::unix::net::UnixListener;
#[cfg(unix)]
use xylem_transport::UnixTransport;

/// Simple UDP echo server for testing
fn start_udp_echo_server(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let socket =
            UdpSocket::bind(format!("127.0.0.1:{}", port)).expect("Failed to bind UDP socket");
        let mut buf = vec![0u8; 65535];

        // Run for 30 seconds
        socket.set_read_timeout(Some(Duration::from_secs(1))).ok();

        for _ in 0..30 {
            match socket.recv_from(&mut buf) {
                Ok((n, src)) => {
                    // Echo back
                    socket.send_to(&buf[..n], src).ok();
                }
                Err(_) => continue,
            }
        }
    })
}

/// Simple Unix socket echo server for testing
fn start_unix_echo_server(path: std::path::PathBuf) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let listener = UnixListener::bind(&path).expect("Failed to bind Unix socket");

        listener.set_nonblocking(false).ok();

        if let Ok((mut stream, _)) = listener.accept() {
            use std::io::{Read, Write};
            let mut buf = vec![0u8; 8192];

            // Echo for 30 seconds
            stream.set_read_timeout(Some(Duration::from_millis(100))).ok();

            for _ in 0..300 {
                match stream.read(&mut buf) {
                    Ok(0) => break, // Connection closed
                    Ok(n) => {
                        stream.write_all(&buf[..n]).ok();
                    }
                    Err(_) => continue,
                }
            }
        }
    })
}

#[test]
#[ignore] // Ignore by default as it requires UDP port availability
fn test_udp_transport_echo() {
    // Start UDP echo server on a random port
    let port = 19999;
    let server_handle = start_udp_echo_server(port);

    // Give server time to start
    thread::sleep(Duration::from_millis(100));

    // Create connection pool with UDP transport
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();
    let policy_scheduler = Box::new(UniformPolicyScheduler::closed_loop());

    let pool = ConnectionPool::new(
        UdpTransport::new,
        addr,
        2,  // 2 connections
        10, // 10 max pending per connection
        policy_scheduler,
        0, // group_id
    );

    assert!(pool.is_ok(), "Failed to create UDP connection pool");
    let mut pool = pool.unwrap();

    // Send a few packets
    for i in 0..5 {
        if let Some(conn) = pool.pick_connection() {
            let test_data = format!("UDP test {}", i);
            assert!(conn.send(test_data.as_bytes(), i).is_ok());
        }
    }

    // Wait for responses
    thread::sleep(Duration::from_millis(100));

    // Receive responses
    let mut received = 0;
    for conn in pool.connections_mut() {
        if let Ok(latencies) = conn.recv_responses(|data| {
            // Simple parser: return all data as one response with ID 0
            if data.is_empty() {
                Ok((0, None))
            } else {
                Ok((data.len(), Some(0u64)))
            }
        }) {
            received += latencies.len();
        }
    }

    pool.close_all().ok();
    server_handle.join().ok();

    // We should have received at least some responses
    assert!(received > 0, "No UDP responses received");
}

#[test]
#[cfg(unix)]
#[ignore] // Ignore by default as it requires file system access
fn test_unix_socket_transport_echo() {
    // Create temporary directory for Unix socket
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let socket_path = temp_dir.path().join("echo.sock");

    // Start Unix socket echo server
    let server_path = socket_path.clone();
    let server_handle = start_unix_echo_server(server_path);

    // Give server time to start
    thread::sleep(Duration::from_millis(100));

    // Create Unix transport and connect
    let mut transport = UnixTransport::new();
    assert!(transport.connect_path(&socket_path).is_ok(), "Failed to connect to Unix socket");

    // Send and receive test data
    for i in 0..5 {
        let test_data = format!("Unix test {}", i);

        // Send
        let send_ts = transport.send(test_data.as_bytes()).expect("Failed to send");

        // Wait a bit
        thread::sleep(Duration::from_millis(10));

        // Poll until readable
        for _ in 0..100 {
            if transport.poll_readable().unwrap_or(false) {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        // Receive
        let (recv_data, recv_ts) = transport.recv().expect("Failed to receive");

        assert_eq!(recv_data, test_data.as_bytes(), "Echo mismatch");
        assert!(recv_ts.instant >= send_ts.instant, "Invalid timestamp order");
    }

    transport.close().ok();
    server_handle.join().ok();
}

#[test]
#[ignore] // Ignore by default
fn test_udp_xylem_echo_protocol() {
    // Start Xylem echo server with UDP support (if available)
    // For now, just test basic UDP functionality

    let port = 19998;
    let server_handle = start_udp_echo_server(port);

    thread::sleep(Duration::from_millis(100));

    // Create protocol and connection pool
    let _protocol = XylemEchoProtocol::new(0); // 0us delay
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();

    let policy_scheduler = Box::new(UniformPolicyScheduler::closed_loop());

    let pool: Result<ConnectionPool<UdpTransport, u64>, _> =
        ConnectionPool::new(UdpTransport::new, addr, 1, 5, policy_scheduler, 0);

    assert!(pool.is_ok(), "Failed to create connection pool");

    pool.unwrap().close_all().ok();
    server_handle.join().ok();
}

#[test]
#[cfg(unix)]
#[ignore] // Ignore by default
fn test_unix_socket_multiple_connections() {
    // Test that we can create multiple Unix socket connections
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    for i in 0..3 {
        let socket_path = temp_dir.path().join(format!("test{}.sock", i));
        let server_path = socket_path.clone();

        let _server = start_unix_echo_server(server_path);
        thread::sleep(Duration::from_millis(50));

        let mut transport = UnixTransport::new();
        assert!(
            transport.connect_path(&socket_path).is_ok(),
            "Failed to connect to socket {}",
            i
        );

        let test_data = b"Hello";
        transport.send(test_data).expect("Send failed");

        thread::sleep(Duration::from_millis(10));

        for _ in 0..100 {
            if transport.poll_readable().unwrap_or(false) {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        let (recv_data, _) = transport.recv().expect("Receive failed");
        assert_eq!(recv_data, test_data);

        transport.close().ok();
    }
}

#[test]
#[ignore] // Ignore by default
fn test_udp_packet_loss_handling() {
    // Test that UDP transport handles packet loss gracefully
    let port = 19997;
    let server_handle = start_udp_echo_server(port);

    thread::sleep(Duration::from_millis(100));

    let mut transport = UdpTransport::new();
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();

    assert!(transport.connect(&addr).is_ok());

    // Send multiple packets quickly
    for i in 0..10 {
        let data = format!("Packet {}", i);
        transport.send(data.as_bytes()).expect("Send failed");
    }

    // Wait and try to receive
    thread::sleep(Duration::from_millis(100));

    let mut received_count = 0;
    for _ in 0..10 {
        if transport.poll_readable().unwrap_or(false) {
            if let Ok((data, _)) = transport.recv() {
                if !data.is_empty() {
                    received_count += 1;
                }
            }
        }
        thread::sleep(Duration::from_millis(10));
    }

    // We should receive at least some packets (UDP may drop some)
    assert!(received_count > 0, "No packets received (expected some)");

    transport.close().ok();
    server_handle.join().ok();
}
