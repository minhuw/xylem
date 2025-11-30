//! Integration tests for different transport types (UDP and Unix sockets)
//!
//! These tests verify that the transport layer works correctly with real servers.

use std::net::UdpSocket;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
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
fn test_udp_transport_echo() {
    // Start UDP echo server on a random port
    let port = 19999;
    let server_handle = start_udp_echo_server(port);

    // Give server time to start
    thread::sleep(Duration::from_millis(100));

    // Create UDP transport and connect
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();
    let mut transport = UdpTransport::new();
    assert!(transport.connect(&addr).is_ok(), "Failed to connect UDP transport");

    // Send a few packets
    for i in 0..5 {
        let test_data = format!("UDP test {}", i);
        assert!(transport.send(test_data.as_bytes()).is_ok());
    }

    // Wait for responses
    thread::sleep(Duration::from_millis(100));

    // Receive responses
    let mut received = 0;
    for _ in 0..10 {
        if transport.poll_readable().unwrap_or(false) {
            if let Ok((data, _)) = transport.recv() {
                if !data.is_empty() {
                    received += 1;
                }
            }
        }
        thread::sleep(Duration::from_millis(10));
    }

    transport.close().ok();
    server_handle.join().ok();

    // We should have received at least some responses
    assert!(received > 0, "No UDP responses received");
}

#[test]
#[cfg(unix)]
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
        assert!(recv_ts.cycles() >= send_ts.cycles(), "Invalid timestamp order");
    }

    transport.close().ok();
    server_handle.join().ok();
}

#[test]
fn test_udp_transport_basic() {
    // Start UDP echo server
    let port = 19998;
    let server_handle = start_udp_echo_server(port);

    thread::sleep(Duration::from_millis(100));

    // Create transport and connect
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();
    let mut transport = UdpTransport::new();
    assert!(transport.connect(&addr).is_ok(), "Failed to connect");

    // Send test data
    let test_data = b"Hello UDP";
    let send_ts = transport.send(test_data).expect("Send failed");

    // Wait for response
    thread::sleep(Duration::from_millis(50));

    // Poll and receive
    for _ in 0..100 {
        if transport.poll_readable().unwrap_or(false) {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    let (recv_data, recv_ts) = transport.recv().expect("Receive failed");
    assert_eq!(recv_data, test_data);
    assert!(recv_ts.cycles() >= send_ts.cycles());

    transport.close().ok();
    server_handle.join().ok();
}

#[test]
#[cfg(unix)]
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
