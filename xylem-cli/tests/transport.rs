//! Integration tests for the UDP and Unix socket transports
//!
//! These tests drive the connection-group API, as the CLI does, against real
//! echo servers.

use std::net::{SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};
use xylem_transport::{ConnectionGroup, RecvOutcome, TransportFactory, UdpTransportFactory};

#[cfg(unix)]
use std::os::unix::net::UnixListener;
#[cfg(unix)]
use tempfile::TempDir;
#[cfg(unix)]
use xylem_transport::UnixTransportFactory;

/// UDP echo server on an ephemeral port; runs until the test process exits
fn start_udp_echo_server() -> SocketAddr {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP socket");
    let addr = socket.local_addr().unwrap();
    thread::spawn(move || {
        let mut buf = vec![0u8; 65535];
        while let Ok((n, src)) = socket.recv_from(&mut buf) {
            socket.send_to(&buf[..n], src).ok();
        }
    });
    addr
}

/// Unix socket echo server serving one connection
#[cfg(unix)]
fn start_unix_echo_server(path: std::path::PathBuf) {
    let listener = UnixListener::bind(&path).expect("Failed to bind Unix socket");
    thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            use std::io::{Read, Write};
            let mut buf = vec![0u8; 8192];
            while let Ok(n) = stream.read(&mut buf) {
                if n == 0 || stream.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
        }
    });
}

/// Wait for `conn_id` to become readable and read one chunk
fn recv_one<G: ConnectionGroup>(group: &mut G, conn_id: usize) -> (Vec<u8>, RecvOutcome) {
    let mut ready = Vec::new();
    let mut buf = vec![0u8; 65536];
    // Other connections may stay readable, so bound the wait by time, not polls
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        group.poll(Duration::from_millis(10), &mut ready).unwrap();
        if ready.contains(&conn_id) {
            let outcome = group.recv(conn_id, &mut buf).unwrap();
            if let RecvOutcome::Data { len, .. } = outcome {
                return (buf[..len].to_vec(), outcome);
            }
        }
    }
    panic!("connection {conn_id} never became readable");
}

#[test]
fn test_udp_transport_basic() {
    let addr = start_udp_echo_server();
    let mut group = UdpTransportFactory::default().create_group().unwrap();
    let conn_id = group.add_connection(&addr).expect("Failed to connect");

    let receipt = group.send(conn_id, b"Hello UDP").expect("Send failed");
    let (data, outcome) = recv_one(&mut group, conn_id);
    assert_eq!(data, b"Hello UDP");
    let RecvOutcome::Data { timestamp, .. } = outcome else {
        unreachable!()
    };
    assert!(timestamp.cycles() >= receipt.timestamp.cycles());

    group.close_all().unwrap();
    assert!(group.is_empty());
}

#[test]
fn test_udp_transport_burst_echo() {
    // Send a burst before reading: every datagram comes back, one per recv
    let addr = start_udp_echo_server();
    let mut group = UdpTransportFactory::default().create_group().unwrap();
    let conn_id = group.add_connection(&addr).unwrap();

    for i in 0..10 {
        group.send(conn_id, format!("Packet {i}").as_bytes()).expect("Send failed");
    }
    let mut received = Vec::new();
    for _ in 0..10 {
        received.push(String::from_utf8(recv_one(&mut group, conn_id).0).unwrap());
    }
    received.sort();
    let mut expected: Vec<_> = (0..10).map(|i| format!("Packet {i}")).collect();
    expected.sort();
    assert_eq!(received, expected);
}

#[test]
#[cfg(unix)]
fn test_unix_socket_transport_echo() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let socket_path = temp_dir.path().join("echo.sock");
    start_unix_echo_server(socket_path.clone());

    let mut group = UnixTransportFactory::default().create_group().unwrap();
    let conn_id = group.add_connection_path(&socket_path).expect("Failed to connect");

    for i in 0..5 {
        let test_data = format!("Unix test {i}");
        let receipt = group.send(conn_id, test_data.as_bytes()).expect("Failed to send");
        let (data, outcome) = recv_one(&mut group, conn_id);
        assert_eq!(data, test_data.as_bytes(), "Echo mismatch");
        let RecvOutcome::Data { timestamp, .. } = outcome else {
            unreachable!()
        };
        assert!(timestamp.cycles() >= receipt.timestamp.cycles(), "Invalid timestamp order");
    }
}

#[test]
#[cfg(unix)]
fn test_unix_socket_multiple_connections() {
    // One group, one connection to each of three servers
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mut group = UnixTransportFactory::default().create_group().unwrap();
    let mut conn_ids = Vec::new();
    for i in 0..3 {
        let socket_path = temp_dir.path().join(format!("test{i}.sock"));
        start_unix_echo_server(socket_path.clone());
        conn_ids.push(group.add_connection_path(&socket_path).expect("Failed to connect"));
    }
    assert_eq!(group.len(), 3);

    for (i, &conn_id) in conn_ids.iter().enumerate() {
        group.send(conn_id, format!("Hello {i}").as_bytes()).expect("Send failed");
    }
    for (i, &conn_id) in conn_ids.iter().enumerate() {
        assert_eq!(recv_one(&mut group, conn_id).0, format!("Hello {i}").as_bytes());
    }
}
