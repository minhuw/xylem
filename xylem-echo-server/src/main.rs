//! Xylem Echo Server
//!
//! A test server implementing the Xylem Echo Protocol for validating
//! latency measurement infrastructure.
//!
//! Protocol:
//! - Request: [request_id: u64][delay_us: u64] (16 bytes)
//! - Response: echoes the request after waiting delay_us microseconds
//!
//! Listens on TCP and UDP on the same address and port, or on a Unix
//! domain socket. Over UDP each datagram is one request, answered with one
//! datagram to its sender.

use anyhow::{Context, Result};
use clap::Parser;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket, UnixListener, UnixStream};
use tokio::time::sleep;

const MESSAGE_SIZE: usize = 16;

#[derive(Parser, Debug)]
#[command(name = "xylem-echo-server")]
#[command(about = "Xylem Echo Protocol server for latency measurement validation")]
struct Args {
    /// Port to listen on, for TCP and UDP
    #[arg(short, long, default_value = "9999")]
    port: u16,

    /// Bind address, for TCP and UDP
    #[arg(short, long, default_value = "0.0.0.0")]
    bind: String,

    /// Unix socket path (enables Unix socket mode instead of TCP)
    #[arg(short, long)]
    unix: Option<PathBuf>,

    /// Number of worker threads (defaults to number of CPU cores)
    #[arg(short, long)]
    threads: Option<usize>,

    /// Maximum delay allowed (microseconds) - requests exceeding this are capped
    #[arg(long, default_value = "10000000")]
    max_delay_us: u64,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

async fn handle_connection<S>(
    mut socket: S,
    client_id: &str,
    max_delay_us: u64,
    verbose: bool,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if verbose {
        println!("New connection from: {client_id}");
    }

    let mut buffer = vec![0u8; MESSAGE_SIZE];
    let mut request_count = 0u64;

    loop {
        match socket.read_exact(&mut buffer).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                if verbose {
                    println!("Client {client_id} disconnected after {request_count} requests");
                }
                break;
            }
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                if verbose {
                    println!("Client {client_id} reset connection after {request_count} requests");
                }
                break;
            }
            Err(e) => {
                if verbose {
                    eprintln!("Error reading from {client_id}: {e}");
                }
                break;
            }
        }

        let delay = request_delay(&buffer, max_delay_us);
        if !delay.is_zero() {
            sleep(delay).await;
        }

        if let Err(e) = socket.write_all(&buffer).await {
            if verbose {
                eprintln!("Error writing to {client_id}: {e}");
            }
            break;
        }

        request_count += 1;
    }

    Ok(())
}

async fn handle_tcp_client(socket: TcpStream, max_delay_us: u64, verbose: bool) -> Result<()> {
    let peer_addr = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    handle_connection(socket, &peer_addr, max_delay_us, verbose).await
}

async fn handle_unix_client(socket: UnixStream, max_delay_us: u64, verbose: bool) -> Result<()> {
    let peer_addr = socket
        .peer_addr()
        .ok()
        .and_then(|a| a.as_pathname().map(|p| p.display().to_string()))
        .unwrap_or_else(|| "unix-client".to_string());
    handle_connection(socket, &peer_addr, max_delay_us, verbose).await
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(threads) = args.threads {
        builder.worker_threads(threads);
    }
    builder.enable_all();

    let runtime = builder.build()?;
    runtime.block_on(run_server(args))
}

/// Delay requested by a 16-byte request, capped at `max_delay_us`
fn request_delay(request: &[u8], max_delay_us: u64) -> Duration {
    let delay_us = u64::from_le_bytes(request[8..MESSAGE_SIZE].try_into().expect("8 bytes"));
    Duration::from_micros(delay_us.min(max_delay_us))
}

/// Answer each 16-byte datagram on `socket` with the same datagram after
/// its delay. Datagrams of any other size are not requests and are dropped.
/// Delayed replies run concurrently, so one request never holds up another.
async fn serve_udp(socket: UdpSocket, max_delay_us: u64, verbose: bool) -> Result<()> {
    let socket = Arc::new(socket);
    // Room for an oversized datagram, so it is seen whole and dropped
    let mut buffer = vec![0u8; 65536];
    loop {
        let (len, peer) = socket.recv_from(&mut buffer).await?;
        if len != MESSAGE_SIZE {
            if verbose {
                eprintln!("Dropping a {len}-byte UDP datagram from {peer}");
            }
            continue;
        }
        let request: [u8; MESSAGE_SIZE] = buffer[..MESSAGE_SIZE].try_into().expect("16 bytes");
        let delay = request_delay(&request, max_delay_us);
        if delay.is_zero() {
            send_udp_reply(&socket, &request, peer, verbose).await;
        } else {
            let socket = Arc::clone(&socket);
            tokio::spawn(async move {
                sleep(delay).await;
                send_udp_reply(&socket, &request, peer, verbose).await;
            });
        }
    }
}

async fn send_udp_reply(socket: &UdpSocket, reply: &[u8], peer: SocketAddr, verbose: bool) {
    if let Err(e) = socket.send_to(reply, peer).await {
        if verbose {
            eprintln!("Error replying to {peer} over UDP: {e}");
        }
    }
}

/// Binding itself atomically refuses any existing filesystem entry, including
/// dangling symlinks and stale sockets. Never unlink a path supplied by a caller.
fn bind_unix_listener(path: &Path) -> Result<UnixListener> {
    UnixListener::bind(path).with_context(|| {
        format!(
            "Failed to bind Unix socket at {}. If the path exists, stop its owner and remove it manually only after confirming it is a stale socket, or choose another --unix path",
            path.display()
        )
    })
}

async fn run_server(args: Args) -> Result<()> {
    let num_threads = args
        .threads
        .unwrap_or_else(|| std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1));

    if let Some(ref unix_path) = args.unix {
        let listener = bind_unix_listener(unix_path)?;

        println!("Xylem Echo Server listening on {}", unix_path.display());
        println!("Worker threads: {num_threads}");
        println!("Protocol: [request_id: u64][delay_us: u64] (16 bytes)");
        println!("Max delay: {}us", args.max_delay_us);

        if args.verbose {
            println!("Verbose logging enabled");
        }

        loop {
            let (socket, _addr) = listener.accept().await?;

            let max_delay_us = args.max_delay_us;
            let verbose = args.verbose;

            tokio::spawn(async move {
                let _ = handle_unix_client(socket, max_delay_us, verbose).await;
            });
        }
    } else {
        // TCP and UDP on the same address and port
        let addr = format!("{}:{}", args.bind, args.port);
        let listener = TcpListener::bind(&addr).await?;
        let udp = UdpSocket::bind(&addr)
            .await
            .with_context(|| format!("Failed to bind UDP socket at {addr}"))?;
        let (max_delay_us, verbose) = (args.max_delay_us, args.verbose);
        tokio::spawn(async move {
            if let Err(e) = serve_udp(udp, max_delay_us, verbose).await {
                eprintln!("UDP listener stopped: {e}");
            }
        });

        println!("Xylem Echo Server listening on {addr} (TCP and UDP)");
        println!("Worker threads: {num_threads}");
        println!("Protocol: [request_id: u64][delay_us: u64] (16 bytes)");
        println!("Max delay: {}us", args.max_delay_us);

        if args.verbose {
            println!("Verbose logging enabled");
        }

        loop {
            let (socket, _addr) = listener.accept().await?;

            let max_delay_us = args.max_delay_us;
            let verbose = args.verbose;

            tokio::spawn(async move {
                let _ = handle_tcp_client(socket, max_delay_us, verbose).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, MetadataExt};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "xylem-echo-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn unix_bind_preserves_existing_paths() {
        let dir = TestDir::new();
        let file = dir.0.join("regular");
        std::fs::write(&file, "keep me").unwrap();
        let link = dir.0.join("link");
        symlink(&file, &link).unwrap();
        let dangling = dir.0.join("dangling");
        symlink(dir.0.join("missing"), &dangling).unwrap();
        let live = dir.0.join("live");
        let listener = bind_unix_listener(&live).unwrap();
        let stale = dir.0.join("stale");
        drop(bind_unix_listener(&stale).unwrap());

        for path in [&file, &link, &dangling, &live, &stale] {
            let before = std::fs::symlink_metadata(path).unwrap();
            let error = bind_unix_listener(path).unwrap_err();
            assert!(error.to_string().contains("choose another --unix path"));
            let after = std::fs::symlink_metadata(path).unwrap();
            assert_eq!(before.ino(), after.ino());
            assert_eq!(before.file_type(), after.file_type());
        }
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "keep me");
        assert_eq!(std::fs::read_link(&link).unwrap(), file);
        assert_eq!(std::fs::read_link(&dangling).unwrap(), dir.0.join("missing"));
        // Refusal must leave the original listener reachable.
        let client = UnixStream::connect(&live).await.unwrap();
        let (_accepted, _) = listener.accept().await.unwrap();
        drop(client);
    }

    #[tokio::test]
    async fn udp_echoes_each_request_datagram_after_its_delay() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        tokio::spawn(serve_udp(server, 1_000_000, false));
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let request = |id: u64, delay_us: u64| [id.to_le_bytes(), delay_us.to_le_bytes()].concat();
        client.connect(addr).await.unwrap();

        // A delayed request does not hold up a later one
        let start = tokio::time::Instant::now();
        client.send(&request(1, 50_000)).await.unwrap();
        client.send(&request(2, 0)).await.unwrap();
        assert_eq!(recv_reply(&client).await, request(2, 0));
        assert_eq!(recv_reply(&client).await, request(1, 50_000));
        assert!(start.elapsed() >= Duration::from_millis(50));

        // A datagram of another size is no request; the next one is answered
        client.send(&[0u8; 17]).await.unwrap();
        client.send(&request(3, 0)).await.unwrap();
        assert_eq!(recv_reply(&client).await, request(3, 0));
    }

    async fn recv_reply(client: &UdpSocket) -> Vec<u8> {
        let mut buf = [0u8; 64];
        let len = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
            .await
            .expect("reply")
            .unwrap();
        buf[..len].to_vec()
    }

    #[test]
    fn delays_are_capped() {
        let request = [7u64.to_le_bytes(), 5_000u64.to_le_bytes()].concat();
        assert_eq!(request_delay(&request, 10_000), Duration::from_millis(5));
        assert_eq!(request_delay(&request, 1_000), Duration::from_millis(1));
    }

    #[tokio::test]
    async fn unix_bind_accepts_absent_path() {
        let dir = TestDir::new();
        let path = dir.0.join("new");
        let listener = bind_unix_listener(&path).unwrap();
        let _client = UnixStream::connect(&path).await.unwrap();
        assert!(listener.accept().await.is_ok());
    }
}
