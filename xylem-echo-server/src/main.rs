//! Xylem Echo Server
//!
//! A test server implementing the Xylem Echo Protocol for validating
//! latency measurement infrastructure.
//!
//! Protocol:
//! - Request: [request_id: u64][delay_us: u64] (16 bytes)
//! - Response: echoes the request after waiting delay_us microseconds
//!
//! Supports both TCP and Unix domain sockets.

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::time::sleep;

const MESSAGE_SIZE: usize = 16;

#[derive(Parser, Debug)]
#[command(name = "xylem-echo-server")]
#[command(about = "Xylem Echo Protocol server for latency measurement validation")]
struct Args {
    /// Port to listen on (for TCP mode)
    #[arg(short, long, default_value = "9999")]
    port: u16,

    /// Bind address (for TCP mode)
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

        let delay_us = u64::from_le_bytes([
            buffer[8], buffer[9], buffer[10], buffer[11], buffer[12], buffer[13], buffer[14],
            buffer[15],
        ]);

        let actual_delay_us = delay_us.min(max_delay_us);

        if actual_delay_us > 0 {
            sleep(Duration::from_micros(actual_delay_us)).await;
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

async fn run_server(args: Args) -> Result<()> {
    let num_threads = args
        .threads
        .unwrap_or_else(|| std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1));

    if let Some(ref unix_path) = args.unix {
        // Unix socket mode
        // Remove existing socket file if it exists
        if unix_path.exists() {
            std::fs::remove_file(unix_path)?;
        }

        let listener = UnixListener::bind(unix_path)?;

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
        // TCP mode
        let addr = format!("{}:{}", args.bind, args.port);
        let listener = TcpListener::bind(&addr).await?;

        println!("Xylem Echo Server listening on {addr}");
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
