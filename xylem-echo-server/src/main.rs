//! Xylem Echo Server
//!
//! A test server implementing the Xylem Echo Protocol for validating
//! latency measurement infrastructure.
//!
//! Protocol:
//! - Request: [request_id: u64][delay_us: u64] (16 bytes)
//! - Response: echoes the request after waiting delay_us microseconds

use anyhow::Result;
use clap::Parser;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

const MESSAGE_SIZE: usize = 16;

#[derive(Parser, Debug)]
#[command(name = "xylem-echo-server")]
#[command(about = "Xylem Echo Protocol server for latency measurement validation")]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "9999")]
    port: u16,

    /// Bind address
    #[arg(short, long, default_value = "0.0.0.0")]
    bind: String,

    /// Maximum delay allowed (microseconds) - requests exceeding this are capped
    #[arg(long, default_value = "10000000")]
    max_delay_us: u64,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

async fn handle_client(mut socket: TcpStream, max_delay_us: u64, verbose: bool) -> Result<()> {
    let peer_addr = socket.peer_addr()?;
    if verbose {
        println!("New connection from: {peer_addr}");
    }

    let mut buffer = vec![0u8; MESSAGE_SIZE];
    let mut request_count = 0u64;

    loop {
        // Read exactly MESSAGE_SIZE bytes
        let n = match socket.read_exact(&mut buffer).await {
            Ok(_) => MESSAGE_SIZE,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Client closed connection
                if verbose {
                    println!("Client {peer_addr} disconnected after {request_count} requests");
                }
                break;
            }
            Err(e) => {
                eprintln!("Error reading from {peer_addr}: {e}");
                break;
            }
        };

        if n == 0 {
            break;
        }

        // Parse the message
        let request_id = u64::from_le_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]);

        let delay_us = u64::from_le_bytes([
            buffer[8], buffer[9], buffer[10], buffer[11], buffer[12], buffer[13], buffer[14],
            buffer[15],
        ]);

        // Cap delay to max_delay_us
        let actual_delay_us = delay_us.min(max_delay_us);

        if verbose && delay_us != actual_delay_us {
            println!(
                "Request {request_id} from {peer_addr}: delay capped from {delay_us}us to {actual_delay_us}us"
            );
        }

        // Wait for the specified delay
        if actual_delay_us > 0 {
            sleep(Duration::from_micros(actual_delay_us)).await;
        }

        // Echo the message back
        if let Err(e) = socket.write_all(&buffer).await {
            eprintln!("Error writing to {peer_addr}: {e}");
            break;
        }

        request_count += 1;

        if verbose && request_count % 10000 == 0 {
            println!("Client {peer_addr}: {request_count} requests processed");
        }
    }

    if verbose {
        println!("Connection from {peer_addr} closed (total requests: {request_count})");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let addr = format!("{}:{}", args.bind, args.port);
    let listener = TcpListener::bind(&addr).await?;

    println!("Xylem Echo Server listening on {addr}");
    println!("Protocol: [request_id: u64][delay_us: u64] (16 bytes)");
    println!("Max delay: {}us", args.max_delay_us);

    if args.verbose {
        println!("Verbose logging enabled");
    }

    loop {
        let (socket, addr) = listener.accept().await?;

        if args.verbose {
            println!("Accepted connection from: {addr}");
        }

        let max_delay_us = args.max_delay_us;
        let verbose = args.verbose;

        // Spawn a task to handle this client
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, max_delay_us, verbose).await {
                eprintln!("Error handling client {addr}: {e}");
            }
        });
    }
}
