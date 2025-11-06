//! TCP transport implementation

use super::Transport;
use crate::{request::Timestamp, Error, Result};
use async_trait::async_trait;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct TcpTransport {
    stream: Option<TcpStream>,
    recv_buffer: Vec<u8>,
}

impl TcpTransport {
    pub fn new() -> Self {
        Self {
            stream: None,
            recv_buffer: vec![0u8; 8192], // 8KB buffer
        }
    }
}

impl Default for TcpTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(&mut self, target: &SocketAddr) -> Result<()> {
        let stream = TcpStream::connect(target).await?;
        stream.set_nodelay(true)?;
        self.stream = Some(stream);
        Ok(())
    }

    async fn send(&mut self, data: &[u8]) -> Result<Timestamp> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        // Write all data
        stream.write_all(data).await?;

        // Capture timestamp immediately after write
        let timestamp = Timestamp::now();

        Ok(timestamp)
    }

    async fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        // Read data into buffer
        let n = stream.read(&mut self.recv_buffer).await?;

        // Capture timestamp immediately after read
        let timestamp = Timestamp::now();

        if n == 0 {
            return Err(Error::Connection("Connection closed by peer".to_string()));
        }

        // Copy data from buffer
        let data = self.recv_buffer[..n].to_vec();

        Ok((data, timestamp))
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            stream.shutdown().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_tcp_connect() {
        // Start a test server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn server task
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // Echo server
            let mut buf = vec![0u8; 1024];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                #[allow(clippy::excessive_nesting)]
                if n == 0 {
                    break;
                }
                socket.write_all(&buf[..n]).await.unwrap();
            }
        });

        // Test client
        let mut transport = TcpTransport::new();
        assert!(transport.connect(&addr).await.is_ok());
    }

    #[tokio::test]
    async fn test_tcp_send_recv() {
        // Start echo server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                #[allow(clippy::excessive_nesting)]
                if n == 0 {
                    break;
                }
                socket.write_all(&buf[..n]).await.unwrap();
            }
        });

        // Test client
        let mut transport = TcpTransport::new();
        transport.connect(&addr).await.unwrap();

        let test_data = b"Hello, World!";
        let send_ts = transport.send(test_data).await.unwrap();

        let (recv_data, recv_ts) = transport.recv().await.unwrap();

        assert_eq!(recv_data, test_data);
        assert!(recv_ts.instant >= send_ts.instant);
    }

    #[tokio::test]
    async fn test_tcp_send_without_connect() {
        let mut transport = TcpTransport::new();
        let result = transport.send(b"test").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tcp_recv_without_connect() {
        let mut transport = TcpTransport::new();
        let result = transport.recv().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tcp_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let mut transport = TcpTransport::new();
        transport.connect(&addr).await.unwrap();
        assert!(transport.close().await.is_ok());
        assert!(transport.stream.is_none());
    }
}
