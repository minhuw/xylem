//! Cleartext HTTP/2 test fixtures: nginx with h2c, and a gRPC server
//!
//! Each guard starts its own compose project and tears it down on drop; the
//! server is reached at its container's address. It also listens on a Unix socket in a
//! temporary directory of the guard's, mounted at `/sock`.

use super::compose::{answers_at, ComposeSpec, ComposeStack, ImageBuild};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// The client connection preface and an empty SETTINGS frame
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00";

/// The type, flags and stream of a SETTINGS frame without ACK, with which an
/// HTTP/2 server answers the preface
const SETTINGS: &[u8] = b"\x04\x00\x00\x00\x00\x00";

/// Guard for an HTTP/2 server in a compose project of its own, torn down on drop
pub struct H2cGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    /// Holds the Unix socket; removed after the project
    _sockets: tempfile::TempDir,
    socket: PathBuf,
    addr: SocketAddr,
}

impl H2cGuard {
    /// nginx answering "OK" at /, 404 at /missing and 300 KB at /big
    pub fn nginx() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start(
            &ComposeSpec {
                name: "nginx (h2c)",
                compose_file: "tests/http2/docker-compose.test.yml",
                image: None,
                is_ready: |stack| Self::ready(stack, "nginx", 80, "nginx.sock"),
            },
            "nginx",
            80,
            "nginx.sock",
        )
    }

    /// The gRPC server of `tests/grpc/server.py`: methods Echo, Big and Fail
    /// of service xylem.Test
    pub fn grpc() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start(
            &ComposeSpec {
                name: "gRPC server",
                compose_file: "tests/grpc/docker-compose.test.yml",
                image: Some(ImageBuild {
                    tag: "xylem-test-grpc:latest",
                    context: "tests/grpc",
                }),
                is_ready: |stack| Self::ready(stack, "grpc", 50051, "grpc.sock"),
            },
            "grpc",
            50051,
            "grpc.sock",
        )
    }

    fn start(
        spec: &ComposeSpec,
        service: &str,
        port: u16,
        socket: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let sockets = tempfile::tempdir()?;
        let env = vec![("XYLEM_SOCKET_DIR".to_string(), sockets.path().display().to_string())];
        let stack = ComposeStack::start_with_env(spec, env)?;
        let addr = stack
            .container_addr(service, port)
            .ok_or_else(|| format!("{} has no container address", spec.name))?;
        let socket = sockets.path().join(socket);
        Ok(Self {
            _stack: stack,
            _sockets: sockets,
            socket,
            addr,
        })
    }

    /// Whether the server answers the preface over TCP and made its socket
    fn ready(stack: &ComposeStack, service: &str, port: u16, socket: &str) -> bool {
        let Some(dir) = stack.env("XYLEM_SOCKET_DIR") else {
            return false;
        };
        Path::new(dir).join(socket).exists()
            && stack
                .container_addr(service, port)
                .is_some_and(|addr| answers_at(addr, PREFACE, SETTINGS))
    }

    /// Address of the TCP listener
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Path of the server's Unix socket
    pub fn socket(&self) -> &Path {
        &self.socket
    }
}
