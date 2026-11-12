//! TF Serving test fixture
//!
//! The guard starts the official tensorflow/serving image in a compose
//! project of its own, serving a SavedModel base directory of the host as
//! model "ranking", and tears it down on drop.

use super::compose::{answers_at, ComposeSpec, ComposeStack};
use std::net::SocketAddr;

/// Guard for TF Serving, torn down on drop
pub struct TfsGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    addr: SocketAddr,
}

impl TfsGuard {
    /// Serve the model at `model` and wait until it is available
    pub fn new(model: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start_with_env(
            &ComposeSpec {
                name: "TF Serving",
                compose_file: "tests/tfs/docker-compose.test.yml",
                build_images: false,
                // The gRPC port opens before the model loads; REST reports it
                is_ready: |stack| {
                    stack.container_addr("tfs", 8501).is_some_and(|addr| {
                        answers_at(addr, b"GET /v1/models/ranking HTTP/1.0\r\n\r\n", b"AVAILABLE")
                    })
                },
            },
            vec![("XYLEM_TFS_MODEL".to_string(), model.to_string())],
        )?;
        let addr =
            stack.container_addr("tfs", 8500).ok_or("TF Serving has no container address")?;
        Ok(Self { _stack: stack, addr })
    }

    /// Address of the gRPC service
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
