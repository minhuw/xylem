//! Configuration types for Xylem core

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

/// Transport type
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportType {
    Tcp,
    Udp,
    Tls,
}

/// Worker thread configuration
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub thread_id: usize,
    pub cpu_affinity: Option<usize>,
    pub target: SocketAddr,
    pub transport_type: TransportType,
    pub connections_per_thread: usize,
}

/// Load pattern for workload generation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LoadPattern {
    Fixed {
        rate: f64,
    },
    Step {
        start: f64,
        end: f64,
        step: f64,
        #[serde(with = "humantime_serde")]
        duration: Duration,
    },
    Adaptive {
        target_rate: f64,
    },
}

/// Inter-arrival time distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum InterArrival {
    Fixed {
        #[serde(with = "humantime_serde")]
        interval: Duration,
    },
    Exponential {
        lambda: f64,
    },
    Bimodal {
        p: f64,
        #[serde(with = "humantime_serde")]
        d1: Duration,
        #[serde(with = "humantime_serde")]
        d2: Duration,
    },
    Uniform {
        #[serde(with = "humantime_serde")]
        min: Duration,
        #[serde(with = "humantime_serde")]
        max: Duration,
    },
}

/// Key generation strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum KeyGeneration {
    Sequential { start: u64 },
    Random,
    Zipfian { theta: f64 },
    RoundRobin,
}
