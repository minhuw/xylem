//! Traffic group abstraction for flexible load generation
//!
//! Traffic groups enable mimicking Lancet's throughput/latency agent separation
//! with much more flexibility. Each group can have:
//! - Its own connection pool
//! - Independent traffic policy
//! - Configurable sampling policy for latency measurement
//! - Explicit thread affinity
//! - Per-group protocol configuration (keys, operations, etc.)

use crate::scheduler::PolicyScheduler;
use crate::stats::SamplingPolicy;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Traffic group configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TrafficGroupConfig {
    /// Group name for identification
    pub name: String,
    /// Protocol for this group (redis, redis-cluster, memcached-binary, memcached-ascii, http, xylem-echo)
    pub protocol: String,
    /// Target address for this group (e.g., "127.0.0.1:6379")
    pub target: String,
    /// Thread IDs this group runs on
    pub threads: Vec<usize>,
    /// Number of connections per thread
    pub connections_per_thread: usize,
    /// Maximum pending requests per connection
    #[serde(default = "default_max_pending")]
    pub max_pending_per_connection: usize,
    /// Traffic policy configuration (rate control)
    pub traffic_policy: PolicyConfig,
    /// Latency sampling policy
    #[serde(default)]
    pub sampling_policy: SamplingPolicy,
    /// Protocol-specific configuration as a JSON value.
    /// Each protocol factory interprets this according to its own schema.
    /// For Redis/Memcached: contains keys configuration (strategy, max, value_size, etc.)
    /// For HTTP: contains path, host, method configuration
    /// For custom protocols: defined by the protocol factory
    #[serde(default)]
    pub protocol_config: Option<serde_json::Value>,
}

fn default_max_pending() -> usize {
    10
}

/// Policy configuration for traffic groups
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum PolicyConfig {
    /// Closed-loop policy (max throughput)
    ClosedLoop,
    /// Fixed-rate policy
    FixedRate {
        /// Rate in requests per second
        rate: f64,
    },
    /// Poisson policy
    Poisson {
        /// Average rate in requests per second
        rate: f64,
    },
    /// Adaptive policy
    Adaptive {
        /// Initial rate in requests per second
        initial_rate: f64,
        /// Target latency in microseconds
        target_latency_us: u64,
        /// Minimum rate (fallback)
        #[serde(default = "default_min_rate")]
        min_rate: f64,
        /// Maximum rate (cap)
        #[serde(default = "default_max_rate")]
        max_rate: f64,
    },
    /// Sinusoidal pattern - varies rate following a sine wave (e.g., diurnal patterns)
    Sinusoidal {
        /// Base rate (center of oscillation) in requests per second
        base_rate: f64,
        /// Amplitude - how much rate varies above/below base
        amplitude: f64,
        /// Period duration (e.g., "60s", "24h")
        #[serde(with = "humantime_serde")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        period: std::time::Duration,
        /// Optional phase shift
        #[serde(default, with = "humantime_serde")]
        #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
        phase_shift: Option<std::time::Duration>,
    },
    /// Ramp pattern - linearly increase/decrease rate over time
    Ramp {
        /// Starting rate in requests per second
        start_rate: f64,
        /// Ending rate in requests per second
        end_rate: f64,
        /// Ramp duration
        #[serde(with = "humantime_serde")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        duration: std::time::Duration,
    },
    /// Spike pattern - sudden burst of traffic
    Spike {
        /// Normal baseline rate in requests per second
        normal_rate: f64,
        /// Peak rate during spike
        spike_rate: f64,
        /// When the spike starts
        #[serde(with = "humantime_serde")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        spike_start: std::time::Duration,
        /// How long the spike lasts
        #[serde(with = "humantime_serde")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        spike_duration: std::time::Duration,
    },
    /// Sawtooth pattern - repeated ramps (ramp up, instant drop, repeat)
    Sawtooth {
        /// Minimum rate (start of ramp)
        min_rate: f64,
        /// Maximum rate (end of ramp)
        max_rate: f64,
        /// Period for one complete cycle
        #[serde(with = "humantime_serde")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        period: std::time::Duration,
    },
}

fn default_min_rate() -> f64 {
    100.0
}

fn default_max_rate() -> f64 {
    10_000_000.0
}

impl PolicyConfig {
    /// Create a policy scheduler from this configuration
    pub fn create_scheduler(&self) -> Result<Box<dyn PolicyScheduler>> {
        match self {
            PolicyConfig::ClosedLoop => {
                Ok(Box::new(crate::scheduler::UniformPolicyScheduler::closed_loop()))
            }
            PolicyConfig::FixedRate { rate } => {
                Ok(Box::new(crate::scheduler::UniformPolicyScheduler::fixed_rate(*rate)))
            }
            PolicyConfig::Poisson { rate } => {
                Ok(Box::new(crate::scheduler::UniformPolicyScheduler::poisson(*rate)?))
            }
            PolicyConfig::Adaptive {
                initial_rate,
                target_latency_us,
                min_rate,
                max_rate,
            } => {
                let target_latency = Duration::from_micros(*target_latency_us);
                Ok(Box::new(crate::scheduler::UniformPolicyScheduler::adaptive(
                    *initial_rate,
                    target_latency,
                    *min_rate,
                    *max_rate,
                )))
            }
            PolicyConfig::Sinusoidal {
                base_rate,
                amplitude,
                period,
                phase_shift,
            } => {
                let pattern =
                    crate::workload::SinusoidalPattern::new(*base_rate, *amplitude, *period);
                let pattern = if let Some(ps) = phase_shift {
                    pattern.with_phase_shift(*ps)
                } else {
                    pattern
                };
                Ok(Box::new(crate::scheduler::PatternPolicyScheduler::new(Box::new(pattern))))
            }
            PolicyConfig::Ramp { start_rate, end_rate, duration } => {
                let pattern = crate::workload::RampPattern::new(*start_rate, *end_rate, *duration);
                Ok(Box::new(crate::scheduler::PatternPolicyScheduler::new(Box::new(pattern))))
            }
            PolicyConfig::Spike {
                normal_rate,
                spike_rate,
                spike_start,
                spike_duration,
            } => {
                let pattern = crate::workload::SpikePattern::new(
                    *normal_rate,
                    *spike_rate,
                    *spike_start,
                    *spike_duration,
                );
                Ok(Box::new(crate::scheduler::PatternPolicyScheduler::new(Box::new(pattern))))
            }
            PolicyConfig::Sawtooth { min_rate, max_rate, period } => {
                let pattern = crate::workload::SawtoothPattern::new(*min_rate, *max_rate, *period);
                Ok(Box::new(crate::scheduler::PatternPolicyScheduler::new(Box::new(pattern))))
            }
        }
    }
}

/// Traffic group metadata
#[derive(Debug, Clone)]
pub struct TrafficGroup {
    /// Group ID (index)
    pub id: usize,
    /// Group name
    pub name: String,
    /// Protocol for this group
    pub protocol: String,
    /// Target address for this group
    pub target: String,
    /// Threads this group runs on
    pub threads: Vec<usize>,
    /// Connections per thread
    pub connections_per_thread: usize,
    /// Max pending per connection
    pub max_pending_per_connection: usize,
    /// Sampling policy
    pub sampling_policy: SamplingPolicy,
    /// Protocol-specific configuration
    pub protocol_config: Option<serde_json::Value>,
}

impl TrafficGroup {
    /// Create from config
    pub fn from_config(id: usize, config: &TrafficGroupConfig) -> Self {
        Self {
            id,
            name: config.name.clone(),
            protocol: config.protocol.clone(),
            target: config.target.clone(),
            threads: config.threads.clone(),
            connections_per_thread: config.connections_per_thread,
            max_pending_per_connection: config.max_pending_per_connection,
            sampling_policy: config.sampling_policy.clone(),
            protocol_config: config.protocol_config.clone(),
        }
    }

    /// Get total connection count for this group
    pub fn total_connections(&self) -> usize {
        self.threads.len() * self.connections_per_thread
    }
}

/// Mapping from thread ID to groups that run on that thread
#[derive(Debug, Default, Clone)]
pub struct ThreadGroupAssignment {
    /// Map: thread_id -> Vec<(group_id, group_metadata)>
    assignments: HashMap<usize, Vec<(usize, TrafficGroup)>>,
}

impl ThreadGroupAssignment {
    /// Create assignment from group configs
    pub fn from_configs(configs: &[TrafficGroupConfig]) -> Self {
        let mut assignments: HashMap<usize, Vec<(usize, TrafficGroup)>> = HashMap::new();

        for (group_id, config) in configs.iter().enumerate() {
            let group = TrafficGroup::from_config(group_id, config);

            for &thread_id in &config.threads {
                assignments.entry(thread_id).or_default().push((group_id, group.clone()));
            }
        }

        Self { assignments }
    }

    /// Get groups assigned to a thread
    pub fn get_groups_for_thread(&self, thread_id: usize) -> Option<&Vec<(usize, TrafficGroup)>> {
        self.assignments.get(&thread_id)
    }

    /// Get total number of threads needed
    pub fn max_thread_id(&self) -> Option<usize> {
        self.assignments.keys().max().copied()
    }

    /// Get all thread IDs that have groups
    pub fn thread_ids(&self) -> Vec<usize> {
        let mut ids: Vec<_> = self.assignments.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Validate thread IDs are non-empty
    ///
    /// Thread IDs represent CPU cores for pinning. They can be any non-negative
    /// values and don't need to be contiguous starting from 0.
    pub fn validate(&self) -> Result<()> {
        let thread_ids = self.thread_ids();

        if thread_ids.is_empty() {
            return Err(crate::Error::Config(
                "No traffic groups assigned to any threads".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traffic_group_from_config() {
        let config = TrafficGroupConfig {
            name: "test-group".to_string(),
            protocol: "redis".to_string(),
            target: "127.0.0.1:6379".to_string(),
            threads: vec![0, 1, 2],
            connections_per_thread: 10,
            max_pending_per_connection: 5,
            traffic_policy: PolicyConfig::ClosedLoop,
            sampling_policy: SamplingPolicy::default(),
            protocol_config: None,
        };

        let group = TrafficGroup::from_config(0, &config);
        assert_eq!(group.id, 0);
        assert_eq!(group.name, "test-group");
        assert_eq!(group.threads, vec![0, 1, 2]);
        assert_eq!(group.total_connections(), 30); // 3 threads * 10 conns
    }

    #[test]
    fn test_thread_group_assignment() {
        let configs = vec![
            TrafficGroupConfig {
                name: "latency".to_string(),
                protocol: "redis".to_string(),
                target: "127.0.0.1:6379".to_string(),
                threads: vec![0, 1],
                connections_per_thread: 5,
                max_pending_per_connection: 1,
                traffic_policy: PolicyConfig::Poisson { rate: 1000.0 },
                sampling_policy: SamplingPolicy::Unlimited,
                protocol_config: Some(serde_json::json!({
                    "keys": {"strategy": "zipfian", "n": 1000000, "theta": 0.99, "value_size": 64}
                })),
            },
            TrafficGroupConfig {
                name: "throughput".to_string(),
                protocol: "redis".to_string(),
                target: "127.0.0.1:6380".to_string(),
                threads: vec![2, 3, 4],
                connections_per_thread: 20,
                max_pending_per_connection: 32,
                traffic_policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::Limited { max_samples: 10_000, rate: 0.01 },
                protocol_config: Some(serde_json::json!({
                    "keys": {"strategy": "random", "max": 10000000, "value_size": 128}
                })),
            },
        ];

        let assignment = ThreadGroupAssignment::from_configs(&configs);

        // Thread 0 should have group 0
        let thread0_groups = assignment.get_groups_for_thread(0).unwrap();
        assert_eq!(thread0_groups.len(), 1);
        assert_eq!(thread0_groups[0].0, 0); // group_id

        // Thread 2 should have group 1
        let thread2_groups = assignment.get_groups_for_thread(2).unwrap();
        assert_eq!(thread2_groups.len(), 1);
        assert_eq!(thread2_groups[0].0, 1); // group_id

        assert_eq!(assignment.max_thread_id(), Some(4));
        assert_eq!(assignment.thread_ids(), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_validation_success() {
        let configs = vec![
            TrafficGroupConfig {
                name: "g1".to_string(),
                protocol: "redis".to_string(),
                target: "127.0.0.1:6379".to_string(),
                threads: vec![0, 1],
                connections_per_thread: 5,
                max_pending_per_connection: 1,
                traffic_policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::default(),
                protocol_config: None,
            },
            TrafficGroupConfig {
                name: "g2".to_string(),
                protocol: "redis".to_string(),
                target: "127.0.0.1:6380".to_string(),
                threads: vec![2],
                connections_per_thread: 10,
                max_pending_per_connection: 1,
                traffic_policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::default(),
                protocol_config: None,
            },
        ];

        let assignment = ThreadGroupAssignment::from_configs(&configs);
        assert!(assignment.validate().is_ok());
    }

    #[test]
    fn test_validation_allows_non_contiguous_threads() {
        // Non-contiguous thread IDs are now allowed for CPU pinning to specific cores
        let configs = vec![
            TrafficGroupConfig {
                name: "g1".to_string(),
                protocol: "redis".to_string(),
                target: "127.0.0.1:6379".to_string(),
                threads: vec![2, 3], // Pin to cores 2, 3
                connections_per_thread: 5,
                max_pending_per_connection: 1,
                traffic_policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::default(),
                protocol_config: None,
            },
            TrafficGroupConfig {
                name: "g2".to_string(),
                protocol: "redis".to_string(),
                target: "127.0.0.1:6380".to_string(),
                threads: vec![5], // Pin to core 5
                connections_per_thread: 10,
                max_pending_per_connection: 1,
                traffic_policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::default(),
                protocol_config: None,
            },
        ];

        let assignment = ThreadGroupAssignment::from_configs(&configs);
        // Should succeed - non-contiguous IDs allowed
        assert!(assignment.validate().is_ok());
        assert_eq!(assignment.thread_ids(), vec![2, 3, 5]);
    }
}
