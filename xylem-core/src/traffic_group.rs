//! Traffic group abstraction for flexible load generation
//!
//! Traffic groups enable mimicking Lancet's throughput/latency agent separation
//! with much more flexibility. Each group can have:
//! - Its own connection pool
//! - Independent traffic policy
//! - Configurable sampling policy for latency measurement
//! - Explicit thread affinity

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
    /// Thread IDs this group runs on
    pub threads: Vec<usize>,
    /// Number of connections per thread
    pub connections_per_thread: usize,
    /// Maximum pending requests per connection
    #[serde(default = "default_max_pending")]
    pub max_pending_per_connection: usize,
    /// Traffic policy configuration
    pub policy: PolicyConfig,
    /// Latency sampling policy
    #[serde(default)]
    pub sampling_policy: SamplingPolicy,
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
    /// Threads this group runs on
    pub threads: Vec<usize>,
    /// Connections per thread
    pub connections_per_thread: usize,
    /// Max pending per connection
    pub max_pending_per_connection: usize,
    /// Sampling policy
    pub sampling_policy: SamplingPolicy,
}

impl TrafficGroup {
    /// Create from config
    pub fn from_config(id: usize, config: &TrafficGroupConfig) -> Self {
        Self {
            id,
            name: config.name.clone(),
            threads: config.threads.clone(),
            connections_per_thread: config.connections_per_thread,
            max_pending_per_connection: config.max_pending_per_connection,
            sampling_policy: config.sampling_policy.clone(),
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

    /// Validate that thread IDs are contiguous starting from 0
    pub fn validate(&self) -> Result<()> {
        let thread_ids = self.thread_ids();

        if thread_ids.is_empty() {
            return Err(crate::Error::Config(
                "No traffic groups assigned to any threads".to_string(),
            ));
        }

        // Check if threads are contiguous starting from 0
        for (i, &thread_id) in thread_ids.iter().enumerate() {
            if thread_id != i {
                return Err(crate::Error::Config(format!(
                    "Thread IDs must be contiguous starting from 0, found gap at {}",
                    i
                )));
            }
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
            threads: vec![0, 1, 2],
            connections_per_thread: 10,
            max_pending_per_connection: 5,
            policy: PolicyConfig::ClosedLoop,
            sampling_policy: SamplingPolicy::default(),
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
                threads: vec![0, 1],
                connections_per_thread: 5,
                max_pending_per_connection: 1,
                policy: PolicyConfig::Poisson { rate: 1000.0 },
                sampling_policy: SamplingPolicy::Unlimited,
            },
            TrafficGroupConfig {
                name: "throughput".to_string(),
                threads: vec![2, 3, 4],
                connections_per_thread: 20,
                max_pending_per_connection: 32,
                policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::Limited { max_samples: 10_000, rate: 0.01 },
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
                threads: vec![0, 1],
                connections_per_thread: 5,
                max_pending_per_connection: 1,
                policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::default(),
            },
            TrafficGroupConfig {
                name: "g2".to_string(),
                threads: vec![2],
                connections_per_thread: 10,
                max_pending_per_connection: 1,
                policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::default(),
            },
        ];

        let assignment = ThreadGroupAssignment::from_configs(&configs);
        assert!(assignment.validate().is_ok());
    }

    #[test]
    fn test_validation_gap_in_threads() {
        let configs = vec![
            TrafficGroupConfig {
                name: "g1".to_string(),
                threads: vec![0, 1],
                connections_per_thread: 5,
                max_pending_per_connection: 1,
                policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::default(),
            },
            TrafficGroupConfig {
                name: "g2".to_string(),
                threads: vec![3], // Gap! Missing thread 2
                connections_per_thread: 10,
                max_pending_per_connection: 1,
                policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::default(),
            },
        ];

        let assignment = ThreadGroupAssignment::from_configs(&configs);
        assert!(assignment.validate().is_err());
    }
}
