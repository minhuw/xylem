//! Traffic group abstraction for flexible load generation
//!
//! Traffic groups enable mimicking Lancet's throughput/latency agent separation
//! with much more flexibility. Each group can have:
//! - Its own connection pool
//! - Independent traffic policy
//! - Configurable sampling policy for latency measurement
//! - Explicit thread affinity
//! - Per-group protocol configuration (keys, operations, etc.)

use crate::scheduler::{
    AdaptivePolicy, FixedRatePolicy, GroupDispatchScheduler, GroupPolicyScheduler,
    PatternAwarePolicy, PoissonPolicy, Policy, PolicyScheduler, UniformPolicyScheduler,
};
use crate::stats::SamplingPolicy;
use crate::workload::LoadPattern;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Traffic group configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TrafficGroupConfig {
    /// Group name for identification
    pub name: String,
    /// Protocol for this group (redis, memcached-binary, memcached-ascii, http, http2, grpc, masstree, dns, nvme-tcp, clickhouse, xylem-echo)
    pub protocol: String,
    /// Target address for this group (e.g., "127.0.0.1:6379" or "/var/run/redis.sock")
    pub target: String,
    /// Transport layer: tcp, udp, unix (defaults to "tcp")
    #[serde(default = "default_transport")]
    pub transport: String,
    /// Thread IDs this group runs on
    pub threads: Vec<usize>,
    /// Number of connections per thread
    pub connections_per_thread: usize,
    /// Maximum pending requests per connection
    #[serde(default = "default_max_pending")]
    pub max_pending_per_connection: usize,
    /// How long a request may wait for its response before it times out,
    /// frees its pipeline slot, and counts as timed out without a latency
    /// (e.g., "500ms"). Only for transport "udp", where it defaults to 1s;
    /// stream transports retransmit and never time out requests.
    #[serde(default, with = "humantime_serde", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    pub request_timeout: Option<Duration>,
    /// What an error reply (a Redis error, an HTTP 4xx or 5xx, ...) does:
    /// "count" it in `requests.errors` (default), or "abort" the run
    #[serde(default)]
    pub on_error: OnError,
    /// Source of the send and receive timestamps latencies are measured
    /// with: "user" (default), "kernel", or { hardware = "<interface>" }.
    /// Only for transports "tcp" and "udp" on Linux; traffic groups that
    /// share a thread must use the same source.
    #[serde(default, skip_serializing_if = "TimestampSourceConfig::is_user")]
    pub timestamps: TimestampSourceConfig,
    /// Traffic policy configuration (rate control)
    pub traffic_policy: PolicyConfig,
    /// Latency sampling policy
    #[serde(default)]
    pub sampling_policy: SamplingPolicy,
    /// Protocol-specific configuration as a JSON value.
    /// Each protocol factory interprets this according to its own schema.
    /// The core is protocol-agnostic - this value is passed directly to the protocol factory.
    #[serde(default)]
    pub protocol_config: Option<serde_json::Value>,
}

fn default_transport() -> String {
    "tcp".to_string()
}

/// What an error reply does to a run
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum OnError {
    /// Complete the request and count the reply in `requests.errors` and
    /// `requests.errors_by_kind`, with its latency in `error_latency`
    #[default]
    Count,
    /// Fail the run, naming the group, connection and reply
    Abort,
}

/// Source of a traffic group's send and receive timestamps
///
/// Every timestamp holds the user-space cycles read when its send or
/// receive call completed. Kernel and NIC timestamps come in addition, and
/// a latency uses them when its send and its receive both have one from the
/// same clock, which splits its round trip in the latency breakdown.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum TimestampSourceConfig {
    /// User-space cycle counts only (default)
    #[default]
    User,
    /// Kernel software timestamps (SO_TIMESTAMPING) of every send and receive
    Kernel,
    /// NIC hardware timestamps of this interface, which must already have
    /// hardware timestamping switched on: the run fails otherwise, and never
    /// switches it on or falls back to another source
    Hardware(String),
    /// The NIC timestamps of a user-space stack that owns the NIC (transport
    /// "ntcp" with backend "dpdk"): RX hardware timestamps converted to the
    /// user-space clock, and the time the stack queued each request to the
    /// NIC
    Nic,
}

impl TimestampSourceConfig {
    /// Whether this is the default, user-space timestamps
    pub fn is_user(&self) -> bool {
        *self == TimestampSourceConfig::User
    }

    /// Whether the group's kernel sockets take kernel timestamps
    /// (`SO_TIMESTAMPING`): `"kernel"` or `{ hardware = "<interface>" }`
    pub fn is_kernel(&self) -> bool {
        matches!(self, TimestampSourceConfig::Kernel | TimestampSourceConfig::Hardware(_))
    }
}

impl std::fmt::Display for TimestampSourceConfig {
    /// The source as written in a profile
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimestampSourceConfig::User => f.write_str("\"user\""),
            TimestampSourceConfig::Kernel => f.write_str("\"kernel\""),
            TimestampSourceConfig::Nic => f.write_str("\"nic\""),
            TimestampSourceConfig::Hardware(interface) => {
                write!(f, "{{ hardware = {interface:?} }}")
            }
        }
    }
}

/// Request timeout of a UDP traffic group that sets none
pub const DEFAULT_UDP_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

impl TrafficGroupConfig {
    /// The request timeout in effect: the configured one, else
    /// [`DEFAULT_UDP_REQUEST_TIMEOUT`] for transport "udp", else none
    pub fn effective_request_timeout(&self) -> Option<Duration> {
        self.request_timeout
            .or((self.transport == "udp").then_some(DEFAULT_UDP_REQUEST_TIMEOUT))
    }
}

fn default_max_pending() -> usize {
    10
}

/// Policy configuration for traffic groups
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum PolicyConfig {
    /// Closed-loop policy (max throughput)
    #[serde(deserialize_with = "crate::strict::unit_variant")]
    ClosedLoop,
    /// Fixed-rate policy
    FixedRate {
        /// Rate in requests per second
        rate: f64,
        /// How arrivals reach the connections: "connection" (default) or "group"
        #[serde(default, skip_serializing_if = "Dispatch::is_connection")]
        dispatch: Dispatch,
    },
    /// Poisson policy
    Poisson {
        /// Average rate in requests per second
        rate: f64,
        /// How arrivals reach the connections: "connection" (default) or "group"
        #[serde(default, skip_serializing_if = "Dispatch::is_connection")]
        dispatch: Dispatch,
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
        /// How arrivals reach the connections: "connection" (default) or "group"
        #[serde(default, skip_serializing_if = "Dispatch::is_connection")]
        dispatch: Dispatch,
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
        /// How arrivals reach the connections: "connection" (default) or "group"
        #[serde(default, skip_serializing_if = "Dispatch::is_connection")]
        dispatch: Dispatch,
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
        /// How arrivals reach the connections: "connection" (default) or "group"
        #[serde(default, skip_serializing_if = "Dispatch::is_connection")]
        dispatch: Dispatch,
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
        /// How arrivals reach the connections: "connection" (default) or "group"
        #[serde(default, skip_serializing_if = "Dispatch::is_connection")]
        dispatch: Dispatch,
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
        /// How arrivals reach the connections: "connection" (default) or "group"
        #[serde(default, skip_serializing_if = "Dispatch::is_connection")]
        dispatch: Dispatch,
    },
}

/// How a rate-driven traffic group's arrivals reach its connections
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum Dispatch {
    /// Every connection runs its own share of the group's arrivals and sends
    /// them itself; an arrival due while its connection's pipeline is full
    /// waits for that connection (default)
    #[default]
    Connection,
    /// Every thread runs one arrival process, its share of the group's rate,
    /// and sends each arrival on the connection idle longest; an arrival due
    /// while no connection is idle waits for the first to free up and counts
    /// as a late send
    Group,
}

impl Dispatch {
    /// Whether this is the default, per-connection dispatch
    pub fn is_connection(&self) -> bool {
        *self == Dispatch::Connection
    }
}

impl std::fmt::Display for Dispatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Dispatch::Connection => "connection",
            Dispatch::Group => "group",
        })
    }
}

fn default_min_rate() -> f64 {
    100.0
}

fn default_max_rate() -> f64 {
    10_000_000.0
}

impl PolicyConfig {
    /// How arrivals reach the connections; closed loop has no arrivals and
    /// dispatches per connection
    pub fn dispatch(&self) -> Dispatch {
        match self {
            PolicyConfig::ClosedLoop => Dispatch::Connection,
            PolicyConfig::FixedRate { dispatch, .. }
            | PolicyConfig::Poisson { dispatch, .. }
            | PolicyConfig::Adaptive { dispatch, .. }
            | PolicyConfig::Sinusoidal { dispatch, .. }
            | PolicyConfig::Ramp { dispatch, .. }
            | PolicyConfig::Spike { dispatch, .. }
            | PolicyConfig::Sawtooth { dispatch, .. } => *dispatch,
        }
    }

    /// Create the policy scheduler of one thread entry's connections of `group`
    ///
    /// The group's connections are numbered across its threads, this entry's
    /// from `group.first_connection`, and share the configured rate:
    /// - Fixed rate: connection `c` of `N` sends its `k`-th request at
    ///   `start + ⌊(c + k·N)·10⁹/rate⌋` ns, so the group's merged stream is
    ///   evenly spaced at `rate`.
    /// - Poisson: each connection is a Poisson process at `rate/N`. With a
    ///   `seed`, connection `c` of group `g` draws from
    ///   [`crate::seed::poisson_seed`]`(seed, g, c)`; without one, from OS entropy.
    /// - Adaptive and the patterns: each connection runs at `1/N` of the
    ///   configured rates and starts `c/N` of a request into its schedule.
    ///
    /// With group dispatch, the thread entries instead play the role of the
    /// connections: entry `t` of `T` (its position in the group's threads)
    /// runs one arrival process by the same rules with `t` of `T`, which its
    /// connections share (see [`crate::scheduler::dispatch`]). Its Poisson
    /// seed is [`crate::seed::poisson_dispatch_seed`]`(seed, g, t)`.
    pub fn create_scheduler(
        &self,
        group: &TrafficGroup,
        seed: Option<u64>,
    ) -> Result<Box<dyn PolicyScheduler>> {
        if self.dispatch() == Dispatch::Group {
            let threads = group.threads.len().max(1);
            let thread = group.first_connection / group.connections_per_thread.max(1);
            let policy = self.arrival_policy(group.id, thread, threads, seed)?;
            return Ok(Box::new(GroupDispatchScheduler::new(policy)));
        }
        let connections = group.total_connections().max(1);
        let num_connections = connections as f64;
        let first = group.first_connection;

        match self {
            PolicyConfig::ClosedLoop => Ok(Box::new(UniformPolicyScheduler::closed_loop())),
            PolicyConfig::FixedRate { rate, .. } => {
                let rate = *rate;
                Ok(Box::new(GroupPolicyScheduler::new(first, move |c| {
                    Box::new(FixedRatePolicy::striped(rate, c, connections))
                })))
            }
            PolicyConfig::Poisson { rate, .. } => {
                let per_conn_rate = *rate / num_connections;
                // Validate the rate once, not on every connection
                PoissonPolicy::with_seed(per_conn_rate, Some(0))?;
                let group_id = group.id;
                Ok(Box::new(GroupPolicyScheduler::new(first, move |c| {
                    let seed = seed.map(|seed| crate::seed::poisson_seed(seed, group_id, c));
                    Box::new(
                        PoissonPolicy::with_seed(per_conn_rate, seed)
                            .expect("rate already validated"),
                    )
                })))
            }
            PolicyConfig::Adaptive {
                initial_rate,
                target_latency_us,
                min_rate,
                max_rate,
                ..
            } => {
                let target_latency = Duration::from_micros(*target_latency_us);
                let per_conn_initial = *initial_rate / num_connections;
                let per_conn_min = *min_rate / num_connections;
                let per_conn_max = *max_rate / num_connections;
                Ok(Box::new(GroupPolicyScheduler::new(first, move |c| {
                    Box::new(
                        AdaptivePolicy::new(
                            per_conn_initial,
                            target_latency,
                            per_conn_min,
                            per_conn_max,
                        )
                        .with_phase(c, connections),
                    )
                })))
            }
            PolicyConfig::Sinusoidal {
                base_rate,
                amplitude,
                period,
                phase_shift,
                ..
            } => {
                let per_conn_base = *base_rate / num_connections;
                let per_conn_amplitude = *amplitude / num_connections;
                let pattern = crate::workload::SinusoidalPattern::new(
                    per_conn_base,
                    per_conn_amplitude,
                    *period,
                );
                let pattern = if let Some(ps) = phase_shift {
                    pattern.with_phase_shift(*ps)
                } else {
                    pattern
                };
                Ok(pattern_scheduler(Arc::new(pattern), first, connections))
            }
            PolicyConfig::Ramp { start_rate, end_rate, duration, .. } => {
                let per_conn_start = *start_rate / num_connections;
                let per_conn_end = *end_rate / num_connections;
                let pattern =
                    crate::workload::RampPattern::new(per_conn_start, per_conn_end, *duration);
                Ok(pattern_scheduler(Arc::new(pattern), first, connections))
            }
            PolicyConfig::Spike {
                normal_rate,
                spike_rate,
                spike_start,
                spike_duration,
                ..
            } => {
                let per_conn_normal = *normal_rate / num_connections;
                let per_conn_spike = *spike_rate / num_connections;
                let pattern = crate::workload::SpikePattern::new(
                    per_conn_normal,
                    per_conn_spike,
                    *spike_start,
                    *spike_duration,
                );
                Ok(pattern_scheduler(Arc::new(pattern), first, connections))
            }
            PolicyConfig::Sawtooth { min_rate, max_rate, period, .. } => {
                let per_conn_min = *min_rate / num_connections;
                let per_conn_max = *max_rate / num_connections;
                let pattern =
                    crate::workload::SawtoothPattern::new(per_conn_min, per_conn_max, *period);
                Ok(pattern_scheduler(Arc::new(pattern), first, connections))
            }
        }
    }

    /// The arrival process of thread entry `thread` of the `threads` of
    /// traffic group `group_id` under group dispatch: its share of the
    /// configured rates, by the rules a connection's share follows under
    /// per-connection dispatch
    fn arrival_policy(
        &self,
        group_id: usize,
        thread: usize,
        threads: usize,
        seed: Option<u64>,
    ) -> Result<Box<dyn Policy>> {
        let share = threads as f64;
        let pattern = |pattern: Arc<dyn LoadPattern>| -> Box<dyn Policy> {
            let start_ns = crate::timing::time_ns();
            Box::new(PatternAwarePolicy::new(pattern, start_ns).with_phase(thread, threads))
        };
        Ok(match self {
            PolicyConfig::ClosedLoop => {
                return Err(crate::Error::Config(
                    "closed-loop traffic has no arrivals to dispatch".to_string(),
                ))
            }
            PolicyConfig::FixedRate { rate, .. } => {
                Box::new(FixedRatePolicy::striped(*rate, thread, threads))
            }
            PolicyConfig::Poisson { rate, .. } => {
                let seed =
                    seed.map(|seed| crate::seed::poisson_dispatch_seed(seed, group_id, thread));
                Box::new(PoissonPolicy::with_seed(*rate / share, seed)?)
            }
            PolicyConfig::Adaptive {
                initial_rate,
                target_latency_us,
                min_rate,
                max_rate,
                ..
            } => Box::new(
                AdaptivePolicy::new(
                    *initial_rate / share,
                    Duration::from_micros(*target_latency_us),
                    *min_rate / share,
                    *max_rate / share,
                )
                .with_phase(thread, threads),
            ),
            PolicyConfig::Sinusoidal {
                base_rate,
                amplitude,
                period,
                phase_shift,
                ..
            } => {
                let sine = crate::workload::SinusoidalPattern::new(
                    *base_rate / share,
                    *amplitude / share,
                    *period,
                );
                pattern(Arc::new(match phase_shift {
                    Some(shift) => sine.with_phase_shift(*shift),
                    None => sine,
                }))
            }
            PolicyConfig::Ramp { start_rate, end_rate, duration, .. } => {
                pattern(Arc::new(crate::workload::RampPattern::new(
                    *start_rate / share,
                    *end_rate / share,
                    *duration,
                )))
            }
            PolicyConfig::Spike {
                normal_rate,
                spike_rate,
                spike_start,
                spike_duration,
                ..
            } => pattern(Arc::new(crate::workload::SpikePattern::new(
                *normal_rate / share,
                *spike_rate / share,
                *spike_start,
                *spike_duration,
            ))),
            PolicyConfig::Sawtooth { min_rate, max_rate, period, .. } => {
                pattern(Arc::new(crate::workload::SawtoothPattern::new(
                    *min_rate / share,
                    *max_rate / share,
                    *period,
                )))
            }
        })
    }
}

/// Pattern policies for connections `first, first + 1, ...` of `connections`.
/// The pattern starts when the worker anchors the schedules.
fn pattern_scheduler(
    pattern: Arc<dyn LoadPattern>,
    first: usize,
    connections: usize,
) -> Box<dyn PolicyScheduler> {
    let start_ns = crate::timing::time_ns();
    Box::new(GroupPolicyScheduler::new(first, move |c| {
        Box::new(PatternAwarePolicy::new(Arc::clone(&pattern), start_ns).with_phase(c, connections))
    }))
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
    /// Transport layer for this group
    pub transport: String,
    /// Threads this group runs on
    pub threads: Vec<usize>,
    /// Connections per thread
    pub connections_per_thread: usize,
    /// Group-wide number of this thread entry's first connection: its
    /// position in `threads` times `connections_per_thread`, so entries never
    /// overlap, even for a thread listed twice. Zero from [`Self::from_config`].
    pub first_connection: usize,
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
            transport: config.transport.clone(),
            threads: config.threads.clone(),
            connections_per_thread: config.connections_per_thread,
            first_connection: 0,
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
    ///
    /// Each entry's [`TrafficGroup::first_connection`] numbers its connections
    /// after those of the entries before it in the group's `threads`.
    pub fn from_configs(configs: &[TrafficGroupConfig]) -> Self {
        let mut assignments: HashMap<usize, Vec<(usize, TrafficGroup)>> = HashMap::new();

        for (group_id, config) in configs.iter().enumerate() {
            let group = TrafficGroup::from_config(group_id, config);

            for (position, &thread_id) in config.threads.iter().enumerate() {
                let mut entry = group.clone();
                entry.first_connection = position * config.connections_per_thread;
                assignments.entry(thread_id).or_default().push((group_id, entry));
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
            transport: "tcp".to_string(),
            threads: vec![0, 1, 2],
            connections_per_thread: 10,
            max_pending_per_connection: 5,
            request_timeout: None,
            on_error: OnError::Count,
            timestamps: TimestampSourceConfig::User,
            traffic_policy: PolicyConfig::ClosedLoop,
            sampling_policy: SamplingPolicy::default(),
            protocol_config: None,
        };

        let group = TrafficGroup::from_config(0, &config);
        assert_eq!(group.id, 0);
        assert_eq!(group.name, "test-group");
        assert_eq!(group.transport, "tcp");
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
                transport: "tcp".to_string(),
                threads: vec![0, 1],
                connections_per_thread: 5,
                max_pending_per_connection: 1,
                request_timeout: None,
                on_error: OnError::Count,
                timestamps: TimestampSourceConfig::User,
                traffic_policy: PolicyConfig::Poisson {
                    rate: 1000.0,
                    dispatch: Dispatch::Connection,
                },
                sampling_policy: SamplingPolicy::Unlimited,
                protocol_config: Some(serde_json::json!({
                    "keys": {"strategy": "zipfian", "n": 1000000, "theta": 0.99, "value_size": 64}
                })),
            },
            TrafficGroupConfig {
                name: "throughput".to_string(),
                protocol: "redis".to_string(),
                target: "127.0.0.1:6380".to_string(),
                transport: "tcp".to_string(),
                threads: vec![2, 3, 4],
                connections_per_thread: 20,
                max_pending_per_connection: 32,
                request_timeout: None,
                on_error: OnError::Count,
                timestamps: TimestampSourceConfig::User,
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
                transport: "tcp".to_string(),
                threads: vec![0, 1],
                connections_per_thread: 5,
                max_pending_per_connection: 1,
                request_timeout: None,
                on_error: OnError::Count,
                timestamps: TimestampSourceConfig::User,
                traffic_policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::default(),
                protocol_config: None,
            },
            TrafficGroupConfig {
                name: "g2".to_string(),
                protocol: "redis".to_string(),
                target: "127.0.0.1:6380".to_string(),
                transport: "tcp".to_string(),
                threads: vec![2],
                connections_per_thread: 10,
                max_pending_per_connection: 1,
                request_timeout: None,
                on_error: OnError::Count,
                timestamps: TimestampSourceConfig::User,
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
                transport: "tcp".to_string(),
                threads: vec![2, 3], // Pin to cores 2, 3
                connections_per_thread: 5,
                max_pending_per_connection: 1,
                request_timeout: None,
                on_error: OnError::Count,
                timestamps: TimestampSourceConfig::User,
                traffic_policy: PolicyConfig::ClosedLoop,
                sampling_policy: SamplingPolicy::default(),
                protocol_config: None,
            },
            TrafficGroupConfig {
                name: "g2".to_string(),
                protocol: "redis".to_string(),
                target: "127.0.0.1:6380".to_string(),
                transport: "tcp".to_string(),
                threads: vec![5], // Pin to core 5
                connections_per_thread: 10,
                max_pending_per_connection: 1,
                request_timeout: None,
                on_error: OnError::Count,
                timestamps: TimestampSourceConfig::User,
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

    fn group(
        name: &str,
        threads: Vec<usize>,
        connections: usize,
        policy: PolicyConfig,
    ) -> TrafficGroupConfig {
        TrafficGroupConfig {
            name: name.to_string(),
            protocol: "xylem-echo".to_string(),
            target: "127.0.0.1:9999".to_string(),
            transport: "tcp".to_string(),
            threads,
            connections_per_thread: connections,
            max_pending_per_connection: 1,
            request_timeout: None,
            on_error: OnError::Count,
            timestamps: TimestampSourceConfig::User,
            traffic_policy: policy,
            sampling_policy: SamplingPolicy::default(),
            protocol_config: None,
        }
    }

    /// A policy's first `sends` intended times when it is anchored at `start`
    /// and each request goes out on time
    fn intended_times(
        policy: &mut dyn crate::scheduler::Policy,
        start: u64,
        sends: usize,
    ) -> Vec<u64> {
        policy.anchor(start);
        policy.next_send_time(start - 1_000_000);
        (0..sends)
            .map(|_| {
                let intended = policy.intended_send_time().unwrap();
                policy.on_request_sent(intended);
                intended
            })
            .collect()
    }

    /// Every connection's first `sends` intended times, per group, built per
    /// thread entry the way the CLI builds them and anchored at `start`
    fn group_schedules(
        configs: &[TrafficGroupConfig],
        seed: Option<u64>,
        start: u64,
        sends: usize,
    ) -> HashMap<usize, Vec<Vec<u64>>> {
        let assignment = ThreadGroupAssignment::from_configs(configs);
        let mut schedules: HashMap<usize, Vec<Vec<u64>>> = HashMap::new();
        for thread_id in assignment.thread_ids() {
            for (group_id, entry) in assignment.get_groups_for_thread(thread_id).unwrap() {
                let mut scheduler =
                    configs[*group_id].traffic_policy.create_scheduler(entry, seed).unwrap();
                // A pool numbers each group's connections from zero
                for index in 0..entry.connections_per_thread {
                    let times = intended_times(&mut *scheduler.assign_policy(index), start, sends);
                    schedules.entry(*group_id).or_default().push(times);
                }
            }
        }
        schedules
    }

    #[test]
    fn fixed_rate_connections_of_every_thread_and_group_merge_into_even_streams() {
        let start = 1_000_000_000;
        // Thread 1 carries both groups, and group 1 lists it twice
        let configs = vec![
            group(
                "a",
                vec![0, 1, 2],
                3,
                PolicyConfig::FixedRate {
                    rate: 90_000.0,
                    dispatch: Dispatch::Connection,
                },
            ),
            group(
                "b",
                vec![1, 3, 1],
                2,
                PolicyConfig::FixedRate {
                    rate: 12_345.5,
                    dispatch: Dispatch::Connection,
                },
            ),
        ];
        let assignment = ThreadGroupAssignment::from_configs(&configs);
        let firsts: Vec<_> = assignment
            .get_groups_for_thread(1)
            .unwrap()
            .iter()
            .map(|(g, e)| (*g, e.first_connection))
            .collect();
        assert_eq!(firsts, vec![(0, 3), (1, 0), (1, 4)]);

        // Group a: 9 connections, 111,112 sends each, about 1e6 in all
        let schedules = group_schedules(&configs, None, start, 111_112);
        for (group_id, (numerator, denominator)) in [(0, (90_000u128, 1u128)), (1, (24_691, 2))] {
            let mut merged: Vec<u64> = schedules[&group_id].iter().flatten().copied().collect();
            merged.sort_unstable();
            let expected: Vec<u64> = (0..merged.len() as u128)
                .map(|j| start + (j * 1_000_000_000 * denominator / numerator) as u64)
                .collect();
            assert!(
                merged == expected,
                "group {group_id}: first difference at send {:?}",
                merged.iter().zip(&expected).position(|(a, b)| a != b)
            );
        }
        // The last of group a's 1,000,008 sends is exactly where the rate puts it
        assert_eq!(schedules[&0].iter().flatten().max(), Some(&(start + 11_111_188_888)));
    }

    #[test]
    fn seeded_poisson_groups_repeat_and_every_connection_has_its_own_stream() {
        let start = 1_000_000_000;
        let configs = vec![
            group(
                "a",
                vec![0, 1],
                2,
                PolicyConfig::Poisson {
                    rate: 4_000.0,
                    dispatch: Dispatch::Connection,
                },
            ),
            group(
                "b",
                vec![1],
                2,
                PolicyConfig::Poisson {
                    rate: 4_000.0,
                    dispatch: Dispatch::Connection,
                },
            ),
        ];
        let schedules = group_schedules(&configs, Some(7), start, 100);
        assert_eq!(schedules, group_schedules(&configs, Some(7), start, 100));
        assert_ne!(schedules, group_schedules(&configs, Some(8), start, 100));
        let mut streams: Vec<_> = schedules.values().flatten().collect();
        assert_eq!(streams.len(), 6);
        streams.sort();
        streams.dedup();
        assert_eq!(streams.len(), 6, "no two connections share a stream");
        // Without a seed, every run differs
        assert_ne!(
            group_schedules(&configs, None, start, 100),
            group_schedules(&configs, None, start, 100)
        );
    }

    #[test]
    fn every_policy_variant_rejects_unknown_keys() {
        for policy in [
            serde_json::json!({ "type": "closed-loop" }),
            serde_json::json!({ "type": "fixed-rate", "rate": 1.0 }),
            serde_json::json!({ "type": "poisson", "rate": 1.0 }),
            serde_json::json!({ "type": "adaptive", "initial_rate": 1.0, "target_latency_us": 10 }),
            serde_json::json!({
                "type": "sinusoidal", "base_rate": 1.0, "amplitude": 1.0, "period": "1s"
            }),
            serde_json::json!({
                "type": "ramp", "start_rate": 1.0, "end_rate": 2.0, "duration": "1s"
            }),
            serde_json::json!({
                "type": "spike", "normal_rate": 1.0, "spike_rate": 2.0,
                "spike_start": "1s", "spike_duration": "1s"
            }),
            serde_json::json!({
                "type": "sawtooth", "min_rate": 1.0, "max_rate": 2.0, "period": "1s"
            }),
        ] {
            serde_json::from_value::<PolicyConfig>(policy.clone()).unwrap();
            let mut with_unknown = policy.clone();
            with_unknown["bogus"] = 1.into();
            let error = serde_json::from_value::<PolicyConfig>(with_unknown).unwrap_err();
            assert!(error.to_string().contains("unknown field `bogus`"), "{policy}: {error}");
        }
    }

    #[test]
    fn every_rate_policy_takes_a_dispatch_mode_and_closed_loop_none() {
        for policy in [
            serde_json::json!({ "type": "fixed-rate", "rate": 1.0 }),
            serde_json::json!({ "type": "poisson", "rate": 1.0 }),
            serde_json::json!({ "type": "adaptive", "initial_rate": 1.0, "target_latency_us": 10 }),
            serde_json::json!({
                "type": "sinusoidal", "base_rate": 1.0, "amplitude": 1.0, "period": "1s"
            }),
            serde_json::json!({
                "type": "ramp", "start_rate": 1.0, "end_rate": 2.0, "duration": "1s"
            }),
            serde_json::json!({
                "type": "spike", "normal_rate": 1.0, "spike_rate": 2.0,
                "spike_start": "1s", "spike_duration": "1s"
            }),
            serde_json::json!({
                "type": "sawtooth", "min_rate": 1.0, "max_rate": 2.0, "period": "1s"
            }),
        ] {
            // The default is per-connection dispatch, and it is not written out
            let config: PolicyConfig = serde_json::from_value(policy.clone()).unwrap();
            assert_eq!(config.dispatch(), Dispatch::Connection);
            assert_eq!(serde_json::to_value(&config).unwrap().get("dispatch"), None);
            let mut group = policy.clone();
            group["dispatch"] = "group".into();
            let config: PolicyConfig = serde_json::from_value(group.clone()).unwrap();
            assert_eq!(config.dispatch(), Dispatch::Group, "{policy}");
            assert_eq!(serde_json::to_value(&config).unwrap()["dispatch"], "group");
            group["dispatch"] = "thread".into();
            assert!(serde_json::from_value::<PolicyConfig>(group).is_err());
        }
        let closed = serde_json::json!({ "type": "closed-loop", "dispatch": "group" });
        let error = serde_json::from_value::<PolicyConfig>(closed).unwrap_err();
        assert!(error.to_string().contains("unknown field `dispatch`"), "{error}");
    }

    /// Every thread entry's group-dispatch arrival process, built the way the
    /// CLI builds them, with its first `sends` intended times anchored at `start`
    fn dispatch_schedules(
        config: &TrafficGroupConfig,
        seed: Option<u64>,
        start: u64,
        sends: usize,
    ) -> Vec<Vec<u64>> {
        let configs = [config.clone()];
        let assignment = ThreadGroupAssignment::from_configs(&configs);
        let mut schedules = Vec::new();
        for thread_id in assignment.thread_ids() {
            for (_, entry) in assignment.get_groups_for_thread(thread_id).unwrap() {
                let mut scheduler = config.traffic_policy.create_scheduler(entry, seed).unwrap();
                assert_eq!(scheduler.assign_policy(0).name(), "ClosedLoop");
                let mut policy = scheduler.dispatch_policy().expect("group dispatch");
                schedules.push(intended_times(&mut *policy, start, sends));
            }
        }
        schedules
    }

    #[test]
    fn group_dispatch_runs_one_arrival_process_per_thread_sharing_the_rate() {
        let start = 1_000_000_000;
        let fixed = PolicyConfig::FixedRate { rate: 4_000.0, dispatch: Dispatch::Group };
        // Each thread's arrivals take every other slot of the 4000 req/s
        // stream, however many connections the thread has
        let schedules = dispatch_schedules(&group("a", vec![0, 1], 8, fixed), None, start, 3);
        assert_eq!(
            schedules,
            [
                [start, start + 500_000, start + 1_000_000],
                [start + 250_000, start + 750_000, start + 1_250_000]
            ]
        );

        let poisson = group(
            "a",
            vec![0, 1],
            8,
            PolicyConfig::Poisson { rate: 4_000.0, dispatch: Dispatch::Group },
        );
        let schedules = dispatch_schedules(&poisson, Some(7), start, 20_000);
        assert_eq!(schedules, dispatch_schedules(&poisson, Some(7), start, 20_000));
        assert_ne!(schedules[0], schedules[1], "every thread has its own stream");
        for times in &schedules {
            // 2000 req/s per thread: 20,000 arrivals take about 10 s
            let span = (times[times.len() - 1] - start) as f64 / 1e9;
            assert!((9.7..10.3).contains(&span), "{span} s");
        }
    }

    #[test]
    fn traffic_groups_reject_unknown_keys() {
        let group = serde_json::json!({
            "name": "main", "protocol": "redis", "target": "127.0.0.1:6379", "threads": [0],
            "connections_per_thread": 1, "traffic_policy": { "type": "closed-loop" }
        });
        serde_json::from_value::<TrafficGroupConfig>(group.clone()).unwrap();
        let mut with_unknown = group;
        with_unknown["sampling_rate"] = 0.5.into();
        let error = serde_json::from_value::<TrafficGroupConfig>(with_unknown).unwrap_err();
        assert!(error.to_string().contains("unknown field `sampling_rate`"), "{error}");
    }
}
