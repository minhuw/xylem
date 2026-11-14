//! Latency breakdown: where each request's latency went
//!
//! A request's latency is its send lag (recorded at the send, see
//! [`super::SendLagStats`]) plus its service latency, from the actual send
//! to the actual receive. When the send and the receive both carry a kernel
//! or NIC timestamp from the same clock, the round trip also splits into
//! the part outside the host's stack and the part inside it:
//!
//! - `network_server`: kernel RX timestamp minus kernel TX timestamp, the
//!   time from the request leaving the kernel (or the NIC) to the response
//!   reaching it: network, server and the NICs.
//! - `host_stack`: the user-space round trip (cycles read when the send and
//!   receive calls returned) minus `network_server`, saturating at zero:
//!   the time from the kernel RX timestamp to the receive call's return,
//!   minus the time from the kernel TX timestamp to the send call's return
//!   (negative when the NIC transmits after the call returned).
//!
//! Only durations are subtracted, each measured in one clock; times from
//! different clocks are never compared.
//!
//! A request without a kernel split says why ([`SplitMiss`]): its send had
//! no kernel TX timestamp, its receive no kernel RX timestamp, or the two
//! came from different clocks. [`BreakdownStats`] counts each cause.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use xylem_transport::{KernelClock, Timestamp};

/// Clock that requests' service latencies were measured in
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakdownClock {
    /// User-space cycles: no kernel split
    UserSpace,
    /// Kernel software timestamps (CLOCK_REALTIME)
    KernelSoftware,
    /// NIC hardware timestamps (the NIC's PHC)
    NicHardware,
    /// A user-space stack's NIC timestamps on the user-space clock: NIC RX
    /// hardware timestamps converted by a fitted calibration, and the time
    /// the stack queued the request to the NIC
    NicCalibrated,
    /// Different requests of the scope used different clocks
    Mixed,
}

impl BreakdownClock {
    fn bit(self) -> u8 {
        match self {
            BreakdownClock::UserSpace => 1,
            BreakdownClock::KernelSoftware => 2,
            BreakdownClock::NicHardware => 4,
            BreakdownClock::NicCalibrated => 8,
            BreakdownClock::Mixed => 15,
        }
    }

    /// The clock of a scope whose requests used the clocks in `bits`
    fn from_bits(bits: u8) -> Option<Self> {
        match bits {
            0 => None,
            1 => Some(BreakdownClock::UserSpace),
            2 => Some(BreakdownClock::KernelSoftware),
            4 => Some(BreakdownClock::NicHardware),
            8 => Some(BreakdownClock::NicCalibrated),
            _ => Some(BreakdownClock::Mixed),
        }
    }
}

/// The kernel split of one request's round trip
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelSplit {
    /// Clock of both kernel timestamps
    pub clock: KernelClock,
    /// Kernel round trip: kernel RX timestamp minus kernel TX timestamp
    pub network_server: Duration,
    /// User-space round trip minus the kernel round trip, zero if negative
    pub host_stack: Duration,
}

/// Why a request's round trip has no kernel split
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitMiss {
    /// The send had no kernel TX timestamp; the receive had one
    Tx,
    /// The receive had no kernel RX timestamp; the send had one
    Rx,
    /// Neither had a kernel timestamp, as with user-space timestamps
    TxAndRx,
    /// Both had one, from different clocks
    ClockMismatch,
}

/// Where one completed request's latency went, apart from its send lag
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencyBreakdown {
    /// Actual receive minus actual send, in the clock the latency uses: the
    /// kernel round trip with a kernel split, the user-space one otherwise
    pub service: Duration,
    /// Present when the send and the receive have kernel timestamps from
    /// the same clock
    pub kernel: Option<KernelSplit>,
    /// Why there is no kernel split; `None` exactly when `kernel` is set
    pub miss: Option<SplitMiss>,
}

impl LatencyBreakdown {
    /// The breakdown of a request sent at `send` whose response was
    /// received at `recv`
    ///
    /// `send` holds the send's own cycles and, once reported, its kernel TX
    /// timestamp (see [`Timestamp::with_kernel`]).
    #[inline]
    pub fn between(send: &Timestamp, recv: &Timestamp) -> Self {
        let miss = match (send.kernel(), recv.kernel()) {
            (Some(tx), Some(rx)) if tx.clock == rx.clock => {
                let kernel_rtt = Duration::from_nanos(rx.nanos.saturating_sub(tx.nanos));
                let user_rtt = recv.cycles_duration_since(send);
                return Self {
                    service: kernel_rtt,
                    kernel: Some(KernelSplit {
                        clock: tx.clock,
                        network_server: kernel_rtt,
                        host_stack: user_rtt.saturating_sub(kernel_rtt),
                    }),
                    miss: None,
                };
            }
            (Some(_), Some(_)) => SplitMiss::ClockMismatch,
            (None, Some(_)) => SplitMiss::Tx,
            (Some(_), None) => SplitMiss::Rx,
            (None, None) => SplitMiss::TxAndRx,
        };
        Self {
            service: recv.cycles_duration_since(send),
            kernel: None,
            miss: Some(miss),
        }
    }

    /// The breakdown of a request measured in user-space cycles alone,
    /// whose send and receive had no kernel timestamp
    pub fn user_space(service: Duration) -> Self {
        Self {
            service,
            kernel: None,
            miss: Some(SplitMiss::TxAndRx),
        }
    }

    /// The clock the service latency was measured in
    pub fn clock(&self) -> BreakdownClock {
        match self.kernel.map(|split| split.clock) {
            None => BreakdownClock::UserSpace,
            Some(KernelClock::Realtime) => BreakdownClock::KernelSoftware,
            Some(KernelClock::NicHardware) => BreakdownClock::NicHardware,
            Some(KernelClock::NicCalibrated) => BreakdownClock::NicCalibrated,
        }
    }
}

/// Durations of one part of the breakdown, in nanoseconds
///
/// Stored like send lags: every duration is exact in the mean and maximum;
/// percentiles come from an HDR histogram with three significant digits.
#[derive(Debug, Clone)]
pub struct PartStats {
    count: u64,
    sum_ns: u128,
    max_ns: u64,
    histogram: Histogram<u64>,
}

impl Default for PartStats {
    fn default() -> Self {
        Self {
            count: 0,
            sum_ns: 0,
            max_ns: 0,
            histogram: Histogram::new(3).expect("valid HDR histogram precision"),
        }
    }
}

impl PartStats {
    /// Record one duration
    #[inline]
    pub fn record(&mut self, duration: Duration) {
        let ns = duration.as_nanos().min(u64::MAX as u128) as u64;
        self.count += 1;
        self.sum_ns = self.sum_ns.saturating_add(ns as u128);
        self.max_ns = self.max_ns.max(ns);
        if self.histogram.record(ns).is_err() {
            // Only a value beyond any resize fails; keep it at the top.
            self.histogram.saturating_record(ns);
        }
    }

    /// Add another part's durations to these
    pub fn merge(&mut self, other: &Self) {
        self.count += other.count;
        self.sum_ns = self.sum_ns.saturating_add(other.sum_ns);
        self.max_ns = self.max_ns.max(other.max_ns);
        self.histogram.add(&other.histogram).expect("auto-resizing histograms merge");
    }

    /// Summarize the recorded durations
    pub fn summary(&self) -> PartSummary {
        let recorded = self.count > 0;
        let quantile =
            |q: f64| recorded.then(|| Duration::from_nanos(self.histogram.value_at_quantile(q)));
        PartSummary {
            sample_count: self.count,
            mean: recorded.then(|| Duration::from_nanos((self.sum_ns / self.count as u128) as u64)),
            p50: quantile(0.5),
            p99: quantile(0.99),
            p999: quantile(0.999),
            max: recorded.then(|| Duration::from_nanos(self.max_ns)),
        }
    }
}

/// Summary of one part; the values are `None` when nothing was recorded
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartSummary {
    pub sample_count: u64,
    pub mean: Option<Duration>,
    pub p50: Option<Duration>,
    pub p99: Option<Duration>,
    pub p999: Option<Duration>,
    pub max: Option<Duration>,
}

/// Requests without a kernel split, by what they lacked
///
/// A request with neither kernel timestamp counts in both `tx` and `rx`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitMissCounts {
    /// Requests whose send had no kernel TX timestamp
    pub tx: u64,
    /// Requests whose receive had no kernel RX timestamp
    pub rx: u64,
    /// Requests whose send and receive had kernel timestamps of different
    /// clocks
    pub clock_mismatch: u64,
}

impl SplitMissCounts {
    #[inline]
    fn record(&mut self, miss: SplitMiss) {
        match miss {
            SplitMiss::Tx => self.tx += 1,
            SplitMiss::Rx => self.rx += 1,
            SplitMiss::TxAndRx => {
                self.tx += 1;
                self.rx += 1;
            }
            SplitMiss::ClockMismatch => self.clock_mismatch += 1,
        }
    }

    fn merge(&mut self, other: &Self) {
        self.tx += other.tx;
        self.rx += other.rx;
        self.clock_mismatch += other.clock_mismatch;
    }
}

/// Latency breakdowns of a scope's completed measurement requests
#[derive(Debug, Clone, Default)]
pub struct BreakdownStats {
    /// [`BreakdownClock::bit`]s of the clocks the requests used
    clocks: u8,
    service: PartStats,
    /// Recorded only for requests with a kernel split, like `network_server`
    host_stack: PartStats,
    network_server: PartStats,
    /// Requests without a kernel split, by cause
    missing: SplitMissCounts,
}

impl BreakdownStats {
    /// Record one request's breakdown
    #[inline]
    pub fn record(&mut self, breakdown: &LatencyBreakdown) {
        self.clocks |= breakdown.clock().bit();
        self.service.record(breakdown.service);
        if let Some(split) = breakdown.kernel {
            self.host_stack.record(split.host_stack);
            self.network_server.record(split.network_server);
        }
        if let Some(miss) = breakdown.miss {
            self.missing.record(miss);
        }
    }

    /// Add another scope's breakdowns to these
    pub fn merge(&mut self, other: &Self) {
        self.clocks |= other.clocks;
        self.service.merge(&other.service);
        self.host_stack.merge(&other.host_stack);
        self.network_server.merge(&other.network_server);
        self.missing.merge(&other.missing);
    }

    /// Summarize the recorded breakdowns
    pub fn summary(&self) -> BreakdownSummary {
        let kernel_split_count = self.network_server.count;
        let split = |part: &PartStats| (kernel_split_count > 0).then(|| part.summary());
        BreakdownSummary {
            clock: BreakdownClock::from_bits(self.clocks),
            kernel_split_count,
            kernel_split_missing: self.missing,
            service: self.service.summary(),
            host_stack: split(&self.host_stack),
            network_server: split(&self.network_server),
        }
    }
}

/// Summary of a scope's latency breakdowns
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BreakdownSummary {
    /// Clock of the service latencies; `None` when no request completed
    pub clock: Option<BreakdownClock>,
    /// Requests whose round trip had a kernel split
    pub kernel_split_count: u64,
    /// Requests whose round trip had none, by cause
    pub kernel_split_missing: SplitMissCounts,
    pub service: PartSummary,
    /// `None` when no request had a kernel split
    pub host_stack: Option<PartSummary>,
    /// `None` when no request had a kernel split
    pub network_server: Option<PartSummary>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use xylem_transport::KernelTimestamp;

    fn kernel(nanos: u64, clock: KernelClock) -> KernelTimestamp {
        KernelTimestamp { nanos, clock }
    }

    /// A send, and a receive 20 ms of cycles later
    fn round_trip() -> (Timestamp, Timestamp) {
        let send = Timestamp::now();
        std::thread::sleep(Duration::from_millis(20));
        (send, Timestamp::now())
    }

    #[test]
    fn same_clock_splits_the_round_trip() {
        let (send, recv) = round_trip();
        let user_rtt = recv.cycles_duration_since(&send);
        for (clock, expected) in [
            (KernelClock::Realtime, BreakdownClock::KernelSoftware),
            (KernelClock::NicHardware, BreakdownClock::NicHardware),
            (KernelClock::NicCalibrated, BreakdownClock::NicCalibrated),
        ] {
            // A 5 ms kernel round trip inside the 20 ms user-space one
            let send = send.with_kernel(kernel(1_000_000_000, clock));
            let recv = recv.with_kernel(kernel(1_005_000_000, clock));
            let breakdown = LatencyBreakdown::between(&send, &recv);
            let kernel_rtt = Duration::from_millis(5);
            assert_eq!(breakdown.service, kernel_rtt);
            assert_eq!(breakdown.service, recv.duration_since(&send), "the latency's clock");
            assert_eq!(
                breakdown.kernel,
                Some(KernelSplit {
                    clock,
                    network_server: kernel_rtt,
                    host_stack: user_rtt - kernel_rtt,
                })
            );
            assert_eq!(breakdown.clock(), expected);
            assert_eq!(breakdown.miss, None);
        }
    }

    #[test]
    fn no_or_mixed_kernel_timestamps_have_no_split() {
        use KernelClock::{NicHardware as Hw, Realtime as Sw};
        let (send, recv) = round_trip();
        let user_rtt = recv.cycles_duration_since(&send);
        for (send_clock, recv_clock, miss) in [
            (None, None, SplitMiss::TxAndRx),
            (Some(Sw), None, SplitMiss::Rx),
            (None, Some(Sw), SplitMiss::Tx),
            (Some(Hw), None, SplitMiss::Rx),
            (None, Some(Hw), SplitMiss::Tx),
            (Some(Sw), Some(Hw), SplitMiss::ClockMismatch),
            (Some(Hw), Some(Sw), SplitMiss::ClockMismatch),
        ] {
            let send = send_clock.map_or(send, |clock| send.with_kernel(kernel(1_000, clock)));
            let recv = recv_clock.map_or(recv, |clock| recv.with_kernel(kernel(2_000, clock)));
            let breakdown = LatencyBreakdown::between(&send, &recv);
            assert_eq!(
                breakdown,
                LatencyBreakdown {
                    service: user_rtt,
                    kernel: None,
                    miss: Some(miss)
                },
                "{send_clock:?} -> {recv_clock:?}"
            );
            assert_eq!(breakdown.clock(), BreakdownClock::UserSpace);
        }
        assert_eq!(LatencyBreakdown::user_space(user_rtt), LatencyBreakdown::between(&send, &recv));
    }

    #[test]
    fn requests_without_a_split_count_by_cause_and_merge() {
        let (send, recv) = round_trip();
        let sw = |ts: Timestamp, nanos| ts.with_kernel(kernel(nanos, KernelClock::Realtime));
        let hw = |ts: Timestamp, nanos| ts.with_kernel(kernel(nanos, KernelClock::NicHardware));
        let mut stats = BreakdownStats::default();
        for (send, recv, times) in [
            (sw(send, 1_000), sw(recv, 2_000), 5),
            (send, sw(recv, 2_000), 4),
            (sw(send, 1_000), recv, 3),
            (send, recv, 2),
            (hw(send, 1_000), sw(recv, 2_000), 1),
        ] {
            for _ in 0..times {
                stats.record(&LatencyBreakdown::between(&send, &recv));
            }
        }
        let summary = stats.summary();
        assert_eq!(summary.kernel_split_count, 5);
        assert_eq!(summary.service.sample_count, 15);
        // Neither timestamp counts in both tx and rx
        let missing = SplitMissCounts { tx: 4 + 2, rx: 3 + 2, clock_mismatch: 1 };
        assert_eq!(summary.kernel_split_missing, missing);

        let mut other = BreakdownStats::default();
        other.record(&LatencyBreakdown::between(&send, &sw(recv, 2_000)));
        stats.merge(&other);
        let merged = SplitMissCounts { tx: missing.tx + 1, ..missing };
        assert_eq!(stats.summary().kernel_split_missing, merged);
    }

    #[test]
    fn host_stack_saturates_when_the_kernel_round_trip_is_longer() {
        // Cycles read back to back, kernel timestamps 1 ms apart
        let send = Timestamp::now().with_kernel(kernel(1_000_000, KernelClock::Realtime));
        let recv = Timestamp::now().with_kernel(kernel(2_000_000, KernelClock::Realtime));
        assert!(recv.cycles_duration_since(&send) < Duration::from_millis(1));
        let split = LatencyBreakdown::between(&send, &recv).kernel.unwrap();
        assert_eq!(split.network_server, Duration::from_millis(1));
        assert_eq!(split.host_stack, Duration::ZERO);
        // A kernel RX timestamp before the TX one saturates too
        let recv = Timestamp::now().with_kernel(kernel(500_000, KernelClock::Realtime));
        assert_eq!(LatencyBreakdown::between(&send, &recv).service, Duration::ZERO);
    }

    fn user(us: u64) -> LatencyBreakdown {
        LatencyBreakdown::user_space(Duration::from_micros(us))
    }

    fn split(clock: KernelClock, host_us: u64, network_us: u64) -> LatencyBreakdown {
        LatencyBreakdown {
            service: Duration::from_micros(network_us),
            kernel: Some(KernelSplit {
                clock,
                network_server: Duration::from_micros(network_us),
                host_stack: Duration::from_micros(host_us),
            }),
            miss: None,
        }
    }

    #[test]
    fn summary_without_requests_or_split() {
        assert_eq!(BreakdownStats::default().summary(), BreakdownSummary::default());
        let mut stats = BreakdownStats::default();
        for us in 1..=1000 {
            stats.record(&user(us));
        }
        let summary = stats.summary();
        assert_eq!(summary.clock, Some(BreakdownClock::UserSpace));
        assert_eq!(summary.kernel_split_count, 0);
        assert_eq!((summary.host_stack, summary.network_server), (None, None));
        let service = summary.service;
        assert_eq!(service.sample_count, 1000);
        assert_eq!(service.mean, Some(Duration::from_nanos(500_500)));
        assert_eq!(service.max, Some(Duration::from_millis(1)));
        let within = |value: Option<Duration>, us: u64| {
            let value = value.unwrap();
            let expected = Duration::from_micros(us);
            assert!(value.abs_diff(expected) <= expected / 500, "{value:?} vs {expected:?}");
        };
        within(service.p50, 500);
        within(service.p99, 990);
        within(service.p999, 999);
    }

    #[test]
    fn merging_across_threads_keeps_exact_means_maxima_and_clocks() {
        // Thread 0 splits every request in kernel software timestamps
        let mut first = BreakdownStats::default();
        for us in 1..=10 {
            first.record(&split(KernelClock::Realtime, us, 10 * us));
        }
        // Thread 1 got no TX timestamp for one request
        let mut second = BreakdownStats::default();
        second.record(&split(KernelClock::Realtime, 50, 500));
        second.record(&user(700));

        let only_first = first.summary();
        assert_eq!(only_first.clock, Some(BreakdownClock::KernelSoftware));
        assert_eq!(only_first.kernel_split_count, 10);

        first.merge(&second);
        let summary = first.summary();
        assert_eq!(summary.clock, Some(BreakdownClock::Mixed));
        assert_eq!(summary.kernel_split_count, 11);
        assert_eq!(summary.service.sample_count, 12);
        assert_eq!(summary.service.max, Some(Duration::from_micros(700)));
        assert_eq!(summary.service.mean, Some(Duration::from_nanos((550 + 500 + 700) * 1000 / 12)));
        let host = summary.host_stack.unwrap();
        assert_eq!((host.sample_count, host.max), (11, Some(Duration::from_micros(50))));
        assert_eq!(host.mean, Some(Duration::from_micros(105) / 11));
        let network = summary.network_server.unwrap();
        assert_eq!((network.sample_count, network.max), (11, Some(Duration::from_micros(500))));

        // Hardware and software splits in one scope are mixed too
        let mut hw = BreakdownStats::default();
        hw.record(&split(KernelClock::NicHardware, 1, 2));
        assert_eq!(hw.summary().clock, Some(BreakdownClock::NicHardware));
        let mut sw = BreakdownStats::default();
        sw.record(&split(KernelClock::Realtime, 1, 2));
        hw.merge(&sw);
        assert_eq!(hw.summary().clock, Some(BreakdownClock::Mixed));
        // Merging an empty scope changes nothing
        let before = hw.summary();
        hw.merge(&BreakdownStats::default());
        assert_eq!(hw.summary(), before);
    }
}
