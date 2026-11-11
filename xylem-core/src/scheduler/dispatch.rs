//! Group-level open-loop dispatch
//!
//! With per-connection dispatch (the default), every connection of a traffic
//! group runs its own share of the group's arrival process. A connection
//! whose pipeline is full cannot send at its intended times even when other
//! connections are idle, so with one request in flight per connection the
//! arrivals wait for responses: the run is no longer open-loop.
//!
//! With group dispatch, one thread's connections of a traffic group share one
//! arrival process, the thread's share of the group's rate. Each arrival goes
//! to an idle connection: one whose protocol can send, that is not setting up,
//! and whose pipeline is not full. Idle connections wait in a FIFO list, so
//! the connection idle longest sends next and every connection takes its
//! turn. An arrival due while no connection is idle waits for the first one to
//! free up. Its send lag runs from its intended time, as for every open-loop
//! request: a wait longer than [`crate::stats::LATE_SEND_THRESHOLD`] counts as
//! a late send, and its latency includes the wait. Arrivals still waiting at
//! the end of the measurement window count as unsent.

use super::policy::{ClosedLoopPolicy, Policy, PolicyScheduler};
use std::collections::VecDeque;
use std::time::Duration;

/// One thread's share of a traffic group's arrival process, and the group's
/// connections on that thread that can take an arrival
///
/// The dispatcher does not check connections itself: the connection pool adds
/// a connection with [`Self::push_idle`] when it becomes idle, and filters out
/// the connections that closed while idle when it takes one.
pub struct GroupDispatcher {
    group_id: usize,
    /// The arrival process; its intended times are the group's
    policy: Box<dyn Policy>,
    /// Idle connections, the one idle longest first
    idle: VecDeque<usize>,
    /// When the next arrival is due, or the policy wants to refresh its rate;
    /// valid while a connection is idle. Zero means at once.
    wake_ns: u64,
}

impl GroupDispatcher {
    /// A dispatcher of traffic group `group_id` whose arrivals follow `policy`
    pub fn new(group_id: usize, policy: Box<dyn Policy>) -> Self {
        Self {
            group_id,
            policy,
            idle: VecDeque::new(),
            wake_ns: 0,
        }
    }

    /// Traffic group of the dispatcher's connections
    pub fn group_id(&self) -> usize {
        self.group_id
    }

    /// Name of the arrival policy
    pub fn policy_name(&self) -> &'static str {
        self.policy.name()
    }

    /// Number of idle connections
    pub fn idle_count(&self) -> usize {
        self.idle.len()
    }

    /// Add an idle connection at the back of the idle list. The first
    /// connection ever added starts the arrival schedule, as a connection's
    /// first wakeup starts its own schedule under per-connection dispatch.
    pub fn push_idle(&mut self, connection_id: usize, current_time_ns: u64) {
        if self.idle.is_empty() {
            self.refresh(current_time_ns);
        }
        self.idle.push_back(connection_id);
    }

    /// When an arrival may next go out: `None` while no connection is idle,
    /// since an arrival due then waits for a response instead
    pub fn next_wakeup(&self) -> Option<u64> {
        (!self.idle.is_empty()).then_some(self.wake_ns)
    }

    /// Take the idle connection the arrival due by `current_time_ns` goes to,
    /// if one is due. Connections for which `usable` is false are dropped
    /// from the idle list. The arrival is taken only by
    /// [`Self::on_request_sent`]: until then it stays due.
    pub fn pop_due(
        &mut self,
        current_time_ns: u64,
        mut usable: impl FnMut(usize) -> bool,
    ) -> Option<usize> {
        if self.idle.is_empty() || self.wake_ns > current_time_ns {
            return None;
        }
        if !self.policy.ready_to_send(current_time_ns) {
            // A refresh wakeup of a dynamic policy, not an arrival
            self.refresh(current_time_ns);
            return None;
        }
        while let Some(connection_id) = self.idle.pop_front() {
            if usable(connection_id) {
                return Some(connection_id);
            }
        }
        None
    }

    /// Intended send time of the arrival that is due
    pub fn intended_send_time(&self) -> Option<u64> {
        self.policy.intended_send_time()
    }

    /// The due arrival went out: advance the schedule to the next one
    pub fn on_request_sent(&mut self, sent_time_ns: u64) {
        self.policy.on_request_sent(sent_time_ns);
        self.refresh(sent_time_ns);
    }

    /// Feedback from a completed measurement response on one of the
    /// dispatcher's connections (see [`Policy::on_response`])
    pub fn on_response(&mut self, latency: Duration) {
        self.policy.on_response(latency);
    }

    /// Count the arrivals intended before `end_ns` that never went out (see
    /// [`Policy::count_intended_before`])
    pub fn count_intended_before(&mut self, end_ns: u64, cap: u64) -> (u64, bool) {
        self.policy.count_intended_before(end_ns, cap)
    }

    /// Anchor the arrival schedule at `start_ns` (see [`Policy::anchor`]) and
    /// empty the idle list; the pool adds its idle connections again
    pub fn anchor(&mut self, start_ns: u64) {
        self.policy.anchor(start_ns);
        self.idle.clear();
    }

    fn refresh(&mut self, current_time_ns: u64) {
        self.wake_ns = self.policy.next_send_time(current_time_ns).unwrap_or(0);
    }
}

/// Policy scheduler of connections that a [`GroupDispatcher`] sends on
///
/// Its connections get no schedule of their own: [`Self::assign_policy`]
/// gives each a closed-loop policy, which the pool never consults for them,
/// and [`PolicyScheduler::dispatch_policy`] hands over the one arrival
/// process they share.
pub struct GroupDispatchScheduler {
    policy: Option<Box<dyn Policy>>,
}

impl GroupDispatchScheduler {
    /// Dispatch the arrivals of `policy` to the scheduler's connections
    pub fn new(policy: Box<dyn Policy>) -> Self {
        Self { policy: Some(policy) }
    }
}

impl PolicyScheduler for GroupDispatchScheduler {
    fn assign_policy(&mut self, _conn_id: usize) -> Box<dyn Policy> {
        Box::new(ClosedLoopPolicy::new())
    }

    fn dispatch_policy(&mut self) -> Option<Box<dyn Policy>> {
        self.policy.take()
    }

    fn name(&self) -> &'static str {
        "GroupDispatch"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::{FixedRatePolicy, PatternAwarePolicy, PoissonPolicy};
    use crate::workload::{LoadPattern, RampPattern};
    use std::sync::Arc;

    const MS: u64 = 1_000_000;

    fn take(dispatcher: &mut GroupDispatcher, now: u64) -> Option<(usize, u64)> {
        let connection_id = dispatcher.pop_due(now, |_| true)?;
        let intended = dispatcher.intended_send_time().unwrap();
        dispatcher.on_request_sent(now);
        Some((connection_id, intended))
    }

    #[test]
    fn arrivals_go_to_the_connection_idle_longest() {
        let mut dispatcher = GroupDispatcher::new(0, Box::new(FixedRatePolicy::new(1000.0)));
        dispatcher.anchor(0);
        assert_eq!(dispatcher.next_wakeup(), None, "no idle connection, no wakeup");
        for id in [3, 1, 2] {
            dispatcher.push_idle(id, 0);
        }
        assert_eq!(dispatcher.next_wakeup(), Some(0));
        assert_eq!(take(&mut dispatcher, 0), Some((3, 0)));
        // The next arrival is not due yet
        assert_eq!(dispatcher.pop_due(MS - 1, |_| true), None);
        assert_eq!(dispatcher.next_wakeup(), Some(MS));
        // Connection 3 frees up and queues behind 1 and 2
        dispatcher.push_idle(3, MS);
        assert_eq!(take(&mut dispatcher, MS), Some((1, MS)));
        assert_eq!(take(&mut dispatcher, 2 * MS), Some((2, 2 * MS)));
        assert_eq!(take(&mut dispatcher, 3 * MS), Some((3, 3 * MS)));
        assert_eq!(dispatcher.idle_count(), 0);
    }

    #[test]
    fn unusable_connections_leave_the_idle_list() {
        let mut dispatcher = GroupDispatcher::new(0, Box::new(FixedRatePolicy::new(1000.0)));
        dispatcher.anchor(0);
        for id in 0..3 {
            dispatcher.push_idle(id, 0);
        }
        assert_eq!(dispatcher.pop_due(0, |id| id == 2), Some(2));
        assert_eq!(dispatcher.idle_count(), 0);
        // An arrival not taken by a send stays due
        assert_eq!(dispatcher.intended_send_time(), Some(0));
        dispatcher.push_idle(0, 5 * MS);
        assert_eq!(take(&mut dispatcher, 5 * MS), Some((0, 0)));
    }

    #[test]
    fn arrivals_due_without_an_idle_connection_wait_and_keep_their_intended_times() {
        let mut dispatcher = GroupDispatcher::new(0, Box::new(FixedRatePolicy::new(1000.0)));
        dispatcher.anchor(0);
        dispatcher.push_idle(7, 0);
        assert_eq!(take(&mut dispatcher, 0), Some((7, 0)));
        // No connection is idle from 0 to 3.5 ms: three arrivals wait
        assert_eq!(dispatcher.pop_due(3 * MS + MS / 2, |_| true), None);
        dispatcher.push_idle(7, 3 * MS + MS / 2);
        // Each goes out as soon as a connection is idle, with its own
        // intended time, so its lag runs from then
        let mut intended = Vec::new();
        while let Some((_, at)) = take(&mut dispatcher, 3 * MS + MS / 2) {
            intended.push(at);
            dispatcher.push_idle(7, 3 * MS + MS / 2);
        }
        assert_eq!(intended, [MS, 2 * MS, 3 * MS]);
        assert_eq!(dispatcher.next_wakeup(), Some(4 * MS));
        // Arrivals before the end of the window that never went out are unsent
        assert_eq!(dispatcher.count_intended_before(6 * MS + 1, u64::MAX), (3, false));
    }

    #[test]
    fn schedule_starts_with_the_first_idle_connection() {
        let mut dispatcher = GroupDispatcher::new(0, Box::new(FixedRatePolicy::new(1000.0)));
        dispatcher.anchor(0);
        // Connections set up until 2.5 ms: the arrivals before are not owed
        dispatcher.push_idle(0, 2 * MS + MS / 2);
        assert_eq!(dispatcher.next_wakeup(), Some(3 * MS));
        assert_eq!(dispatcher.count_intended_before(3 * MS, u64::MAX), (0, false));
    }

    /// Sends in a simulated second with `connections` connections, each busy
    /// for `service_ns` after a send; returns sends per connection and the
    /// number of sends that went out more than 100 µs after their intended time
    fn simulate(policy: Box<dyn Policy>, connections: usize, service_ns: u64) -> (Vec<u64>, u64) {
        let mut dispatcher = GroupDispatcher::new(0, policy);
        dispatcher.anchor(0);
        for id in 0..connections {
            dispatcher.push_idle(id, 0);
        }
        let mut busy_until = vec![None; connections];
        let mut sends = vec![0; connections];
        let mut late = 0;
        let mut now = 0;
        while now < 1_000 * MS {
            for (id, busy) in busy_until.iter_mut().enumerate() {
                if busy.is_some_and(|until| until <= now) {
                    *busy = None;
                    dispatcher.push_idle(id, now);
                }
            }
            while let Some((id, intended)) = take(&mut dispatcher, now) {
                sends[id] += 1;
                late += u64::from(now - intended > 100_000);
                busy_until[id] = Some(now + service_ns);
            }
            now += 10_000;
        }
        (sends, late)
    }

    #[test]
    fn fixed_rate_dispatch_keeps_its_rate_and_spreads_over_connections() {
        // 10k req/s, 1 ms service time: ~10 connections busy at a time
        let (sends, late) = simulate(Box::new(FixedRatePolicy::new(10_000.0)), 16, MS);
        assert_eq!(sends.iter().sum::<u64>(), 10_000);
        assert_eq!(late, 0);
        // FIFO idle list: every connection takes its turn
        let (min, max) = (sends.iter().min().unwrap(), sends.iter().max().unwrap());
        assert!(max - min <= 1, "{sends:?}");
    }

    #[test]
    fn poisson_dispatch_keeps_its_rate_without_starving_a_connection() {
        let policy = PoissonPolicy::with_seed(10_000.0, Some(42)).unwrap();
        let (sends, late) = simulate(Box::new(policy), 32, MS);
        let total: u64 = sends.iter().sum();
        assert!((9_700..=10_300).contains(&total), "{total} sends for 10k req/s");
        // 32 connections leave enough idle ones for a Poisson burst
        assert_eq!(late, 0);
        let mean = total / 32;
        assert!(sends.iter().all(|&s| s > mean / 2), "no connection starves: {sends:?}");
    }

    #[test]
    fn too_few_connections_make_arrivals_late_but_not_lost() {
        // 10k req/s with 1 ms service needs 10 connections; 5 serve 5k req/s
        let (sends, late) = simulate(Box::new(FixedRatePolicy::new(10_000.0)), 5, MS);
        let total: u64 = sends.iter().sum();
        assert!((4_900..=5_100).contains(&total), "{total}");
        assert!(late > total * 9 / 10, "{late} late of {total}");
    }

    #[test]
    fn pattern_refresh_wakeups_are_not_arrivals() {
        let pattern: Arc<dyn LoadPattern> =
            Arc::new(RampPattern::new(10.0, 10.0, Duration::from_secs(10)));
        let mut dispatcher = GroupDispatcher::new(0, Box::new(PatternAwarePolicy::new(pattern, 0)));
        dispatcher.anchor(0);
        dispatcher.push_idle(0, 0);
        // The first request is due at once, the next 100 ms later
        assert_eq!(take(&mut dispatcher, 0), Some((0, 0)));
        dispatcher.push_idle(0, 0);
        let mut wakeups = 0;
        let mut now = 0;
        loop {
            now = dispatcher.next_wakeup().unwrap().max(now);
            if let Some((_, intended)) = take(&mut dispatcher, now) {
                assert_eq!(intended, 100 * MS);
                break;
            }
            wakeups += 1;
        }
        assert!(wakeups >= 9, "refreshes every 10 ms: {wakeups}");
    }

    #[test]
    fn group_dispatch_scheduler_hands_over_its_policy_once() {
        let mut scheduler = GroupDispatchScheduler::new(Box::new(FixedRatePolicy::new(1.0)));
        assert_eq!(scheduler.assign_policy(0).name(), "ClosedLoop");
        assert_eq!(scheduler.dispatch_policy().unwrap().name(), "FixedRate");
        assert!(scheduler.dispatch_policy().is_none());
    }
}
