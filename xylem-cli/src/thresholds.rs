//! Pass/fail thresholds on the results, and checks that a run applied the
//! load it was configured with and measured it with the configured
//! timestamps
//!
//! A threshold compares a metric of the final results with a limit:
//! `"latency{group=api}" = ["p99 < 250us"]`. A metric is a path into a
//! `stats` object of the results JSON, the global one or a traffic group's,
//! so every numeric field there is a metric without this module naming it.
//! Paths are checked at load against [`probe_stats`], stats with every field
//! present, so a misspelled metric fails before the run.

use crate::config::ProfileConfig;
use crate::output::{
    BreakdownPartStats, DetailedExperimentResults, GlobalStats, GroupStats, LatencyBreakdownStats,
    LatencyStats, SendLagStats,
};
use anyhow::{bail, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use xylem_core::stats::{BreakdownClock, ErrorKindCounts, RequestCounts, LATE_SEND_THRESHOLD};
use xylem_core::traffic_group::{PolicyConfig, TimestampSourceConfig, TrafficGroupConfig};

/// Outcome of a run, from its validity checks and thresholds
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Every validity check and threshold passed, or none applied
    #[default]
    Pass,
    /// The run is valid and a threshold failed
    ThresholdsFailed,
    /// A validity check failed: the results do not describe the configured
    /// load. Takes precedence over failed thresholds.
    InvalidRun,
}

impl Verdict {
    /// Exit status of the CLI: 0 pass, 2 thresholds failed, 3 invalid run
    pub fn exit_code(self) -> i32 {
        match self {
            Verdict::Pass => 0,
            Verdict::ThresholdsFailed => 2,
            Verdict::InvalidRun => 3,
        }
    }

    /// Label in the console summary and the HTML report
    pub fn label(self) -> &'static str {
        match self {
            Verdict::Pass => "PASS",
            Verdict::ThresholdsFailed => "THRESHOLDS FAILED",
            Verdict::InvalidRun => "INVALID RUN",
        }
    }
}

/// Ends the `field` of a rate threshold
const RATE_DIVISOR: &str = " / requests.sent";

/// `name`, followed by the filter of `group`: `latency{group=api}`
fn scoped(name: &str, group: Option<&str>) -> String {
    match group {
        Some(group) => format!("{name}{{group={group}}}"),
        None => name.to_string(),
    }
}

/// One evaluated threshold expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThresholdResult {
    /// The metric as configured, without its group filter
    pub metric: String,
    /// Traffic group the metric was read from; null for the global stats
    pub group: Option<String>,
    /// The expression as configured
    pub expression: String,
    /// The stats field compared, such as `latency.p999_us`; a rate divides
    /// its count by `requests.sent`
    pub field: String,
    /// The value compared, in the unit of its field (microseconds for
    /// durations) or a fraction for a rate; null when the run has no value,
    /// which fails the threshold
    pub observed: Option<f64>,
    pub ok: bool,
}

impl ThresholdResult {
    /// The threshold's key, as in the profile: `latency{group=api}`
    pub fn key(&self) -> String {
        scoped(&self.metric, self.group.as_deref())
    }

    /// The observed value with its unit, or `n/a`
    pub fn observed_text(&self) -> String {
        match self.observed {
            None => "n/a".to_string(),
            Some(value) if self.field.ends_with(RATE_DIVISOR) => format!("{value:.6}"),
            Some(value) if self.field.ends_with("_us") => format!("{value:.2} μs"),
            Some(value) if value.fract() == 0.0 => format!("{value:.0}"),
            Some(value) => format!("{value:.2}"),
        }
    }
}

/// Checks that a run applied its configured load, and measured it with
/// its configured timestamps
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValidityCheck {
    /// Fraction of a rate-driven group's intended sends that never went out
    Unsent,
    /// Fraction of a rate-driven group's sends that went out late
    LateSends,
    /// Fraction of the completed requests of a group with kernel or hardware
    /// timestamps that had a kernel split; fails at zero
    KernelSplit,
}

impl ValidityCheck {
    /// The check's name in the profile and the results
    pub fn name(self) -> &'static str {
        match self {
            ValidityCheck::Unsent => "unsent",
            ValidityCheck::LateSends => "late_sends",
            ValidityCheck::KernelSplit => "kernel_split",
        }
    }
}

/// One evaluated validity check
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidityResult {
    pub check: ValidityCheck,
    /// Traffic group checked; null for a check of the whole run
    pub group: Option<String>,
    /// The fraction measured; null when the group recorded nothing
    pub observed: Option<f64>,
    /// Largest fraction that passes; for `kernel_split`, the fraction the
    /// observed one must exceed (0)
    pub limit: f64,
    pub ok: bool,
    /// Why the check failed; omitted when it passed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// A fraction as a percentage with two decimals
fn percent(fraction: f64) -> String {
    format!("{:.2}%", fraction * 100.0)
}

impl ValidityResult {
    /// The check's name with its group: `unsent{group=api}`
    pub fn key(&self) -> String {
        scoped(self.check.name(), self.group.as_deref())
    }

    /// The observed fraction as a percentage, or `n/a`
    pub fn observed_text(&self) -> String {
        self.observed.map_or_else(|| "n/a".to_string(), percent)
    }

    /// The limit as a percentage; `> 0.00%` for `kernel_split`, which the
    /// observed fraction must exceed
    pub fn limit_text(&self) -> String {
        match self.check {
            ValidityCheck::KernelSplit => format!("> {}", percent(self.limit)),
            _ => percent(self.limit),
        }
    }
}

/// Checks that a run applied the load it was configured with, and measured
/// it with the timestamps it was configured with. "unsent" and "late_sends"
/// apply to every traffic group with a rate-driven traffic policy (a closed
/// loop has no schedule to fall behind); "kernel_split" to every traffic
/// group with `timestamps = "kernel"` or `{ hardware = ... }` that
/// completed a request: it fails when none of them had a kernel split.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ValidityConfig {
    /// Run the checks (default true). With false, a run is never invalid.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Largest fraction of a group's intended sends (`sent + unsent`) that may
    /// remain unsent at the end of the window (default 0.001)
    #[serde(default = "default_max_unsent_ratio")]
    pub max_unsent_ratio: f64,
    /// Largest fraction of a group's sent requests that may lag their
    /// intended send time by more than 100 µs (default 0.01)
    #[serde(default = "default_max_late_send_ratio")]
    pub max_late_send_ratio: f64,
    /// Checks to leave out: "unsent", "late_sends", "kernel_split"
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip: Vec<ValidityCheck>,
}

fn default_enabled() -> bool {
    true
}

fn default_max_unsent_ratio() -> f64 {
    0.001
}

fn default_max_late_send_ratio() -> f64 {
    0.01
}

impl Default for ValidityConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            max_unsent_ratio: default_max_unsent_ratio(),
            max_late_send_ratio: default_max_late_send_ratio(),
            skip: Vec::new(),
        }
    }
}

impl ValidityConfig {
    /// Add an error to `errors` for each limit that is not a fraction from
    /// 0 to 1
    pub fn validate(&self, errors: &mut Vec<anyhow::Error>) {
        for (name, limit) in [
            ("max_unsent_ratio", self.max_unsent_ratio),
            ("max_late_send_ratio", self.max_late_send_ratio),
        ] {
            if !(0.0..=1.0).contains(&limit) {
                errors.push(anyhow::anyhow!(
                    "validity.{name} must be a fraction from 0 to 1, got {limit}"
                ));
            }
        }
    }

    /// The checks to run, with their limits
    fn checks(&self) -> Vec<(ValidityCheck, f64)> {
        if !self.enabled {
            return Vec::new();
        }
        [
            (ValidityCheck::Unsent, self.max_unsent_ratio),
            (ValidityCheck::LateSends, self.max_late_send_ratio),
            (ValidityCheck::KernelSplit, 0.0),
        ]
        .into_iter()
        .filter(|(check, _)| !self.skip.contains(check))
        .collect()
    }
}

/// Comparison operators of a threshold expression
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}

impl Op {
    fn holds(self, observed: f64, limit: f64) -> bool {
        match self {
            Op::Lt => observed < limit,
            Op::Le => observed <= limit,
            Op::Gt => observed > limit,
            Op::Ge => observed >= limit,
            Op::Eq => observed == limit,
            Op::Ne => observed != limit,
        }
    }
}

/// A threshold expression on one metric, checked against the probe stats
#[derive(Debug, Clone, PartialEq)]
pub struct Threshold {
    metric: String,
    group: Option<String>,
    expression: String,
    /// Path of the compared field in a stats object
    path: Vec<String>,
    /// Whether the field is divided by `requests.sent` of the same stats
    rate: bool,
    op: Op,
    /// Limit in the unit of the field: microseconds for a duration
    limit: f64,
}

impl Threshold {
    /// The stats field compared, as written in results
    fn field(&self) -> String {
        let field = self.path.join(".");
        if self.rate {
            format!("{field}{RATE_DIVISOR}")
        } else {
            field
        }
    }

    /// The value this threshold compares in `stats`, if the run has one
    fn observe(&self, stats: &Value) -> Option<f64> {
        // Older summaries may contain numeric placeholders in unavailable
        // duration fields, even for statistics needing two samples.
        if let [summary, field] = self.path.as_slice() {
            if matches!(summary.as_str(), "latency" | "error_latency") && field.ends_with("_us") {
                let summary = stats.get(summary)?;
                let samples = summary.get("sample_count")?.as_u64()?;
                if samples == 0
                    || (samples < 2
                        && matches!(field.as_str(), "std_dev_us" | "confidence_interval_us"))
                {
                    return None;
                }
                let percentile =
                    field.strip_prefix('p').and_then(|name| name.strip_suffix("_us")).is_some_and(
                        |digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()),
                    );
                if percentile && summary.get("percentile_sample_count")?.as_u64()? == 0 {
                    return None;
                }
            }
        }
        let value = lookup(stats, &self.path)?.as_f64()?;
        if !self.rate {
            return Some(value);
        }
        let sent = stats.get("requests")?.get("sent")?.as_f64()?;
        (sent > 0.0).then(|| value / sent)
    }
}

/// The field at `path` in `value`
fn lookup<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    path.iter().try_fold(value, |value, name| value.get(name))
}

/// Metric names that stand for a stats field
const ALIASES: [(&str, &str); 1] = [("throughput", "throughput_rps")];

/// Duration units and their length in microseconds
const DURATION_UNITS: [(&str, f64); 6] = [
    ("ns", 0.001),
    ("us", 1.0),
    ("µs", 1.0),
    ("μs", 1.0),
    ("ms", 1_000.0),
    ("s", 1_000_000.0),
];

/// Stats with every field present and numeric, which metric paths are
/// checked against at load. Struct literals, so the compiler asks for every
/// new stats field: give an `Option` `Some` and a map its keys, and the field
/// is a metric with no other change. Returns the global and group stats.
///
/// `requests.errors_by_kind` stays empty: its keys are the error kinds a
/// run meets, unknown at load, so its entries are no metrics (see
/// [`RUNTIME_KEYED`]); `requests.errors` counts them all.
fn probe_stats() -> (Value, Value) {
    let latency = LatencyStats {
        min_us: Some(0.0),
        max_us: Some(0.0),
        mean_us: 0.0,
        p50_us: 0.0,
        p95_us: 0.0,
        p99_us: 0.0,
        p999_us: 0.0,
        p9999_us: 0.0,
        p99999_us: 0.0,
        std_dev_us: Some(0.0),
        confidence_interval_us: Some(0.0),
        sample_count: 0,
        percentile_sample_count: Some(0),
        clamped_samples: Some(0),
    };
    let requests = RequestCounts {
        sent: 0,
        completed: 0,
        errors: 0,
        errors_by_kind: ErrorKindCounts::default(),
        timed_out: 0,
        late_responses: 0,
        failed: 0,
        late_sends: 0,
        unsent: 0,
        unsent_capped: false,
    };
    let send_lag = SendLagStats {
        sample_count: 0,
        mean_us: Some(0.0),
        p99_us: Some(0.0),
        max_us: Some(0.0),
        late_threshold_us: 0.0,
    };
    let part = BreakdownPartStats {
        sample_count: 0,
        mean_us: Some(0.0),
        p50_us: Some(0.0),
        p99_us: Some(0.0),
        p999_us: Some(0.0),
        max_us: Some(0.0),
    };
    let latency_breakdown = LatencyBreakdownStats {
        clock: Some(BreakdownClock::KernelSoftware),
        kernel_split_count: 0,
        kernel_split_missing: Some(Default::default()),
        service_us: part.clone(),
        host_stack_us: Some(part.clone()),
        network_server_us: Some(part),
    };
    let global = GlobalStats {
        total_requests: 0,
        throughput_rps: 0.0,
        throughput_mbps: 0.0,
        latency: latency.clone(),
        error_latency: latency.clone(),
        requests: requests.clone(),
        send_lag: send_lag.clone(),
        latency_breakdown: latency_breakdown.clone(),
    };
    let group = GroupStats {
        total_requests: 0,
        throughput_rps: 0.0,
        throughput_mbps: 0.0,
        latency: latency.clone(),
        error_latency: latency,
        requests,
        send_lag,
        latency_breakdown,
    };
    (
        serde_json::to_value(global).expect("stats serialize"),
        serde_json::to_value(group).expect("stats serialize"),
    )
}

/// Whether an object summarizes durations, such as `latency`: it has
/// numeric fields in microseconds, which aggregations name
fn is_summary(fields: &Map<String, Value>) -> bool {
    fields.iter().any(|(name, value)| name.ends_with("_us") && value.is_number())
}

/// The aggregation that names a summary field: `p999_us` is `p99.9`
fn aggregation_name(field: &str) -> Option<String> {
    let name = field.strip_suffix("_us")?;
    let Some(digits) = name.strip_prefix('p').filter(|d| d.bytes().all(|b| b.is_ascii_digit()))
    else {
        return Some(name.to_string());
    };
    if digits.len() <= 2 {
        return Some(name.to_string());
    }
    Some(format!("p{}.{}", &digits[..2], &digits[2..]))
}

/// The summary field an aggregation names: `p99.9` and `p999` are `p999_us`,
/// `mean` is `mean_us`
fn aggregation_field(aggregation: &str) -> Option<String> {
    let percentile = aggregation
        .strip_prefix('p')
        .filter(|rest| rest.starts_with(|c: char| c.is_ascii_digit()));
    if let Some(percentile) = percentile {
        let (whole, fraction) = percentile.split_once('.').unwrap_or((percentile, "0"));
        let digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
        return (digits(whole) && digits(fraction))
            .then(|| format!("p{}_us", percentile.replace('.', "")));
    }
    let identifier = aggregation.starts_with(|c: char| c.is_ascii_lowercase())
        && aggregation
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    identifier.then(|| format!("{aggregation}_us"))
}

/// The aggregations of a summary, for error messages
fn aggregation_names(fields: &Map<String, Value>) -> String {
    let names: Vec<String> = fields
        .iter()
        .filter(|(_, value)| value.is_number())
        .filter_map(|(name, _)| aggregation_name(name))
        .collect();
    names.join(", ")
}

/// Every metric of a stats probe, for error messages: summaries and numeric
/// fields, except the fields of a summary that aggregations name
fn metric_names(probe: &Value) -> String {
    fn visit(fields: &Map<String, Value>, prefix: &str, summary: bool, names: &mut Vec<String>) {
        for (name, value) in fields {
            let path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}.{name}")
            };
            match value {
                Value::Object(fields) if is_summary(fields) => {
                    names.push(path.clone());
                    visit(fields, &path, true, names);
                }
                Value::Object(fields) => visit(fields, &path, false, names),
                Value::Number(_) if !(summary && name.ends_with("_us")) => names.push(path),
                _ => {}
            }
        }
    }
    let mut names = Vec::new();
    if let Value::Object(fields) = probe {
        visit(fields, "", false, &mut names);
    }
    names.join(", ")
}

/// What a metric path names in the probe stats
enum Field<'a> {
    /// A summary of durations, such as `latency`; an aggregation picks a field
    Summary(&'a Map<String, Value>),
    /// A single number
    Number {
        /// In microseconds: the field name ends in `_us`
        duration: bool,
        /// An integer count, which `rate` can divide
        count: bool,
    },
}

/// Split a threshold key into its metric and the group of its filter:
/// `latency{group=api}` is `latency` of group `api`
fn parse_key(key: &str) -> Result<(String, Option<String>)> {
    let key = key.trim();
    let Some((metric, filter)) = key.split_once('{') else {
        return Ok((key.to_string(), None));
    };
    let Some(filter) = filter.strip_suffix('}') else {
        bail!("a filter ends with '}}', as in latency{{group=name}}");
    };
    let Some((label, group)) = filter.split_once('=') else {
        bail!("a filter is group=name, as in latency{{group=name}}");
    };
    if label.trim() != "group" {
        bail!("unknown filter '{}'; only group=name is supported", label.trim());
    }
    let group = group.trim();
    let group = group.strip_prefix('"').and_then(|g| g.strip_suffix('"')).unwrap_or(group);
    if group.is_empty() {
        bail!("the group filter names no group");
    }
    Ok((metric.trim().to_string(), Some(group.to_string())))
}

/// Check that a filter's group names exactly one traffic group
fn check_group(group: &str, groups: &[TrafficGroupConfig]) -> Result<()> {
    match groups.iter().filter(|g| g.name == group).count() {
        1 => Ok(()),
        0 => {
            let names: Vec<&str> = groups.iter().map(|g| g.name.as_str()).collect();
            bail!("unknown traffic group '{group}'; groups: {}", names.join(", "))
        }
        count => bail!(
            "traffic group name '{group}' is used by {count} groups; a filter needs a unique name"
        ),
    }
}

/// Stats maps whose keys a run names, such as each error kind: the probe
/// has none of their entries, so a threshold cannot be checked at load, and
/// a key the run never meets would have no value. Each is with the metric
/// to use instead.
const RUNTIME_KEYED: [(&str, &str); 1] = [("requests.errors_by_kind", "requests.errors")];

/// The path of a metric in the stats and what it names in `probe`
fn resolve<'a>(metric: &str, probe: &'a Value) -> Result<(Vec<String>, Field<'a>)> {
    let metric = ALIASES.iter().find(|(alias, _)| *alias == metric).map_or(metric, |(_, f)| f);
    for (map, instead) in RUNTIME_KEYED {
        if metric == map || metric.strip_prefix(map).is_some_and(|rest| rest.starts_with('.')) {
            bail!(
                "'{map}' is keyed by what the run meets, so its entries cannot be checked at \
                 load; use '{instead}'"
            );
        }
    }
    let path: Vec<String> = metric.split('.').map(str::to_string).collect();
    let unknown = || {
        anyhow::anyhow!("unknown metric '{metric}'; metrics: throughput, {}", metric_names(probe))
    };
    if path.iter().any(String::is_empty) {
        return Err(unknown());
    }
    let field = match lookup(probe, &path).ok_or_else(unknown)? {
        Value::Object(fields) if is_summary(fields) => Field::Summary(fields),
        Value::Object(fields) => {
            let names: Vec<String> = fields.keys().map(|name| format!("{metric}.{name}")).collect();
            bail!("'{metric}' is a group of metrics; name one: {}", names.join(", "))
        }
        Value::Number(number) => Field::Number {
            duration: metric.ends_with("_us"),
            count: number.is_u64() || number.is_i64(),
        },
        _ => bail!("'{metric}' is not a number"),
    };
    Ok((path, field))
}

/// Split an expression into its aggregation, operator and limit text:
/// `p99 < 250us` is `p99`, `<` and `250us`
fn split_expression(expression: &str) -> Result<(&str, Op, &str)> {
    let syntax = "write [aggregation] <op> <value>, with op <, <=, >, >=, == or !=";
    let Some(start) = expression.find(['<', '>', '=', '!']) else {
        bail!("no comparison; {syntax}");
    };
    let (aggregation, rest) = expression.split_at(start);
    let (op, width) = match rest.get(..2) {
        Some("<=") => (Op::Le, 2),
        Some(">=") => (Op::Ge, 2),
        Some("==") => (Op::Eq, 2),
        Some("!=") => (Op::Ne, 2),
        _ if rest.starts_with('<') => (Op::Lt, 1),
        _ if rest.starts_with('>') => (Op::Gt, 1),
        _ => bail!("unknown operator; {syntax}"),
    };
    Ok((aggregation.trim(), op, rest[width..].trim()))
}

/// Parse a limit, in microseconds for a duration
fn parse_limit(text: &str, duration: bool, what: &str) -> Result<f64> {
    let number_end = text.trim_end_matches(|c: char| !c.is_ascii_digit() && c != '.').len();
    let (number, unit) = (text[..number_end].trim(), text[number_end..].trim());
    let Ok(value) = number.parse::<f64>() else {
        bail!("'{text}' is not a number");
    };
    if !value.is_finite() {
        bail!("'{text}' is out of range");
    }
    match (duration, unit) {
        (true, "") => bail!("{what} is a duration; give a unit: ns, us, ms or s (as in 250us)"),
        (true, unit) => match DURATION_UNITS.iter().find(|(name, _)| *name == unit) {
            Some((_, micros)) => Ok(value * micros),
            None => bail!("unknown unit '{unit}'; durations take ns, us, ms or s"),
        },
        (false, "") => Ok(value),
        (false, unit) => bail!("{what} is a plain number; remove '{unit}'"),
    }
}

/// Parse one expression on a metric that resolved to `field` at `path`
fn parse_expression(
    metric: &str,
    expression: &str,
    mut path: Vec<String>,
    field: &Field,
) -> Result<(Vec<String>, bool, Op, f64)> {
    let (aggregation, op, limit) = split_expression(expression)?;
    let (rate, duration, what) = match field {
        Field::Summary(fields) => {
            if aggregation.is_empty() {
                bail!(
                    "'{metric}' is a summary; name an aggregation, as in \"p99 < 250us\": {}",
                    aggregation_names(fields)
                );
            }
            let name = aggregation_field(aggregation)
                .filter(|name| fields.get(name).is_some_and(Value::is_number));
            let Some(name) = name else {
                bail!(
                    "unknown aggregation '{aggregation}' for '{metric}'; aggregations: {}",
                    aggregation_names(fields)
                );
            };
            path.push(name);
            (false, true, format!("{metric} {aggregation}"))
        }
        Field::Number { duration, count } => match aggregation {
            "" => (false, *duration, format!("'{metric}'")),
            "rate" if *count => (true, false, format!("a rate of '{metric}'")),
            "rate" => bail!("rate divides a count by requests.sent; '{metric}' is not a count"),
            _ => bail!(
                "unknown aggregation '{aggregation}': '{metric}' is a single value; compare \
                 it directly (\"> 0\") or as a fraction of requests.sent (\"rate < 0.01\")"
            ),
        },
    };
    let limit = parse_limit(limit, duration, &what)?;
    Ok((path, rate, op, limit))
}

/// Parse a profile's `[thresholds]`, checking every metric, group and
/// expression; one error reports every invalid one
pub fn parse_thresholds(
    thresholds: &BTreeMap<String, Vec<String>>,
    groups: &[TrafficGroupConfig],
) -> Result<Vec<Threshold>> {
    let (global_probe, group_probe) = probe_stats();
    let mut parsed = Vec::new();
    let mut errors = Vec::new();
    for (key, expressions) in thresholds {
        let resolved = parse_key(key).and_then(|(metric, group)| {
            if let Some(group) = &group {
                check_group(group, groups)?;
            }
            let probe = if group.is_some() {
                &group_probe
            } else {
                &global_probe
            };
            let (path, field) = resolve(&metric, probe)?;
            Ok((metric, group, path, field))
        });
        let (metric, group, path, field) = match resolved {
            Ok(resolved) => resolved,
            Err(error) => {
                errors.push(format!("{key:?}: {error}"));
                continue;
            }
        };
        if expressions.is_empty() {
            errors.push(format!("{key:?}: no expressions"));
        }
        for expression in expressions {
            match parse_expression(&metric, expression, path.clone(), &field) {
                Ok((path, rate, op, limit)) => parsed.push(Threshold {
                    metric: metric.clone(),
                    group: group.clone(),
                    expression: expression.trim().to_string(),
                    path,
                    rate,
                    op,
                    limit,
                }),
                Err(error) => errors.push(format!("{key:?} = {expression:?}: {error}")),
            }
        }
    }
    if !errors.is_empty() {
        bail!("Invalid thresholds:\n  {}", errors.join("\n  "));
    }
    Ok(parsed)
}

/// Evaluate the profile's thresholds and validity checks against the final
/// results, record them in `results`, and return the verdict
pub fn evaluate(results: &mut DetailedExperimentResults, config: &ProfileConfig) -> Verdict {
    let global = serde_json::to_value(&results.global).expect("stats serialize");
    let groups: Vec<(&str, Value)> = results
        .traffic_groups
        .iter()
        .map(|group| {
            let stats = serde_json::to_value(&group.stats).expect("stats serialize");
            (group.name.as_str(), stats)
        })
        .collect();
    let thresholds: Vec<ThresholdResult> = config
        .threshold_checks
        .iter()
        .map(|threshold| {
            let stats = match &threshold.group {
                None => Some(&global),
                Some(name) => groups.iter().find(|(group, _)| group == name).map(|(_, s)| s),
            };
            let observed = stats.and_then(|stats| threshold.observe(stats));
            ThresholdResult {
                metric: threshold.metric.clone(),
                group: threshold.group.clone(),
                expression: threshold.expression.clone(),
                field: threshold.field(),
                observed,
                ok: observed.is_some_and(|value| threshold.op.holds(value, threshold.limit)),
            }
        })
        .collect();
    let validity = check_validity(results, &config.validity, &config.traffic_groups);

    let verdict = if validity.iter().any(|check| !check.ok) {
        Verdict::InvalidRun
    } else if thresholds.iter().any(|threshold| !threshold.ok) {
        Verdict::ThresholdsFailed
    } else {
        Verdict::Pass
    };
    results.verdict = verdict;
    results.thresholds = thresholds;
    results.validity = validity;
    verdict
}

/// Run the validity checks on every traffic group they apply to
///
/// Warns about every group with kernel or hardware timestamps that split
/// some, but not all, of its completed requests' round trips, and about
/// one that split none if "kernel_split" does not run.
fn check_validity(
    results: &DetailedExperimentResults,
    validity: &ValidityConfig,
    groups: &[TrafficGroupConfig],
) -> Vec<ValidityResult> {
    let checks = validity.checks();
    let mut outcomes = Vec::new();
    for (id, group) in groups.iter().enumerate() {
        let stats = results.traffic_groups.iter().find(|g| g.id == id).map(|g| &g.stats);
        // A closed loop sends when a response frees its pipeline, so it has
        // no schedule to fall behind.
        let closed_loop = matches!(group.traffic_policy, PolicyConfig::ClosedLoop);
        for &(check, limit) in &checks {
            if check == ValidityCheck::KernelSplit || closed_loop {
                continue;
            }
            let requests = stats.map(|stats| &stats.requests);
            outcomes.push(check_group_validity(check, limit, &group.name, requests));
        }
        let Some(breakdown) = stats.map(|stats| &stats.latency_breakdown) else {
            continue;
        };
        let Some(split) = kernel_split(group, breakdown) else {
            continue;
        };
        let checked = checks.iter().any(|&(check, _)| check == ValidityCheck::KernelSplit);
        if let Some(reason) = &split.reason {
            if split.observed > 0.0 || !checked {
                tracing::warn!("Traffic group '{}': {}", group.name, reason);
            } else {
                tracing::error!("Invalid run: traffic group '{}': {}", group.name, reason);
            }
        }
        if checked {
            outcomes.push(ValidityResult {
                check: ValidityCheck::KernelSplit,
                group: Some(group.name.clone()),
                observed: Some(split.observed),
                limit: 0.0,
                ok: split.observed > 0.0,
                reason: split.reason.filter(|_| split.observed == 0.0),
            });
        }
    }
    outcomes
}

/// A group's kernel split: the fraction of its completed requests that had
/// one, and unless all had one, why the others had none
struct KernelSplitOutcome {
    observed: f64,
    reason: Option<String>,
}

/// The kernel split of a group with kernel or hardware timestamps that
/// completed requests; `None` for any other group
fn kernel_split(
    group: &TrafficGroupConfig,
    breakdown: &LatencyBreakdownStats,
) -> Option<KernelSplitOutcome> {
    if !group.timestamps.is_kernel() {
        return None;
    }
    let completed = breakdown.service_us.sample_count;
    let split = breakdown.kernel_split_count;
    if completed == 0 {
        return None;
    }
    let observed = split as f64 / completed as f64;
    if split == completed {
        return Some(KernelSplitOutcome { observed, reason: None });
    }
    let missing = breakdown.kernel_split_missing.unwrap_or_default();
    let hardware = matches!(group.timestamps, TimestampSourceConfig::Hardware(_));
    let interface = match &group.timestamps {
        TimestampSourceConfig::Hardware(interface) => interface.as_str(),
        _ => "<interface>",
    };
    let mut fixes = Vec::new();
    if missing.tx > 0 {
        fixes.push(if hardware {
            format!(
                "no TX timestamps: the NIC stamps no transmitted packet; check that \
                 `ethtool -T {interface}` lists hardware-transmit and that its hardware \
                 timestamping is on (`hwstamp_ctl -i {interface}` reports tx_type 1)"
            )
        } else {
            format!(
                "no TX timestamps: the interface lacks software-transmit timestamping (its \
                 driver never calls skb_tx_timestamp()); check `ethtool -T {interface}` on \
                 the interface that reaches the target"
            )
        });
    }
    if missing.rx > 0 {
        fixes.push(if hardware {
            format!(
                "no RX timestamps: the NIC's RX filter skips these packets; check that \
                 `ethtool -T {interface}` lists hardware-receive and that the rx_filter \
                 `hwstamp_ctl -i {interface}` reports covers them"
            )
        } else {
            format!(
                "no RX timestamps: check that `ethtool -T {interface}` lists software-receive \
                 on the interface that reaches the target"
            )
        });
    }
    if missing.clock_mismatch > 0 {
        fixes.push(
            "TX and RX timestamps of different clocks: one direction was stamped by the NIC, \
             the other by the kernel"
                .to_string(),
        );
    }
    let causes = format!(
        "{} lacked a kernel TX timestamp, {} a kernel RX timestamp, and {} had TX and RX \
         timestamps of different clocks",
        missing.tx, missing.rx, missing.clock_mismatch
    );
    let reason = if split == 0 {
        format!(
            "none of the {completed} completed requests had a kernel split with timestamps = \
             {}: {causes}. Likely fix: {}",
            group.timestamps,
            fixes.join("; ")
        )
    } else {
        format!(
            "{split} of the {completed} completed requests had a kernel split with timestamps \
             = {}; of the others, {causes}",
            group.timestamps
        )
    };
    Some(KernelSplitOutcome { observed, reason: Some(reason) })
}

/// Run one validity check on a group's request counts
fn check_group_validity(
    check: ValidityCheck,
    limit: f64,
    group: &str,
    requests: Option<&RequestCounts>,
) -> ValidityResult {
    let outcome = |observed: Option<f64>, reason: Option<String>| ValidityResult {
        check,
        group: Some(group.to_string()),
        observed,
        limit,
        ok: reason.is_none(),
        reason,
    };
    let Some(requests) = requests else {
        return outcome(None, Some("the group recorded no requests".into()));
    };
    let (count, total) = match check {
        ValidityCheck::Unsent => (requests.unsent, requests.sent + requests.unsent),
        ValidityCheck::LateSends => (requests.late_sends, requests.sent),
        ValidityCheck::KernelSplit => unreachable!("check_validity checks kernel splits"),
    };
    let observed = if total == 0 {
        0.0
    } else {
        count as f64 / total as f64
    };
    if observed <= limit {
        return outcome(Some(observed), None);
    }
    let reason = match check {
        ValidityCheck::Unsent => format!(
            "{count}{bound} of {total} intended sends never went out ({}, limit {}): the \
             pipelines were full or the generator fell behind, so the configured rate was \
             not applied",
            percent(observed),
            percent(limit),
            bound = if requests.unsent_capped {
                " (a lower bound)"
            } else {
                ""
            },
        ),
        ValidityCheck::LateSends => format!(
            "{count} of {total} sends lagged their intended time by more than {} µs ({}, \
             limit {}): the pipelines were full or the generator fell behind its schedule",
            LATE_SEND_THRESHOLD.as_micros(),
            percent(observed),
            percent(limit),
        ),
        ValidityCheck::KernelSplit => unreachable!("check_validity checks kernel splits"),
    };
    outcome(Some(observed), Some(reason))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};

    /// A validated profile with a fixed-rate group "api" and a closed-loop
    /// group "batch", followed by `extra` TOML
    fn profile(extra: &str) -> Result<ProfileConfig> {
        let text = format!(
            r#"
[experiment]
name = "thresholds"
duration = "1s"

[[traffic_groups]]
name = "api"
target = "127.0.0.1:9"
protocol = "xylem-echo"
threads = [0]
connections_per_thread = 1
traffic_policy = {{ type = "fixed-rate", rate = 1000.0 }}

[[traffic_groups]]
name = "batch"
target = "127.0.0.1:9"
protocol = "xylem-echo"
threads = [1]
connections_per_thread = 1
traffic_policy = {{ type = "closed-loop" }}

[output]
file = "unused"

{extra}
"#
        );
        let mut config: ProfileConfig = toml::from_str(&text)?;
        config.validate()?;
        Ok(config)
    }

    /// Stats of the test profile: group "api" completed 100 requests of
    /// 1..=100 µs, each with a kernel split of a 1 µs host stack, group
    /// "batch" 10 requests of 1..=10 ms with user-space timestamps only
    fn stats() -> TupleStatsCollector {
        use xylem_core::stats::{KernelSplit, LatencyBreakdown};
        let mut stats = TupleStatsCollector::new(SamplingPolicy::default(), Duration::from_secs(1));
        stats.register_group_policy(0, SamplingPolicy::default());
        stats.register_group_policy(1, SamplingPolicy::default());
        for us in 1..=100 {
            let latency = Duration::from_micros(us);
            stats.record_latency(0, 0, latency);
            let kernel = KernelSplit {
                clock: xylem_transport::KernelClock::Realtime,
                network_server: latency,
                host_stack: Duration::from_micros(1),
            };
            stats.record_breakdown(
                0,
                &LatencyBreakdown {
                    service: latency,
                    kernel: Some(kernel),
                    miss: None,
                },
            );
        }
        for ms in 1..=10 {
            let latency = Duration::from_millis(ms);
            stats.record_latency(1, 0, latency);
            stats.record_breakdown(1, &LatencyBreakdown::user_space(latency));
        }
        stats
    }

    /// Results of `config`, from [`stats`]. Every request was sent on time.
    fn results(config: &ProfileConfig) -> DetailedExperimentResults {
        results_of(config, &stats())
    }

    /// Results of `config` from `stats`, with "api" and "batch" sending 100
    /// and 10 requests
    fn results_of(
        config: &ProfileConfig,
        stats: &TupleStatsCollector,
    ) -> DetailedExperimentResults {
        let mut results = DetailedExperimentResults::from_tuple_stats(
            "thresholds".into(),
            None,
            None,
            "127.0.0.1:9".into(),
            "xylem-echo".into(),
            "tcp".into(),
            Duration::from_secs(1),
            stats,
            &config.traffic_groups,
            false,
        );
        for (group, sent) in results.traffic_groups.iter_mut().zip([100, 10]) {
            group.stats.requests.sent = sent;
        }
        results.global.requests.sent = 110;
        results
    }

    #[test]
    fn empty_latency_summaries_are_unavailable_but_zero_samples_are_valid() {
        let config = profile(
            r#"
[thresholds]
latency = ["mean < 1us", "p50 < 1us", "p95 < 1us", "p99 < 1us", "p99.9 < 1us", "p99.99 < 1us", "p99.999 < 1us"]
"latency.p99_us" = ["!= 1us"]
"latency.sample_count" = [">= 0"]
"latency{group=api}" = ["mean < 1us", "p99 < 1us"]
error_latency = ["mean < 1us", "p99 < 1us"]
"error_latency{group=api}" = ["mean < 1us", "p99 < 1us"]
"send_lag.late_threshold_us" = ["> 0us"]
"#,
        )
        .unwrap();
        let mut results = results(&config);
        results.global.latency = LatencyStats::default();
        results.global.error_latency = LatencyStats::default();
        results.traffic_groups[0].stats.latency = LatencyStats::default();
        results.traffic_groups[0].stats.error_latency = LatencyStats::default();

        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        for threshold in &results.thresholds {
            if threshold.metric == "latency.sample_count"
                || threshold.metric == "send_lag.late_threshold_us"
            {
                assert!(threshold.ok, "{threshold:?}");
            } else {
                assert_eq!(threshold.observed, None, "{threshold:?}");
                assert!(!threshold.ok, "{threshold:?}");
            }
        }

        // A zero duration with a recorded sample is a real value. Give each
        // scope a different count to verify that it uses its own summary.
        results.global.latency.sample_count = 1;
        results.global.latency.percentile_sample_count = Some(1);
        results.traffic_groups[0].stats.error_latency.sample_count = 1;
        results.traffic_groups[0].stats.error_latency.percentile_sample_count = Some(1);
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        for threshold in &results.thresholds {
            let available = (threshold.metric.starts_with("latency") && threshold.group.is_none())
                || (threshold.metric == "error_latency"
                    && threshold.group.as_deref() == Some("api"))
                || threshold.metric == "send_lag.late_threshold_us";
            if threshold.metric == "latency.sample_count" {
                assert_eq!(threshold.observed, Some(1.0));
                assert!(threshold.ok);
            } else if available {
                assert!(threshold.observed.is_some(), "{threshold:?}");
                if threshold.metric != "send_lag.late_threshold_us" {
                    assert_eq!(threshold.observed, Some(0.0), "{threshold:?}");
                }
                assert!(threshold.ok, "{threshold:?}");
            } else {
                assert_eq!(threshold.observed, None, "{threshold:?}");
                assert!(!threshold.ok, "{threshold:?}");
            }
        }

        results.global.error_latency.sample_count = 1;
        results.global.error_latency.percentile_sample_count = Some(1);
        results.traffic_groups[0].stats.latency.sample_count = 1;
        results.traffic_groups[0].stats.latency.percentile_sample_count = Some(1);
        assert_eq!(evaluate(&mut results, &config), Verdict::Pass);
        assert!(results.thresholds.iter().all(|threshold| threshold.ok));
    }

    #[test]
    fn percentile_thresholds_require_retained_samples() {
        let mut config = profile(
            r#"
[thresholds]
latency = ["p50 < 1ms", "p95 < 1ms", "p99 < 1ms", "p99.9 < 1ms", "p99.99 < 1ms", "p99.999 < 1ms", "mean == 5us", "min == 0us", "max == 10us"]
"latency.p99_us" = ["!= 1ms"]
"latency{group=api}" = ["p99 < 1ms", "mean == 5us"]
error_latency = ["p99 < 1ms", "mean == 5us"]
"error_latency{group=api}" = ["p99 < 1ms", "mean == 5us"]
"latency.percentile_sample_count" = ["== 0"]
"requests.sent" = ["> 0"]
"#,
        )
        .unwrap();
        let policy = SamplingPolicy::Limited { max_samples: 0, rate: 1.0 };
        config.traffic_groups[0].sampling_policy = policy.clone();
        config.validate().unwrap();
        let mut collector = TupleStatsCollector::new(policy.clone(), Duration::from_secs(1));
        collector.register_group_policy(0, policy);
        for latency in [Duration::ZERO, Duration::from_micros(10)] {
            collector.record_latency(0, 0, latency);
            collector.record_error_reply(0, 0, "error", latency);
        }
        let mut results = results_of(&config, &collector);
        for summary in [
            &results.global.latency,
            &results.global.error_latency,
            &results.traffic_groups[0].stats.latency,
            &results.traffic_groups[0].stats.error_latency,
        ] {
            assert_eq!((summary.sample_count, summary.percentile_sample_count), (2, Some(0)));
            assert_eq!(summary.p99_us, 0.0);
            assert_eq!(summary.mean_us, 5.0);
        }
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        for threshold in &results.thresholds {
            if threshold.field.ends_with("_us")
                && threshold.field.split('.').next_back().unwrap().starts_with('p')
            {
                assert_eq!(threshold.observed, None, "{threshold:?}");
                assert!(!threshold.ok, "{threshold:?}");
            } else {
                assert!(threshold.ok, "{threshold:?}");
            }
        }
    }

    #[test]
    fn percentile_thresholds_treat_unknown_counts_as_unavailable() {
        let config = profile(
            r#"
[thresholds]
latency = ["p99 < 1us", "mean == 0us"]
"latency{group=api}" = ["p99 < 1us", "mean == 0us"]
error_latency = ["p99 < 1us", "mean == 0us"]
"error_latency{group=api}" = ["p99 < 1us", "mean == 0us"]
"#,
        )
        .unwrap();
        let policy = SamplingPolicy::Limited { max_samples: 1, rate: 1.0 };
        let mut collector = TupleStatsCollector::new(policy.clone(), Duration::from_secs(1));
        collector.register_group_policy(0, policy);
        collector.record_latency(0, 0, Duration::ZERO);
        collector.record_error_reply(0, 0, "error", Duration::ZERO);
        let mut results = results_of(&config, &collector);
        assert_eq!(evaluate(&mut results, &config), Verdict::Pass);
        assert!(results.thresholds.iter().all(|t| t.observed == Some(0.0) && t.ok));

        // Deserialize older summaries without the estimator count, and also
        // exercise explicit null and zero separately in the other scopes.
        let mut legacy = serde_json::to_value(&results.global.latency).unwrap();
        legacy.as_object_mut().unwrap().remove("percentile_sample_count");
        results.global.latency = serde_json::from_value(legacy).unwrap();
        let mut legacy_error = serde_json::to_value(&results.global.error_latency).unwrap();
        legacy_error["percentile_sample_count"] = serde_json::Value::Null;
        results.global.error_latency = serde_json::from_value(legacy_error).unwrap();
        results.traffic_groups[0].stats.latency.percentile_sample_count = Some(0);
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        for threshold in &results.thresholds {
            if threshold.field.ends_with("p99_us") {
                let known_positive =
                    threshold.metric == "error_latency" && threshold.group.is_some();
                assert_eq!(threshold.observed, known_positive.then_some(0.0), "{threshold:?}");
                assert_eq!(threshold.ok, known_positive, "{threshold:?}");
            } else {
                assert_eq!(threshold.observed, Some(0.0), "{threshold:?}");
                assert!(threshold.ok, "{threshold:?}");
            }
        }
    }

    #[test]
    fn legacy_numeric_latency_placeholders_require_samples() {
        let mut config = profile("").unwrap();
        let mut thresholds = BTreeMap::new();
        for summary in ["latency", "error_latency"] {
            for filter in ["", "{group=api}"] {
                thresholds.insert(
                    format!("{summary}{filter}"),
                    ["min", "max", "std_dev", "confidence_interval"]
                        .iter()
                        .map(|aggregation| format!("{aggregation} < 1us"))
                        .collect(),
                );
                for field in ["sample_count", "clamped_samples"] {
                    thresholds.insert(format!("{summary}.{field}{filter}"), vec![">= 0".into()]);
                }
                for field in ["min_us", "max_us", "std_dev_us", "confidence_interval_us"] {
                    thresholds.insert(format!("{summary}.{field}{filter}"), vec!["< 1us".into()]);
                }
            }
        }
        thresholds.insert("latency.max_us".into(), vec!["!= 1us".into()]);
        thresholds.insert("send_lag.late_threshold_us".into(), vec!["> 0us".into()]);
        config.threshold_checks = parse_thresholds(&thresholds, &config.traffic_groups).unwrap();

        // Legacy JSON may contain numeric placeholders even in optional fields.
        let legacy: LatencyStats = serde_json::from_value(serde_json::json!({
            "min_us": 0.0, "max_us": 0.0, "mean_us": 0.0,
            "p50_us": 0.0, "p95_us": 0.0, "p99_us": 0.0,
            "p999_us": 0.0, "p9999_us": 0.0, "p99999_us": 0.0,
            "std_dev_us": 0.0, "confidence_interval_us": 0.0,
            "sample_count": 0, "clamped_samples": 0
        }))
        .unwrap();
        let mut results = results(&config);
        results.global.latency = legacy.clone();
        results.global.error_latency = legacy.clone();
        results.traffic_groups[0].stats.latency = legacy.clone();
        results.traffic_groups[0].stats.error_latency = legacy;

        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        for threshold in &results.thresholds {
            if threshold.field.ends_with("sample_count")
                || threshold.field.ends_with("clamped_samples")
                || threshold.metric == "send_lag.late_threshold_us"
            {
                assert!(threshold.observed.is_some() && threshold.ok, "{threshold:?}");
            } else {
                assert_eq!(threshold.observed, None, "{threshold:?}");
                assert!(!threshold.ok, "{threshold:?}");
            }
        }

        // A zero legacy count is inconclusive even if the stored fields are
        // nonzero; do not infer the presence of observations from their values.
        let group = &mut results.traffic_groups[0].stats;
        for summary in [
            &mut results.global.latency,
            &mut results.global.error_latency,
            &mut group.latency,
            &mut group.error_latency,
        ] {
            summary.min_us = Some(7.0);
            summary.max_us = Some(8.0);
            summary.std_dev_us = Some(9.0);
            summary.confidence_interval_us = Some(10.0);
        }
        let before = serde_json::to_value(&results.global.latency).unwrap();
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        assert_eq!(serde_json::to_value(&results.global.latency).unwrap(), before);
        for threshold in &results.thresholds {
            if threshold.field.ends_with("_us") && threshold.metric != "send_lag.late_threshold_us"
            {
                assert_eq!(threshold.observed, None, "{threshold:?}");
            }
        }
        let group = &mut results.traffic_groups[0].stats;
        for summary in [
            &mut results.global.latency,
            &mut results.global.error_latency,
            &mut group.latency,
            &mut group.error_latency,
        ] {
            summary.min_us = Some(0.0);
            summary.max_us = Some(0.0);
            summary.std_dev_us = Some(0.0);
            summary.confidence_interval_us = Some(0.0);
        }

        // A reported count of one makes min and max available, but not legacy
        // deviation or CI placeholders, for either spelling or scope.
        results.global.latency.sample_count = 1;
        results.traffic_groups[0].stats.error_latency.sample_count = 1;
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        for threshold in &results.thresholds {
            if threshold.field.ends_with("_us") && threshold.metric != "send_lag.late_threshold_us"
            {
                let has_sample = (threshold.group.is_none()
                    && threshold.field.starts_with("latency."))
                    || (threshold.group.as_deref() == Some("api")
                        && threshold.field.starts_with("error_latency."));
                let is_range =
                    threshold.field.ends_with(".min_us") || threshold.field.ends_with(".max_us");
                let available = has_sample && is_range;
                assert_eq!(threshold.observed, available.then_some(0.0), "{threshold:?}");
                assert_eq!(threshold.ok, available, "{threshold:?}");
            }
        }

        results.global.error_latency.sample_count = 1;
        results.traffic_groups[0].stats.latency.sample_count = 1;
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        for threshold in &results.thresholds {
            if threshold.field.ends_with("_us") && threshold.metric != "send_lag.late_threshold_us"
            {
                let is_range =
                    threshold.field.ends_with(".min_us") || threshold.field.ends_with(".max_us");
                assert_eq!(threshold.observed, is_range.then_some(0.0), "{threshold:?}");
                assert_eq!(threshold.ok, is_range, "{threshold:?}");
            }
        }
        // Nonzero legacy placeholders are equally unavailable at count one.
        let group = &mut results.traffic_groups[0].stats;
        for summary in [
            &mut results.global.latency,
            &mut results.global.error_latency,
            &mut group.latency,
            &mut group.error_latency,
        ] {
            summary.std_dev_us = Some(9.0);
            summary.confidence_interval_us = Some(10.0);
        }
        let before = serde_json::to_value(&results.global.error_latency).unwrap();
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        assert_eq!(serde_json::to_value(&results.global.error_latency).unwrap(), before);
        for threshold in &results.thresholds {
            if threshold.field.ends_with(".std_dev_us")
                || threshold.field.ends_with(".confidence_interval_us")
            {
                assert_eq!(threshold.observed, None, "{threshold:?}");
                assert!(!threshold.ok, "{threshold:?}");
            }
        }
        let group = &mut results.traffic_groups[0].stats;
        for summary in [
            &mut results.global.latency,
            &mut results.global.error_latency,
            &mut group.latency,
            &mut group.error_latency,
        ] {
            summary.std_dev_us = Some(0.0);
            summary.confidence_interval_us = Some(0.0);
        }
        results.global.error_latency.sample_count = 0;
        results.traffic_groups[0].stats.latency.sample_count = 0;

        // With a reported count of two, numeric zeros are available for all
        // four metrics; real measured zeros are covered separately below.
        results.global.latency.sample_count = 2;
        results.traffic_groups[0].stats.error_latency.sample_count = 2;
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        for threshold in &results.thresholds {
            if threshold.field.ends_with("_us") && threshold.metric != "send_lag.late_threshold_us"
            {
                let available = (threshold.group.is_none()
                    && threshold.field.starts_with("latency."))
                    || (threshold.group.as_deref() == Some("api")
                        && threshold.field.starts_with("error_latency."));
                assert_eq!(threshold.observed, available.then_some(0.0), "{threshold:?}");
                assert_eq!(threshold.ok, available, "{threshold:?}");
            }
        }
        results.global.error_latency.sample_count = 2;
        results.traffic_groups[0].stats.latency.sample_count = 2;
        assert_eq!(evaluate(&mut results, &config), Verdict::Pass);
        assert!(results.thresholds.iter().all(|threshold| threshold.ok));
    }

    #[test]
    fn deviation_thresholds_require_two_actual_samples_and_preserve_nulls() {
        let mut config = profile("").unwrap();
        let mut checks = BTreeMap::new();
        for summary in ["latency", "error_latency"] {
            for filter in ["", "{group=api}"] {
                checks.insert(
                    format!("{summary}{filter}"),
                    ["min", "max", "std_dev", "confidence_interval"]
                        .iter()
                        .map(|name| format!("{name} == 0us"))
                        .collect(),
                );
                for field in ["std_dev_us", "confidence_interval_us"] {
                    checks.insert(format!("{summary}.{field}{filter}"), vec!["== 0us".into()]);
                }
            }
        }
        config.threshold_checks = parse_thresholds(&checks, &config.traffic_groups).unwrap();
        let mut collector =
            TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));
        for count in 1..=2 {
            collector.record_latency(0, 0, Duration::ZERO);
            collector.record_error_reply(0, 0, "error", Duration::ZERO);
            let mut results = results_of(&config, &collector);
            assert_eq!(
                evaluate(&mut results, &config),
                if count == 1 {
                    Verdict::ThresholdsFailed
                } else {
                    Verdict::Pass
                }
            );
            for threshold in &results.thresholds {
                let needs_two = threshold.field.ends_with(".std_dev_us")
                    || threshold.field.ends_with(".confidence_interval_us");
                let expected = (!(count == 1 && needs_two)).then_some(0.0);
                assert_eq!(threshold.observed, expected, "{threshold:?}");
                assert_eq!(threshold.ok, expected.is_some(), "{threshold:?}");
            }
            if count == 2 {
                let group = &mut results.traffic_groups[0].stats;
                for summary in [
                    &mut results.global.latency,
                    &mut results.global.error_latency,
                    &mut group.latency,
                    &mut group.error_latency,
                ] {
                    summary.std_dev_us = None;
                    summary.confidence_interval_us = None;
                }
                assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
                for threshold in &results.thresholds {
                    let needs_two = threshold.field.ends_with(".std_dev_us")
                        || threshold.field.ends_with(".confidence_interval_us");
                    assert_eq!(threshold.observed, (!needs_two).then_some(0.0), "{threshold:?}");
                    assert_eq!(threshold.ok, !needs_two, "{threshold:?}");
                }
            }
        }

        let mut collector =
            TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));
        for duration in [Duration::ZERO, Duration::from_micros(10)] {
            collector.record_latency(0, 0, duration);
            collector.record_error_reply(0, 0, "error", duration);
        }
        let mut results = results_of(&config, &collector);
        let std_dev = results.global.latency.std_dev_us.unwrap();
        let ci = results.global.latency.confidence_interval_us.unwrap();
        assert!(std_dev > 7.0 && ci > 9.0);
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        for threshold in &results.thresholds {
            let expected = if threshold.field.ends_with(".min_us") {
                0.0
            } else if threshold.field.ends_with(".max_us") {
                10.0
            } else if threshold.field.ends_with(".std_dev_us") {
                std_dev
            } else {
                ci
            };
            assert_eq!(threshold.observed, Some(expected), "{threshold:?}");
            assert_eq!(threshold.ok, expected == 0.0, "{threshold:?}");
        }
    }

    /// Parse the thresholds of one metric key against the test profile
    fn parse(key: &str, expressions: &[&str]) -> Result<Vec<Threshold>> {
        let config = profile("")?;
        let thresholds = BTreeMap::from([(
            key.to_string(),
            expressions.iter().map(|e| e.to_string()).collect(),
        )]);
        parse_thresholds(&thresholds, &config.traffic_groups)
    }

    /// The single threshold of one expression
    fn parse_one(key: &str, expression: &str) -> Threshold {
        let mut parsed = parse(key, &[expression]).unwrap_or_else(|e| panic!("{key} {e:#}"));
        assert_eq!(parsed.len(), 1);
        parsed.remove(0)
    }

    /// The error of parsing one expression
    fn parse_error(key: &str, expression: &str) -> String {
        match parse(key, &[expression]) {
            Ok(parsed) => panic!("{key} = {expression:?} parsed: {parsed:?}"),
            Err(error) => format!("{error:#}"),
        }
    }

    #[test]
    fn durations_take_a_unit_and_compare_in_microseconds() {
        for (expression, limit) in [
            ("p99 < 250us", 250.0),
            ("p99 < 250µs", 250.0),
            ("p99 < 250μs", 250.0),
            ("p99 < 800ns", 0.8),
            ("p99 < 1.5ms", 1_500.0),
            ("p99 < 2s", 2_000_000.0),
            ("p99<1e3us", 1_000.0),
            ("p99 < 5 ms", 5_000.0),
        ] {
            assert_eq!(parse_one("latency", expression).limit, limit, "{expression}");
        }
        assert!(parse_error("latency", "p99 < 250").contains("give a unit"));
        assert!(parse_error("latency", "p99 < 250m").contains("unknown unit 'm'"));
        assert!(parse_error("latency.p99_us", "< 250").contains("give a unit"));
        assert_eq!(parse_one("latency.p99_us", "< 250us").limit, 250.0);
    }

    #[test]
    fn counts_and_rates_take_plain_numbers() {
        assert_eq!(parse_one("requests.unsent", "== 0").limit, 0.0);
        assert_eq!(parse_one("throughput", "> 90000").limit, 90_000.0);
        assert_eq!(parse_one("requests.timed_out", "rate < 0.001").limit, 0.001);
        assert!(parse_error("requests.unsent", "== 0us").contains("plain number; remove 'us'"));
        assert!(parse_error("requests.timed_out", "rate < 1%").contains("remove '%'"));
        assert!(parse_error("throughput", "> 90000 rps").contains("remove 'rps'"));
    }

    #[test]
    fn aggregations_name_summary_fields() {
        for (aggregation, field) in [
            ("p50", "p50_us"),
            ("p95", "p95_us"),
            ("p99", "p99_us"),
            ("p99.9", "p999_us"),
            ("p999", "p999_us"),
            ("p99.99", "p9999_us"),
            ("p99.999", "p99999_us"),
            ("mean", "mean_us"),
            ("min", "min_us"),
            ("max", "max_us"),
            ("std_dev", "std_dev_us"),
        ] {
            let threshold = parse_one("latency", &format!("{aggregation} < 1ms"));
            assert_eq!(threshold.path, ["latency", field], "{aggregation}");
            assert!(!threshold.rate);
        }
        assert_eq!(parse_one("send_lag", "p99 < 20us").path, ["send_lag", "p99_us"]);
        let rate = parse_one("requests.timed_out", "rate < 0.001");
        assert_eq!((rate.path, rate.rate), (vec!["requests".into(), "timed_out".into()], true));
        assert_eq!(parse_one("throughput", "> 1").path, ["throughput_rps"]);
    }

    #[test]
    fn unknown_or_misplaced_aggregations_are_rejected() {
        let error = parse_error("latency", "p90 < 1ms");
        assert!(error.contains("unknown aggregation 'p90' for 'latency'"), "{error}");
        assert!(error.contains("p50, ") && error.contains("p99.9") && error.contains("mean"));
        assert!(parse_error("latency", "p99.x < 1ms").contains("unknown aggregation 'p99.x'"));
        assert!(parse_error("latency", "median < 1ms").contains("unknown aggregation"));
        assert!(parse_error("latency", "< 1ms").contains("'latency' is a summary"));
        assert!(parse_error("latency", "rate < 0.1").contains("unknown aggregation 'rate'"));
        assert!(parse_error("requests.unsent", "p99 == 0").contains("is a single value"));
        assert!(parse_error("throughput", "rate < 1").contains("'throughput' is not a count"));
        assert!(parse_error("latency.p99_us", "rate < 1").contains("is not a count"));
    }

    #[test]
    fn every_operator_compares() {
        for (op, holds) in [
            ("<", [true, false, false]),
            ("<=", [true, true, false]),
            (">", [false, false, true]),
            (">=", [false, true, true]),
            ("==", [false, true, false]),
            ("!=", [true, false, true]),
        ] {
            let threshold = parse_one("requests.sent", &format!("{op} 5"));
            for (observed, expected) in [4.0, 5.0, 6.0].into_iter().zip(holds) {
                assert_eq!(
                    threshold.op.holds(observed, threshold.limit),
                    expected,
                    "{observed} {op}"
                );
            }
        }
    }

    #[test]
    fn malformed_expressions_and_keys_are_rejected() {
        for (key, expression, expected) in [
            ("latency", "p99 250us", "no comparison"),
            ("latency", "p99 = 250us", "unknown operator"),
            ("latency", "p99 =< 250us", "unknown operator"),
            ("latency", "p99 ! 250us", "unknown operator"),
            ("latency", "p99 <", "is not a number"),
            ("latency", "p99 < < 250us", "is not a number"),
            ("latency", "p99 < fast", "is not a number"),
            ("requests.sent", "> inf", "is not a number"),
            ("requests.sent", "> 1e999", "out of range"),
            ("latency{group=api", "p99 < 1ms", "a filter ends with '}'"),
            ("latency{api}", "p99 < 1ms", "a filter is group=name"),
            ("latency{thread=0}", "p99 < 1ms", "unknown filter 'thread'"),
            ("latency{group=}", "p99 < 1ms", "names no group"),
            ("latency..p99_us", "< 1ms", "unknown metric"),
        ] {
            let error = parse_error(key, expression);
            assert!(error.contains(expected), "{key} = {expression:?}: {error}");
        }
        let error = parse("latency", &[]).unwrap_err();
        assert!(format!("{error:#}").contains("no expressions"));
    }

    #[test]
    fn unknown_metrics_and_groups_fail_at_load() {
        let error =
            format!("{:#}", profile("[thresholds]\n\"latncy\" = [\"p99 < 1ms\"]").unwrap_err());
        assert!(error.contains("unknown metric 'latncy'"), "{error}");
        // The error lists the metrics, including fields this module never names
        for metric in ["latency", "send_lag", "requests.late_responses", "throughput_mbps"] {
            assert!(error.contains(metric), "{metric}: {error}");
        }
        assert!(!error.contains("latency.p99_us"), "aggregations are not listed: {error}");

        let error = format!(
            "{:#}",
            profile("[thresholds]\n\"latency{group=api2}\" = [\"p99 < 1ms\"]").unwrap_err()
        );
        assert!(error.contains("unknown traffic group 'api2'; groups: api, batch"), "{error}");

        let error = format!("{:#}", profile("[thresholds]\nrequests = [\"> 0\"]").unwrap_err());
        assert!(error.contains("'requests' is a group of metrics"), "{error}");
        assert!(error.contains("requests.unsent"), "{error}");
        let error = format!(
            "{:#}",
            profile("[thresholds]\n\"requests.unsent_capped\" = [\"== 0\"]").unwrap_err()
        );
        assert!(error.contains("is not a number"), "{error}");

        // Every invalid threshold is reported at once
        let thresholds = r#"
[thresholds]
latency = ["p99 < 1", "p99 < 1ms", "p0 < 1ms"]
nope = ["> 1"]
"#;
        let error = format!("{:#}", profile(thresholds).unwrap_err());
        assert_eq!(error.lines().count(), 4, "{error}");
        assert!(error.starts_with("Invalid thresholds:"), "{error}");
    }

    #[test]
    fn a_group_filter_needs_a_unique_name() {
        let mut config = profile("").unwrap();
        config.traffic_groups[1].name = "api".into();
        let thresholds = BTreeMap::from([("latency{group=api}".into(), vec!["p99 < 1ms".into()])]);
        let error = parse_thresholds(&thresholds, &config.traffic_groups).unwrap_err();
        assert!(error.to_string().contains("used by 2 groups"), "{error:#}");
        // Quotes around a name are optional
        let threshold = parse_one("latency{ group = \"batch\" }", "p99 < 1ms");
        assert_eq!((threshold.metric, threshold.group), ("latency".into(), Some("batch".into())));
    }

    /// Every numeric path in `value`
    fn numeric_paths(value: &Value, prefix: &[String], paths: &mut Vec<Vec<String>>) {
        match value {
            Value::Object(fields) => {
                for (name, field) in fields {
                    let mut path = prefix.to_vec();
                    path.push(name.clone());
                    numeric_paths(field, &path, paths);
                }
            }
            Value::Number(_) => paths.push(prefix.to_vec()),
            _ => {}
        }
    }

    #[test]
    fn the_probe_has_every_stats_field_as_a_number() {
        // A null hides the fields of an Option; a probe field must be Some.
        fn assert_no_null(value: &Value, path: &str) {
            match value {
                Value::Null => panic!("probe field {path} is null; set it in probe_stats"),
                Value::Object(fields) => {
                    fields.iter().for_each(|(name, v)| assert_no_null(v, &format!("{path}.{name}")))
                }
                Value::Array(items) => items.iter().for_each(|v| assert_no_null(v, path)),
                _ => {}
            }
        }
        let (global, group) = probe_stats();
        assert_no_null(&global, "global");
        assert_no_null(&group, "group");
        // Results write every field the probe has, so its metrics exist in results
        let config = profile("").unwrap();
        let results = results(&config);
        for (probe, stats) in [
            (&global, serde_json::to_value(&results.global).unwrap()),
            (&group, serde_json::to_value(&results.traffic_groups[0].stats).unwrap()),
        ] {
            let mut paths = Vec::new();
            numeric_paths(probe, &[], &mut paths);
            for path in paths {
                // clamped_samples is written only for a bounded HDR histogram,
                // kernel_split_missing only with kernel or hardware timestamps
                if path.last().unwrap() != "clamped_samples"
                    && !path.contains(&"kernel_split_missing".to_string())
                {
                    assert!(lookup(&stats, &path).is_some(), "{path:?}");
                }
            }
        }
    }

    #[test]
    fn error_and_breakdown_fields_are_metrics() {
        let path =
            |names: &[&str]| -> Vec<String> { names.iter().map(|n| n.to_string()).collect() };
        for (key, expression, expected, rate) in [
            ("requests.errors", "== 0", path(&["requests", "errors"]), false),
            ("requests.errors", "rate < 0.01", path(&["requests", "errors"]), true),
            ("requests.errors{group=api}", "rate < 0.01", path(&["requests", "errors"]), true),
            ("error_latency", "p99 < 1ms", path(&["error_latency", "p99_us"]), false),
            ("error_latency", "mean < 1ms", path(&["error_latency", "mean_us"]), false),
            (
                "error_latency{group=api}",
                "max < 1ms",
                path(&["error_latency", "max_us"]),
                false,
            ),
            (
                "error_latency.sample_count",
                "> 0",
                path(&["error_latency", "sample_count"]),
                false,
            ),
            (
                "latency_breakdown.service_us",
                "p99.9 < 1ms",
                path(&["latency_breakdown", "service_us", "p999_us"]),
                false,
            ),
            (
                "latency_breakdown.host_stack_us",
                "mean < 20us",
                path(&["latency_breakdown", "host_stack_us", "mean_us"]),
                false,
            ),
            (
                "latency_breakdown.network_server_us{group=api}",
                "p99 < 1ms",
                path(&["latency_breakdown", "network_server_us", "p99_us"]),
                false,
            ),
            (
                "latency_breakdown.network_server_us",
                "max < 1ms",
                path(&["latency_breakdown", "network_server_us", "max_us"]),
                false,
            ),
            (
                "latency_breakdown.kernel_split_count",
                "> 0",
                path(&["latency_breakdown", "kernel_split_count"]),
                false,
            ),
            (
                "latency_breakdown.kernel_split_count",
                "rate > 0.5",
                path(&["latency_breakdown", "kernel_split_count"]),
                true,
            ),
        ] {
            let threshold = parse_one(key, expression);
            assert_eq!((&threshold.path, threshold.rate), (&expected, rate), "{key}");
        }
        // A breakdown part is a summary of durations
        let error = parse_error("latency_breakdown.service_us", "p99 < 1");
        assert!(error.contains("give a unit"), "{error}");
        let error = parse_error("latency_breakdown.host_stack_us", "p90 < 1ms");
        for aggregation in ["mean", "p50", "p99", "p99.9", "max"] {
            assert!(error.contains(aggregation), "{aggregation}: {error}");
        }
        assert!(parse_error("latency_breakdown", "> 0").contains("is a group of metrics"));
        assert!(parse_error("latency_breakdown.clock", "> 0").contains("is not a number"));
        // Errors by kind are keyed by the kinds a run meets
        for key in ["requests.errors_by_kind", "requests.errors_by_kind.WRONGTYPE"] {
            let error = parse_error(key, "> 0");
            assert!(error.contains("cannot be checked at load; use 'requests.errors'"), "{error}");
        }
        assert!(parse_error("requests.errors_by_kinds", "> 0").contains("unknown metric"));
        // The metrics list names them
        let error = parse_error("nope", "> 0");
        for metric in [
            "requests.errors,",
            "error_latency,",
            "latency_breakdown.kernel_split_count",
            "latency_breakdown.service_us,",
            "latency_breakdown.host_stack_us,",
            "latency_breakdown.network_server_us",
        ] {
            assert!(error.contains(metric), "{metric}: {error}");
        }
        assert!(!error.contains("errors_by_kind"), "{error}");
    }

    #[test]
    fn error_and_breakdown_thresholds_evaluate_against_the_results() {
        let mut stats = stats();
        for _ in 0..2 {
            stats.record_error_reply(0, 0, "WRONGTYPE", Duration::from_millis(5));
        }
        let thresholds = r#"
[thresholds]
"requests.errors" = ["== 2", "rate < 0.02", "rate < 0.01"]
"requests.errors{group=api}" = ["rate == 0.02"]
"requests.errors{group=batch}" = ["== 0", "rate == 0"]
error_latency = ["p99 > 4ms", "max < 1ms"]
"latency_breakdown.network_server_us{group=api}" = ["max == 100us", "p99 < 99us"]
"latency_breakdown.host_stack_us" = ["max == 1us"]
"latency_breakdown.host_stack_us{group=batch}" = ["max < 1s"]
"latency_breakdown.kernel_split_count" = ["== 100"]
"latency_breakdown.service_us{group=batch}" = ["max == 10ms"]
"#;
        let config = profile(thresholds).unwrap();
        let mut results = results_of(&config, &stats);
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        // Percentiles come from HDR histograms, within 0.1%; means and
        // maxima are exact
        let close = |observed: Option<f64>, expected: f64| {
            observed.is_some_and(|observed| (observed - expected).abs() <= expected * 0.001)
        };
        let expected: [(&str, &str, Option<f64>, bool); 14] = [
            ("error_latency", "p99 > 4ms", Some(5_000.0), true),
            ("error_latency", "max < 1ms", Some(5_000.0), false),
            ("latency_breakdown.host_stack_us", "max == 1us", Some(1.0), true),
            // A group without a kernel split has no host stack value
            ("latency_breakdown.host_stack_us{group=batch}", "max < 1s", None, false),
            ("latency_breakdown.kernel_split_count", "== 100", Some(100.0), true),
            (
                "latency_breakdown.network_server_us{group=api}",
                "max == 100us",
                Some(100.0),
                true,
            ),
            (
                "latency_breakdown.network_server_us{group=api}",
                "p99 < 99us",
                Some(99.0),
                false,
            ),
            ("latency_breakdown.service_us{group=batch}", "max == 10ms", Some(10_000.0), true),
            ("requests.errors", "== 2", Some(2.0), true),
            // 2 errors of the 110 sent
            ("requests.errors", "rate < 0.02", Some(2.0 / 110.0), true),
            ("requests.errors", "rate < 0.01", Some(2.0 / 110.0), false),
            ("requests.errors{group=api}", "rate == 0.02", Some(0.02), true),
            ("requests.errors{group=batch}", "== 0", Some(0.0), true),
            ("requests.errors{group=batch}", "rate == 0", Some(0.0), true),
        ];
        assert_eq!(results.thresholds.len(), expected.len());
        for (threshold, (key, expression, observed, ok)) in results.thresholds.iter().zip(expected)
        {
            assert_eq!(
                (threshold.key().as_str(), threshold.expression.as_str()),
                (key, expression)
            );
            match observed {
                Some(observed) => assert!(close(threshold.observed, observed), "{threshold:?}"),
                None => assert_eq!(threshold.observed, None, "{threshold:?}"),
            }
            assert_eq!(threshold.ok, ok, "{threshold:?}");
        }
        // Error replies are counted, by kind, apart from the latency
        let api = &results.traffic_groups[0].stats;
        assert_eq!(api.requests.errors_by_kind.get("WRONGTYPE"), 2);
        assert_eq!(api.latency.sample_count, 100);
        assert_eq!(api.latency_breakdown.service_us.sample_count, 100);
    }

    #[test]
    fn every_numeric_stats_field_is_a_metric_without_naming_it() {
        let config = profile("").unwrap();
        let mut results = results(&config);
        // Exercise numeric error-latency fields with a sample present.
        results.traffic_groups[0].stats.error_latency.sample_count = 1;
        results.traffic_groups[0].stats.error_latency.percentile_sample_count = Some(1);
        let stats = serde_json::to_value(&results.traffic_groups[0].stats).unwrap();
        let mut paths = Vec::new();
        numeric_paths(&stats, &[], &mut paths);
        assert!(paths.len() > 20, "{paths:?}");

        // Compare each field with itself: ">= observed" holds exactly
        let mut thresholds = BTreeMap::new();
        for path in &paths {
            let value = lookup(&stats, path).unwrap().as_f64().unwrap();
            let unit = if path.last().unwrap().ends_with("_us") {
                "us"
            } else {
                ""
            };
            let key = format!("{}{{group=api}}", path.join("."));
            thresholds.insert(key, vec![format!(">= {value}{unit}"), format!("< {value}{unit}")]);
        }
        let mut config = config;
        config.threshold_checks = parse_thresholds(&thresholds, &config.traffic_groups).unwrap();
        evaluate(&mut results, &config);
        assert_eq!(results.thresholds.len(), 2 * paths.len());
        for pair in results.thresholds.chunks(2) {
            let expected =
                lookup(&stats, &pair[0].field.split('.').map(String::from).collect::<Vec<_>>())
                    .and_then(Value::as_f64);
            assert_eq!(pair[0].observed, expected, "{}", pair[0].field);
            assert!(pair[0].ok && !pair[1].ok, "{pair:?}");
        }
    }

    #[test]
    fn metrics_resolve_in_their_scope() {
        let config = profile(
            r#"
[thresholds]
"latency" = ["max < 1ms", "p99.9 >= 1ms"]
"latency{group=api}" = ["max <= 100us", "mean < 51us", "std_dev > 0us"]
"latency{group=batch}" = ["min >= 1ms"]
"requests.late_responses{group=api}" = ["rate == 0.05"]
"requests.timed_out" = ["rate < 0.001"]
"send_lag.sample_count" = ["== 0"]
"#,
        )
        .unwrap();
        let mut results = results(&config);
        results.traffic_groups[0].stats.requests.late_responses = 5;
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        let outcome: Vec<(String, &str, Option<f64>, bool)> = results
            .thresholds
            .iter()
            .map(|t| (t.key(), t.expression.as_str(), t.observed.map(|v| v.round()), t.ok))
            .collect();
        assert_eq!(
            outcome,
            [
                ("latency".into(), "max < 1ms", Some(10_000.0), false),
                // The HDR histogram keeps three significant digits
                ("latency".into(), "p99.9 >= 1ms", Some(10_002.0), true),
                ("latency{group=api}".into(), "max <= 100us", Some(100.0), true),
                ("latency{group=api}".into(), "mean < 51us", Some(51.0), true),
                ("latency{group=api}".into(), "std_dev > 0us", Some(29.0), true),
                ("latency{group=batch}".into(), "min >= 1ms", Some(1_000.0), true),
                ("requests.late_responses{group=api}".into(), "rate == 0.05", Some(0.0), true),
                ("requests.timed_out".into(), "rate < 0.001", Some(0.0), true),
                ("send_lag.sample_count".into(), "== 0", Some(0.0), true),
            ]
        );
        assert_eq!(results.thresholds[0].field, "latency.max_us");
        assert_eq!(results.thresholds[6].field, "requests.late_responses / requests.sent");
        assert_eq!(results.thresholds[6].observed, Some(0.05));
    }

    #[test]
    fn a_metric_without_a_value_fails() {
        let config = profile(
            r#"
[thresholds]
"send_lag" = ["p99 < 20us"]
"requests.timed_out{group=batch}" = ["rate < 0.001"]
"#,
        )
        .unwrap();
        let mut results = results(&config);
        // Nothing was sent on a schedule, so the send lag is null; group
        // "batch" sent nothing, so its rate has no denominator
        results.traffic_groups[1].stats.requests.sent = 0;
        assert_eq!(evaluate(&mut results, &config), Verdict::ThresholdsFailed);
        assert!(results.thresholds.iter().all(|t| t.observed.is_none() && !t.ok));
        assert_eq!(results.thresholds[0].observed_text(), "n/a");

        // A group without results has no values either
        results.traffic_groups.remove(1);
        evaluate(&mut results, &config);
        assert!(!results.thresholds[0].ok);
    }

    #[test]
    fn verdict_is_invalid_before_thresholds_failed_before_pass() {
        let passing = "[thresholds]\n\"latency{group=api}\" = [\"p50 < 1ms\"]";
        let failing = "[thresholds]\n\"latency{group=api}\" = [\"p50 < 1us\"]";
        for (thresholds, unsent, expected) in [
            ("", 0, Verdict::Pass),
            (passing, 0, Verdict::Pass),
            (failing, 0, Verdict::ThresholdsFailed),
            (passing, 50, Verdict::InvalidRun),
            (failing, 50, Verdict::InvalidRun),
        ] {
            let config = profile(thresholds).unwrap();
            let mut results = results(&config);
            results.traffic_groups[0].stats.requests.unsent = unsent;
            assert_eq!(evaluate(&mut results, &config), expected, "{thresholds} {unsent}");
            assert_eq!(results.verdict, expected);
            // Thresholds are recorded even when the run is invalid
            assert_eq!(results.thresholds.len(), usize::from(!thresholds.is_empty()));
        }
        assert_eq!(
            [Verdict::Pass, Verdict::ThresholdsFailed, Verdict::InvalidRun].map(Verdict::exit_code),
            [0, 2, 3]
        );
        assert_eq!(
            serde_json::to_value([Verdict::Pass, Verdict::ThresholdsFailed, Verdict::InvalidRun])
                .unwrap(),
            serde_json::json!(["pass", "thresholds_failed", "invalid_run"])
        );
    }

    #[test]
    fn validity_checks_rate_driven_groups() {
        let config = profile("").unwrap();
        let mut results = results(&config);
        // Within the default limits: 0.1% unsent, 1% late
        let api = &mut results.traffic_groups[0].stats.requests;
        (api.sent, api.unsent, api.late_sends) = (9_990, 10, 99);
        // The closed-loop group is exempt, whatever its counts
        results.traffic_groups[1].stats.requests.unsent = 1_000;
        assert_eq!(evaluate(&mut results, &config), Verdict::Pass);
        let checks: Vec<(String, Option<f64>, f64, bool)> =
            results.validity.iter().map(|c| (c.key(), c.observed, c.limit, c.ok)).collect();
        assert_eq!(
            checks,
            [
                ("unsent{group=api}".into(), Some(0.001), 0.001, true),
                ("late_sends{group=api}".into(), Some(99.0 / 9990.0), 0.01, true),
            ]
        );
        assert!(results.validity.iter().all(|c| c.reason.is_none()));

        let api = &mut results.traffic_groups[0].stats.requests;
        (api.unsent, api.late_sends, api.unsent_capped) = (11, 100, true);
        assert_eq!(evaluate(&mut results, &config), Verdict::InvalidRun);
        let reasons: Vec<&str> =
            results.validity.iter().map(|c| c.reason.as_deref().unwrap()).collect();
        assert!(
            reasons[0].starts_with(
                "11 (a lower bound) of 10001 intended sends never went out (0.11%, limit 0.10%)"
            ),
            "{}",
            reasons[0]
        );
        assert!(
            reasons[1].starts_with(
                "100 of 9990 sends lagged their intended time by more than 100 µs (1.00%"
            ),
            "{}",
            reasons[1]
        );
        let json = serde_json::to_value(&results.validity[0]).unwrap();
        assert_eq!(json["check"], "unsent");
        assert_eq!(json["group"], "api");
        assert_eq!(json["ok"], false);
    }

    /// Results of the test profile with `timestamps` on both groups, where
    /// "api" completed 10 requests split as `splits` says and "batch" none
    fn kernel_results(
        timestamps: TimestampSourceConfig,
        validity: &str,
        splits: &[Option<xylem_core::stats::SplitMiss>],
    ) -> (ProfileConfig, DetailedExperimentResults) {
        use xylem_core::stats::{KernelSplit, LatencyBreakdown};
        let mut config = profile(validity).unwrap();
        for group in &mut config.traffic_groups {
            group.timestamps = timestamps.clone();
        }
        let mut stats = TupleStatsCollector::new(SamplingPolicy::default(), Duration::from_secs(1));
        stats.register_group_policy(0, SamplingPolicy::default());
        stats.register_group_policy(1, SamplingPolicy::default());
        let latency = Duration::from_micros(10);
        for &miss in splits {
            stats.record_latency(0, 0, latency);
            let kernel = miss.is_none().then_some(KernelSplit {
                clock: xylem_transport::KernelClock::Realtime,
                network_server: latency,
                host_stack: Duration::ZERO,
            });
            stats.record_breakdown(0, &LatencyBreakdown { service: latency, kernel, miss });
        }
        let results = results_of(&config, &stats);
        (config, results)
    }

    #[test]
    fn a_kernel_timestamp_group_that_splits_nothing_makes_the_run_invalid() {
        use xylem_core::stats::SplitMiss::{ClockMismatch, Rx, Tx, TxAndRx};
        let splits =
            [Some(Tx); 7].into_iter().chain([Some(TxAndRx), Some(Rx), Some(ClockMismatch)]);
        let (config, mut results) =
            kernel_results(TimestampSourceConfig::Kernel, "", &splits.collect::<Vec<_>>());
        let verdict = evaluate(&mut results, &config);
        assert_eq!((verdict, verdict.exit_code()), (Verdict::InvalidRun, 3));
        let check = results.validity.iter().find(|c| c.check == ValidityCheck::KernelSplit);
        let check = check.unwrap();
        assert_eq!(
            (check.key().as_str(), check.observed, check.ok),
            ("kernel_split{group=api}", Some(0.0), false)
        );
        assert_eq!(check.limit_text(), "> 0.00%");
        let reason = check.reason.as_deref().unwrap();
        assert!(
            reason.starts_with(
                "none of the 10 completed requests had a kernel split with timestamps = \
                 \"kernel\": 8 lacked a kernel TX timestamp, 2 a kernel RX timestamp, and 1 \
                 had TX and RX timestamps of different clocks. Likely fix: no TX timestamps: \
                 the interface lacks software-transmit timestamping"
            ),
            "{reason}"
        );
        assert!(reason.contains("check `ethtool -T <interface>`"), "{reason}");
        assert!(reason.contains("no RX timestamps: check that `ethtool -T"), "{reason}");
        assert!(reason.contains("TX and RX timestamps of different clocks: one"), "{reason}");
        // "batch" completed nothing: it has no kernel_split check
        assert_eq!(
            results
                .validity
                .iter()
                .filter(|c| c.check == ValidityCheck::KernelSplit)
                .count(),
            1
        );
        // The counts are in the results
        let json = serde_json::to_value(&results).unwrap();
        let api = &json["traffic_groups"][0]["stats"]["latency_breakdown"];
        assert_eq!(api["kernel_split_count"], 0);
        let missing = serde_json::json!({ "tx": 8, "rx": 2, "clock_mismatch": 1 });
        assert_eq!(api["kernel_split_missing"], missing);
        assert_eq!(json["global"]["latency_breakdown"]["kernel_split_missing"], missing);
        assert_eq!(json["validity"].as_array().unwrap().last().unwrap()["check"], "kernel_split");
    }

    #[test]
    fn a_partial_kernel_split_passes_and_a_full_one_is_checked_too() {
        use xylem_core::stats::SplitMiss::Tx;
        for (splits, observed) in
            [(vec![None, Some(Tx), Some(Tx), Some(Tx)], 0.25), (vec![None; 4], 1.0)]
        {
            let (config, mut results) = kernel_results(TimestampSourceConfig::Kernel, "", &splits);
            assert_eq!(evaluate(&mut results, &config), Verdict::Pass);
            let check = results.validity.iter().find(|c| c.check == ValidityCheck::KernelSplit);
            let check = check.unwrap();
            assert_eq!((check.observed, check.ok, &check.reason), (Some(observed), true, &None));
        }
    }

    #[test]
    fn hardware_timestamps_that_split_nothing_name_the_interface_in_the_fix() {
        use xylem_core::stats::SplitMiss::Rx;
        let hardware = TimestampSourceConfig::Hardware("data0".into());
        let (config, mut results) = kernel_results(hardware, "", &[Some(Rx); 3]);
        assert_eq!(evaluate(&mut results, &config), Verdict::InvalidRun);
        let reason = results.validity.last().unwrap().reason.clone().unwrap();
        assert!(
            reason.starts_with(
                "none of the 3 completed requests had a kernel split with timestamps = { \
                 hardware = \"data0\" }: 0 lacked a kernel TX timestamp, 3 a kernel RX \
                 timestamp, and 0 had TX and RX timestamps of different clocks. Likely fix: \
                 no RX timestamps: the NIC's RX filter skips these packets; check that \
                 `ethtool -T data0` lists hardware-receive and that the rx_filter \
                 `hwstamp_ctl -i data0` reports covers them"
            ),
            "{reason}"
        );
        assert!(!reason.contains("no TX timestamps"), "{reason}");
    }

    #[test]
    fn the_kernel_split_check_can_be_skipped_and_skips_other_timestamps() {
        use xylem_core::stats::SplitMiss::Tx;
        for validity in ["[validity]\nskip = [\"kernel_split\"]", "[validity]\nenabled = false"] {
            let (config, mut results) =
                kernel_results(TimestampSourceConfig::Kernel, validity, &[Some(Tx)]);
            evaluate(&mut results, &config);
            assert!(results.validity.iter().all(|c| c.check != ValidityCheck::KernelSplit));
        }
        // User-space timestamps never split, and report no missing splits
        let (config, mut results) = kernel_results(
            TimestampSourceConfig::User,
            "",
            &[Some(xylem_core::stats::SplitMiss::TxAndRx)],
        );
        evaluate(&mut results, &config);
        assert!(results.validity.iter().all(|c| c.check != ValidityCheck::KernelSplit));
        let json = serde_json::to_value(&results).unwrap();
        assert!(json["global"]["latency_breakdown"].get("kernel_split_missing").is_none());
        let api = &json["traffic_groups"][0]["stats"]["latency_breakdown"];
        assert!(api.get("kernel_split_missing").is_none(), "{api}");
    }

    #[test]
    fn validity_checks_can_be_tuned_skipped_or_disabled() {
        let invalid = |validity: &str| {
            let config = profile(validity).unwrap();
            let mut results = results(&config);
            let api = &mut results.traffic_groups[0].stats.requests;
            (api.sent, api.unsent, api.late_sends) = (900, 100, 90);
            evaluate(&mut results, &config);
            results.validity.iter().map(|c| (c.check, c.ok)).collect::<Vec<_>>()
        };
        use ValidityCheck::{LateSends, Unsent};
        assert_eq!(invalid(""), [(Unsent, false), (LateSends, false)]);
        assert_eq!(
            invalid("[validity]\nmax_unsent_ratio = 0.1\nmax_late_send_ratio = 0.1"),
            [(Unsent, true), (LateSends, true)]
        );
        assert_eq!(invalid("[validity]\nskip = [\"late_sends\"]"), [(Unsent, false)]);
        assert_eq!(invalid("[validity]\nenabled = false"), []);

        // A rate-driven group without results recorded nothing it was asked to
        let config = profile("").unwrap();
        let mut results = results(&config);
        results.traffic_groups.remove(0);
        assert_eq!(evaluate(&mut results, &config), Verdict::InvalidRun);
        assert_eq!(results.validity[0].reason.as_deref(), Some("the group recorded no requests"));

        for (validity, expected) in [
            ("max_unsent_ratio = 1.5", "validity.max_unsent_ratio must be a fraction"),
            ("max_late_send_ratio = -0.1", "validity.max_late_send_ratio must be a fraction"),
            ("max_late_send_ratio = nan", "validity.max_late_send_ratio must be a fraction"),
            ("skip = [\"slow\"]", "unknown variant `slow`"),
            ("max_unsent = 0.1", "unknown field `max_unsent`"),
        ] {
            let error = format!("{:#}", profile(&format!("[validity]\n{validity}")).unwrap_err());
            assert!(error.contains(expected), "{validity}: {error}");
        }
    }
}
