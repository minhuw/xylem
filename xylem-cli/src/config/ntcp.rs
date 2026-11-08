//! The `[ntcp]` profile section: user-space TCP transport settings
//!
//! The section is part of the profile format whether or not xylem was built
//! with the `ntcp` feature; only running a group with `transport = "ntcp"`
//! needs the feature.

use anyhow::{bail, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr};
use xylem_core::traffic_group::{TimestampSourceConfig, TrafficGroupConfig};

/// Name of the transport in `traffic_groups[].transport`
pub const TRANSPORT: &str = "ntcp";

/// Whether this build can run the ntcp transport
pub const SUPPORTED: bool = cfg!(all(target_os = "linux", feature = "ntcp"));

/// Whether this build can run the ntcp transport's DPDK backend
pub const SUPPORTED_DPDK: bool = cfg!(all(target_os = "linux", feature = "ntcp-dpdk"));

/// User-space TCP (ntcp) settings, required when a traffic group uses
/// `transport = "ntcp"`
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NtcpConfig {
    /// Packet backend: "tun" (an existing Linux TUN device per thread, in
    /// `endpoints`) or "dpdk" (one DPDK port shared by the threads, in
    /// `[ntcp.dpdk]`)
    #[serde(default)]
    pub backend: NtcpBackend,
    /// IP MTU of every endpoint's link
    #[serde(default = "default_mtu")]
    pub mtu: u32,
    /// Backend "tun": one endpoint per worker thread that runs ntcp traffic
    /// groups, in ascending thread-id order (ignored by backend "dpdk")
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<NtcpEndpoint>,
    /// Backend "dpdk": the port and the threads' addresses (ignored by
    /// backend "tun")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dpdk: Option<NtcpDpdk>,
}

/// Packet backend of the ntcp transport
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum NtcpBackend {
    /// An existing Linux TUN device (IFF_TUN | IFF_NO_PI)
    #[default]
    Tun,
    /// A DPDK port (needs a build with the `ntcp-dpdk` feature)
    Dpdk,
}

/// The device and address one worker thread's ntcp stack uses
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NtcpEndpoint {
    /// Name of the TUN interface (e.g. "ntcp0")
    pub ifname: String,
    /// ntcp's own IP address on that link, not one of the kernel's
    pub local_ip: IpAddr,
}

/// The DPDK port of backend "dpdk"
///
/// Each worker thread that runs ntcp traffic groups gets one queue pair of
/// the port and its own IPv4 address, steered to its queue by a flow rule.
/// The threads share one ARP table. The port is not isolated: it receives
/// all of the device's traffic, so the kernel no longer sees the device's
/// traffic for other addresses while xylem runs (mlx5 VFs: bifurcated).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NtcpDpdk {
    /// EAL devargs of the port: a PCI address, optionally with PMD arguments
    /// (e.g. "0000:05:00.0,dv_flow_en=1"), or a virtual device
    pub device: String,
    /// More EAL arguments (e.g. ["--in-memory", "--file-prefix=xylem"]);
    /// unless they name lcores, EAL's main lcore is the first CPU xylem may
    /// run on
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub eal_args: Vec<String>,
    /// ntcp's own IPv4 address for each worker thread that runs ntcp traffic
    /// groups, in ascending thread-id order
    pub local_ips: Vec<Ipv4Addr>,
    /// Prefix length of the on-link network of `local_ips`
    pub prefix_len: u8,
    /// Router for destinations outside that network
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway: Option<Ipv4Addr>,
}

fn default_mtu() -> u32 {
    1500
}

impl NtcpConfig {
    /// The endpoint of worker thread `thread_id`, if it runs ntcp groups
    pub fn endpoint_for_thread(
        &self,
        groups: &[TrafficGroupConfig],
        thread_id: usize,
    ) -> Option<&NtcpEndpoint> {
        self.endpoints.get(thread_index(groups, thread_id)?)
    }
}

/// Worker threads that run ntcp traffic groups, ascending
pub fn thread_ids(groups: &[TrafficGroupConfig]) -> Vec<usize> {
    let mut ids: Vec<usize> = groups
        .iter()
        .filter(|g| g.transport == TRANSPORT)
        .flat_map(|g| g.threads.iter().copied())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Position of worker thread `thread_id` among the ntcp threads: the index
/// of its endpoint, DPDK address and queue
pub fn thread_index(groups: &[TrafficGroupConfig], thread_id: usize) -> Option<usize> {
    thread_ids(groups).iter().position(|&id| id == thread_id)
}

/// Reject `transport = "ntcp"` when this build cannot run it
pub fn check_supported(group: &TrafficGroupConfig, index: usize) -> Result<()> {
    if group.transport == TRANSPORT && !SUPPORTED {
        bail!(
            "Traffic group {} '{}' uses transport 'ntcp', but xylem was built without ntcp \
             support; rebuild on Linux with `--features ntcp`",
            index,
            group.name
        );
    }
    Ok(())
}

/// Check the `[ntcp]` section against the traffic groups
///
/// Only the selected backend's settings are checked: a profile may keep
/// both backends' settings and switch with `--set ntcp.backend=...`.
pub fn validate(config: Option<&NtcpConfig>, groups: &[TrafficGroupConfig]) -> Result<()> {
    let threads = thread_ids(groups);
    let Some(config) = config else {
        if !threads.is_empty() {
            bail!(
                "Traffic groups use transport 'ntcp' but the profile has no [ntcp] section; \
                 add one with an endpoint for each of the threads {:?}",
                threads
            );
        }
        return Ok(());
    };

    if !(68..=65535).contains(&config.mtu) {
        bail!("ntcp.mtu must be between 68 and 65535, got {}", config.mtu);
    }
    match config.backend {
        NtcpBackend::Tun => validate_tun(config, &threads)?,
        NtcpBackend::Dpdk => validate_dpdk(config, &threads)?,
    }
    for (i, group) in groups.iter().enumerate() {
        if group.transport == TRANSPORT
            && group.timestamps == TimestampSourceConfig::Nic
            && config.backend != NtcpBackend::Dpdk
        {
            bail!(
                "Traffic group {} '{}' sets timestamps = \"nic\", which needs ntcp.backend = \
                 \"dpdk\"",
                i,
                group.name
            );
        }
    }
    Ok(())
}

fn validate_tun(config: &NtcpConfig, threads: &[usize]) -> Result<()> {
    let mut ips = HashSet::new();
    let mut ifnames = HashSet::new();
    for (i, endpoint) in config.endpoints.iter().enumerate() {
        if endpoint.ifname.is_empty() {
            bail!("ntcp.endpoints[{}].ifname cannot be empty", i);
        }
        let ip = endpoint.local_ip;
        if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
            bail!("ntcp.endpoints[{}].local_ip must be a unicast address, got {}", i, ip);
        }
        if !ips.insert(ip) {
            bail!("ntcp.endpoints[{}].local_ip {} is used by another endpoint", i, ip);
        }
        if !ifnames.insert(endpoint.ifname.as_str()) {
            bail!(
                "ntcp.endpoints[{}].ifname '{}' is used by another endpoint; \
                 each thread needs its own TUN device",
                i,
                endpoint.ifname
            );
        }
    }
    if !threads.is_empty() && config.endpoints.len() != threads.len() {
        bail!(
            "ntcp.endpoints has {} entries but {} threads run ntcp traffic groups {:?}; \
             give one endpoint per thread, in ascending thread order",
            config.endpoints.len(),
            threads.len(),
            threads
        );
    }
    Ok(())
}

/// Whether `ip` and `other` share the first `prefix_len` bits
fn same_network(ip: Ipv4Addr, other: Ipv4Addr, prefix_len: u8) -> bool {
    let mask = u32::MAX.checked_shl(32 - u32::from(prefix_len)).unwrap_or(0);
    u32::from(ip) & mask == u32::from(other) & mask
}

fn unicast(ip: Ipv4Addr) -> bool {
    !(ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() || ip.is_broadcast())
}

fn validate_dpdk(config: &NtcpConfig, threads: &[usize]) -> Result<()> {
    let Some(dpdk) = &config.dpdk else {
        bail!(
            "ntcp.backend = \"dpdk\" needs an [ntcp.dpdk] section with the port's device, \
             local_ips and prefix_len"
        );
    };
    if !threads.is_empty() && !SUPPORTED_DPDK {
        bail!(
            "ntcp.backend = \"dpdk\", but xylem was built without DPDK support; rebuild on \
             Linux with `--features ntcp-dpdk` in the nix dev shell"
        );
    }
    if dpdk.device.trim().is_empty() {
        bail!("ntcp.dpdk.device cannot be empty");
    }
    if !(1..=32).contains(&dpdk.prefix_len) {
        bail!("ntcp.dpdk.prefix_len must be between 1 and 32, got {}", dpdk.prefix_len);
    }
    if config.mtu > u32::from(u16::MAX) {
        bail!("ntcp.mtu must fit 16 bits for the DPDK backend, got {}", config.mtu);
    }
    let mut ips = HashSet::new();
    for (i, &ip) in dpdk.local_ips.iter().enumerate() {
        if !unicast(ip) {
            bail!("ntcp.dpdk.local_ips[{}] must be a unicast address, got {}", i, ip);
        }
        if !ips.insert(ip) {
            bail!("ntcp.dpdk.local_ips[{}] {} is used by another thread", i, ip);
        }
        if let Some(gateway) = dpdk.gateway {
            if gateway == ip || !same_network(ip, gateway, dpdk.prefix_len) {
                bail!(
                    "ntcp.dpdk.gateway {} must be another address in the network {}/{}",
                    gateway,
                    ip,
                    dpdk.prefix_len
                );
            }
        }
    }
    if let Some(gateway) = dpdk.gateway {
        if !unicast(gateway) {
            bail!("ntcp.dpdk.gateway must be a unicast address, got {}", gateway);
        }
    }
    if !threads.is_empty() && dpdk.local_ips.len() != threads.len() {
        bail!(
            "ntcp.dpdk.local_ips has {} entries but {} threads run ntcp traffic groups {:?}; \
             give one address per thread, in ascending thread order",
            dpdk.local_ips.len(),
            threads.len(),
            threads
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::config::ProfileConfig;

    /// A profile with one xylem-echo group per `(transport, threads)` and
    /// the given `[ntcp]` section (TOML, may be empty)
    fn profile(groups: &[(&str, &str)], ntcp: &str) -> ProfileConfig {
        let mut toml_str = String::from(
            "[experiment]\nname = \"ntcp-test\"\nduration = \"1s\"\n\n\
             [output]\nformat = \"json\"\nfile = \"/tmp/ntcp-test.json\"\n\n",
        );
        for (i, (transport, threads)) in groups.iter().enumerate() {
            toml_str.push_str(&format!(
                "[[traffic_groups]]\nname = \"g{i}\"\nprotocol = \"xylem-echo\"\n\
                 target = \"10.73.0.1:9999\"\ntransport = \"{transport}\"\nthreads = {threads}\n\
                 connections_per_thread = 1\ntraffic_policy = {{ type = \"closed-loop\" }}\n\n"
            ));
        }
        toml_str.push_str(ntcp);
        toml::from_str(&toml_str).unwrap()
    }

    const TWO_ENDPOINTS: &str = r#"
[ntcp]
backend = "tun"
endpoints = [
    { ifname = "ntcp0", local_ip = "10.73.0.2" },
    { ifname = "ntcp1", local_ip = "10.74.0.2" },
]
"#;

    fn error(groups: &[(&str, &str)], ntcp: &str) -> String {
        format!("{:#}", profile(groups, ntcp).validate().unwrap_err())
    }

    #[test]
    fn section_parses_with_defaults() {
        let config = profile(&[("tcp", "[0]")], TWO_ENDPOINTS);
        let ntcp = config.ntcp.unwrap();
        assert_eq!(ntcp.backend, super::NtcpBackend::Tun);
        assert_eq!(ntcp.mtu, 1500);
        assert_eq!(ntcp.endpoints[1].ifname, "ntcp1");
        assert_eq!(ntcp.endpoints[1].local_ip.to_string(), "10.74.0.2");
        assert!(profile(&[("tcp", "[0]")], "").ntcp.is_none());
    }

    #[test]
    fn unknown_backend_and_fields_are_rejected() {
        let toml_str = "backend = \"xdp\"\nendpoints = []\n";
        assert!(toml::from_str::<super::NtcpConfig>(toml_str).is_err());
        let toml_str = "backend = \"dpdk\"\n[dpdk]\ndevice = \"x\"\nlocal_ips = []\n\
                        prefix_len = 24\nqueues = 2\n";
        assert!(toml::from_str::<super::NtcpConfig>(toml_str).is_err());
        let toml_str = "endpoints = [{ ifname = \"a\", local_ip = \"10.0.0.2\", mac = \"x\" }]\n";
        assert!(toml::from_str::<super::NtcpConfig>(toml_str).is_err());
        let toml_str = "endpoints = [{ ifname = \"a\", local_ip = \"not-an-ip\" }]\n";
        assert!(toml::from_str::<super::NtcpConfig>(toml_str).is_err());
    }

    #[test]
    fn endpoints_map_to_ntcp_threads_in_ascending_order() {
        // Thread 1 runs kernel TCP; threads 4 and 2 run ntcp
        let config = profile(&[("tcp", "[1]"), ("ntcp", "[4, 2]")], TWO_ENDPOINTS);
        assert_eq!(super::thread_ids(&config.traffic_groups), [2, 4]);
        let ntcp = config.ntcp.as_ref().unwrap();
        let ifname = |thread| {
            ntcp.endpoint_for_thread(&config.traffic_groups, thread)
                .map(|e| e.ifname.as_str())
        };
        assert_eq!(ifname(2), Some("ntcp0"));
        assert_eq!(ifname(4), Some("ntcp1"));
        assert_eq!(ifname(1), None);
    }

    #[test]
    fn unused_section_is_accepted() {
        profile(&[("tcp", "[0]")], TWO_ENDPOINTS).validate().unwrap();
        profile(&[("tcp", "[0]")], DPDK).validate().unwrap();
    }

    const DPDK: &str = r#"
[ntcp]
backend = "dpdk"

[ntcp.dpdk]
device = "0000:05:00.0,dv_flow_en=1"
eal_args = ["--in-memory"]
local_ips = ["10.80.0.12", "10.80.0.13"]
prefix_len = 24
"#;

    #[test]
    fn dpdk_section_parses() {
        let config = profile(&[("tcp", "[0]")], DPDK);
        let ntcp = config.ntcp.unwrap();
        assert_eq!(ntcp.backend, super::NtcpBackend::Dpdk);
        assert!(ntcp.endpoints.is_empty());
        let dpdk = ntcp.dpdk.unwrap();
        assert_eq!(dpdk.device, "0000:05:00.0,dv_flow_en=1");
        assert_eq!(dpdk.eal_args, ["--in-memory"]);
        assert_eq!(dpdk.local_ips[1].to_string(), "10.80.0.13");
        assert_eq!((dpdk.prefix_len, dpdk.gateway), (24, None));
    }

    #[test]
    fn dpdk_backend_needs_its_section() {
        let message = error(&[("tcp", "[0]")], "[ntcp]\nbackend = \"dpdk\"\n");
        assert!(message.contains("needs an [ntcp.dpdk] section"), "{message}");
    }

    /// Both backends' settings, backend "tun"
    #[cfg(all(target_os = "linux", feature = "ntcp"))]
    fn both_backends() -> String {
        let dpdk = DPDK.split_once("[ntcp.dpdk]").unwrap().1;
        format!("{TWO_ENDPOINTS}\n[ntcp.dpdk]{dpdk}")
    }

    #[test]
    fn dpdk_values_are_checked() {
        let tcp = [("tcp", "[0]")];
        for (from, to, expected) in [
            (
                "device = \"0000:05:00.0,dv_flow_en=1\"",
                "device = \" \"",
                "device cannot be empty",
            ),
            ("prefix_len = 24", "prefix_len = 0", "prefix_len must be between"),
            ("prefix_len = 24", "prefix_len = 33", "prefix_len must be between"),
            ("\"10.80.0.13\"", "\"10.80.0.12\"", "used by another thread"),
            ("\"10.80.0.13\"", "\"224.0.0.1\"", "unicast"),
            (
                "prefix_len = 24",
                "prefix_len = 24\ngateway = \"10.81.0.1\"",
                "gateway 10.81.0.1",
            ),
            (
                "prefix_len = 24",
                "prefix_len = 24\ngateway = \"10.80.0.12\"",
                "gateway 10.80.0.12",
            ),
        ] {
            let message = error(&tcp, &DPDK.replace(from, to));
            assert!(message.contains(expected), "{to}: {message}");
        }
        let gateway = DPDK.replace("prefix_len = 24", "prefix_len = 24\ngateway = \"10.80.0.1\"");
        profile(&tcp, &gateway).validate().unwrap();
    }

    #[cfg(all(target_os = "linux", feature = "ntcp"))]
    mod supported {
        use super::*;

        #[test]
        fn the_other_backends_settings_are_ignored() {
            profile(&[("ntcp", "[0, 1]")], &both_backends()).validate().unwrap();
        }

        #[test]
        fn valid_ntcp_profile_passes() {
            profile(&[("ntcp", "[0, 1]")], TWO_ENDPOINTS).validate().unwrap();
            // Two groups sharing a thread share its endpoint
            profile(&[("ntcp", "[0]"), ("ntcp", "[0, 1]")], TWO_ENDPOINTS)
                .validate()
                .unwrap();
        }

        #[test]
        fn missing_section_is_rejected() {
            let message = error(&[("ntcp", "[0]")], "");
            assert!(message.contains("no [ntcp] section"), "{message}");
        }

        #[test]
        fn endpoint_count_must_match_threads() {
            let message = error(&[("ntcp", "[0]")], TWO_ENDPOINTS);
            assert!(message.contains("2 entries but 1 threads"), "{message}");
            let message = error(&[("ntcp", "[0, 1, 2]")], TWO_ENDPOINTS);
            assert!(message.contains("2 entries but 3 threads"), "{message}");
        }

        #[test]
        fn endpoint_ips_and_devices_must_be_unique() {
            let duplicate_ip = r#"
[ntcp]
endpoints = [
    { ifname = "ntcp0", local_ip = "10.73.0.2" },
    { ifname = "ntcp1", local_ip = "10.73.0.2" },
]
"#;
            let message = error(&[("ntcp", "[0, 1]")], duplicate_ip);
            assert!(message.contains("used by another endpoint"), "{message}");

            let duplicate_device = r#"
[ntcp]
endpoints = [
    { ifname = "ntcp0", local_ip = "10.73.0.2" },
    { ifname = "ntcp0", local_ip = "10.74.0.2" },
]
"#;
            let message = error(&[("ntcp", "[0, 1]")], duplicate_device);
            assert!(message.contains("its own TUN device"), "{message}");
        }

        #[test]
        fn bad_endpoint_values_are_rejected() {
            let message = error(
                &[("ntcp", "[0]")],
                "[ntcp]\nendpoints = [{ ifname = \"\", local_ip = \"10.73.0.2\" }]\n",
            );
            assert!(message.contains("ifname cannot be empty"), "{message}");
            let message = error(
                &[("ntcp", "[0]")],
                "[ntcp]\nendpoints = [{ ifname = \"t\", local_ip = \"127.0.0.1\" }]\n",
            );
            assert!(message.contains("unicast"), "{message}");
            let message = error(
                &[("ntcp", "[0]")],
                "[ntcp]\nmtu = 40\nendpoints = [{ ifname = \"t\", local_ip = \"10.73.0.2\" }]\n",
            );
            assert!(message.contains("ntcp.mtu"), "{message}");
        }
    }

    /// One xylem-echo group over ntcp on `threads` with `timestamps`
    #[cfg(all(target_os = "linux", feature = "ntcp"))]
    fn timestamped(threads: &str, timestamps: &str, ntcp: &str) -> ProfileConfig {
        let mut config = profile(&[("ntcp", threads)], ntcp);
        config.traffic_groups[0].timestamps =
            toml::from_str::<Timestamps>(&format!("timestamps = {timestamps}"))
                .unwrap()
                .timestamps;
        config
    }

    #[cfg(all(target_os = "linux", feature = "ntcp"))]
    #[derive(serde::Deserialize)]
    struct Timestamps {
        timestamps: xylem_core::traffic_group::TimestampSourceConfig,
    }

    #[cfg(all(target_os = "linux", feature = "ntcp-dpdk"))]
    mod dpdk_supported {
        use super::*;

        #[test]
        fn valid_dpdk_profile_passes() {
            profile(&[("ntcp", "[2, 3]")], DPDK).validate().unwrap();
            timestamped("[2, 3]", "\"nic\"", DPDK).validate().unwrap();
            let both = both_backends().replace("backend = \"tun\"", "backend = \"dpdk\"");
            profile(&[("ntcp", "[2, 3]")], &both).validate().unwrap();
        }

        #[test]
        fn one_address_per_thread() {
            let message = error(&[("ntcp", "[2]")], DPDK);
            assert!(message.contains("local_ips has 2 entries but 1 threads"), "{message}");
        }
    }

    #[cfg(all(target_os = "linux", feature = "ntcp"))]
    #[test]
    fn nic_timestamps_need_the_dpdk_backend() {
        let message = format!(
            "{:#}",
            timestamped("[0, 1]", "\"nic\"", TWO_ENDPOINTS).validate().unwrap_err()
        );
        assert!(message.contains("needs ntcp.backend = \"dpdk\""), "{message}");
    }

    #[cfg(all(target_os = "linux", feature = "ntcp", not(feature = "ntcp-dpdk")))]
    #[test]
    fn dpdk_backend_needs_the_feature() {
        let message = error(&[("ntcp", "[2, 3]")], DPDK);
        assert!(message.contains("built without DPDK support"), "{message}");
    }

    #[cfg(not(all(target_os = "linux", feature = "ntcp")))]
    #[test]
    fn ntcp_transport_needs_the_feature() {
        let message = error(&[("ntcp", "[0]")], TWO_ENDPOINTS);
        assert!(message.contains("built without ntcp support"), "{message}");
    }
}
