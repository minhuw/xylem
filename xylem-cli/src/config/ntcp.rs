//! The `[ntcp]` profile section: user-space TCP transport settings
//!
//! The section is part of the profile format whether or not xylem was built
//! with the `ntcp` feature; only running a group with `transport = "ntcp"`
//! needs the feature.

use anyhow::{bail, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::IpAddr;
use xylem_core::traffic_group::TrafficGroupConfig;

/// Name of the transport in `traffic_groups[].transport`
pub const TRANSPORT: &str = "ntcp";

/// Whether this build can run the ntcp transport
pub const SUPPORTED: bool = cfg!(all(target_os = "linux", feature = "ntcp"));

/// User-space TCP (ntcp) settings, required when a traffic group uses
/// `transport = "ntcp"`
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NtcpConfig {
    /// Packet backend; only "tun" (an existing Linux TUN device) for now
    #[serde(default)]
    pub backend: NtcpBackend,
    /// IP MTU of every endpoint's link
    #[serde(default = "default_mtu")]
    pub mtu: u32,
    /// One endpoint per worker thread that runs ntcp traffic groups, in
    /// ascending thread-id order
    pub endpoints: Vec<NtcpEndpoint>,
}

/// Packet backend of the ntcp transport
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum NtcpBackend {
    /// An existing Linux TUN device (IFF_TUN | IFF_NO_PI)
    #[default]
    Tun,
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
        let index = thread_ids(groups).iter().position(|&id| id == thread_id)?;
        self.endpoints.get(index)
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
        let toml_str = "backend = \"dpdk\"\nendpoints = []\n";
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
    }

    #[cfg(all(target_os = "linux", feature = "ntcp"))]
    mod supported {
        use super::*;

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

    #[cfg(not(all(target_os = "linux", feature = "ntcp")))]
    #[test]
    fn ntcp_transport_needs_the_feature() {
        let message = error(&[("ntcp", "[0]")], TWO_ENDPOINTS);
        assert!(message.contains("built without ntcp support"), "{message}");
    }
}
