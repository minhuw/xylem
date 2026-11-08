//! ntcp over a DPDK port (`ntcp-dpdk` feature)
//!
//! [`NtcpDpdkPort`] initializes DPDK's EAL, starts one Ethernet port with a
//! queue pair per member (worker thread), steers each member's IPv4 address
//! to its queue, and shares one ARP neighbour table among the members.
//! [`NtcpDpdkFactory`] then builds a member's group on its queue pair, on
//! the worker thread.
//!
//! The port is not isolated: traffic that no flow rule matches, ARP
//! included, lands on queue 0, whose member hands ARP to the shared table.
//! (On mlx5 an isolated port cannot read the device clock, so it would have
//! no NIC timestamps.) Queues are busy-polled.
//!
//! EAL pins the thread that initializes it to the main lcore's CPU; `start`
//! restores the calling thread's affinity afterwards. A worker thread must
//! be pinned before `create_group`: attaching a queue registers the thread
//! with EAL with its CPU set at that time.

use super::{
    NtcpBackend, NtcpConnectionGroup, NIC_RX_TIMESTAMP_RANGES, NIC_TX_TIMESTAMP_SPARE,
    UNITS_PER_SECOND,
};
use crate::{Error, Result, TransportFactory};
use ntcp_io::ClockDescriptor;
use ntcp_io_dpdk::{Eal, Flow, HostedQueue, Port, PortConfig, QueueHandle};
use ntcp_native::{EthernetNextHop, Link, NetworkConfig, SharedNeighborLimits, SharedNeighbors};
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::os::fd::RawFd;
use std::sync::{Arc, Mutex};

/// Dynamic ARP entries of the port's shared table
const NEIGHBOR_ENTRIES: usize = 64;

/// ARP replies the shared table queues for transmission
const NEIGHBOR_REPLIES: usize = 16;

impl NtcpBackend for HostedQueue {
    fn wait_fd(&self) -> Option<RawFd> {
        None
    }

    fn device_clock(&self) -> Option<ClockDescriptor> {
        self.clock()
    }

    fn read_device_clock(&self) -> io::Result<u64> {
        self.read_clock()
    }
}

/// One member of the port: a worker thread with its own queue pair, IPv4
/// address and ntcp network
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NtcpDpdkMember {
    /// The member's own address, steered to its queue
    pub local_ip: Ipv4Addr,
    /// Connections the member opens: sizes its stack and its share of the
    /// neighbour table's held output
    pub max_connections: usize,
    /// Most requests in flight on one of its connections: sizes the TX
    /// timestamp records of NIC timestamps
    pub max_pending: usize,
    /// Timestamp sends and receives with the NIC's clock
    pub nic_timestamps: bool,
}

/// A DPDK port and the members that share it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NtcpDpdkConfig {
    /// EAL devargs of the port: a PCI address, optionally with PMD
    /// arguments (`0000:05:00.0,dv_flow_en=1`), or a virtual device
    pub device: String,
    /// More EAL arguments, after `-a DEVICE` (or `--vdev DEVICE`); unless
    /// they name lcores, EAL gets `--lcores=0@CPU` for the first CPU the
    /// calling thread may run on, so it starts no lcore threads of its own
    pub eal_args: Vec<String>,
    /// IP MTU of the port
    pub mtu: u16,
    /// On-link prefix of the members' addresses: other destinations go
    /// through `gateway`
    pub prefix_len: u8,
    pub gateway: Option<Ipv4Addr>,
    /// Queue `i` belongs to member `i`
    pub members: Vec<NtcpDpdkMember>,
}

/// A started DPDK port shared by the members' groups
///
/// Keep it until every group is dropped: it holds the flow rules that steer
/// the members' traffic to their queues.
pub struct NtcpDpdkPort {
    config: NtcpDpdkConfig,
    neighbors: SharedNeighbors,
    /// Queue pairs not yet taken by a member's group; they, the attached
    /// queues and the flows keep the port started
    queues: Vec<Mutex<Option<QueueHandle>>>,
    /// Destroyed when the port is dropped, after the workers' groups
    _flows: Mutex<Vec<Flow>>,
}

fn io_error(context: &str, e: io::Error) -> Error {
    Error::Io(io::Error::new(e.kind(), format!("{context}: {e}")))
}

fn ntcp_error(context: &str, e: ntcp_native::Error) -> Error {
    Error::Config(format!("{context}: {e}"))
}

/// Whether devargs name a PCI device, `[DDDD:]BB:DD.F`, possibly followed
/// by `,key=value` PMD arguments, rather than a virtual device
fn is_pci(devargs: &str) -> bool {
    let name = devargs.split(',').next().unwrap_or("");
    let Some((head, function)) = name.rsplit_once('.') else {
        return false;
    };
    let fields: Vec<_> = head.split(':').collect();
    matches!(fields.len(), 2 | 3)
        && fields
            .iter()
            .chain([&function])
            .all(|f| !f.is_empty() && f.chars().all(|c| c.is_ascii_hexdigit()))
}

/// Whether EAL arguments choose the lcores themselves
fn names_lcores(args: &[String]) -> bool {
    args.iter().any(|arg| {
        ["-l", "-c", "--lcores", "-L"]
            .iter()
            .any(|flag| arg == flag || arg.starts_with(&format!("{flag}=")))
            || (arg.starts_with("-l") || arg.starts_with("-c")) && !arg.starts_with("--")
    })
}

fn affinity() -> io::Result<libc::cpu_set_t> {
    // SAFETY: an all-zero cpu_set_t is the empty set
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: writes at most size_of::<cpu_set_t>() bytes into `set`
    if unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(set)
}

fn set_affinity(set: &libc::cpu_set_t) -> io::Result<()> {
    // SAFETY: reads size_of::<cpu_set_t>() bytes of `set`; 0 is this thread
    if unsafe { libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), set) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn first_cpu(set: &libc::cpu_set_t) -> Option<usize> {
    // SAFETY: every index is below CPU_SETSIZE
    (0..libc::CPU_SETSIZE as usize).find(|&cpu| unsafe { libc::CPU_ISSET(cpu, set) })
}

/// The EAL arguments for `config`; `cpu` is the main lcore's CPU unless the
/// configured arguments name lcores
fn eal_args(config: &NtcpDpdkConfig, cpu: usize) -> Vec<String> {
    let flag = if is_pci(&config.device) {
        "-a"
    } else {
        "--vdev"
    };
    let mut args = vec![flag.to_string(), config.device.clone()];
    if !names_lcores(&config.eal_args) {
        args.push(format!("--lcores=0@{cpu}"));
    }
    args.extend(config.eal_args.iter().cloned());
    args
}

impl NtcpDpdkPort {
    /// Initialize EAL (once per process), start the port and install the
    /// members' flow rules, on the calling thread
    pub fn start(config: NtcpDpdkConfig) -> Result<Arc<Self>> {
        let members = config.members.len();
        if members == 0 || members > usize::from(u16::MAX) {
            return Err(Error::Config(format!(
                "a DPDK port needs 1 to {} members, got {members}",
                u16::MAX
            )));
        }
        let original = affinity().map_err(|e| io_error("sched_getaffinity", e))?;
        let cpu = first_cpu(&original)
            .ok_or_else(|| Error::Config("the calling thread may run on no CPU".to_string()))?;
        let args = eal_args(&config, cpu);
        tracing::info!("ntcp DPDK: EAL {}", args.join(" "));
        let eal = Eal::init(args);
        // rte_eal_init pinned this thread to the main lcore's CPU
        set_affinity(&original).map_err(|e| io_error("restore the CPU affinity after EAL", e))?;
        let eal = eal.map_err(|e| io_error("DPDK EAL initialization", e))?;

        let mut port_config = PortConfig::new(config.device.clone());
        port_config.queues = members as u16;
        port_config.mtu = config.mtu;
        port_config.isolate = false;
        port_config.rx_timestamps = config.members.iter().any(|m| m.nic_timestamps);
        let port = Port::start(eal, port_config)
            .map_err(|e| io_error(&format!("start DPDK port {}", config.device), e))?;
        let mac = port.mac_address().map_err(|e| io_error("read the port's MAC address", e))?;
        tracing::info!(
            "ntcp DPDK: port {} ({}) mac={} queues={} rx_timestamps={}",
            port.port_id(),
            config.device,
            mac.map(|o| format!("{o:02x}")).join(":"),
            port.queues(),
            port.clock().is_some()
        );

        let neighbors = SharedNeighbors::new(
            mac,
            UNITS_PER_SECOND,
            SharedNeighborLimits {
                members,
                entries: NEIGHBOR_ENTRIES,
                // Each member reserves max_connections + 1 output owners
                held: config.members.iter().map(|m| m.max_connections.max(1) + 1).sum(),
                replies: NEIGHBOR_REPLIES,
            },
        )
        .map_err(|e| ntcp_error("ntcp shared neighbour table", e))?;

        let mut flows = Vec::with_capacity(members);
        for (queue, member) in config.members.iter().enumerate() {
            match port.steer_ipv4(member.local_ip, queue as u16) {
                Ok(flow) => flows.push(flow),
                // A single queue receives everything without isolation
                Err(e) if members == 1 => tracing::info!(
                    "ntcp DPDK: no flow for {} ({e}); queue 0 receives all traffic",
                    member.local_ip
                ),
                Err(e) => {
                    return Err(io_error(&format!("steer {} to queue {queue}", member.local_ip), e))
                }
            }
        }
        let queues = (0..members as u16)
            .map(|queue| {
                let handle =
                    port.take_queue(queue).map_err(|e| io_error("take a DPDK queue", e))?;
                Ok(Mutex::new(Some(handle)))
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new(Self {
            config,
            neighbors,
            queues,
            _flows: Mutex::new(flows),
        }))
    }

    /// The factory of member `member`'s group
    pub fn factory(self: &Arc<Self>, member: usize) -> NtcpDpdkFactory {
        assert!(member < self.config.members.len(), "no DPDK member {member}");
        NtcpDpdkFactory { port: Arc::clone(self), member }
    }
}

/// Builds one member's group on its queue pair of an [`NtcpDpdkPort`]
///
/// A member has one queue pair, so its factory creates one group.
#[derive(Clone)]
pub struct NtcpDpdkFactory {
    port: Arc<NtcpDpdkPort>,
    member: usize,
}

impl TransportFactory for NtcpDpdkFactory {
    type Group = NtcpConnectionGroup<HostedQueue>;

    /// Attach the member's queue on the calling thread, which must already
    /// run on the CPU set it keeps, and build its network
    fn create_group(&self) -> Result<Self::Group> {
        let port = &self.port.config;
        let member = &port.members[self.member];
        let handle = self.port.queues[self.member]
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| {
                Error::Config(format!("DPDK queue {} already has a group", self.member))
            })?;
        let queue = handle.attach().map_err(|e| io_error("attach a DPDK queue", e))?;

        let mut config = NetworkConfig::new(IpAddr::V4(member.local_ip), u32::from(port.mtu));
        config.tcp.max_connections = member.max_connections.max(1);
        config.link = Link::Ethernet {
            local_mac: self.port.neighbors.local_mac(),
            next_hop: EthernetNextHop::Arp {
                prefix_len: port.prefix_len,
                gateway: port.gateway,
            },
        };
        if member.nic_timestamps {
            config.rx_timestamp_ranges = NIC_RX_TIMESTAMP_RANGES;
            config.tx_timestamp_phases = member.max_pending.max(1) + NIC_TX_TIMESTAMP_SPARE;
        }
        let mut group =
            NtcpConnectionGroup::with_shared_neighbors(config, queue, &self.port.neighbors)?;
        if member.nic_timestamps {
            group.enable_nic_timestamps()?;
        }
        Ok(group)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(device: &str, eal_args: &[&str]) -> NtcpDpdkConfig {
        NtcpDpdkConfig {
            device: device.to_string(),
            eal_args: eal_args.iter().map(|a| a.to_string()).collect(),
            mtu: 1500,
            prefix_len: 24,
            gateway: None,
            members: Vec::new(),
        }
    }

    #[test]
    fn devargs_name_pci_or_virtual_devices() {
        assert!(is_pci("0000:05:00.0"));
        assert!(is_pci("05:00.0,dv_flow_en=1"));
        assert!(!is_pci("net_ring0"));
        assert!(!is_pci("net_tap0,iface=x"));
    }

    #[test]
    fn eal_gets_the_device_and_one_lcore_unless_named() {
        let args = eal_args(&config("0000:05:00.0,dv_flow_en=1", &["--in-memory"]), 3);
        assert_eq!(args, ["-a", "0000:05:00.0,dv_flow_en=1", "--lcores=0@3", "--in-memory"]);
        let args = eal_args(&config("net_ring0", &["-l", "1"]), 3);
        assert_eq!(args, ["--vdev", "net_ring0", "-l", "1"]);
        for named in ["--lcores=0@1", "-l1", "-c", "-c0x2"] {
            assert!(names_lcores(&[named.to_string()]), "{named}");
        }
        assert!(!names_lcores(&["--in-memory".to_string(), "--log-level=8".to_string()]));
    }
}
