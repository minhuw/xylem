# ntcp (User-Space TCP)

The `ntcp` transport runs TCP in user space with [ntcp](https://github.com/minhuw/ntcp), a caller-driven TCP/IP stack, instead of the kernel's TCP. Each worker thread runs its own ntcp stack, which sends and receives packets through a packet backend: a Linux TUN device (raw IP packets), a DPDK port, or AF_XDP sockets on a network interface (Ethernet frames, with ARP).

The target server sees an ordinary TCP client. Only the client side changes, so you can compare the kernel stack with a user-space stack against the same server.

## Building

ntcp is opt-in and Linux-only. Build xylem with the `ntcp` feature for the TUN backend:

```bash
cargo build --release --features ntcp
```

The DPDK backend needs the `ntcp-dpdk` feature, which links DPDK statically. Build it inside the nix dev shell, which provides the DPDK SDK (`pkg-config libdpdk`, with the mlx5 drivers) from ntcp's flake, on x86_64 Linux:

```bash
nix develop --command cargo build --release --features xylem-cli/ntcp-dpdk
```

Build and run DPDK binaries with nix's toolchain only: a binary linked against the host's glibc fails to load nix's rdma-core when EAL starts.

The AF_XDP backend needs the `ntcp-af-xdp` feature. It links libbpf and compiles an XDP program for the BPF target; the nix dev shell provides libbpf, a BPF-capable clang (`NTCP_BPF_CLANG`) and the Linux UAPI headers (`NTCP_BPF_CFLAGS`):

```bash
nix develop --command cargo build --release --features xylem-cli/ntcp-af-xdp
```

A build without the feature rejects `transport = "ntcp"` (or `backend = "dpdk"` or `"af_xdp"`) with an error saying that xylem was built without that support.

## TUN setup

Every worker thread that runs ntcp traffic needs its own TUN device and its own IP address. That address belongs to ntcp, not to the kernel: the kernel holds a different address on the same link and routes the replies to ntcp through the device.

```bash
# One device per ntcp thread; plain `ip tuntap add` creates IFF_NO_PI, which ntcp requires
sudo ip tuntap add dev ntcp0 mode tun user "$USER"
sudo ip addr add 10.73.0.1/24 dev ntcp0     # the kernel's side of the link
sudo ip link set ntcp0 mtu 1500 up
# ntcp then uses 10.73.0.2, which nothing else may use
```

The device must exist before xylem starts; xylem attaches to it and never creates, configures or removes it. The kernel side sees ntcp's packets arrive on `ntcp0`. To reach a server elsewhere, the kernel has to forward them (IP forwarding plus NAT, or routes back to `10.73.0.0/24` on the server's side). A server running on the same host can listen on `10.73.0.1` directly.

## DPDK setup

The DPDK backend drives one Ethernet port. Each worker thread that runs ntcp traffic gets one queue pair of the port and its own IPv4 address on the port's network; a flow rule steers that address to the thread's queue. The threads share one ARP table: ntcp resolves its next hops and answers ARP requests for its addresses itself.

- **The device.** EAL probes only the device you name (`-a`). With NVIDIA/Mellanox mlx5 NICs the port stays bound to `mlx5_core` (bifurcated driver) and needs `ib_uverbs` and `mlx5_ib` loaded; other NICs must be bound to `vfio-pci`. Name PMD arguments in the device string: `0000:05:00.0,dv_flow_en=1`.
- **Hugepages.** EAL needs hugepages, e.g. 1 GiB pages reserved at boot. `--in-memory` keeps EAL's files out of `/dev/hugepages` and `/var/run`.
- **Privileges.** Run xylem as root (or with the capabilities DPDK needs for the device).
- **Addresses.** ntcp's addresses must be unused on the network, and must not be the kernel's address on that device.
- **The port takes the device's traffic.** The port is not isolated (on mlx5, an isolated port cannot read the NIC's clock, which NIC timestamps need), so while xylem runs, the kernel no longer receives the device's traffic. Keep management traffic on another interface.
- **CPUs.** xylem pins worker thread N to CPU N, and attaches the thread's queue after pinning it. Unless `eal_args` name lcores, EAL's main lcore is the first CPU xylem may run on, and EAL starts no threads of its own. Busy-polling keeps each ntcp thread's CPU at 100%.

## AF_XDP setup

The AF_XDP backend shares a network interface with the kernel. xylem attaches an XDP program to the interface; it hands IPv4 packets addressed to ntcp's addresses, and ARP for them, to an AF_XDP socket per worker thread, and passes all other traffic to the kernel, which keeps its own address and connections on the interface.

- **Privileges.** Run xylem as root (it needs `CAP_BPF`, `CAP_NET_ADMIN` and `CAP_NET_RAW`). Hosts with `kernel.unprivileged_bpf_disabled` still allow root.
- **Mode.** `mode = "native"` (default) runs the program in the driver and fails on a driver without native XDP; `"generic"` works on any interface, veth included, but is slower.
- **Queues.** With several threads, xylem installs ntuple rules (`ethtool -N`) that steer each thread's address to its queue, and ARP to one queue; the interface needs that many combined queues and ntuple filtering. One thread needs neither.
- **Addresses.** ntcp's addresses must be unused on the network and must not be the kernel's addresses.
- **Cleanup.** xylem removes the program, its addresses and its rules when the run ends.
- **Timestamps.** Copy mode, user-space timestamps only: no NIC timestamps, so `timestamps` must be `"user"`.

## Configuration

Set `transport = "ntcp"` in the traffic groups and add a top-level `[ntcp]` section. With the TUN backend:

```toml
[[traffic_groups]]
name = "echo"
protocol = "xylem-echo"
target = "10.73.0.1:9999"      # an ordinary socket address
transport = "ntcp"
threads = [0, 1]
connections_per_thread = 4
traffic_policy = { type = "closed-loop" }

[ntcp]
backend = "tun"                # the default
mtu = 1500                     # IP MTU of the links (default 1500)
endpoints = [
    { ifname = "ntcp0", local_ip = "10.73.0.2" },   # thread 0
    { ifname = "ntcp1", local_ip = "10.74.0.2" },   # thread 1
]
```

With the DPDK backend:

```toml
[[traffic_groups]]
name = "echo"
protocol = "xylem-echo"
target = "10.80.0.1:9999"
transport = "ntcp"
threads = [2, 3]
connections_per_thread = 1
traffic_policy = { type = "closed-loop" }
timestamps = "nic"             # optional: NIC timestamps, see below

[ntcp]
backend = "dpdk"
mtu = 1500

[ntcp.dpdk]
device = "0000:05:00.0,dv_flow_en=1"
eal_args = ["--in-memory", "--file-prefix=xylem"]
local_ips = ["10.80.0.12", "10.80.0.13"]   # thread 2, thread 3
prefix_len = 24
# gateway = "10.80.0.254"      # for targets outside 10.80.0.0/24
```

With the AF_XDP backend, the `[ntcp]` section is:

```toml
[ntcp]
backend = "af_xdp"

[ntcp.af_xdp]
ifname = "enp5s0"
mode = "native"                # or "generic"
local_ips = ["10.80.0.20", "10.80.0.21"]
prefix_len = 24
```

| Field | Description |
|-------|-------------|
| `backend` | Packet backend: `"tun"` (default), an existing Linux TUN device without packet information headers per thread; or `"dpdk"`, one DPDK port shared by the threads. |
| `mtu` | IP MTU that ntcp uses on every link, 68 to 65535. Match the device's MTU. |
| `endpoints` | TUN: one `{ ifname, local_ip }` per worker thread that runs ntcp traffic groups. They map to those threads in ascending thread-ID order: with `threads = [4, 2]`, the first endpoint goes to thread 2 and the second to thread 4. |
| `endpoints[].ifname` | Name of that thread's TUN device. |
| `endpoints[].local_ip` | ntcp's own address on that device: unicast, not one of the kernel's addresses. |
| `dpdk.device` | DPDK: EAL devargs of the port, a PCI address (optionally with PMD arguments) or a virtual device. |
| `dpdk.eal_args` | More EAL arguments, after `-a DEVICE`. |
| `dpdk.local_ips` | ntcp's IPv4 address for each worker thread that runs ntcp traffic groups, in ascending thread-ID order like `endpoints`. |
| `dpdk.prefix_len` | Prefix length of the on-link network of `local_ips`, 1 to 32. |
| `dpdk.gateway` | Router for targets outside that network (optional): another address on it. |
| `af_xdp.ifname` | AF_XDP: the network interface. |
| `af_xdp.mode` | `"native"` (default) or `"generic"` XDP. |
| `af_xdp.local_ips`, `af_xdp.prefix_len`, `af_xdp.gateway` | As for DPDK: one address per ntcp thread, the on-link prefix and an optional router. |

Validation requires the `[ntcp]` section whenever a group uses ntcp, and checks the selected backend's settings: exactly one endpoint (TUN) or address (DPDK) per ntcp thread, each address used once, and for TUN a different device per endpoint. The other backend's settings are ignored, so a profile can keep both and switch with `--set ntcp.backend=dpdk`. Groups sharing a thread share its endpoint or address. Groups on a thread must all use the same transport, as with every transport.

## NIC timestamps (DPDK)

With the DPDK backend, `timestamps = "nic"` on a traffic group timestamps its requests with the NIC's clock:

- **RX**: the NIC's hardware timestamp of the packet that carried a response's last byte.
- **TX**: the time ntcp handed the request's last byte to the NIC's transmit queue (ntcp's *Scheduled* stage). The NIC sends it soon after; this backend has no hardware TX timestamps.

Each worker thread converts the NIC's clock to xylem's clock with a calibration fitted from paired readings of both clocks, taken about every 100 ms over a sliding window of 32 (and 8 readings when the thread starts). The latency breakdown then splits each round trip as with kernel timestamps, with the clock `nic_calibrated`: `network_server_us` is the time from the request being queued to the NIC to the response reaching the NIC, and `host_stack_us` the rest of the user-space round trip. The JSON results list each thread's calibration under `nic_clock_calibration`, with its residuals (the typical conversion error, about 0.1–0.2 µs on a ConnectX-5 VF) and the number of receives without a usable timestamp. See [Output Formats](../output-formats.md).

## Testing without root

`just test-ntcp` runs an end-to-end test in an unprivileged user and network namespace (`unshare -rn`), with TUN devices that exist only inside it. It runs `xylem-echo-server` on the kernel side and compares ntcp with kernel TCP over the same devices, for one thread and for two. See `tests/ntcp/tun_netns.sh`. The DPDK backend needs a real NIC (or a virtual device) and root; its unit tests run the same code over a simulated NIC.

## Current limitations

- **TCP only.** There is no user-space UDP.
- **Timestamps.** With TUN and AF_XDP, user-space timestamps only: latencies use the CPU timestamp counter read when the send was accepted by ntcp and when a receive completed, and `poll_tx_timestamps` reports nothing. With DPDK, `timestamps = "nic"` adds the NIC timestamps above; there are no kernel timestamps.
- **One endpoint per thread.** Threads never share a stack.
- **Polling.** A group drives its stack whenever xylem sends or polls. With TUN it sleeps on the device's file descriptor when the stack has nothing left to do; with DPDK and AF_XDP it busy-polls its queue. Each connection's stack work costs CPU time on the worker thread, and that time is part of the measured latency.
- **No startup wait.** Like Linux after a reboot, ntcp opens connections as soon as a run starts. It does not wait out the 2×MSL (240 s) TIME-WAIT state of a previous run. A run that reuses an ntcp address and port against the same server within 4 minutes of an earlier run could in principle meet that run's old segments; ntcp's per-run secret and TCP timestamps make this very unlikely.

## See Also

- [Transports](../transports.md)
- [Transport Configuration](../configuration/transport.md)
