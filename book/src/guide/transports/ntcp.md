# ntcp (User-Space TCP)

The `ntcp` transport runs TCP in user space with [ntcp](https://github.com/minhuw/ntcp), a caller-driven TCP/IP stack, instead of the kernel's TCP. Each worker thread runs its own ntcp stack, which sends and receives raw IP packets through a packet backend. The first backend is a Linux TUN device; DPDK and AF_XDP backends are planned.

The target server sees an ordinary TCP client. Only the client side changes, so you can compare the kernel stack with a user-space stack against the same server.

## Building

ntcp is opt-in and Linux-only. Build xylem with the `ntcp` feature:

```bash
cargo build --release --features ntcp
```

A build without the feature rejects `transport = "ntcp"` with an error saying that xylem was built without ntcp support.

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

## Configuration

Set `transport = "ntcp"` in the traffic groups and add a top-level `[ntcp]` section:

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
backend = "tun"                # the only backend for now (default)
mtu = 1500                     # IP MTU of the links (default 1500)
endpoints = [
    { ifname = "ntcp0", local_ip = "10.73.0.2" },   # thread 0
    { ifname = "ntcp1", local_ip = "10.74.0.2" },   # thread 1
]
```

| Field | Description |
|-------|-------------|
| `backend` | Packet backend. Only `"tun"`: an existing Linux TUN device without packet information headers. |
| `mtu` | IP MTU that ntcp uses on every endpoint's link, 68 to 65535. Match the device's MTU. |
| `endpoints` | One `{ ifname, local_ip }` per worker thread that runs ntcp traffic groups. They map to those threads in ascending thread-ID order: with `threads = [4, 2]`, the first endpoint goes to thread 2 and the second to thread 4. |
| `endpoints[].ifname` | Name of that thread's TUN device. |
| `endpoints[].local_ip` | ntcp's own address on that device: unicast, not one of the kernel's addresses. |

Validation requires the `[ntcp]` section whenever a group uses ntcp, exactly one endpoint per ntcp thread, and a different address and device for every endpoint. Groups sharing a thread share its endpoint. Groups on a thread must all use the same transport, as with every transport.

## Testing without root

`just test-ntcp` runs an end-to-end test in an unprivileged user and network namespace (`unshare -rn`), with TUN devices that exist only inside it. It runs `xylem-echo-server` on the kernel side and compares ntcp with kernel TCP over the same devices, for one thread and for two. See `tests/ntcp/tun_netns.sh`.

## Current limitations

- **TCP only.** There is no user-space UDP.
- **User-space timestamps only.** Latencies use the CPU timestamp counter read when the send was accepted by ntcp and when a receive completed. There are no kernel or NIC TX/RX timestamps, and `poll_tx_timestamps` reports nothing. The TUN backend has no packet timestamps.
- **One TUN device and one address per thread.** Threads never share a stack.
- **Polling.** A group drives its stack whenever xylem sends or polls, and sleeps on the TUN file descriptor only when the stack has nothing left to do. Each connection's stack work costs CPU time on the worker thread, and that time is part of the measured latency.
- **No startup wait.** Like Linux after a reboot, ntcp opens connections as soon as a run starts. It does not wait out the 2×MSL (240 s) TIME-WAIT state of a previous run. A run that reuses an ntcp address and port against the same server within 4 minutes of an earlier run could in principle meet that run's old segments; ntcp's per-run secret and TCP timestamps make this very unlikely.

## See Also

- [Transports](../transports.md)
- [Transport Configuration](../configuration/transport.md)
