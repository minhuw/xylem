# NVMe/TCP Protocol

`protocol = "nvme-tcp"` puts NVM read and write load on an NVMe over Fabrics
target over TCP (NVMe/TCP 1.0), such as SPDK's `nvmf_tgt` or the Linux
kernel target. It speaks to one namespace of one subsystem, over `tcp`,
`unix` or `ntcp`.

```toml
[experiment]
name = "spdk-4k-randread"
duration = "10s"
seed = 1

[[traffic_groups]]
name = "nvme"
protocol = "nvme-tcp"
target = "127.0.0.1:4420"
threads = [0]
connections_per_thread = 4        # I/O queues per thread
max_pending_per_connection = 32   # queue depth of each I/O queue

[traffic_groups.protocol_config]
subnqn = "nqn.2016-06.io.spdk:cnode1"   # required
nsid = 1                  # default 1
block_size = 4096         # bytes per command; default 4096
read_percent = 70         # the rest are writes; default 100
access = "random"         # or "sequential"; default "random"
lba_start = 0             # first LBA of the range; default 0
lba_count = 262144        # LBAs in the range; default: to the end of the namespace
io_queues_per_controller = 2  # default: all of a thread's connections
verify = false            # readback-compare mode; default false

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/nvme.json"
```

`hostnqn` sets the host NQN, which defaults to a fixed
`nqn.2014-08.org.nvmexpress:uuid:` NQN.

## Controllers and queues

`connections_per_thread` counts **I/O queues**. Each thread connects one
controller per `io_queues_per_controller` of them, and each controller adds
one admin queue connection of its own, so the example opens 2 admin and 4
I/O queue connections per thread. Every connection exchanges ICReq and
ICResp first. The admin queue then connects (Fabrics Connect, QID 0) and
gets the controller ID, reads CAP, enables the controller, waits for
CSTS.RDY, and identifies the controller (in-capsule data size, MDTS) and the
namespace (LBA size and count). Only then do the controller's I/O queues
connect, with its controller ID and `max_pending_per_connection` entries.

All of that is connection setup: none of it is measured, and an open-loop
schedule starts once a queue has connected. Admin queues never carry load,
and with `dispatch = "group"` no arrival goes to one. Statistics come only
from I/O queue commands. A target that rejects a step, such as more I/O
queues than its `max_io_qpairs_per_ctrlr` or a queue depth beyond its
CAP.MQES, fails the run with the step and status.

Connect sets no keep alive timeout (KATO 0). SPDK's target then runs no keep
alive timer, nor does the Linux target, so no Keep Alive commands are sent.

## Commands

Each command reads or writes `block_size` bytes, which must be a whole
number of the namespace's LBAs, at a block of the range picked by `access`:
blocks in turn from `lba_start`, wrapping at the end of the range, or
uniformly random ones. The command ID is the request ID.

- A **write** of up to the target's in-capsule data size (IOCCSZ; SPDK's
  `in_capsule_data_size`, 4096 by default) carries its data in the command
  capsule. A larger one waits for the target's R2Ts and answers each with
  H2CData PDUs of at most the MAXH2CDATA the target granted. Those PDUs are
  protocol-driven sends: they are no requests and carry no latency.
- A **read** gets its data in C2HData PDUs, which are parsed as they arrive
  however large they are. A C2HData PDU with the SUCCESS flag completes the
  read; otherwise its CapsuleResp does.

A command that fails is an error reply named after its status, such as
`LBA_OUT_OF_RANGE` or `INVALID_FIELD`, counted per the group's `on_error`.
Header and data digests are off; a target that turns them on fails the run.

## Throughput

`throughput_rps` is IOPS. `throughput_mbps` counts megabits per second of
every byte on the wire both ways, PDU headers included (a 4 KiB read is a
72-byte capsule out and a 4120-byte C2HData PDU in), so payload MiB/s is
`throughput_rps × block_size / 2^20`.

## Readback-compare mode

With `verify = true` every write carries a pattern fixed by its LBA alone,
and every read must return the pattern of its LBAs, or the run fails naming
the first LBA and byte that differ. Before any measured command each thread
writes the whole range once (warmup writes, not measured), as
[preload](../output-formats.md#run-phases): no thread measures until every
thread's prefill writes have completed. Writes of the mix
rewrite the same pattern, so reads and writes can share the range. Prefer a
small `lba_count`: every thread writes all of it, and checking the data
costs CPU.

## Testing against SPDK

`cargo nextest run --profile integration -E 'binary(nvme_tcp_integration)'`
(or `just test-nvme`) starts SPDK's `nvmf_tgt` with a Malloc bdev on a free
loopback port: unprivileged, without hugepages (`--no-huge -s 512`), on one
core (the last CPU, or `XYLEM_NVMF_CORE`). The binary is `XYLEM_NVMF_TGT`,
which the flake's devShell (`nix develop`) sets to the `nvmf_tgt` of its
nixpkgs' SPDK.
