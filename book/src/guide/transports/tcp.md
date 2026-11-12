# TCP Transport

`transport = "tcp"` (the default) opens each connection to the numeric
`"ip:port"` or `"[ipv6]:port"` in `target`.

## Socket options

Every TCP connection sets `TCP_NODELAY`. With pipelined requests, Nagle's
algorithm would otherwise hold a small request back until the previous one is
acknowledged, which inflates the measured latency. There is no option to turn
it off.

## Timestamps

On Linux, `timestamps = "kernel"` or `{ hardware = "<interface>" }` adds
kernel or NIC TX and RX timestamps; see
[Transport Configuration: Timestamps](../configuration/transport.md#timestamps).
A TCP TX timestamp covers every byte sent up to the one it reports, so
requests the kernel transmits together share one.
