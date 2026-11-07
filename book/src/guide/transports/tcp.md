# TCP Transport

> **Note:** This page is TODO. Configuration details need validation against actual implementation.

## Socket options

Every TCP connection sets `TCP_NODELAY`. With pipelined requests, Nagle's
algorithm would otherwise hold a small request back until the previous one is
acknowledged, which inflates the measured latency. There is no option to turn
it off.
