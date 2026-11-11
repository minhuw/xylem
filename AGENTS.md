# Agent rules for xylem

- Keep transport and application protocol separate: `xylem-protocols` never depends on `xylem-transport` or branches on the transport, and transports never parse protocol bytes; extend the `Protocol` trait (`xylem-common/src/protocol.rs`) or the worker generically instead.
