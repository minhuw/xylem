# Transport Layer

The transport layer (`xylem-transport`) provides network communication primitives for Xylem.

## Transport Trait

All transport implementations conform to the `Transport` trait interface:

```rust
pub trait Transport: Send {
    /// Connect to the target
    fn connect(&mut self) -> Result<()>;
    
    /// Send data
    fn send(&mut self, data: &[u8]) -> Result<usize>;
    
    /// Receive data
    fn recv(&mut self, buf: &mut [u8]) -> Result<usize>;
    
    /// Close the connection
    fn close(&mut self) -> Result<()>;
    
    /// Check if connected
    fn is_connected(&self) -> bool;
}
```

The trait provides a uniform interface for transport mechanisms, enabling the core engine to perform network operations independently of the underlying transport implementation.

## Transport Design

Transport implementations are lightweight wrappers around operating system network primitives. Each implementation handles protocol-specific communication details while presenting a consistent interface to higher layers. The design prioritizes efficiency and correctness, with explicit error handling and resource management.

## Supported Transports

The following transport implementations are available:

- **TCP** - Connection-oriented byte streams using TCP sockets
- **UDP** - Connectionless datagram communication
- **Unix Domain Sockets** - Local inter-process communication

Detailed documentation for each transport is available in the [Transports](../guide/transports.md) section of the User Guide.

## Non-Blocking I/O

All transport implementations support non-blocking operation for integration with the event-driven architecture. The core engine uses `mio` for I/O multiplexing, enabling a single thread to manage multiple concurrent connections. Transports register file descriptors with the event loop and respond to readiness notifications.

## Connection Management

The transport layer provides primitives for connection lifecycle management. The core engine implements higher-level functionality such as connection pooling and reconnection strategies. This separation maintains transport implementation simplicity while allowing sophisticated connection management policies in the core.

## See Also

- [Transport implementations in User Guide](../guide/transports.md)
- [Architecture Overview](./overview.md)
- [Protocol Layer](./protocols.md)
