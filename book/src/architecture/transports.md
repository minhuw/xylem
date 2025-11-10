# Transport Layer

The transport layer (`xylem-transport`) handles all network communication for Xylem.

## Transport Trait

All transports implement a common `Transport` trait that defines the interface for network operations:

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

This uniform interface allows the core engine to work with any transport mechanism without knowing the underlying implementation details. Whether communicating over TCP, UDP, or Unix domain sockets, the core engine uses the same set of operations.

## Transport Design

Transports are designed to be lightweight wrappers around operating system primitives, adding minimal overhead to network operations. Each transport handles the specifics of its communication mechanism while presenting a consistent interface to higher layers. The implementations focus on efficiency and correctness, with careful attention to error handling and resource management.

## Supported Transports

Xylem includes implementations for several transport mechanisms:

- **TCP** - Reliable, connection-oriented byte streams using standard TCP sockets
- **UDP** - Connectionless datagram communication for lower latency scenarios  
- **Unix Domain Sockets** - Local inter-process communication with minimal overhead

Each transport implementation is documented in detail in the User Guide's [Transports](../guide/transports.md) section.

## Non-Blocking I/O

All transports support non-blocking operation, which is essential for integration with Xylem's event-driven architecture. The core engine uses `mio` for efficient I/O multiplexing, allowing a single thread to manage many concurrent connections. Transports register their file descriptors with the event loop and handle readiness notifications appropriately.

## Connection Management

The transport layer provides primitives for connection management, but the core engine handles higher-level concerns like connection pooling and reconnection strategies. This separation allows the core to implement sophisticated connection management policies while keeping transport implementations simple and focused.

## See Also

- [Transport implementations in User Guide](../guide/transports.md)
- [Architecture Overview](./overview.md)
- [Protocol Layer](./protocols.md)
