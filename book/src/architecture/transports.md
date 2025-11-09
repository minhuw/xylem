# Transport Layer

The transport layer (`xylem-transport`) handles network communication.

## Transport Trait

All transports implement the `Transport` trait:

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

## Transport Implementations

### TCP Transport

Standard TCP implementation using `std::net::TcpStream`:

```rust
pub struct TcpTransport {
    stream: Option<TcpStream>,
    addr: SocketAddr,
    options: TcpOptions,
}

pub struct TcpOptions {
    pub nodelay: bool,
    pub keepalive: bool,
    pub send_buffer: Option<usize>,
    pub recv_buffer: Option<usize>,
}
```

**Features:**
- Non-blocking I/O
- TCP_NODELAY support
- Configurable buffers
- Keep-alive support

### UDP Transport

Connectionless UDP implementation:

```rust
pub struct UdpTransport {
    socket: UdpSocket,
    addr: SocketAddr,
    max_packet_size: usize,
}
```

**Features:**
- Stateless operation
- Configurable packet size
- No connection establishment

### Unix Socket Transport

Unix domain socket implementation:

```rust
pub struct UnixTransport {
    stream: Option<UnixStream>,
    path: PathBuf,
}
```

**Features:**
- Lowest latency
- File system permissions
- No network overhead

### TLS Transport

Encrypted TCP using `rustls`:

```rust
pub struct TlsTransport {
    stream: Option<TlsStream<TcpStream>>,
    config: Arc<ClientConfig>,
    addr: SocketAddr,
    domain: ServerName,
}
```

**Features:**
- TLS 1.2 and 1.3
- Server verification
- Client certificates (mTLS)
- Custom CA certificates

## Non-Blocking I/O

All transports support non-blocking operation for integration with the event loop:

```rust
impl TcpTransport {
    pub fn set_nonblocking(&mut self, nonblocking: bool) -> Result<()> {
        if let Some(stream) = &self.stream {
            stream.set_nonblocking(nonblocking)?;
        }
        Ok(())
    }
}
```

## Connection Management

### Connection Establishment

```rust
pub fn connect(&mut self) -> Result<()> {
    match TcpStream::connect_timeout(&self.addr, Duration::from_secs(5)) {
        Ok(stream) => {
            // Configure socket options
            if self.options.nodelay {
                stream.set_nodelay(true)?;
            }
            self.stream = Some(stream);
            Ok(())
        }
        Err(e) => Err(anyhow!("Connection failed: {}", e)),
    }
}
```

### Connection Pooling

Reuse connections for better performance:

```rust
pub struct ConnectionPool {
    connections: Vec<Box<dyn Transport>>,
    available: VecDeque<usize>,
}

impl ConnectionPool {
    pub fn acquire(&mut self) -> Option<&mut dyn Transport> {
        self.available.pop_front()
            .map(|idx| &mut *self.connections[idx])
    }
    
    pub fn release(&mut self, idx: usize) {
        self.available.push_back(idx);
    }
}
```

## Error Handling

Transport-specific errors:

```rust
#[derive(Error, Debug)]
pub enum TransportError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("TLS error: {0}")]
    Tls(String),
    
    #[error("Timeout")]
    Timeout,
}
```

## Event Loop Integration

Transports register with the event loop using `mio`:

```rust
use mio::{Poll, Token, Interest};

pub fn register(&mut self, poll: &Poll, token: Token) -> Result<()> {
    if let Some(stream) = &mut self.stream {
        poll.registry().register(
            &mut SourceFd(&stream.as_raw_fd()),
            token,
            Interest::READABLE | Interest::WRITABLE,
        )?;
    }
    Ok(())
}
```

## Performance Optimizations

### Zero-Copy

Minimize data copying:
```rust
pub fn send_vectored(&mut self, bufs: &[IoSlice]) -> Result<usize> {
    self.stream.as_ref()
        .unwrap()
        .write_vectored(bufs)
        .map_err(Into::into)
}
```

### Buffer Tuning

Optimize socket buffers:
```rust
pub fn set_buffer_sizes(&mut self, send: usize, recv: usize) -> Result<()> {
    let stream = self.stream.as_ref().unwrap();
    let socket = socket2::Socket::from(stream.as_raw_fd());
    socket.set_send_buffer_size(send)?;
    socket.set_recv_buffer_size(recv)?;
    Ok(())
}
```

## TLS Configuration

### Server Verification

```rust
let mut root_store = RootCertStore::empty();
root_store.add_parsable_certificates(
    &rustls_native_certs::load_native_certs()?
);

let config = ClientConfig::builder()
    .with_root_certificates(root_store)
    .with_no_client_auth();
```

### Client Certificates

```rust
let client_cert = load_cert("client.pem")?;
let client_key = load_key("client-key.pem")?;

let config = ClientConfig::builder()
    .with_root_certificates(root_store)
    .with_client_auth_cert(client_cert, client_key)?;
```

## Testing

Transport tests ensure correct behavior:

```rust
#[test]
fn test_tcp_connect() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let mut transport = TcpTransport::new(addr, TcpOptions::default());
    assert!(transport.connect().is_ok());
    assert!(transport.is_connected());
}
```

## See Also

- [Architecture Overview](./overview.md)
- [Transport Configuration](../guide/configuration/transport.md)
- [Performance Tuning](../advanced/performance.md)
