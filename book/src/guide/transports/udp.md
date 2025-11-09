# UDP Transport

UDP (User Datagram Protocol) provides low-latency, connectionless communication.

## Basic Configuration

```json
{
  "transport": {
    "type": "udp",
    "host": "localhost",
    "port": 8080
  }
}
```

## Options

### `host`

**Type:** String  
**Required:** Yes

Target host address.

### `port`

**Type:** Integer  
**Required:** Yes

Target port number.

### `max_packet_size`

**Type:** Integer  
**Default:** 1472

Maximum UDP packet size in bytes. Should be smaller than MTU to avoid fragmentation.

```json
{
  "transport": {
    "type": "udp",
    "max_packet_size": 1400
  }
}
```

## Characteristics

### Advantages

- **Lower Latency** - No connection establishment or acknowledgments
- **Simpler Protocol** - Less overhead than TCP
- **Broadcast/Multicast** - Support for one-to-many communication

### Limitations

- **No Reliability** - Packets may be lost, duplicated, or reordered
- **No Flow Control** - Can overwhelm receiver
- **Size Limits** - Packet size limited by MTU (typically 1500 bytes)

## Use Cases

UDP transport is suitable for:

- Latency-sensitive applications
- Loss-tolerant workloads
- DNS, DHCP, and similar protocols
- Real-time streaming

## Examples

### Basic UDP Benchmark

```bash
xylem --protocol custom --transport udp --host localhost --port 8080
```

### Configuration

```json
{
  "transport": {
    "type": "udp",
    "host": "localhost",
    "port": 8080,
    "max_packet_size": 1400
  },
  "workload": {
    "duration": "60s",
    "rate": 10000
  }
}
```

## Troubleshooting

### Packet Loss

Monitor packet loss in the output statistics. High loss may indicate:
- Network congestion
- Rate too high
- Receiver buffer overflow

### MTU Issues

If experiencing fragmentation:
- Reduce `max_packet_size`
- Check network MTU: `ip link show`

## See Also

- [TCP Transport](./tcp.md)
- [Transport Configuration](../configuration/transport.md)
