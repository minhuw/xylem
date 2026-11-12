# gRPC Protocol

`protocol = "grpc"` makes gRPC unary calls of one method over cleartext
HTTP/2 with prior knowledge, as `ghz --insecure` and `grpcurl -plaintext`
do. It shares the [HTTP/2 protocol](./http2.md)'s connections, streams,
flow control and GOAWAY handling. Xylem knows no protobuf schema: the
request message is raw protobuf bytes, built once, and response messages
are not decoded.

## Configuration

```toml
[experiment]
name = "grpc-echo"
duration = "30s"

[[traffic_groups]]
name = "grpc"
protocol = "grpc"
target = "127.0.0.1:50051"
threads = [0, 1]
connections_per_thread = 4
# Calls in flight per connection
max_pending_per_connection = 4

[traffic_groups.protocol_config]
call = "helloworld.Greeter/SayHello"
# HelloRequest { name: "xylem" }
request_hex = "0a0578796c656d"
metadata = { x-tenant = "bench" }

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/grpc-echo.json"
```

| Key | Meaning |
|-----|---------|
| `call` | Full method name, `package.Service/Method` (a leading `/` is allowed) |
| `request_file` | File holding the serialized request message |
| `request_hex` | The serialized message in hexadecimal (whitespace allowed) |
| `request_base64` | The serialized message in standard base64 |
| `messages_file` | Several messages, each with gRPC's 5-byte prefix (flag 0, big-endian length); each thread sends them in turn |
| `metadata` | Custom metadata sent with every call, by lowercase name |
| `authority` | `:authority`; defaults to the target, or `localhost` over a Unix socket |

Set at most one of `request_file`, `request_hex`, `request_base64` and
`messages_file`; with none, the message is empty (every field at its
default). Files are read when the profile is validated, relative to the
working directory. Each call is a POST to `/<call>` with
`content-type: application/grpc`, `te: trailers` and
`user-agent: grpc-xylem`, and the message with gRPC's length prefix,
uncompressed. Metadata may not set those headers, pseudo-headers or
`grpc-*` names.

## Status

A call succeeds when `:status` is 200 and `grpc-status` is 0, from the
trailers or a trailers-only response. Any other `grpc-status` is an error
reply whose kind is the code (`"14"`) and whose message is its name
(`UNAVAILABLE`), counted per the group's `on_error`; an HTTP status other
than 200 is an error reply named after it, as for [HTTP/2](./http2.md). A
response without `grpc-status` fails the run.

Xylem does not check response messages. Check results in a separate
correctness pass, such as one `grpcurl` call per input, instead of inside
the load generator.

## TensorFlow Serving

TF Serving's `tensorflow.serving.PredictionService/Predict` takes a
`PredictRequest`. `tools/tfs-predict-request.py` (Python 3, no packages)
turns PredictRequests in protobuf's JSON mapping, the input of grpcurl and
ghz, into protobuf bytes:

```sh
# One request, for request_file
tools/tfs-predict-request.py request.json predict.bin
# Several, each prefixed, for messages_file
tools/tfs-predict-request.py --framed requests.json predict-many.bin
```

The input is one request object, a list of them, or an object whose
`requests` list holds `{"request": ...}` entries, such as the
`validation.json` of Stackbench's TF Serving benchmark. It supports
`model_spec`, `output_filter` and `inputs` of TensorProtos with `dtype`,
`tensor_shape`, `tensor_content` and the float, double, int, string, int64
and bool values, and fails on anything else. Its output matches protobuf's
deterministic serialization byte for byte.

A profile like Stackbench's ghz load, 8 connections with 32 calls in
flight, on TF Serving's gRPC port:

```toml
[experiment]
name = "tfs-predict"
duration = "30s"

[[traffic_groups]]
name = "predict"
protocol = "grpc"
target = "127.0.0.1:8500"
threads = [0, 1, 2, 3]
connections_per_thread = 2
max_pending_per_connection = 4

[traffic_groups.protocol_config]
call = "tensorflow.serving.PredictionService/Predict"
# PredictRequest { model_spec { name: "ranking" } }
request_hex = "0a090a0772616e6b696e67"

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/tfs-predict.json"
```

Real requests carry inputs: point `request_file` or `messages_file` at the
script's output instead of `request_hex`.

## Testing

`just test-grpc` runs the integration tests against a grpcio server
(`tests/grpc`), over TCP and a Unix socket. With `XYLEM_TFS_MODEL` set to a
SavedModel base directory (served as model `ranking`) and
`XYLEM_TFS_REQUESTS` to PredictRequests for it in JSON, one test also calls
Predict on the official `tensorflow/serving:2.21.0` image.
