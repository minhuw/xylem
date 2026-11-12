#!/usr/bin/env python3
"""Encode TF Serving PredictRequests from JSON, for xylem's grpc protocol.

Usage: tfs-predict-request.py [--framed] INPUT.json OUTPUT

INPUT holds PredictRequests in protobuf's JSON mapping, as grpcurl and ghz
take them (snake_case or lowerCamelCase field names; bytes such as
string_val in base64): one request object, a list of them, or an object
whose "requests" list holds objects with a "request" (the validation.json
of Stackbench's TF Serving benchmark).

Without --framed, OUTPUT is the serialized request, for protocol_config's
request_file; INPUT must then hold exactly one. With --framed, OUTPUT holds
every request, each with gRPC's 5-byte message prefix, for messages_file,
which xylem sends in turn.

Supported: model_spec (name, version, version_label, signature_name),
inputs (TensorProto: dtype, tensor_shape, tensor_content and the
float/double/int/string/int64/bool values) and output_filter. Anything else
is an error. Map entries are sorted by key, as deterministic protobuf
serialization does.
"""

import base64
import json
import re
import struct
import sys

DTYPES = {
    "DT_INVALID": 0, "DT_FLOAT": 1, "DT_DOUBLE": 2, "DT_INT32": 3, "DT_UINT8": 4,
    "DT_INT16": 5, "DT_INT8": 6, "DT_STRING": 7, "DT_COMPLEX64": 8, "DT_INT64": 9,
    "DT_BOOL": 10, "DT_UINT16": 17, "DT_HALF": 19, "DT_UINT32": 22, "DT_UINT64": 23,
}


def varint(value):
    """Base 128 varint; negative values take ten bytes, as int32/int64 do."""
    value &= (1 << 64) - 1
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def key(field, wire_type):
    return varint(field << 3 | wire_type)


def length_delimited(field, payload):
    return key(field, 2) + varint(len(payload)) + payload


def string(field, text):
    return length_delimited(field, text.encode("utf-8"))


def snake(name):
    return re.sub(r"(?<=[a-z0-9])([A-Z])", lambda m: "_" + m.group(1).lower(), name)


def fields(message, allowed, what):
    """`message` with snake_case names, failing on any name not in `allowed`."""
    if not isinstance(message, dict):
        sys.exit(f"tfs-predict-request: {what} is not an object")
    normalized = {snake(name): value for name, value in message.items()}
    unknown = sorted(set(normalized) - set(allowed))
    if unknown:
        sys.exit(f"tfs-predict-request: unsupported {what} fields {unknown}")
    return normalized


def integer(value):
    # The JSON mapping writes 64-bit integers as strings
    return int(value)


def model_spec(spec):
    spec = fields(spec, ("name", "version", "version_label", "signature_name"), "model_spec")
    if "version" in spec and "version_label" in spec:
        sys.exit("tfs-predict-request: model_spec sets both version and version_label")
    out = b""
    if spec.get("name"):
        out += string(1, spec["name"])
    if "version" in spec:
        # google.protobuf.Int64Value: the JSON mapping is the bare number
        version = integer(spec["version"])
        out += length_delimited(2, key(1, 0) + varint(version) if version else b"")
    if spec.get("signature_name"):
        out += string(3, spec["signature_name"])
    if spec.get("version_label"):
        out += string(4, spec["version_label"])
    return out


def tensor_shape(shape):
    shape = fields(shape, ("dim", "unknown_rank"), "tensor_shape")
    out = b""
    for dim in shape.get("dim", []):
        dim = fields(dim, ("size", "name"), "tensor_shape.dim")
        body = b""
        if integer(dim.get("size", 0)):
            body += key(1, 0) + varint(integer(dim["size"]))
        if dim.get("name"):
            body += string(2, dim["name"])
        out += length_delimited(2, body)
    if shape.get("unknown_rank"):
        out += key(3, 0) + varint(1)
    return out


def tensor(proto):
    proto = fields(
        proto,
        ("dtype", "tensor_shape", "version_number", "tensor_content", "float_val",
         "double_val", "int_val", "string_val", "int64_val", "bool_val"),
        "TensorProto",
    )
    out = b""
    dtype = proto.get("dtype", 0)
    dtype = DTYPES[dtype] if isinstance(dtype, str) else int(dtype)
    if dtype:
        out += key(1, 0) + varint(dtype)
    if "tensor_shape" in proto:
        out += length_delimited(2, tensor_shape(proto["tensor_shape"]))
    if proto.get("version_number"):
        out += key(3, 0) + varint(int(proto["version_number"]))
    if proto.get("tensor_content"):
        out += length_delimited(4, base64.b64decode(proto["tensor_content"], validate=True))
    # Fields in number order; repeated scalars are packed in proto3
    packed = {
        5: ("float_val", lambda v: struct.pack("<f", float(v))),
        6: ("double_val", lambda v: struct.pack("<d", float(v))),
        7: ("int_val", lambda v: varint(int(v))),
        10: ("int64_val", lambda v: varint(integer(v))),
        11: ("bool_val", lambda v: varint(1 if v else 0)),
    }
    for field in (5, 6, 7, 8, 10, 11):
        if field == 8:
            for value in proto.get("string_val", []):
                out += length_delimited(8, base64.b64decode(value, validate=True))
            continue
        name, encode = packed[field]
        values = proto.get(name, [])
        if values:
            out += length_delimited(field, b"".join(encode(v) for v in values))
    return out


def predict_request(request):
    request = fields(request, ("model_spec", "inputs", "output_filter"), "PredictRequest")
    out = b""
    if "model_spec" in request:
        out += length_delimited(1, model_spec(request["model_spec"]))
    for name in sorted(request.get("inputs", {})):
        entry = string(1, name) + length_delimited(2, tensor(request["inputs"][name]))
        out += length_delimited(2, entry)
    for name in request.get("output_filter", []):
        out += string(3, name)
    return out


def requests_of(document):
    if isinstance(document, list):
        return document
    if isinstance(document, dict) and isinstance(document.get("requests"), list):
        return [entry["request"] for entry in document["requests"]]
    return [document]


def main(argv):
    framed = argv[:1] == ["--framed"]
    if framed:
        argv = argv[1:]
    if len(argv) != 2:
        sys.exit(__doc__)
    with open(argv[0]) as f:
        requests = [predict_request(r) for r in requests_of(json.load(f))]
    if not requests:
        sys.exit("tfs-predict-request: the input holds no request")
    if framed:
        data = b"".join(b"\0" + struct.pack(">I", len(r)) + r for r in requests)
    elif len(requests) == 1:
        data = requests[0]
    else:
        sys.exit(f"tfs-predict-request: the input holds {len(requests)} requests; use --framed")
    with open(argv[1], "wb") as f:
        f.write(data)
    print(f"tfs-predict-request: wrote {len(requests)} requests, {len(data)} bytes", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1:])
