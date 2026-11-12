"""A gRPC server for xylem's tests, on TCP port 50051 and /sock/grpc.sock

Its methods take and return raw bytes, so it needs no generated code:
- /xylem.Test/Echo returns the request message
- /xylem.Test/Big returns a 300 KB message
- /xylem.Test/Fail fails with UNAVAILABLE
Any other method is UNIMPLEMENTED.
"""

import os
from concurrent import futures

import grpc

BIG = b"\0" * 300_000


def fail(request, context):
    context.abort(grpc.StatusCode.UNAVAILABLE, "failing as asked")


HANDLERS = {
    "/xylem.Test/Echo": lambda request, context: request,
    "/xylem.Test/Big": lambda request, context: BIG,
    "/xylem.Test/Fail": fail,
}


class Handler(grpc.GenericRpcHandler):
    def service(self, details):
        handler = HANDLERS.get(details.method)
        return handler and grpc.unary_unary_rpc_method_handler(handler)


def main():
    # The host must be able to connect to the Unix socket
    os.umask(0)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    server.add_generic_rpc_handlers((Handler(),))
    server.add_insecure_port("[::]:50051")
    server.add_insecure_port("unix:/sock/grpc.sock")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
