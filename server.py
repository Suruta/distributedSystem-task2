"""Counter service (Tasks A2 + A3).

Synchronization: a single threading.Lock (self._lock) protects BOTH the counter
values and the deduplication store. The idempotency-key check and the counter
mutation happen inside one critical section, so two concurrent retries carrying
the same key cannot both pass the check.
"""
import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._values = {}  # counter_id -> int
        self._seen = {}    # idempotency_key -> (counter_id, resulting value)

    def Increment(self, request, context):
        key = request.idempotency_key
        if not key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "idempotency_key is required")
        if not request.counter_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "counter_id is required")

        with self._lock:
            # Duplicate: return the stored result, do NOT apply the delta again.
            if key in self._seen:
                _, stored_value = self._seen[key]
                return counter_pb2.IncrementReply(
                    new_value=stored_value, was_duplicate=True)

            # First time we see this key: apply delta and remember the result.
            new_value = self._values.get(request.counter_id, 0) + request.delta
            self._values[request.counter_id] = new_value
            self._seen[key] = (request.counter_id, new_value)
            return counter_pb2.IncrementReply(
                new_value=new_value, was_duplicate=False)

    def Get(self, request, context):
        with self._lock:
            if request.counter_id in self._values:
                return counter_pb2.GetReply(
                    value=self._values[request.counter_id], found=True)
            return counter_pb2.GetReply(value=0, found=False)


def build_server(port, max_workers=8):
    """Create (but do not start) a server. port=0 picks a free port.

    Returns (server, bound_port, servicer) so tests can reuse it.
    """
    servicer = CounterServicer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    return server, bound_port, servicer


def main():
    parser = argparse.ArgumentParser(description="Counter gRPC server")
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()

    server, port, _ = build_server(args.port)
    server.start()
    print(f"Counter server listening on port {port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)


if __name__ == "__main__":
    main()
