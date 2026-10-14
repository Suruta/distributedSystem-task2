"""Counter service (Tasks A2 + A3 + B1).

Synchronization: a single threading.Lock (self._lock) protects BOTH the counter
values and the deduplication store. The idempotency-key check and the counter
mutation happen inside one critical section, so two concurrent retries with the
same key cannot both pass the check.

Lamport clock: each server owns a LamportClock (its own lock). Lock order is
always counter-lock -> clock-lock, never the reverse, so there is no deadlock.
"""
import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from lamport import LamportClock


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica-A", clock=None):
        self._lock = threading.Lock()
        self._values = {}  # counter_id -> int
        self._seen = {}    # idempotency_key -> (counter_id, resulting value)
        self.clock = clock or LamportClock(name)

    def Increment(self, request, context):
        cid, delta = request.counter_id, request.delta
        self.clock.receive(
            request.lamport_time,
            f"RECV Increment(counter={cid}, delta={delta})")

        key = request.idempotency_key
        if not key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "idempotency_key is required")
        if not cid:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "counter_id is required")

        with self._lock:
            if key in self._seen:
                # Duplicate: return the stored result, do NOT apply again.
                _, new_value = self._seen[key]
                was_duplicate = True
                self.clock.tick(
                    f"DEDUP counter={cid} -> {new_value} (key already applied)")
            else:
                new_value = self._values.get(cid, 0) + delta
                self._values[cid] = new_value
                self._seen[key] = (cid, new_value)
                was_duplicate = False
                self.clock.tick(f"APPLY counter={cid} -> {new_value}")

        send_l = self.clock.tick(f"SEND IncrementReply(new_value={new_value})")
        return counter_pb2.IncrementReply(
            new_value=new_value, was_duplicate=was_duplicate,
            lamport_time=send_l)

    def Get(self, request, context):
        cid = request.counter_id
        self.clock.receive(request.lamport_time, f"RECV Get(counter={cid})")
        with self._lock:
            found = cid in self._values
            value = self._values.get(cid, 0)
        send_l = self.clock.tick(f"SEND GetReply(value={value}, found={found})")
        return counter_pb2.GetReply(value=value, found=found,
                                    lamport_time=send_l)


def build_server(port, max_workers=8, name="replica-A", clock=None):
    """Create (but do not start) a server. port=0 picks a free port.

    Returns (server, bound_port, servicer) so tests can reuse it.
    """
    servicer = CounterServicer(name=name, clock=clock)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    return server, bound_port, servicer


def main():
    parser = argparse.ArgumentParser(description="Counter gRPC server")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A",
                        help="process name used in Lamport log lines")
    args = parser.parse_args()

    server, port, _ = build_server(args.port, name=args.name)
    server.start()
    print(f"Counter server '{args.name}' listening on port {port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)


if __name__ == "__main__":
    main()
