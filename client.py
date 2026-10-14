"""Counter client (Tasks A2 + B1).

Failure handling:
  * every RPC carries a deadline (default 2.0 s)
  * on DEADLINE_EXCEEDED / UNAVAILABLE: at most 3 retries, backoff 0.2/0.4/0.8 s
  * the idempotency key is generated ONCE per logical operation and reused on
    every retry, so the server's dedup store can suppress double application

Lamport clock: every attempt (including a retry) is its own SEND event, so the
clock is ticked and stamped into the message per attempt.
"""
import argparse
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc
from lamport import LamportClock

DEFAULT_TARGET = "localhost:50051"
TIMEOUT_S = 2.0
MAX_RETRIES = 3
BACKOFF_BASE_S = 0.2
RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class CounterClient:
    def __init__(self, stub, name="client-1", clock=None):
        self.stub = stub
        self.clock = clock or LamportClock(name)

    def _call(self, rpc, request, send_text, recv_text, timeout, max_retries,
              backoff_base):
        """Send `request` with retries. The request (and thus the idempotency
        key) is identical on each attempt; only lamport_time is re-stamped."""
        attempt = 0
        while True:
            request.lamport_time = self.clock.tick(send_text)  # SEND event
            try:
                reply = rpc(request, timeout=timeout)
            except grpc.RpcError as err:
                if err.code() not in RETRYABLE or attempt >= max_retries:
                    raise
                delay = backoff_base * (2 ** attempt)  # 0.2, 0.4, 0.8
                attempt += 1
                print(f"[retry {attempt}/{max_retries}] {err.code().name}; "
                      f"sleeping {delay:.1f}s", file=sys.stderr)
                time.sleep(delay)
                continue
            self.clock.receive(reply.lamport_time, recv_text(reply))  # RECV
            return reply

    def increment(self, counter_id, delta, key=None, timeout=TIMEOUT_S,
                  max_retries=MAX_RETRIES, backoff_base=BACKOFF_BASE_S):
        """One logical increment. The key is created once, here."""
        key = key or str(uuid.uuid4())
        request = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta, idempotency_key=key)
        return self._call(
            self.stub.Increment, request,
            f"SEND Increment(counter={counter_id}, delta={delta})",
            lambda r: f"RECV IncrementReply(new_value={r.new_value})",
            timeout, max_retries, backoff_base)

    def get(self, counter_id, timeout=TIMEOUT_S, max_retries=MAX_RETRIES,
            backoff_base=BACKOFF_BASE_S):
        request = counter_pb2.GetRequest(counter_id=counter_id)
        return self._call(
            self.stub.Get, request,
            f"SEND Get(counter={counter_id})",
            lambda r: f"RECV GetReply(value={r.value}, found={r.found})",
            timeout, max_retries, backoff_base)


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--target", default=DEFAULT_TARGET,
                        help="server address (default: %(default)s)")
    parser.add_argument("--name", default="client-1",
                        help="process name used in Lamport log lines")
    sub = parser.add_subparsers(dest="command", required=True)

    p_incr = sub.add_parser("incr", help="increment a counter")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None,
                        help="idempotency key (default: fresh uuid4)")

    p_get = sub.add_parser("get", help="read a counter")
    p_get.add_argument("counter_id")

    args = parser.parse_args()

    with grpc.insecure_channel(args.target) as channel:
        client = CounterClient(counter_pb2_grpc.CounterStub(channel),
                               name=args.name)
        try:
            if args.command == "incr":
                reply = client.increment(args.counter_id, args.by, key=args.key)
                dup = "yes" if reply.was_duplicate else "no"
                print(f"OK committed value={reply.new_value} (duplicate: {dup})")
            else:
                reply = client.get(args.counter_id)
                if reply.found:
                    print(f"value={reply.value}")
                else:
                    print(f"counter '{args.counter_id}' not found")
        except grpc.RpcError as err:
            print(f"FAILED: {err.code().name}: {err.details()}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
