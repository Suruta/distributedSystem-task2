"""Counter client (Task A2).

Failure handling:
  * every RPC carries a deadline (default 2.0 s)
  * on DEADLINE_EXCEEDED / UNAVAILABLE: at most 3 retries, backoff 0.2/0.4/0.8 s
  * the idempotency key is generated ONCE per logical operation and reused on
    every retry, so the server's dedup store can suppress double application
"""
import argparse
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc

DEFAULT_TARGET = "localhost:50051"
TIMEOUT_S = 2.0
MAX_RETRIES = 3
BACKOFF_BASE_S = 0.2
RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


def _call_with_retries(rpc, request, timeout=TIMEOUT_S, max_retries=MAX_RETRIES,
                       backoff_base=BACKOFF_BASE_S):
    """Invoke rpc(request); retry retryable failures with exponential backoff.

    The SAME request object (hence the same idempotency key) is sent each time.
    """
    attempt = 0
    while True:
        try:
            return rpc(request, timeout=timeout)
        except grpc.RpcError as err:
            if err.code() not in RETRYABLE or attempt >= max_retries:
                raise
            delay = backoff_base * (2 ** attempt)  # 0.2, 0.4, 0.8
            attempt += 1
            print(f"[retry {attempt}/{max_retries}] {err.code().name}; "
                  f"sleeping {delay:.1f}s", file=sys.stderr)
            time.sleep(delay)


def increment(stub, counter_id, delta, key=None, **kwargs):
    """One logical increment. A key is created once here unless supplied."""
    key = key or str(uuid.uuid4())
    request = counter_pb2.IncrementRequest(
        counter_id=counter_id, delta=delta, idempotency_key=key)
    return _call_with_retries(stub.Increment, request, **kwargs)


def get(stub, counter_id, **kwargs):
    request = counter_pb2.GetRequest(counter_id=counter_id)
    return _call_with_retries(stub.Get, request, **kwargs)


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--target", default=DEFAULT_TARGET,
                        help="server address (default: %(default)s)")
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
        stub = counter_pb2_grpc.CounterStub(channel)
        try:
            if args.command == "incr":
                reply = increment(stub, args.counter_id, args.by, key=args.key)
                dup = "yes" if reply.was_duplicate else "no"
                print(f"OK committed value={reply.new_value} (duplicate: {dup})")
            else:
                reply = get(stub, args.counter_id)
                if reply.found:
                    print(f"value={reply.value}")
                else:
                    print(f"counter '{args.counter_id}' not found")
        except grpc.RpcError as err:
            print(f"FAILED: {err.code().name}: {err.details()}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
