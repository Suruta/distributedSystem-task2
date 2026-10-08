import argparse
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc


MAX_RETRIES = 3
DEADLINE_SECONDS = 2.0
INITIAL_BACKOFF_SECONDS = 0.2


class CounterClient:
    def __init__(self, address="localhost:50052"):
        self.channel = grpc.insecure_channel(address)
        self.stub = counter_pb2_grpc.CounterStub(self.channel)

    def increment(self, counter_id, delta):
        # Generate the idempotency key ONCE for this
        # logical operation.
        idempotency_key = str(uuid.uuid4())

        request = counter_pb2.IncrementRequest(
            counter_id=counter_id,
            delta=delta,
            idempotency_key=idempotency_key,
        )

        # Initial attempt + at most 3 retries.
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = self.stub.Increment(
                    request,
                    timeout=DEADLINE_SECONDS,
                )

                return response

            except grpc.RpcError as e:
                # Retry only on these errors.
                if e.code() not in (
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    grpc.StatusCode.UNAVAILABLE,
                ):
                    raise

                # No more retries.
                if attempt == MAX_RETRIES:
                    raise

                # Exponential backoff:
                # 0.2s, 0.4s, 0.8s
                backoff = INITIAL_BACKOFF_SECONDS * (2 ** attempt)
                time.sleep(backoff)

        raise RuntimeError("Increment failed unexpectedly")

    def get(self, counter_id):
        request = counter_pb2.GetRequest(
            counter_id=counter_id,
        )

        # Get also gets a deadline.
        response = self.stub.Get(
            request,
            timeout=DEADLINE_SECONDS,
        )

        return response


def main():
    parser = argparse.ArgumentParser(
        description="Distributed counter client"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    # -------------------------
    # incr command
    # -------------------------
    incr_parser = subparsers.add_parser(
        "incr",
        help="increment a counter",
    )

    incr_parser.add_argument(
        "counter_id",
        help="counter ID, e.g. likes:post-42",
    )

    incr_parser.add_argument(
        "--by",
        type=int,
        required=True,
        help="amount to increment by",
    )

    # -------------------------
    # get command
    # -------------------------
    get_parser = subparsers.add_parser(
        "get",
        help="get a counter value",
    )

    get_parser.add_argument(
        "counter_id",
        help="counter ID, e.g. likes:post-42",
    )

    args = parser.parse_args()

    client = CounterClient()

    if args.command == "incr":
        response = client.increment(
            counter_id=args.counter_id,
            delta=args.by,
        )

        print(
            f"value={response.new_value} "
            f"was_duplicate={response.was_duplicate}"
        )

    elif args.command == "get":
        response = client.get(args.counter_id)

        if response.found:
            print(f"value={response.value}")
        else:
            print("value=0")


if __name__ == "__main__":
    main()







