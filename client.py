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
    def __init__(
        self,
        address="localhost:50052",
        client_name="client-1",
    ):
        self._name = client_name

        # Lamport clock for this client process.
        self._lamport = 0

        self.channel = grpc.insecure_channel(address)
        self.stub = counter_pb2_grpc.CounterStub(self.channel)

    def _local_event(self):
        """Advance the Lamport clock for a local event."""
        self._lamport += 1
        return self._lamport

    def _receive_event(self, received_lamport):
        """Update Lamport clock when receiving a message."""
        self._lamport = max(
            self._lamport,
            received_lamport,
        ) + 1

        return self._lamport

    def increment(self, counter_id, delta):
        # Generate the idempotency key ONCE for this
        # logical operation.
        idempotency_key = str(uuid.uuid4())

        for attempt in range(MAX_RETRIES + 1):

            # Sending the request is a local event.
            send_lamport = self._local_event()

            request = counter_pb2.IncrementRequest(
                counter_id=counter_id,
                delta=delta,
                idempotency_key=idempotency_key,
                lamport_time=send_lamport,
            )

            print(
                f"[{self._name}] SEND "
                f"Increment(counter={counter_id}, "
                f"delta={delta}) "
                f"L={send_lamport}"
            )

            try:
                response = self.stub.Increment(
                    request,
                    timeout=DEADLINE_SECONDS,
                )

                # Receiving the reply is an event.
                recv_lamport = self._receive_event(
                    response.lamport_time
                )

                print(
                    f"[{self._name}] RECV "
                    f"IncrementReply(new_value="
                    f"{response.new_value}) "
                    f"L={recv_lamport} "
                    f"(received L={response.lamport_time})"
                )

                return response

            except grpc.RpcError as e:
                if e.code() not in (
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    grpc.StatusCode.UNAVAILABLE,
                ):
                    raise

                if attempt == MAX_RETRIES:
                    raise

                backoff = (
                    INITIAL_BACKOFF_SECONDS
                    * (2 ** attempt)
                )

                time.sleep(backoff)

        raise RuntimeError(
            "Increment failed unexpectedly"
        )

    def get(self, counter_id):
        request = counter_pb2.GetRequest(
            counter_id=counter_id,
        )

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

    incr_parser = subparsers.add_parser(
        "incr",
        help="increment a counter",
    )

    incr_parser.add_argument(
        "counter_id",
    )

    incr_parser.add_argument(
        "--by",
        type=int,
        required=True,
    )

    get_parser = subparsers.add_parser(
        "get",
        help="get a counter value",
    )

    get_parser.add_argument(
        "counter_id",
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

        print(f"value={response.value}")


if __name__ == "__main__":
    main()
