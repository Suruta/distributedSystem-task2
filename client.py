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
                # Only these errors are retryable.
                if e.code() not in (
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    grpc.StatusCode.UNAVAILABLE,
                ):
                    raise

                # Maximum number of retries reached.
                if attempt == MAX_RETRIES:
                    raise

                # Exponential backoff:
                # attempt 0 -> 0.2 seconds
                # attempt 1 -> 0.4 seconds
                # attempt 2 -> 0.8 seconds
                backoff = INITIAL_BACKOFF_SECONDS * (2 ** attempt)
                time.sleep(backoff)

        raise RuntimeError("Increment failed unexpectedly")


def main():
    client = CounterClient()

    response = client.increment(
        counter_id="counter-1",
        delta=5,
    )

    print(f"new_value: {response.new_value}")
    print(f"was_duplicate: {response.was_duplicate}")


if __name__ == "__main__":
    main()