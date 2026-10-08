import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._values = {}  # counter_id -> int

        # idempotency_key -> (counter_id, resulting value)
        self._seen = {}

    def Increment(self, request, context):
        with self._lock:
            # Check whether this logical operation was already applied.
            if request.idempotency_key in self._seen:
                counter_id, value = self._seen[request.idempotency_key]

                return counter_pb2.IncrementReply(
                    new_value=value,
                    was_duplicate=True,
                )

            # Get the current counter value.
            current_value = self._values.get(request.counter_id, 0)

            # Apply the delta.
            new_value = current_value + request.delta

            # Update the counter.
            self._values[request.counter_id] = new_value

            # Store the result so retries with the same key
            # do not apply the delta again.
            self._seen[request.idempotency_key] = (
                request.counter_id,
                new_value,
            )

            return counter_pb2.IncrementReply(
                new_value=new_value,
                was_duplicate=False,
            )

    def Get(self, request, context):
        with self._lock:
            if request.counter_id not in self._values:
                return counter_pb2.GetReply(
                    value=0,
                    found=False,
                )

            return counter_pb2.GetReply(
                value=self._values[request.counter_id],
                found=True,
            )


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(),
        server,
    )

    server.add_insecure_port("[::]:50052")
    server.start()

    print("Counter server listening on port 50052")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
