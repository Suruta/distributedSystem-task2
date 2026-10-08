import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, replica_name="replica-A"):
        self._name = replica_name

        # Lamport clock for this replica.
        self._lamport = 0

        self._lock = threading.Lock()

        self._values = {}  # counter_id -> int

        # idempotency_key -> (counter_id, resulting value)
        self._seen = {}

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

    def Increment(self, request, context):
        with self._lock:
            # Receiving the Increment message is an event.
            recv_lamport = self._receive_event(
                request.lamport_time
            )

            print(
                f"[{self._name}] RECV "
                f"Increment(counter={request.counter_id}, "
                f"delta={request.delta}) "
                f"L={recv_lamport} "
                f"(received L={request.lamport_time})"
            )

            # Check for a duplicate request.
            if request.idempotency_key in self._seen:
                counter_id, value = self._seen[
                    request.idempotency_key
                ]

                # Sending the reply is a local event.
                send_lamport = self._local_event()

                print(
                    f"[{self._name}] SEND "
                    f"IncrementReply(new_value={value}) "
                    f"L={send_lamport}"
                )

                return counter_pb2.IncrementReply(
                    new_value=value,
                    was_duplicate=True,
                    lamport_time=send_lamport,
                )

            # Applying the increment is a local event.
            apply_lamport = self._local_event()

            current_value = self._values.get(
                request.counter_id,
                0,
            )

            new_value = current_value + request.delta

            self._values[request.counter_id] = new_value

            # Remember the result for idempotent retries.
            self._seen[request.idempotency_key] = (
                request.counter_id,
                new_value,
            )

            print(
                f"[{self._name}] APPLY "
                f"counter={request.counter_id} -> "
                f"{new_value} "
                f"L={apply_lamport}"
            )

            # Sending the reply is another local event.
            send_lamport = self._local_event()

            print(
                f"[{self._name}] SEND "
                f"IncrementReply(new_value={new_value}) "
                f"L={send_lamport}"
            )

            return counter_pb2.IncrementReply(
                new_value=new_value,
                was_duplicate=False,
                lamport_time=send_lamport,
            )

    def Get(self, request, context):
        with self._lock:
            value = self._values.get(request.counter_id, 0)
            found = request.counter_id in self._values

            return counter_pb2.GetReply(
                value=value,
                found=found,
            )


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer("replica-A"),
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
