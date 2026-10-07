import threading
from concurrent import futures

import grpc
import counter_pb2
import counter_pb2_grpc

class CounterServicer(counter_pb2_grpc.CounterServicer):
	def __init__(self):
		self._lock = threading.Lock()
		self._values = {} # counter_id -> int
		self._seen = {} # idempotency_key -> (counter_id, resulting value)

	def Increment(self, request, context):
		with self._lock:
		# If we've already processed this operation, return the
		# previously computed result without applying delta again.
			if request.idempotency_key in self._seen:
				counter_id, result = self._seen[request.idempotency_key]
			
				return counter_pb2.IncrementReply(
					new_value=result,
					was_duplicate=True
				)
		
			# Apply the increment.
			current_value = self._values.get(request.counter_id, 0)
			new_value = current_value + request.delta
			self._values[request.counter_id] = new_value

			# Remember the result for future retries.
			self._seen[request.idempotency_key] = (
				request.counter_id,
				new_value
			)

			return counter_pb2.IncrementReply(
				new_value=new_value,
				was_duplicate=False
			)

def serve():
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

	counter_pb2_grpc.add_CounterServicer_to_server(
		CounterServicer(),
			server
		)

	server.add_insecure_port("[::]:50051")
	server.start()

	print("Server started on port 50051")

	server.wait_for_termination()

if __name__ == "__main__":
	serve()





