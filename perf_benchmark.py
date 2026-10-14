import argparse
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc

import counter_pb2
import counter_pb2_grpc


def percentile(values, percent):
    ordered = sorted(values)
    if not ordered:
        return 0.0

    index = max(0, min(
        len(ordered) - 1,
        int((percent / 100) * len(ordered) + 0.999999) - 1,
    ))
    return ordered[index]


def run_benchmark(address, operations, concurrency, counter_id):
    latencies = []
    errors = []
    completed = 0

    def worker(operation_number):
        # Each worker owns its channel and stub.
        with grpc.insecure_channel(address) as channel:
            stub = counter_pb2_grpc.CounterStub(channel)

            request = counter_pb2.IncrementRequest(
                counter_id=counter_id,
                delta=1,
                idempotency_key=str(uuid.uuid4()),
                lamport_time=1,
            )

            start = time.perf_counter()
            try:
                response = stub.Increment(request, timeout=2.0)
                elapsed = time.perf_counter() - start

                if response.new_value < 1:
                    raise RuntimeError("Unexpected counter value")

                return elapsed, None
            except grpc.RpcError as exc:
                elapsed = time.perf_counter() - start
                return elapsed, str(exc.code())

    wall_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(worker, i)
            for i in range(operations)
        ]

        for future in as_completed(futures):
            elapsed, error = future.result()
            latencies.append(elapsed)

            if error:
                errors.append(error)
            else:
                completed += 1

    wall_time = time.perf_counter() - wall_start

    # Read the final counter value.
    with grpc.insecure_channel(address) as channel:
        stub = counter_pb2_grpc.CounterStub(channel)
        final = stub.Get(
            counter_pb2.GetRequest(counter_id=counter_id),
            timeout=2.0,
        )

    print("\n=== Counter benchmark ===")
    print(f"Address:             {address}")
    print(f"Requested operations:{operations:>8}")
    print(f"Concurrency:         {concurrency:>8}")
    print(f"Successful:          {completed:>8}")
    print(f"Failed:              {len(errors):>8}")
    print(f"Elapsed time:        {wall_time:>8.3f} s")
    print(f"Throughput:          {completed / wall_time:>8.2f} ops/s")

    if latencies:
        print(f"Average latency:     {statistics.mean(latencies) * 1000:>8.2f} ms")
        print(f"Median latency p50:  {percentile(latencies, 50) * 1000:>8.2f} ms")
        print(f"p95 latency:         {percentile(latencies, 95) * 1000:>8.2f} ms")
        print(f"p99 latency:         {percentile(latencies, 99) * 1000:>8.2f} ms")

    print(f"Final counter value: {final.value}")
    print(f"Counter found:       {final.found}")

    if errors:
        print("\nError summary:")
        for error in sorted(set(errors)):
            print(f"  {error}: {errors.count(error)}")


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark the gRPC counter service"
    )
    parser.add_argument("--address", default="localhost:50052")
    parser.add_argument("--operations", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument(
        "--counter-id",
        default=f"benchmark:{uuid.uuid4()}",
    )
    args = parser.parse_args()

    if args.operations <= 0:
        parser.error("--operations must be positive")
    if args.concurrency <= 0:
        parser.error("--concurrency must be positive")

    run_benchmark(
        args.address,
        args.operations,
        args.concurrency,
        args.counter_id,
    )


if __name__ == "__main__":
    main()
