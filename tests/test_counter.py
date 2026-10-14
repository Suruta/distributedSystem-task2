import threading
from concurrent.futures import ThreadPoolExecutor

import counter_pb2
from server import CounterServicer


def make_request(counter_id, delta, key, lamport_time=1):
    return counter_pb2.IncrementRequest(
        counter_id=counter_id,
        delta=delta,
        idempotency_key=key,
        lamport_time=lamport_time,
    )


def test_increment_updates_counter():
    service = CounterServicer()
    response = service.Increment(
        make_request("likes:post-42", 5, "key-1"),
        None,
    )

    assert response.new_value == 5
    assert response.was_duplicate is False


def test_get_returns_updated_value():
    service = CounterServicer()

    service.Increment(
        make_request("likes:post-42", 5, "key-1"),
        None,
    )

    response = service.Get(
        counter_pb2.GetRequest(counter_id="likes:post-42"),
        None,
    )

    assert response.found is True
    assert response.value == 5


def test_get_missing_counter():
    service = CounterServicer()

    response = service.Get(
        counter_pb2.GetRequest(counter_id="missing-counter"),
        None,
    )

    assert response.found is False
    assert response.value == 0


def test_duplicate_key_does_not_increment_twice():
    service = CounterServicer()
    request = make_request("likes:post-42", 5, "same-key")

    first = service.Increment(request, None)
    second = service.Increment(request, None)

    assert first.new_value == 5
    assert first.was_duplicate is False
    assert second.new_value == 5
    assert second.was_duplicate is True

    current = service.Get(
        counter_pb2.GetRequest(counter_id="likes:post-42"),
        None,
    )
    assert current.value == 5


def test_same_key_concurrently_applies_once():
    service = CounterServicer()
    request = make_request("likes:post-42", 1, "concurrent-key")

    def increment():
        return service.Increment(request, None)

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(lambda _: increment(), range(100)))

    current = service.Get(
        counter_pb2.GetRequest(counter_id="likes:post-42"),
        None,
    )

    assert current.value == 1
    assert sum(not r.was_duplicate for r in results) == 1
    assert sum(r.was_duplicate for r in results) == 99


def test_different_keys_apply_independently():
    service = CounterServicer()

    first = service.Increment(
        make_request("likes:post-42", 2, "key-A"),
        None,
    )
    second = service.Increment(
        make_request("likes:post-42", 3, "key-B"),
        None,
    )

    assert first.new_value == 2
    assert second.new_value == 5

    current = service.Get(
        counter_pb2.GetRequest(counter_id="likes:post-42"),
        None,
    )
    assert current.value == 5


def test_increment_reply_contains_lamport_time():
    service = CounterServicer()

    response = service.Increment(
        make_request("likes:post-42", 1, "clock-key", 4),
        None,
    )

    # Receiving L=4 advances the replica to at least 5.
    # APPLY and SEND each advance it again.
    assert response.lamport_time >= 7