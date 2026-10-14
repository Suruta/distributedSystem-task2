import grpc
import pytest

import counter_pb2
from client import CounterClient


class FakeRpcError(grpc.RpcError):
    def __init__(self, status):
        super().__init__()
        self._status = status

    def code(self):
        return self._status


class FakeStub:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.requests = []

    def Increment(self, request, timeout=None):
        self.requests.append(request)

        outcome = self.outcomes.pop(0)

        if isinstance(outcome, Exception):
            raise outcome

        return outcome


class FakeChannel:
    def close(self):
        pass


def make_reply(value, lamport=10, duplicate=False):
    return counter_pb2.IncrementReply(
        new_value=value,
        was_duplicate=duplicate,
        lamport_time=lamport,
    )


def make_client(outcomes):
    client = CounterClient.__new__(CounterClient)
    client._name = "client-test"
    client._lamport = 0
    client.channel = FakeChannel()
    client.stub = FakeStub(outcomes)
    return client


def test_unavailable_retries_with_same_idempotency_key(monkeypatch):
    sleeps = []
    monkeypatch.setattr("client.time.sleep", sleeps.append)

    client = make_client([
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        make_reply(5),
    ])

    response = client.increment("likes:post-42", 5)

    assert response.new_value == 5
    assert len(client.stub.requests) == 2

    keys = [
        request.idempotency_key
        for request in client.stub.requests
    ]
    assert keys[0] == keys[1]
    assert keys[0] != ""

    # A new send is a new local event and gets a fresh timestamp.
    clocks = [
        request.lamport_time
        for request in client.stub.requests
    ]
    assert clocks[1] > clocks[0]
    assert sleeps == [0.2]


def test_deadline_exceeded_retries(monkeypatch):
    monkeypatch.setattr("client.time.sleep", lambda _: None)

    client = make_client([
        FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED),
        make_reply(7),
    ])

    response = client.increment("counter-A", 7)

    assert response.new_value == 7
    assert len(client.stub.requests) == 2
    assert (
        client.stub.requests[0].idempotency_key
        == client.stub.requests[1].idempotency_key
    )


def test_retries_at_most_three_times(monkeypatch):
    sleeps = []
    monkeypatch.setattr("client.time.sleep", sleeps.append)

    client = make_client([
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
    ])

    with pytest.raises(FakeRpcError):
        client.increment("counter-A", 1)

    assert len(client.stub.requests) == 4
    assert sleeps == [0.2, 0.4, 0.8]

    keys = {
        request.idempotency_key
        for request in client.stub.requests
    }
    assert len(keys) == 1


def test_non_retryable_error_is_not_retried(monkeypatch):
    sleeps = []
    monkeypatch.setattr("client.time.sleep", sleeps.append)

    client = make_client([
        FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT),
        make_reply(1),
    ])

    with pytest.raises(FakeRpcError):
        client.increment("counter-A", 1)

    assert len(client.stub.requests) == 1
    assert sleeps == []


def test_duplicate_reply_is_returned_without_extra_attempt(monkeypatch):
    monkeypatch.setattr("client.time.sleep", lambda _: None)

    client = make_client([
        make_reply(5, duplicate=True),
    ])

    response = client.increment("counter-A", 5)

    assert response.new_value == 5
    assert response.was_duplicate is True
    assert len(client.stub.requests) == 1
