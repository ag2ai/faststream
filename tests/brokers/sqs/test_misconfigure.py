import pytest

from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.sqs import FifoQueue, SQSBroker
from faststream.sqs.subscriber.usecase import ConcurrentSQSSubscriber


@pytest.mark.sqs()
def test_max_workers_selects_concurrent_subscriber() -> None:
    sub = SQSBroker().subscriber("queue", max_workers=3)
    assert isinstance(sub, ConcurrentSQSSubscriber)


@pytest.mark.sqs()
def test_max_workers_with_batch_forbidden() -> None:
    with pytest.raises(SetupError):
        SQSBroker().subscriber("queue", batch=True, max_workers=3)


@pytest.mark.sqs()
def test_max_workers_with_fifo_forbidden() -> None:
    with pytest.raises(SetupError):
        SQSBroker().subscriber(FifoQueue(name="queue"), max_workers=3)


@pytest.mark.sqs()
def test_request_attempt_id_non_fifo_forbidden() -> None:
    with pytest.raises(SetupError):
        SQSBroker().subscriber("queue", request_attempt_id="id")


@pytest.mark.sqs()
def test_extend_visibility_requires_visibility_timeout() -> None:
    with pytest.raises(SetupError):
        SQSBroker().subscriber("queue", extend_visibility=True)


@pytest.mark.sqs()
def test_use_only_sqs_router() -> None:
    broker = SQSBroker()
    with pytest.raises(SetupError):
        broker.include_router(NatsRouter())
