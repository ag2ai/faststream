import pytest

from faststream.nats import JStream, NatsBroker, NatsRouter
from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import NatsTestcaseConfig


@pytest.mark.nats()
class TestSubscriber(NatsTestcaseConfig, IncludeSubscriberTestcase):
    pass


@pytest.mark.nats()
class TestPublisher(NatsTestcaseConfig, IncludePublisherTestcase):
    pass


@pytest.mark.nats()
def test_included_stream_subjects_respects_prefix() -> None:
    stream = JStream("stream", subjects=["useless"])

    router = NatsRouter()

    # subject we should add prefix to
    router.subscriber("*", stream=stream)

    broker = NatsBroker()

    # stream is shared between router and broker
    broker.subscriber("test.logs", stream=stream)
    # stream already exist in original builder
    broker.subscriber("logs", stream=JStream("stream"))

    broker.include_router(router, prefix="test.")

    _, subjects = broker._stream_builder.get(stream)
    assert set(subjects) == {"test.*", "logs", "useless"}
