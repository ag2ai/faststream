from typing import Any

import pytest

from faststream.sqs import SQSRouter
from faststream.sqs.fastapi import SQSRouter as _SQSStreamRouter
from tests.brokers.base.fastapi import FastAPILocalTestcase, FastAPITestcase

from .basic import ELASTICMQ_CONNECTION, SQSMemoryTestcaseConfig, SQSTestcaseConfig


class StreamRouter(_SQSStreamRouter):
    """FastAPI SQS router pre-wired to the local ElasticMQ emulator.

    The shared FastAPI test cases instantiate ``router_class()`` with no
    arguments, so the connection defaults have to be baked into the class.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        for key, value in ELASTICMQ_CONNECTION.items():
            kwargs.setdefault(key, value)
        super().__init__(*args, **kwargs)


@pytest.mark.connected()
@pytest.mark.sqs()
class TestRouter(SQSTestcaseConfig, FastAPITestcase):
    router_class = StreamRouter
    broker_router_class = SQSRouter


@pytest.mark.sqs()
class TestRouterLocal(SQSMemoryTestcaseConfig, FastAPILocalTestcase):
    router_class = StreamRouter
    broker_router_class = SQSRouter


@pytest.mark.sqs()
def test_subscriber_accepts_batch() -> None:
    router = StreamRouter()

    sub = router.subscriber("test-queue", batch=True)

    assert sub._batch
