from typing import Any

import pytest

from faststream.sqs import SQSRouter
from faststream.sqs.fastapi import SQSRouter as _SQSStreamRouter
from tests.brokers.base.fastapi import FastAPILocalTestcase, FastAPITestcase

from .basic import SQSMemoryTestcaseConfig, SQSTestcaseConfig


class StreamRouter(_SQSStreamRouter):
    """FastAPI SQS router pre-wired to the local ElasticMQ emulator.

    The shared FastAPI test cases instantiate ``router_class()`` with no
    arguments, so the connection defaults have to be baked into the class.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("endpoint_url", "http://localhost:9324")
        kwargs.setdefault("region_name", "us-east-1")
        kwargs.setdefault("aws_access_key_id", "test")
        kwargs.setdefault("aws_secret_access_key", "test")
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
