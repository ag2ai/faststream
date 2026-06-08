import os
from typing import Any

import pytest

from faststream.sqs import SQSRouter
from faststream.sqs.fastapi import SQSRouter as _SQSStreamRouter
from tests.brokers.base.fastapi import FastAPILocalTestcase, FastAPITestcase

from .basic import SQSMemoryTestcaseConfig, SQSTestcaseConfig


class StreamRouter(_SQSStreamRouter):
    """FastAPI SQS router pre-wired to LocalStack.

    The shared FastAPI test cases instantiate ``router_class()`` with no
    arguments, so the connection defaults have to be baked into the class.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault(
            "endpoint_url",
            os.environ.get("SQS_ENDPOINT_URL", "http://localhost:4566"),
        )
        kwargs.setdefault("region_name", os.environ.get("AWS_REGION", "us-east-1"))
        kwargs.setdefault(
            "aws_access_key_id",
            os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        )
        kwargs.setdefault(
            "aws_secret_access_key",
            os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        )
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
