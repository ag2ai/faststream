from typing import Any

import pytest
from nats.aio.client import Client

from faststream.exceptions import SetupError
from faststream.nats import NatsRouter, annotations
from faststream.nats.broker.broker import NatsBroker
from faststream.rabbit import RabbitRouter
from tests.brokers.base.driver_annotations import DriverAnnotationTestcase

from .basic import NatsMemoryTestcaseConfig


@pytest.mark.nats()
def test_use_only_nats_router() -> None:
    broker = NatsBroker()
    router: Any = RabbitRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers: list[Any] = [NatsRouter(), RabbitRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(*routers)


@pytest.mark.nats()
class TestDriverAnnotations(NatsMemoryTestcaseConfig, DriverAnnotationTestcase):
    driver_class = Client
    driver_path = "nats.aio.client.Client"
    context_annotation = annotations.Client
    annotation_import = "from faststream.nats.annotations import Client"
