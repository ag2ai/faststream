from typing import Any

import pytest
from aio_pika import RobustConnection

from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.rabbit import RabbitBroker, RabbitRouter, annotations
from tests.brokers.base.driver_annotations import DriverAnnotationTestcase

from .basic import RabbitMemoryTestcaseConfig


@pytest.mark.rabbit()
def test_use_only_rabbit_router() -> None:
    broker = RabbitBroker()
    router: Any = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers: list[Any] = [RabbitRouter(), NatsRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(*routers)


@pytest.mark.rabbit()
class TestDriverAnnotations(RabbitMemoryTestcaseConfig, DriverAnnotationTestcase):
    driver_class = RobustConnection
    driver_path = "aio_pika.robust_connection.RobustConnection"
    context_annotation = annotations.Connection
    annotation_import = "from faststream.rabbit.annotations import Connection"
