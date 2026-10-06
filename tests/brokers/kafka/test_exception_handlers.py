from typing import TYPE_CHECKING, Any

import pytest
from typing_extensions import override

from faststream.kafka.testing import build_message
from tests.brokers.base.exception_handlers import ExceptionHandlersTestcase

from .basic import KafkaMemoryTestcaseConfig

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord

    from faststream.kafka import KafkaBroker


@pytest.mark.kafka()
class TestExceptionHandlers(KafkaMemoryTestcaseConfig, ExceptionHandlersTestcase):
    @override
    async def get_consume_message(
        self,
        broker: "KafkaBroker",
        body: Any,
        queue: str,
    ) -> "ConsumerRecord":
        return await build_message(
            message=body,
            topic=queue,
            serializer=broker.config.fd_config._serializer,
            codec=broker.config.broker_codec,
            id_generator=broker.config.id_generator,
        )
