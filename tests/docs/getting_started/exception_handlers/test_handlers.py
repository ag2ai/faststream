import pytest

from faststream import TestApp
from tests.marks import require_aiokafka


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
@pytest.mark.parametrize("message", ("subscriber", "broker"))
async def test_kafka_example_suppresses_handled_errors(message: str) -> None:
    from docs.docs_src.getting_started.exception_handlers.kafka import app, broker, handle
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker, connect_only=True) as br, TestApp(app):
        await br.publish(message, "exception-handlers")

        handle.mock.assert_called_once_with(message)


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_example_propagates_error_after_app_declines() -> None:
    from docs.docs_src.getting_started.exception_handlers.kafka import app, broker, handle
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker, connect_only=True) as br, TestApp(app):
        with pytest.raises(TypeError, match="application error"):
            await br.publish("application", "exception-handlers")

        handle.mock.assert_called_once_with("application")
