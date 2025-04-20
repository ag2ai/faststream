import pytest

from tests.marks import require_aiokafka


@pytest.mark.asyncio()
@require_aiokafka
async def test_index_dep() -> None:
    from docs.docs_src.index.dependencies import base_handler, broker
    from faststream.kafka import TestKafkaBroker

    data = {
        "user": "John",
        "user_id": 1,
    }

    async with TestKafkaBroker(broker) as br:
        await br.publish(data, "in-test")

        base_handler.mock.assert_called_once_with(data)
