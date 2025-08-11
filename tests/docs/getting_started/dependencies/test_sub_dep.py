import pytest

from faststream import TestApp
from tests.marks import require_aiokafka


@pytest.mark.asyncio()
@require_aiokafka
async def test_sub_dep_kafka() -> None:
    from docs.docs_src.getting_started.dependencies.sub_dep_kafka import (
        app,
        broker,
        handle,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with(
            {
                "name": "John",
                "user_id": 1,
            },
        )

        with pytest.raises(AssertionError):
            await broker.publish({"name": "Ted", "user_id": 1}, "test-topic")
