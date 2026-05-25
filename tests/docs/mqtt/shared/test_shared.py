import pytest

from docs.docs_src.mqtt.shared.basic import broker, handle_job
from faststream.mqtt import TestMQTTBroker


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_shared_subscription_example() -> None:
    async with TestMQTTBroker(broker):
        await broker.publish({"id": 1}, "workers/jobs/import")
        handle_job.mock.assert_called_once_with({"id": 1})
