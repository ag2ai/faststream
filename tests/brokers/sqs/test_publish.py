import asyncio
from unittest.mock import MagicMock

import pytest

from faststream import Context
from faststream.sqs import SQSResponse
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import SQSTestcaseConfig


@pytest.mark.connected()
@pytest.mark.sqs()
class TestPublish(SQSTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_response(self, queue: str, mock: MagicMock) -> None:
        """A handler returning ``SQSResponse`` publishes it to the next queue."""
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        @pub_broker.publisher(queue + "1")
        async def handle():
            return SQSResponse(1, correlation_id="1")

        @pub_broker.subscriber(queue + "1")
        async def handle_next(msg=Context("message")) -> None:
            mock(body=msg.body, correlation_id=msg.correlation_id)
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue, correlation_id="wrong")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with(body=b"1", correlation_id="1")
