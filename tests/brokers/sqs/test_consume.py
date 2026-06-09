import asyncio
from unittest.mock import MagicMock, patch

import pytest

from faststream import AckPolicy
from faststream.sqs.message import SQSMessage
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator

from .basic import SQSTestcaseConfig


@pytest.mark.connected()
@pytest.mark.sqs()
class TestConsume(SQSTestcaseConfig, BrokerRealConsumeTestcase):
    @pytest.mark.asyncio()
    async def test_manual_ack_not_called_automatically(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """With ``AckPolicy.MANUAL`` the framework must not delete the message."""
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue, ack_policy=AckPolicy.MANUAL)

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(body: str) -> None:
            mock(body)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(SQSMessage, "ack", spy_decorator(SQSMessage.ack)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=self.timeout,
                )
                assert not m.mock.called

        assert event.is_set()
        mock.assert_called_once_with("hello")
