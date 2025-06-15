import asyncio
from unittest.mock import Mock

import pytest
from fast_depends.msgspec import MsgSpecSerializer

from tests.brokers.base.publish import BrokerPublishTestcase, parametrized
from tests.brokers.rabbit.basic import RabbitMemoryTestcaseConfig


@pytest.mark.rabbit()
class TestMsgspec(RabbitMemoryTestcaseConfig, BrokerPublishTestcase):

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("message", "message_type", "expected_message"),
        parametrized,
    )
    async def test_msgspec_serialize(
        self,
        queue: str,
        message,
        message_type,
        expected_message,
        mock: Mock
    ):
        pub_broker = self.get_broker(apply_types=True, serializer=MsgSpecSerializer)
        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(m: message_type) -> None:
            mock(m)

        async with self.patch_broker(pub_broker) as br:
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(message, queue)),
                ),
                timeout=self.timeout,
            )

        mock.assert_called_with(expected_message)
