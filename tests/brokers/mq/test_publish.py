import asyncio

import pytest

from faststream import Context, Response
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import MQMemoryTestcaseConfig

VALID_CORRELATION_ID = "03" * 24


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestPublish(MQMemoryTestcaseConfig, BrokerPublishTestcase):
    async def test_response(self, queue: str, mock) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        @pub_broker.publisher(queue + "1")
        async def m():
            return Response(
                1,
                headers={"custom": "1"},
                correlation_id=VALID_CORRELATION_ID,
            )

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @pub_broker.subscriber(*args2, **kwargs2)
        async def m_next(msg=Context("message")) -> None:
            event.set()
            mock(
                body=msg.body,
                headers=msg.headers["custom"],
                correlation_id=msg.correlation_id,
            )

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(None, queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        mock.assert_called_with(
            body=b"1",
            correlation_id=VALID_CORRELATION_ID,
            headers="1",
        )
