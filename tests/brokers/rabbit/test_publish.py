import asyncio
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest

from faststream import Context
from faststream.rabbit import RabbitResponse
from faststream.rabbit.publisher.producer import AioPikaFastProducer
from tests.brokers.base.publish import BrokerPublishTestcase
from tests.tools import spy_decorator

from .basic import RabbitTestcaseConfig

if TYPE_CHECKING:
    from faststream.rabbit.response import RabbitPublishCommand


@pytest.mark.rabbit()
class TestPublish(RabbitTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_reply_config(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker()

        reply_queue = queue + "reply"

        @pub_broker.subscriber(reply_queue)
        async def reply_handler(m) -> None:
            event.set()
            mock(m)

        @pub_broker.subscriber(queue)
        async def handler(m):
            return RabbitResponse(m, persist=True)

        async with self.patch_broker(pub_broker) as br:
            with patch.object(
                AioPikaFastProducer,
                "publish",
                spy_decorator(AioPikaFastProducer.publish),
            ) as m:
                await br.start()

                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish("Hello!", queue, reply_to=reply_queue),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )

                cmd: RabbitPublishCommand = m.mock.call_args[0][1]
                assert cmd.message_options["persist"]
                assert not cmd.publish_options["immediate"]

        assert event.is_set()
        mock.assert_called_with("Hello!")

    @pytest.mark.asyncio()
    async def test_response(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        @pub_broker.publisher(queue + "1")
        async def handle():
            return RabbitResponse(1, persist=True)

        @pub_broker.subscriber(queue + "1")
        async def handle_next(msg=Context("message")) -> None:
            mock(body=msg.body)
            event.set()

        async with self.patch_broker(pub_broker) as br:
            with patch.object(
                AioPikaFastProducer,
                "publish",
                spy_decorator(AioPikaFastProducer.publish),
            ) as m:
                await br.start()

                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )

                assert event.is_set()

                cmd: RabbitPublishCommand = m.mock.call_args[0][1]
                assert cmd.message_options["persist"]

        mock.assert_called_once_with(body=b"1")

    @pytest.mark.asyncio()
    async def test_response_for_rpc(
        self,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        async def handle():
            return RabbitResponse("Hi!", correlation_id="1")

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            response = await asyncio.wait_for(
                br.request("", queue),
                timeout=3,
            )

            assert await response.decode() == "Hi!", response
