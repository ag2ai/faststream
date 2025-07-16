import asyncio
import datetime as dt
from unittest.mock import Mock, patch

import pytest
from dirty_equals import IsNow

from faststream import Context
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitResponse, ReplyConfig
from faststream.rabbit.publisher.producer import AioPikaFastProducer
from tests.brokers.base.publish import BrokerPublishTestcase
from tests.tools import spy_decorator


@pytest.mark.rabbit
class TestPublish(BrokerPublishTestcase):
    def get_broker(self, apply_types: bool = False) -> RabbitBroker:
        return RabbitBroker(apply_types=apply_types)

    @pytest.mark.asyncio
    async def test_reply_config(
        self,
        queue: str,
        event: asyncio.Event,
        mock: Mock,
    ):
        pub_broker = self.get_broker()

        reply_queue = queue + "reply"

        @pub_broker.subscriber(reply_queue)
        async def reply_handler(m):
            event.set()
            mock(m)

        with pytest.warns(DeprecationWarning):

            @pub_broker.subscriber(queue, reply_config=ReplyConfig(persist=True))
            async def handler(m):
                return m

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
                            br.publish("Hello!", queue, reply_to=reply_queue)
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )

                assert m.mock.call_args.kwargs.get("persist")
                assert m.mock.call_args.kwargs.get("immediate") is False

        assert event.is_set()
        mock.assert_called_with("Hello!")

    @pytest.mark.asyncio
    async def test_response(
        self,
        queue: str,
        event: asyncio.Event,
        mock: Mock,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        @pub_broker.publisher(queue + "1")
        async def handle():
            return RabbitResponse(
                1,
                persist=True,
            )

        @pub_broker.subscriber(queue + "1")
        async def handle_next(msg=Context("message")):
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

                assert m.mock.call_args.kwargs.get("persist")

        mock.assert_called_once_with(body=b"1")

    @pytest.mark.asyncio
    async def test_response_for_rpc(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        async def handle():
            return RabbitResponse("Hi!", correlation_id="1")

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            response = await asyncio.wait_for(
                br.publish("", queue, rpc=True),
                timeout=3,
            )

            assert response == "Hi!", response

    @pytest.mark.asyncio
    async def test_default_timestamp(
        self,
        queue: str,
        event: asyncio.Event,
        mock: Mock,
    ):
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        async def handle(msg=Context("message")):
            mock(body=msg.body, timestamp=msg.raw_message.timestamp)
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            assert event.is_set()

        mock.assert_called_once_with(
            body=b"", timestamp=IsNow(delta=3, tz=dt.timezone.utc)
        )

    @pytest.mark.asyncio
    async def test_reply_to_with_exchange(
        self,
        queue: str,
        event: asyncio.Event,
        mock: Mock,
    ):
        pub_broker = self.get_broker(apply_types=True)
        exchange = RabbitExchange(name="reply_exchange")

        @pub_broker.subscriber(queue=queue + "reply", exchange=exchange)
        async def reply_handler(m):
            event.set()
            mock(m)

        args2, kwargs2 = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args2, **kwargs2)
        async def handler(m):
            return m

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish("Hello!", queue, reply_to=queue + "reply|reply_exchange")
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with("Hello!")
