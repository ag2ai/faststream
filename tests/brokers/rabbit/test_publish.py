import asyncio
import datetime as dt
import logging
import uuid
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from dirty_equals import IsNow

from faststream import Context
from faststream.rabbit import RabbitExchange, RabbitMessage, RabbitResponse
from faststream.rabbit.publisher.producer import AioPikaFastProducerImpl
from tests.brokers.base.publish import BrokerPublishTestcase
from tests.tools import spy_decorator

from .basic import RabbitTestcaseConfig

if TYPE_CHECKING:
    from faststream.rabbit.response import RabbitPublishCommand


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestPublish(RabbitTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_reply_config(
        self,
        queue: str,
        mock: MagicMock,
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
                AioPikaFastProducerImpl,
                "publish",
                spy_decorator(AioPikaFastProducerImpl.publish),
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
        mock: MagicMock,
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
                AioPikaFastProducerImpl,
                "publish",
                spy_decorator(AioPikaFastProducerImpl.publish),
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

    @pytest.mark.asyncio()
    async def test_default_timestamp(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ) -> None:
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

        assert mock.call_args.kwargs == {
            "body": b"",
            "timestamp": IsNow(delta=dt.timedelta(seconds=10), tz=dt.timezone.utc),
        }

    @pytest.mark.asyncio()
    async def test_reply_to_with_exchange(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        pub_broker = self.get_broker()

        @pub_broker.subscriber(queue)
        async def handler(m):
            return m

        @pub_broker.subscriber(queue=queue + "reply", exchange="reply_exchange")
        async def reply_handler(m):
            event.set()
            mock(m)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish(
                            "Hello!", queue, reply_to=queue + "reply|reply_exchange"
                        )
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with("Hello!")


@pytest.mark.rabbit()
@pytest.mark.connected()
@pytest.mark.asyncio()
class TestPublishWithExchange(RabbitTestcaseConfig):
    async def test_response_with_exchange(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2651."""
        broker = self.get_broker(apply_types=True, log_level=logging.DEBUG)

        exchange_name = str(uuid.uuid4())

        @broker.subscriber(queue)
        @broker.publisher(queue + "1")
        async def handle() -> RabbitResponse:
            return RabbitResponse(1, exchange=exchange_name)

        @broker.subscriber(queue + "1", exchange=exchange_name)
        async def handle_next(msg: RabbitMessage) -> None:
            mock(body=msg.body)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3.0,
            )

        assert event.is_set()

        mock.assert_called_once_with(body=b"1")

    async def test_publisher_with_exchange(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2651."""
        broker = self.get_broker(apply_types=True, log_level=logging.DEBUG)

        exchange_name = str(uuid.uuid4())

        @broker.subscriber(queue)
        @broker.publisher(queue + "1", exchange=exchange_name)
        async def handle() -> RabbitResponse:
            return RabbitResponse(1)

        @broker.subscriber(queue + "1", exchange=exchange_name)
        async def handle_next(msg: RabbitMessage) -> None:
            mock(body=msg.body)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3.0,
            )

        assert event.is_set()

        mock.assert_called_once_with(body=b"1")

    async def test_response_exchange_override_publisher(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2651."""
        broker = self.get_broker(apply_types=True, log_level=logging.DEBUG)
        exchange_name, exchange_name_2 = str(uuid.uuid4()), str(uuid.uuid4())

        @broker.subscriber(queue)
        @broker.publisher(queue + "1", exchange=exchange_name)
        async def handle() -> RabbitResponse:
            return RabbitResponse(1, exchange=exchange_name_2)

        @broker.subscriber(queue + "1", exchange=exchange_name_2)
        async def handle_next(msg: RabbitMessage) -> None:
            mock(body=msg.body)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3.0,
            )

        assert event.is_set()

        mock.assert_called_once_with(body=b"1")
        assert not mock.error.called

    async def test_response_default_exchange_override_publisher(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2651."""
        broker = self.get_broker(apply_types=True, log_level=logging.DEBUG)
        exchange_name = str(uuid.uuid4())

        @broker.subscriber(queue)
        @broker.publisher(queue + "1", exchange=exchange_name)
        async def handle() -> RabbitResponse:
            return RabbitResponse(1, exchange=RabbitExchange())

        @broker.subscriber(queue + "1")
        async def handle_next(msg: int) -> None:
            mock(body=msg)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3.0,
            )

        assert event.is_set()

        mock.assert_called_once_with(body=1)
        assert not mock.error.called
