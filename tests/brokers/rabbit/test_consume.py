import asyncio
from unittest.mock import patch

import pytest
from aio_pika import IncomingMessage, Message
from aiormq.abc import ConfirmationFrameType

from faststream import AckPolicy
from faststream.exceptions import AckMessage, NackMessage, RejectMessage, SkipMessage
from faststream.rabbit import RabbitExchange, RabbitQueue
from faststream.rabbit.annotations import RabbitMessage
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator

from .basic import RabbitTestcaseConfig


@pytest.mark.rabbit()
class TestConsume(RabbitTestcaseConfig, BrokerRealConsumeTestcase):
    @pytest.mark.asyncio()
    async def test_consume_from_exchange(
        self,
        queue: str,
        exchange: RabbitExchange,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        def h(m) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            result = await br.publish("hello", queue=queue, exchange=exchange)
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )
            assert isinstance(result, ConfirmationFrameType), result

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_with_get_old(
        self,
        queue: str,
        exchange: RabbitExchange,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            queue=RabbitQueue(name=queue, declare=False),
            exchange=RabbitExchange(name=exchange.name, declare=False),
        )
        def h(m) -> None:
            event.set()

        async with self.patch_broker(consume_broker, connect_only=True) as br:
            q = await br.declare_queue(RabbitQueue(queue))
            ex = await br.declare_exchange(exchange)
            await q.bind(ex, routing_key=queue)

            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br.publish(
                            Message(b"hello"),
                            queue=queue,
                            exchange=exchange.name,
                        ),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_ack(
        self,
        queue: str,
        exchange: RabbitExchange,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        async def handler(msg: RabbitMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                IncomingMessage,
                "ack",
                spy_decorator(IncomingMessage.ack),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish("hello", queue=queue, exchange=exchange),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_manual_ack(
        self,
        queue: str,
        exchange: RabbitExchange,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        async def handler(msg: RabbitMessage) -> None:
            await msg.ack()
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                IncomingMessage,
                "ack",
                spy_decorator(IncomingMessage.ack),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish("hello", queue=queue, exchange=exchange),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()
        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_exception_ack(
        self,
        queue: str,
        exchange: RabbitExchange,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        async def handler(msg: RabbitMessage) -> None:
            try:
                raise AckMessage
            finally:
                event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                IncomingMessage,
                "ack",
                spy_decorator(IncomingMessage.ack),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish("hello", queue=queue, exchange=exchange),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()
        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_manual_nack(
        self,
        queue: str,
        exchange: RabbitExchange,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        async def handler(msg: RabbitMessage):
            await msg.nack()
            event.set()
            raise ValueError

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                IncomingMessage,
                "nack",
                spy_decorator(IncomingMessage.nack),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish("hello", queue=queue, exchange=exchange),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()
        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_exception_nack(
        self,
        queue: str,
        exchange: RabbitExchange,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        async def handler(msg: RabbitMessage) -> None:
            try:
                raise NackMessage
            finally:
                event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                IncomingMessage,
                "nack",
                spy_decorator(IncomingMessage.nack),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish("hello", queue=queue, exchange=exchange),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()
        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_manual_reject(
        self,
        queue: str,
        exchange: RabbitExchange,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        async def handler(msg: RabbitMessage):
            await msg.reject()
            event.set()
            raise ValueError

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                IncomingMessage,
                "reject",
                spy_decorator(IncomingMessage.reject),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish("hello", queue=queue, exchange=exchange),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()
        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_exception_reject(
        self,
        queue: str,
        exchange: RabbitExchange,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        async def handler(msg: RabbitMessage) -> None:
            try:
                raise RejectMessage
            finally:
                event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                IncomingMessage,
                "reject",
                spy_decorator(IncomingMessage.reject),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish("hello", queue=queue, exchange=exchange),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_called_once()
        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_skip_message(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue)
        async def handler(msg: RabbitMessage) -> None:
            try:
                raise SkipMessage
            finally:
                event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with (
                patch.object(
                    IncomingMessage,
                    "reject",
                    spy_decorator(IncomingMessage.reject),
                ) as m,
                patch.object(
                    IncomingMessage,
                    "reject",
                    spy_decorator(IncomingMessage.reject),
                ) as m1,
                patch.object(
                    IncomingMessage,
                    "reject",
                    spy_decorator(IncomingMessage.reject),
                ) as m2,
            ):
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                assert not m.mock.called
                assert not m1.mock.called
                assert not m2.mock.called

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_no_ack(
        self,
        queue: str,
        exchange: RabbitExchange,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue,
            exchange=exchange,
            ack_policy=AckPolicy.DO_NOTHING,
        )
        async def handler(msg: RabbitMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                IncomingMessage,
                "ack",
                spy_decorator(IncomingMessage.ack),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish("hello", queue=queue, exchange=exchange),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )
                m.mock.assert_not_called()

            assert event.is_set()
