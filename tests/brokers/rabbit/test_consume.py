import asyncio
from typing import Any
from unittest.mock import ANY, MagicMock, patch

import pytest
from aio_pika import IncomingMessage, Message, RobustQueue
from aiormq.abc import ConfirmationFrameType

from faststream import AckPolicy
from faststream._internal.endpoint.subscriber.utils import MultiLock
from faststream.exceptions import AckMessage, NackMessage, RejectMessage, SkipMessage
from faststream.rabbit import RabbitExchange, RabbitQueue
from faststream.rabbit.annotations import RabbitMessage
from faststream.rabbit.subscriber.usecase import RabbitSubscriber
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator

from .basic import RabbitTestcaseConfig


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestConsume(RabbitTestcaseConfig, BrokerRealConsumeTestcase):
    @pytest.mark.asyncio()
    async def test_consume_from_exchange(
        self, queue: str, exchange: RabbitExchange, event: asyncio.Event
    ) -> None:
        consume_broker = self.get_broker()

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        def h(m: Any) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            result = await br.publish("hello", queue=queue, exchange=exchange)
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )
            assert isinstance(result, ConfirmationFrameType), result  # type: ignore[arg-type]

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_with_get_old(
        self, queue: str, exchange: RabbitExchange, event: asyncio.Event
    ) -> None:
        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            queue=RabbitQueue(name=queue, declare=False),
            exchange=RabbitExchange(name=exchange.name, declare=False),
        )
        def h(m: Any) -> None:
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
        self, queue: str, exchange: RabbitExchange, event: asyncio.Event
    ) -> None:
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
        self, queue: str, exchange: RabbitExchange, event: asyncio.Event
    ) -> None:
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
        self, queue: str, exchange: RabbitExchange, event: asyncio.Event
    ) -> None:
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
        self, queue: str, exchange: RabbitExchange, event: asyncio.Event
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        async def handler(msg: RabbitMessage) -> None:
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
        self, queue: str, exchange: RabbitExchange, event: asyncio.Event
    ) -> None:
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
        self, queue: str, exchange: RabbitExchange, event: asyncio.Event
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue=queue, exchange=exchange)
        async def handler(msg: RabbitMessage) -> None:
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
        self, queue: str, exchange: RabbitExchange, event: asyncio.Event
    ) -> None:
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
    async def test_consume_skip_message(self, queue: str, event: asyncio.Event) -> None:
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
        self, queue: str, exchange: RabbitExchange, event: asyncio.Event
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue,
            exchange=exchange,
            ack_policy=AckPolicy.MANUAL,
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

    @pytest.mark.asyncio()
    async def test_stop_leaves_new_messages_in_queue(
        self,
        queue: str,
        event: asyncio.Event,
        event2: asyncio.Event,
        event3: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker(graceful_timeout=10.0)

        subscriber = consume_broker.subscriber(queue)

        @subscriber
        async def handler(msg: Any) -> None:
            event.set()
            await event2.wait()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish("first", queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

            with (
                patch.object(
                    RobustQueue,
                    "cancel",
                    spy_decorator(RobustQueue.cancel),
                ) as cancel,
                patch.object(
                    MultiLock,
                    "wait_release",
                    spy_decorator(MultiLock.wait_release),
                ) as wait_release,
            ):
                wait_release.mock.side_effect = lambda *_, **__: event3.set()

                stop_task = asyncio.create_task(subscriber.stop())
                # "first" is still in flight when `stop` starts waiting for it,
                # and the consumer is cancelled by then
                await asyncio.wait_for(event3.wait(), timeout=self.timeout)
                cancel.mock.assert_awaited_once()

            # without a consumer, "second" stays in the queue for the other consumers
            await br.publish("second", queue)
            q = await br.declare_queue(RabbitQueue(queue))
            message = await q.get(no_ack=True, fail=False)

            event2.set()
            await asyncio.wait_for(stop_task, timeout=self.timeout)

        assert message is not None
        assert message.body == b"second"

    @pytest.mark.asyncio()
    async def test_consume_after_stop_requeues_message(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        consume_broker = self.get_broker()

        subscriber = consume_broker.subscriber(
            queue, ack_policy=AckPolicy.REJECT_ON_ERROR
        )

        @subscriber
        async def handler(msg: Any) -> None:
            mock(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()
            # a delivery that arrives while `stop` cancels the consumer
            subscriber.running = False

            with patch.object(
                IncomingMessage,
                "reject",
                spy_decorator(IncomingMessage.reject),
            ) as m:
                m.mock.side_effect = lambda *_, **__: event.set()

                await br.publish("hello", queue)
                await asyncio.wait_for(event.wait(), timeout=self.timeout)

                m.mock.assert_awaited_with(ANY, requeue=True)

        mock.assert_not_called()

    @pytest.mark.asyncio()
    async def test_consume_after_stop_skips_no_ack_message(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        consume_broker = self.get_broker()

        subscriber = consume_broker.subscriber(queue, ack_policy=AckPolicy.ACK_FIRST)

        @subscriber
        async def handler(msg: Any) -> None:
            mock(msg)

        with patch.object(
            RabbitSubscriber,
            "consume",
            spy_decorator(RabbitSubscriber.consume),
        ) as consume:
            consume.mock.side_effect = lambda *_, **__: event.set()

            async with self.patch_broker(consume_broker) as br:
                await br.start()
                # a delivery that arrives while `stop` cancels the consumer
                subscriber.running = False

                with patch.object(
                    IncomingMessage,
                    "reject",
                    spy_decorator(IncomingMessage.reject),
                ) as m:
                    await br.publish("hello", queue)
                    await asyncio.wait_for(event.wait(), timeout=self.timeout)

                    m.mock.assert_not_called()

        mock.assert_not_called()
