from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
import logging
from typing import TYPE_CHECKING, Any, Optional

import anyio
from typing_extensions import override

from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream._internal.endpoint.utils import process_msg
from faststream.exceptions import AckMessage, NackMessage, RejectMessage, StopConsume
from faststream.message import AckStatus
from faststream.middlewares import AckPolicy
from faststream.mq.helpers import AsyncMQConnection
from faststream.mq.parser import MQParser
from faststream.mq.publisher.fake import MQFakePublisher
from faststream.mq.publisher.producer import AsyncMQConnectionProducer

if TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream._internal.endpoint.subscriber.specification import (
        SubscriberSpecification,
    )
    from faststream.message import StreamMessage
    from faststream.mq.message import MQMessage, MQRawMessage
    from faststream.mq.schemas import MQQueue

    from .config import MQSubscriberConfig


class MQSubscriber(SubscriberUsecase["MQRawMessage"]):
    def __init__(
        self,
        config: "MQSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[MQRawMessage]",
    ) -> None:
        parser = MQParser()
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        super().__init__(config, specification=specification, calls=calls)

        self.queue = config.queue
        self.wait_interval = config.wait_interval
        self._consumer: AsyncMQConnection | None = None
        self._consumer_task: asyncio.Task[None] | None = None
        self._test_messages: asyncio.Queue[MQRawMessage] | None = None

    def routing(self) -> str:
        return f"{self._outer_config.prefix}{self.queue.routing()}"

    @override
    async def start(self) -> None:
        await super().start()

        if getattr(self._outer_config.producer, "is_test_producer", False):
            self._test_messages = asyncio.Queue()
            self._post_start()
            return

        self._consumer = AsyncMQConnection(
            connection_config=self._outer_config.connection_config,
        )
        await self._consumer.connect()
        await self._consumer.start_consumer(self.routing())

        if self.calls:
            self._consumer_task = asyncio.create_task(self._consume_loop())

        self._post_start()

    async def _consume_loop(self) -> None:
        assert self._consumer is not None

        while self.running:
            try:
                raw_message = await self._consumer.get_message(timeout=self.wait_interval)
            except Exception:
                if self.running:
                    raise
                break

            if raw_message is None:
                continue

            await self.consume(raw_message)

    @override
    async def consume(self, msg: "MQRawMessage") -> Any:
        if not self.running:
            return None

        try:
            return await self.process_message(msg)

        except StopConsume as exc:
            await self._settle_unresolved_message(msg, exc)
            await self.stop()

        except SystemExit as exc:
            await self._settle_unresolved_message(msg, exc)
            await self.stop()

            if app := self._outer_config.fd_config.context.get("app"):
                app.exit()

        except Exception as exc:  # nosec B110
            await self._settle_unresolved_message(msg, exc)

    async def stop(self) -> None:
        await super().stop()

        if self._test_messages is not None:
            self._test_messages = None
            return

        if self._consumer_task is not None:
            self._consumer_task.cancel()
            with anyio.CancelScope(shield=True):
                await asyncio.gather(self._consumer_task, return_exceptions=True)
            self._consumer_task = None

        if self._consumer is not None:
            await self._consumer.stop_consumer()
            await self._consumer.disconnect()
            self._consumer = None

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "MQMessage | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        if self._test_messages is not None:
            try:
                raw_message = await asyncio.wait_for(self._test_messages.get(), timeout)
            except asyncio.TimeoutError:
                return None
        else:
            assert self._consumer is not None, "You should start subscriber at first."
            raw_message = await self._consumer.get_message(timeout=timeout)

        if raw_message is None:
            return None

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        try:
            return await process_msg(
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=async_parser,
                decoder=async_decoder,
            )
        except Exception as exc:
            await self._settle_unresolved_message(raw_message, exc)
            raise

    @override
    async def __aiter__(self) -> AsyncIterator["MQMessage"]:
        assert not self.calls, (
            "You can't use iterator method if subscriber has registered handlers."
        )

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        while self.running:
            if self._test_messages is not None:
                try:
                    raw_message = await asyncio.wait_for(
                        self._test_messages.get(),
                        self.wait_interval,
                    )
                except asyncio.TimeoutError:
                    continue
            else:
                assert self._consumer is not None, "You should start subscriber at first."
                raw_message = await self._consumer.get_message(timeout=self.wait_interval)

            if raw_message is None:
                continue
            try:
                msg = await process_msg(
                    msg=raw_message,
                    middlewares=(
                        m(raw_message, context=context) for m in self._broker_middlewares
                    ),
                    parser=async_parser,
                    decoder=async_decoder,
                )
            except Exception as exc:
                await self._settle_unresolved_message(raw_message, exc)
                raise
            yield msg

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence["PublisherProto"]:
        producer = self._outer_config.producer
        if message.raw_message.connection is not None:
            producer = AsyncMQConnectionProducer(
                message.raw_message.connection,
                serializer=self._outer_config.fd_config._serializer,
            )

        return (
            MQFakePublisher(
                producer,
                queue=message.reply_to,
                native_correlation_id=message.raw_message.native_message_id,
            ),
        )

    @staticmethod
    def build_log_context(
        message: Optional["StreamMessage[Any]"],
        queue: "MQQueue",
    ) -> dict[str, str]:
        return {
            "queue": queue.name,
            "message_id": getattr(message, "message_id", ""),
        }

    def get_log_context(
        self,
        message: Optional["StreamMessage[Any]"],
    ) -> dict[str, str]:
        return self.build_log_context(message=message, queue=self.queue)

    async def put_test_message(self, message: "MQRawMessage") -> None:
        assert self._test_messages is not None, "Test buffer is not initialized."
        await self._test_messages.put(message)

    async def _settle_unresolved_message(
        self,
        raw_message: "MQRawMessage",
        exc: BaseException,
    ) -> None:
        if raw_message.connection is None or raw_message.settled is not None:
            return

        try:
            if self._should_commit(exc):
                await asyncio.shield(raw_message.connection.commit())
                raw_message.settled = AckStatus.ACKED
            else:
                await asyncio.shield(raw_message.connection.backout())
                raw_message.settled = AckStatus.NACKED

        except asyncio.CancelledError:
            pass

        except Exception as err:
            self._log(logging.CRITICAL, repr(err), exc_info=err)

    def _should_commit(self, exc: BaseException) -> bool:
        if isinstance(exc, (AckMessage, RejectMessage)):
            return True

        if isinstance(exc, NackMessage):
            return False

        return self.ack_policy in {
            AckPolicy.ACK,
            AckPolicy.ACK_FIRST,
            AckPolicy.REJECT_ON_ERROR,
        }
