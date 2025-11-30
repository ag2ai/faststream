import asyncio
import contextlib
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any, Optional, cast

import anyio
from typing_extensions import override

from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream._internal.endpoint.utils import process_msg
from faststream.rabbit.parser import AioPikaParser
from faststream.rabbit.publisher.fake import RabbitFakePublisher
from faststream.rabbit.schemas import RabbitExchange
from faststream.rabbit.schemas.constants import REPLY_TO_QUEUE_EXCHANGE_DELIMITER

if TYPE_CHECKING:
    from aio_pika import IncomingMessage, RobustQueue

    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream._internal.endpoint.subscriber.specification import (
        SubscriberSpecification,
    )
    from faststream.message import StreamMessage
    from faststream.rabbit.configs import RabbitBrokerConfig
    from faststream.rabbit.message import RabbitMessage
    from faststream.rabbit.schemas import RabbitQueue

    from .config import RabbitSubscriberConfig


class RabbitSubscriber(SubscriberUsecase["IncomingMessage"]):
    """A class to handle logic for RabbitMQ message consumption."""

    _outer_config: "RabbitBrokerConfig"

    def __init__(
        self,
        config: "RabbitSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[IncomingMessage]",
    ) -> None:
        parser = AioPikaParser(pattern=config.queue.path_regex)
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        super().__init__(
            config,
            specification=specification,
            calls=calls,
        )

        self.queue = config.queue
        self.exchange = config.exchange

        self.consume_args = config.consume_args or {}

        self.__no_ack = config.ack_first

        self._consumer_tag: str | None = None
        self._queue_obj: RobustQueue | None = None
        self.channel = config.channel

    @property
    def app_id(self) -> str | None:
        return self._outer_config.app_id

    def routing(self) -> str:
        return f"{self._outer_config.prefix}{self.queue.routing()}"

    @override
    async def start(self) -> None:
        """Starts the consumer for the RabbitMQ queue."""
        await super().start()

        queue_to_bind = self.queue.add_prefix(self._outer_config.prefix)

        declarer = self._outer_config.declarer

        self._queue_obj = queue = await declarer.declare_queue(
            queue_to_bind,
            channel=self.channel,
        )

        if (
            self.exchange is not None
            and queue_to_bind.declare  # queue just getted from RMQ
            and self.exchange.name  # check Exchange is not default
        ):
            exchange = await declarer.declare_exchange(
                self.exchange,
                channel=self.channel,
            )

            await queue.bind(
                exchange,
                routing_key=queue_to_bind.routing(),
                arguments=queue_to_bind.bind_arguments,
                timeout=queue_to_bind.timeout,
                robust=self.queue.robust,
            )

        if self.calls:
            self._consumer_tag = await self._queue_obj.consume(
                # NOTE: aio-pika expects AbstractIncomingMessage, not IncomingMessage
                self.consume,  # type: ignore[arg-type]
                no_ack=self.__no_ack,
                arguments=self.consume_args,
            )

        self._post_start()

    async def stop(self) -> None:
        await super().stop()

        if self._queue_obj is not None:
            if self._consumer_tag is not None:  # pragma: no branch
                if not self._queue_obj.channel.is_closed:
                    await self._queue_obj.cancel(self._consumer_tag)
                self._consumer_tag = None

            self._queue_obj = None

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
        no_ack: bool = True,
    ) -> "RabbitMessage | None":
        assert self._queue_obj, "You should start subscriber at first."
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        sleep_interval = timeout / 10

        raw_message: IncomingMessage | None = None
        with (
            contextlib.suppress(asyncio.exceptions.CancelledError),
            anyio.move_on_after(timeout),
        ):
            while (  # noqa: ASYNC110
                raw_message := await self._queue_obj.get(
                    fail=False,
                    no_ack=no_ack,
                    timeout=timeout,
                )
            ) is None:
                await anyio.sleep(sleep_interval)

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        msg: RabbitMessage | None = await process_msg(  # type: ignore[assignment]
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=async_parser,
            decoder=async_decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["RabbitMessage"]:  # type: ignore[override]
        assert self._queue_obj, "You should start subscriber at first."
        assert not self.calls, (
            "You can't use iterator method if subscriber has registered handlers."
        )

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        async with self._queue_obj.iterator() as queue_iter:
            async for raw_message in queue_iter:
                raw_message = cast("IncomingMessage", raw_message)

                msg: RabbitMessage = await process_msg(  # type: ignore[assignment]
                    msg=raw_message,
                    middlewares=(
                        m(raw_message, context=context) for m in self._broker_middlewares
                    ),
                    parser=async_parser,
                    decoder=async_decoder,
                )
                yield msg

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence["PublisherProto"]:
        if REPLY_TO_QUEUE_EXCHANGE_DELIMITER in message.reply_to:
            queue_name, exchange_name = message.reply_to.split(
                REPLY_TO_QUEUE_EXCHANGE_DELIMITER, 2
            )
            publisher = RabbitFakePublisher(
                self._outer_config.producer,
                app_id=self.app_id,
                routing_key=queue_name,
                exchange=RabbitExchange.validate(exchange_name),
            )
        else:
            publisher = RabbitFakePublisher(
                self._outer_config.producer,
                app_id=self.app_id,
                routing_key=message.reply_to,
                exchange=RabbitExchange(),
            )

        return (publisher,)

    @staticmethod
    def build_log_context(
        message: Optional["StreamMessage[Any]"],
        queue: "RabbitQueue",
        exchange: Optional["RabbitExchange"] = None,
    ) -> dict[str, str]:
        return {
            "queue": queue.name,
            "exchange": getattr(exchange, "name", ""),
            "message_id": getattr(message, "message_id", ""),
        }

    def get_log_context(
        self,
        message: Optional["StreamMessage[Any]"],
    ) -> dict[str, str]:
        return self.build_log_context(
            message=message,
            queue=self.queue,
            exchange=self.exchange,
        )
