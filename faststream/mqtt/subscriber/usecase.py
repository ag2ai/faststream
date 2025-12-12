import asyncio
import logging
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any

import aiomqtt
from typing_extensions import override

from faststream._internal.endpoint.publisher import PublisherProto
from faststream._internal.endpoint.subscriber import (
    SubscriberSpecification,
    SubscriberUsecase,
)
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.message import StreamMessage
from faststream.mqtt.parser import MQTTParser

if TYPE_CHECKING:
    from aiomqtt import Message

    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.mqtt.configs.broker import MQTTBrokerConfig
    from faststream.mqtt.message import MQTTMessage
    from faststream.mqtt.subscriber.config import (
        MQTTSubscriberConfig,
    )


class MQTTSubscriber(TasksMixin, SubscriberUsecase["Message"]):
    _outer_config: "MQTTBrokerConfig"

    def __init__(
        self,
        config: "MQTTSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Message]",
    ) -> None:
        parser = MQTTParser()
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        super().__init__(config, specification, calls)
        self.topic = config.topic
        self.__client: aiomqtt.Client | None = None
        self.__message_iterator: AsyncIterator[aiomqtt.Message] | None = None

    @override
    async def start(self) -> None:
        await super().start()
        self.__client = self._outer_config.create_client()
        await self.__client.__aenter__()
        await self.__client.subscribe(self.topic)

        self._post_start()
        if self.calls:
            self.add_task(self.__run_consume_loop)

    @override
    async def stop(self) -> None:
        await super().stop()
        if self.__client is not None:
            await self.__client.unsubscribe(self.topic)
            await self.__client.__aexit__(None, None, None)
            self.__client = None

    @property
    def _message_iterator(self) -> AsyncIterator[aiomqtt.Message]:
        assert self.__client, "Client is not initialized"
        if self.__message_iterator is None:
            self.__message_iterator = self.__client.messages
        return self.__message_iterator

    @override
    async def __aiter__(self) -> AsyncIterator["MQTTMessage"]:  # type: ignore[override]
        assert not self.calls, (
            "You can't iterate over a subscriber with registered handlers."
        )
        assert self.__client, "You should start subscriber at first."

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        async for raw_message in self._message_iterator:
            msg: MQTTMessage = await process_msg(  # type: ignore[assignment]
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=async_parser,
                decoder=async_decoder,
            )
            yield msg

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
        no_ack: bool = True,
    ) -> "MQTTMessage | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )
        assert self.__client, "You should start subscriber at first."

        try:
            raw_message = await asyncio.wait_for(
                self.__client.messages.__anext__(), timeout
            )
        except TimeoutError:
            return None

        context = self._outer_config.fd_config.context

        async_parser, async_decoder = self._get_parser_and_decoder()

        msg: MQTTMessage | None = await process_msg(  # type: ignore[assignment]
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=async_parser,
            decoder=async_decoder,
        )
        return msg

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence["PublisherProto"]:
        raise NotImplementedError

    async def get_msg(self) -> aiomqtt.Message:
        return await self._message_iterator.__anext__()

    def get_log_context(
        self,
        message: StreamMessage[aiomqtt.Message] | None,
    ) -> dict[str, str]:
        topic = self.topic if message is None else message.raw_message.topic.value
        return {
            "topic": topic,
            "message_id": "" if message is None else str(message.raw_message.mid),
        }

    async def __run_consume_loop(self) -> None:
        # simple implementation
        while self.running:
            try:
                msg = await self.get_msg()
            except aiomqtt.MqttError as e:  # noqa: PERF203
                self._log(logging.ERROR, "MQTT error occurred", exc_info=e)
                # TODO now it breaks
                raise
            else:
                if msg:
                    await self.consume(msg)
