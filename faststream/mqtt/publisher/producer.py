import asyncio
from typing import TYPE_CHECKING, Any, Optional

import zmqtt
from typing_extensions import override
from zmqtt import PublishProperties

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException, IncorrectState
from faststream.message import encode_message, gen_cor_id
from faststream.mqtt.parser import MQTTParserV5, MQTTParserV311
from faststream.mqtt.response import MQTTPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.types import AsyncCallable, CustomCallable


class ZmqttBaseProducer(ProducerProto[MQTTPublishCommand]):
    _parser: "AsyncCallable"
    _decoder: "AsyncCallable"

    def __init__(
        self,
        default_parser: Any,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self.serializer: SerializerProto | None = None
        self._client: zmqtt.MQTTClient | None = None

        self._parser = ParserComposition(parser, default_parser.parse_message)
        self._decoder = ParserComposition(decoder, default_parser.decode_message)

    def connect(
        self,
        client: "zmqtt.MQTTClient",
        serializer: Optional["SerializerProto"],
    ) -> None:
        self._client = client
        self.serializer = serializer

    def disconnect(self) -> None:
        self._client = None
        self.serializer = None

    @property
    def _connected_client(self) -> "zmqtt.MQTTClient":
        if self._client is None:
            msg = "Producer is not connected. Call connect() first."
            raise IncorrectState(msg)
        return self._client

    @override
    async def publish(self, cmd: "MQTTPublishCommand") -> None:
        raise NotImplementedError

    @override
    async def request(self, cmd: "MQTTPublishCommand") -> Any:
        raise NotImplementedError

    @override
    async def publish_batch(self, cmd: "MQTTPublishCommand") -> None:
        msg = "MQTT does not support batch publishing."
        raise FeatureNotSupportedException(msg)


class ZmqttProducerV311(ZmqttBaseProducer):
    """Producer for MQTT 3.1.1 — publishes raw bytes only.

    Headers, correlation_id, and other metadata are not supported.
    Use MQTT 5.0 for those features.  Request/reply is supported via
    an explicit reply_to topic provided by the caller.
    """

    def __init__(
        self,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        super().__init__(MQTTParserV311(), parser, decoder)

    @override
    async def publish(self, cmd: "MQTTPublishCommand") -> None:
        if cmd.headers:
            msg = "MQTT 3.1.1 does not support message headers. Use MQTT 5.0."
            raise FeatureNotSupportedException(msg)
        payload, _ = encode_message(cmd.body, self.serializer)
        await self._connected_client.publish(
            cmd.destination,
            payload,
            qos=zmqtt.QoS(cmd.qos),
            retain=cmd.retain,
        )

    @override
    async def request(self, cmd: "MQTTPublishCommand") -> "zmqtt.Message":
        """Request/reply for MQTT 3.1.1 via explicit reply topic.

        The caller must supply ``cmd.reply_to``.  FastStream subscribes to
        that topic, publishes the raw request payload, then waits for the
        first message on the reply topic.  The handler side must publish
        its response to the same topic (e.g. via ``@broker.publisher``).
        """
        if not cmd.reply_to:
            msg = "MQTT 3.1.1 request() requires an explicit reply_to topic."
            raise FeatureNotSupportedException(msg)

        sub = self._connected_client.subscribe(cmd.reply_to)
        await sub.start()

        try:
            payload, _ = encode_message(cmd.body, self.serializer)
            await self._connected_client.publish(
                cmd.destination,
                payload,
                qos=cmd.qos,
                retain=cmd.retain,
            )
            return await asyncio.wait_for(
                sub.get_message(),
                timeout=cmd.timeout or 30.0,
            )
        finally:
            await sub.stop()


class ZmqttProducerV5(ZmqttBaseProducer):
    """Producer for MQTT 5.0 — publishes with PublishProperties."""

    def __init__(
        self,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        super().__init__(MQTTParserV5(), parser, decoder)

    @override
    async def publish(self, cmd: "MQTTPublishCommand") -> None:
        payload, content_type = encode_message(cmd.body, self.serializer)

        user_props: list[tuple[str, str]] = [
            (k, str(v)) for k, v in (cmd.headers or {}).items()
        ]

        properties = PublishProperties(
            content_type=content_type or None,
            response_topic=cmd.reply_to or None,
            correlation_data=cmd.correlation_id.encode() if cmd.correlation_id else None,
            user_properties=tuple(user_props),
            message_expiry_interval=cmd.message_expiry_interval,
        )

        await self._connected_client.publish(
            cmd.destination,
            payload,
            qos=cmd.qos,
            retain=cmd.retain,
            properties=properties,
        )

    @override
    async def request(self, cmd: "MQTTPublishCommand") -> "zmqtt.Message":
        """Request/reply for MQTT 5.0 via zmqtt's native client.request().

        zmqtt auto-generates a unique reply topic.  We pass our correlation
        ID explicitly so the responder echoes it back and the caller can
        verify it on the response StreamMessage.
        """
        payload, content_type = encode_message(cmd.body, self.serializer)
        correlation_id = cmd.correlation_id or gen_cor_id()

        user_props: list[tuple[str, str]] = [
            (k, str(v)) for k, v in (cmd.headers or {}).items()
        ]

        # Pass correlation_data explicitly so the responder echoes it back.
        # Do NOT set response_topic — let zmqtt generate it.
        properties = PublishProperties(
            content_type=content_type or None,
            correlation_data=correlation_id.encode(),
            user_properties=tuple(user_props),
            message_expiry_interval=cmd.message_expiry_interval,
        )

        return await self._connected_client.request(
            cmd.destination,
            payload,
            qos=cmd.qos,
            timeout=cmd.timeout or 30.0,
            properties=properties,
        )


class ZmqttFakeProducer(ZmqttBaseProducer):
    def __init__(self) -> None: ...
    def __bool__(self) -> bool:
        return False

    def connect(
        self,
        client: "zmqtt.MQTTClient",
        serializer: Optional["SerializerProto"],
    ) -> None:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError
