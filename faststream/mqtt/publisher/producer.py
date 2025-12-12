from typing import Any

import aiomqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from typing_extensions import override

from faststream._internal._compat import json_dumps
from faststream._internal.constants import ContentTypes
from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.producer import ProducerProto
from faststream._internal.types import CustomCallable
from faststream.exceptions import FeatureNotSupportedException
from faststream.mqtt.parser import MQTTParser
from faststream.mqtt.response import MQTTPublishCommand


class AiomqttFastProducer(ProducerProto[MQTTPublishCommand]):
    def __init__(
        self,
        connection: aiomqtt.Client,
        parser: "CustomCallable | None" = None,
        decoder: "CustomCallable | None" = None,
    ) -> None:
        self._connection = connection
        default = MQTTParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)

    @override
    async def publish(self, cmd: "MQTTPublishCommand") -> None:
        # todo support features
        body = cmd.body
        ct = ContentTypes.TEXT.value
        if isinstance(body, (dict, list)):  # TODO refactor
            body = json_dumps(body)
            ct = ContentTypes.JSON.value
        properties = Properties(PacketTypes.PUBLISH)  # type: ignore[no-untyped-call]
        properties.ContentType = ct
        await self._connection.publish(cmd.destination, body, properties=properties)

    @override
    async def request(self, cmd: "MQTTPublishCommand") -> Any:
        msg = "Request feature is not supported in mqtt"
        raise FeatureNotSupportedException(msg)

    @override
    async def publish_batch(self, cmd: "MQTTPublishCommand") -> Any:
        msg = "Batch publishing is not implemented yet"
        raise FeatureNotSupportedException(msg)
