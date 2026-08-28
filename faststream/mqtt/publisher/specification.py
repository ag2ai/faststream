from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.mqtt.broker.config import MQTTBrokerConfig
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.specification.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    mqtt as mqtt_bindings,
)

from .config import MQTTPublisherSpecificationConfig


class MQTTPublisherSpecification(
    PublisherSpecification[MQTTBrokerConfig, MQTTPublisherSpecificationConfig],
):
    @property
    def topic(self) -> str:
        return f"{self._outer_config.prefix}{self.config.topic}"

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.topic}:Publisher"

    def get_schema(self) -> dict[str, PublisherSpec]:
        payloads = self.get_payloads()

        return {
            self.name: PublisherSpec(
                description=self.config.description_,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
                    ),
                    bindings=OperationBinding(
                        mqtt=mqtt_bindings.OperationBinding(
                            qos=self.config.qos,
                            retain=self.config.retain,
                        ),
                    ),
                ),
                bindings=ChannelBinding(
                    mqtt=mqtt_bindings.ChannelBinding(
                        topic=self.topic,
                        qos=self.config.qos,
                        retain=self.config.retain,
                    ),
                ),
            ),
        }
