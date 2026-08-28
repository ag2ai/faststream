from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.mqtt.broker.config import MQTTBrokerConfig
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    mqtt as mqtt_bindings,
)

from .config import MQTTSubscriberSpecificationConfig

if TYPE_CHECKING:
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection


class MQTTSubscriberSpecification(
    SubscriberSpecification[MQTTBrokerConfig, MQTTSubscriberSpecificationConfig],
):
    def __init__(
        self,
        _outer_config: "MQTTBrokerConfig",
        specification_config: "MQTTSubscriberSpecificationConfig",
        calls: "CallsCollection[Any]",
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)

    @property
    def topic(self) -> str:
        base = f"{self._outer_config.prefix}{self.config.topic}"
        if self.config.shared:
            return f"$share/{self.config.shared}/{base}"
        return base

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_
        return f"{self.topic}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        return {
            self.name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                    bindings=OperationBinding(
                        mqtt=mqtt_bindings.OperationBinding(
                            qos=self.config.qos,
                        ),
                    ),
                ),
                bindings=ChannelBinding(
                    mqtt=mqtt_bindings.ChannelBinding(
                        topic=self.topic,
                        qos=self.config.qos,
                    ),
                ),
            ),
        }
