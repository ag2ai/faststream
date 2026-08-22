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
    def address(self) -> str:
        """The topic a message actually arrives on.

        A shared subscription is asked for by prefixing the topic with
        `$share/<group>/`, but no message ever carries that prefix in its topic, so
        the address does not either. The channel name and the MQTT channel binding
        both keep it, which is where the group stays visible.
        """
        return f"{self._outer_config.prefix}{self.config.topic}"

    @property
    def topic(self) -> str:
        if self.config.shared:
            return f"$share/{self.config.shared}/{self.address}"
        return self.address

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_
        return f"{self.topic}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        return {
            self.name: SubscriberSpec(
                address=self.address,
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
