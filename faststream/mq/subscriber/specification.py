from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.mq.configs import MQBrokerConfig
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec

from .config import MQSubscriberSpecificationConfig


class MQSubscriberSpecification(
    SubscriberSpecification[MQBrokerConfig, MQSubscriberSpecificationConfig],
):
    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_
        return f"{self._outer_config.prefix}{self.config.queue.name}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        channel_name = self.name
        return {
            channel_name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    bindings=None,
                    message=Message(
                        title=f"{channel_name}:Message",
                        payload=resolve_payloads(self.get_payloads()),
                    ),
                ),
                bindings=None,
            ),
        }
