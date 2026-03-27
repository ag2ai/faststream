from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.mq.configs import MQBrokerConfig
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec

from .config import MQPublisherSpecificationConfig


class MQPublisherSpecification(
    PublisherSpecification[MQBrokerConfig, MQPublisherSpecificationConfig],
):
    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_
        return f"{self._outer_config.prefix}{self.config.queue.name}:Publisher"

    def get_schema(self) -> dict[str, PublisherSpec]:
        return {
            self.name: PublisherSpec(
                description=self.config.description_,
                operation=Operation(
                    bindings=None,
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(self.get_payloads(), "Publisher"),
                    ),
                ),
                bindings=None,
            ),
        }
