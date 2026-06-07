from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.specification.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    sqs as sqs_bindings,
)
from faststream.sqs.configs import SQSBrokerConfig

from .config import SQSPublisherSpecificationConfig


class SQSPublisherSpecification(
    PublisherSpecification[SQSBrokerConfig, SQSPublisherSpecificationConfig],
):
    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self.config.queue}"

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_
        return f"{self.queue}:Publisher"

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
                        sqs=sqs_bindings.OperationBinding(),
                    ),
                ),
                bindings=ChannelBinding(
                    sqs=sqs_bindings.ChannelBinding(
                        queue={
                            "name": self.queue,
                            "fifo": self.queue.endswith(".fifo"),
                        },
                    ),
                ),
            ),
        }
