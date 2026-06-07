from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    sqs as sqs_bindings,
)
from faststream.sqs.configs import SQSBrokerConfig

from .config import SQSSubscriberSpecificationConfig

if TYPE_CHECKING:
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection


class SQSSubscriberSpecification(
    SubscriberSpecification[SQSBrokerConfig, SQSSubscriberSpecificationConfig],
):
    def __init__(
        self,
        _outer_config: "SQSBrokerConfig",
        specification_config: "SQSSubscriberSpecificationConfig",
        calls: "CallsCollection[Any]",
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)

    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self.config.queue}"

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_
        return f"{self.queue}:{self.call_name}"

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
