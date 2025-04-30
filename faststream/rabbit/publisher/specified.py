from faststream._internal.configs import SpecificationConfigs
from faststream._internal.publisher.specified import (
    SpecificationPublisher as SpecificationPublisherMixin,
)
from faststream.rabbit.publisher.configs import (
    RabbitPublisherBaseConfigs,
)
from faststream.rabbit.schemas.base import RabbitBaseConfigs
from faststream.rabbit.schemas.proto import BaseRMQInformation as RMQSpecificationMixin
from faststream.rabbit.utils import is_routing_exchange
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import (
    Message,
    Operation,
    PublisherSpec,
)
from faststream.specification.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    amqp,
)

from .usecase import LogicPublisher


class SpecificationPublisher(
    SpecificationPublisherMixin,
    RMQSpecificationMixin,
    LogicPublisher,
):
    """AsyncAPI-compatible Rabbit Publisher class."""

    def __init__(
        self,
        *,
        base_configs: RabbitPublisherBaseConfigs,
        rmq_base_configs: RabbitBaseConfigs,
        specification_configs: SpecificationConfigs,
    ) -> None:
        super().__init__(
            specification_configs=specification_configs,
            # propagate to RMQSpecificationMixin
            rmq_base_configs=rmq_base_configs,
        )

        LogicPublisher.__init__(self, base_configs=base_configs)

    def get_default_name(self) -> str:
        routing = (
            self.routing_key
            or (self.queue.routing if is_routing_exchange(self.exchange) else None)
            or "_"
        )

        return f"{routing}:{getattr(self.exchange, 'name', None) or '_'}:Publisher"

    def get_schema(self) -> dict[str, PublisherSpec]:
        payloads = self.get_payloads()

        exchange_binding = amqp.Exchange.from_exchange(self.exchange)
        queue_binding = amqp.Queue.from_queue(self.queue)

        return {
            self.name: PublisherSpec(
                description=self.description,
                operation=Operation(
                    bindings=OperationBinding(
                        amqp=amqp.OperationBinding(
                            routing_key=self.routing or None,
                            queue=queue_binding,
                            exchange=exchange_binding,
                            ack=True,
                            persist=self.message_options.get("persist"),
                            priority=self.message_options.get("priority"),
                            reply_to=self.message_options.get("reply_to"),
                            mandatory=self.publish_options.get("mandatory"),
                        ),
                    ),
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(
                            payloads,
                            "Publisher",
                            served_words=2 if self.title_ is None else 1,
                        ),
                    ),
                ),
                bindings=ChannelBinding(
                    amqp=amqp.ChannelBinding(
                        virtual_host=self.virtual_host,
                        queue=queue_binding,
                        exchange=exchange_binding,
                    ),
                ),
            ),
        }
