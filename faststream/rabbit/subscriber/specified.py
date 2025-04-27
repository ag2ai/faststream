
from faststream._internal.configs import SpecificationConfigs
from faststream._internal.subscriber.specified import (
    SpecificationSubscriber as SpecificationSubscriberMixin,
)
from faststream.rabbit.schemas.base import RabbitBaseConfigs
from faststream.rabbit.schemas.proto import BaseRMQInformation as RMQSpecificationMixin
from faststream.rabbit.subscriber.configs import (
    RabbitSubscriberBaseConfigs,
)
from faststream.rabbit.subscriber.usecase import LogicSubscriber
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import (
    Message,
    Operation,
    SubscriberSpec,
)
from faststream.specification.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    amqp,
)


class SpecificationSubscriber(
    SpecificationSubscriberMixin,
    RMQSpecificationMixin,
    LogicSubscriber,
):
    """AsyncAPI-compatible Rabbit Subscriber class."""

    def __init__(
        self,
        *,
        base_configs: RabbitSubscriberBaseConfigs,
        rabbit_mq_base_configs: RabbitBaseConfigs,
        specification_configs: SpecificationConfigs,
    ) -> None:
        super().__init__(
            specification_configs=specification_configs,
            # propagate to RMQSpecificationMixin
            rabbit_mq_options=rabbit_mq_base_configs,
        )

        LogicSubscriber.__init__(self, base_configs=base_configs)

    def get_default_name(self) -> str:
        return f"{self.queue.name}:{getattr(self.exchange, 'name', None) or '_'}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        exchange_binding = amqp.Exchange.from_exchange(self.exchange)
        queue_binding = amqp.Queue.from_queue(self.queue)

        return {
            self.name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    bindings=OperationBinding(
                        amqp=amqp.OperationBinding(
                            routing_key=self.queue.routing,
                            queue=queue_binding,
                            exchange=exchange_binding,
                            ack=True,
                            reply_to=None,
                            persist=None,
                            mandatory=None,
                            priority=None,
                        ),
                    ),
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads),
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
