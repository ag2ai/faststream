from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.rabbit.configs import RabbitBrokerConfig
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

from .config import RabbitSubscriberSpecificationConfig


class RabbitSubscriberSpecification(
    SubscriberSpecification[RabbitBrokerConfig, RabbitSubscriberSpecificationConfig],
):
    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        queue_name = self.config.queue.name

        exchange_name = getattr(self.config.exchange, "name", None)

        return f"{self._outer_config.prefix}{queue_name}:{exchange_name or '_'}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        queue = self.config.queue.add_prefix(self._outer_config.prefix)

        exchange_binding = amqp.Exchange.from_exchange(self.config.exchange)
        queue_binding = amqp.Queue.from_queue(queue)

        channel_name = self.name

        return {
            channel_name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    bindings=OperationBinding(
                        amqp=amqp.OperationBinding(
                            routing_key=queue.routing(),
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
                        title=f"{channel_name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                ),
                bindings=ChannelBinding(
                    amqp=amqp.ChannelBinding(
                        virtual_host=self._outer_config.virtual_host,
                        queue=queue_binding,
                        exchange=exchange_binding,
                    ),
                ),
            ),
        }
