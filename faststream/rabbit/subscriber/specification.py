from typing import TYPE_CHECKING

from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.rabbit.address import broker_exchange, broker_queue
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

if TYPE_CHECKING:
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue


class RabbitSubscriberSpecification(
    SubscriberSpecification[RabbitBrokerConfig, RabbitSubscriberSpecificationConfig],
):
    @property
    def queue(self) -> "RabbitQueue":
        return broker_queue(self._outer_config, self.config.queue)

    @property
    def exchange(self) -> "RabbitExchange":
        return broker_exchange(self._outer_config, self.config.exchange)

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        queue_name = self.queue.name

        exchange_name = getattr(self.exchange, "name", None)

        return f"{queue_name}:{exchange_name or '_'}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        queue = self.queue

        exchange_binding = amqp.Exchange.from_exchange(self.exchange)
        queue_binding = amqp.Queue.from_queue(queue)

        channel_name = self.name

        return {
            channel_name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    bindings=OperationBinding(
                        amqp=amqp.OperationBinding(
                            routing_key=queue.routing_template(),
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
