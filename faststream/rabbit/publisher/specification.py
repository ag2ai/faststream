from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.rabbit.configs import RabbitBrokerConfig
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

from .config import RabbitPublisherSpecificationConfig


class RabbitPublisherSpecification(
    PublisherSpecification[RabbitBrokerConfig, RabbitPublisherSpecificationConfig],
):
    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        if self.config.routing_key:
            routing: str | None = self.config.routing_key

        elif is_routing_exchange(self.config.exchange):
            routing = self.config.queue.routing()

        else:
            routing = None

        exchange_name = getattr(self.config.exchange, "name", None)

        return f"{routing or '_'}:{exchange_name or '_'}:Publisher"

    def get_schema(self) -> dict[str, "PublisherSpec"]:
        payloads = self.get_payloads()

        exchange_binding = amqp.Exchange.from_exchange(self.config.exchange)
        queue_binding = amqp.Queue.from_queue(self.config.queue)

        r = self.config.routing_key or self.config.queue.routing()
        routing_key = f"{self._outer_config.prefix}{r}"

        return {
            self.name: PublisherSpec(
                description=self.config.description_,
                operation=Operation(
                    bindings=OperationBinding(
                        amqp=amqp.OperationBinding(
                            routing_key=routing_key or None,
                            queue=queue_binding,
                            exchange=exchange_binding,
                            ack=True,
                            persist=self.config.message_kwargs.get("persist"),
                            priority=self.config.message_kwargs.get("priority"),
                            reply_to=self.config.message_kwargs.get("reply_to"),
                            mandatory=self.config.message_kwargs.get("mandatory"),
                        ),
                    ),
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(
                            payloads,
                            "Publisher",
                            served_words=2 if self.config.title_ is None else 1,
                        ),
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
