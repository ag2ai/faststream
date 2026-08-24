from typing import TYPE_CHECKING

from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.rabbit.address import (
    broker_exchange,
    broker_queue,
    broker_reply_to,
    broker_routing_key,
)
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

if TYPE_CHECKING:
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue


class RabbitPublisherSpecification(
    PublisherSpecification[RabbitBrokerConfig, RabbitPublisherSpecificationConfig],
):
    @property
    def queue(self) -> "RabbitQueue":
        return broker_queue(self._outer_config, self.config.queue)

    @property
    def exchange(self) -> "RabbitExchange":
        return broker_exchange(self._outer_config, self.config.exchange)

    @property
    def routing_key(self) -> str:
        return broker_routing_key(self._outer_config, self.config.routing_key)

    @property
    def reply_to(self) -> str:
        return broker_reply_to(self._outer_config, self.config.reply_to)

    @property
    def name(self) -> str:
        """The channel title, built from the addresses messages really go to.

        Every part of it is a Broker address, prefix and all. A channel named
        after the undecorated declaration while its own `cc` binding carries the
        prefix would be a document contradicting itself — and the Subscriber
        side, Kafka and Redis all title their channels this way.
        """
        if self.config.title_:
            return self.config.title_

        exchange = self.exchange

        if routing_key := self.routing_key:
            routing: str | None = routing_key

        elif is_routing_exchange(exchange):
            routing = self.queue.routing_template()

        else:
            routing = None

        exchange_name = getattr(exchange, "name", None)

        return f"{routing or '_'}:{exchange_name or '_'}:Publisher"

    def get_schema(self) -> dict[str, "PublisherSpec"]:
        payloads = self.get_payloads()

        exchange_binding = amqp.Exchange.from_exchange(self.exchange)
        queue_binding = amqp.Queue.from_queue(self.queue)

        routing_key = self.routing_key or self.queue.routing_template()

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
                            reply_to=self.reply_to or None,
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
