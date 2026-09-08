from typing import TYPE_CHECKING

from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.rabbit.address import (
    as_declared,
    broker_exchange,
    broker_queue,
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
        return broker_routing_key(self._outer_config, self.config.routing_address)

    @property
    def declared_queue(self) -> "RabbitQueue":
        return as_declared(self._outer_config, self.config.queue)

    @property
    def declared_routing_key(self) -> str:
        return as_declared(self._outer_config, self.config.routing_address).template

    @property
    def routing(self) -> str | None:
        """The routing key as declared, for the channel name; `address` is the prefixed one."""
        # An explicit key names the channel whatever the exchange does with it.
        if routing_key := self.declared_routing_key:
            return routing_key

        if is_routing_exchange(self.exchange):
            return self.declared_queue.routing_template()

        return None

    @property
    def address(self) -> str | None:
        """The routing key a message published here travels by, with the Router prefix."""
        # The exchange decides first: a fanout ignores routing keys, so one declared
        # anyway addresses nothing. The subscriber gates on the same question.
        if not is_routing_exchange(self.exchange):
            return None

        # `None` rather than `""`: AsyncAPI reads an absent address as unknown and an
        # empty one as an address zero characters long.
        if not self.routing:
            return None

        return self.routing_key or self.queue.routing_template()

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        exchange_name = getattr(self.exchange, "name", None)

        return f"{self.routing or '_'}:{exchange_name or '_'}:Publisher"

    def get_schema(self) -> dict[str, "PublisherSpec"]:
        payloads = self.get_payloads()

        exchange_binding = amqp.Exchange.from_exchange(self.exchange)
        queue_binding = amqp.Queue.from_queue(self.declared_queue)

        # Not `self.address`: the binding hands the key over whatever the exchange
        # type, and the renderer is what drops it where the exchange ignores one.
        routing_key = self.routing_key or self.queue.routing_template()

        return {
            self.name: PublisherSpec(
                address=self.address,
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
