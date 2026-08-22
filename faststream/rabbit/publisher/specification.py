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
    def routing(self) -> str | None:
        """The routing key this publisher was declared with, as the name reads it.

        This is the channel name's question, not the address's: an explicit
        `routing_key` names the channel whatever the exchange does with it. See
        `address` for the string a message actually travels by.
        """
        if self.config.routing_key:
            return self.config.routing_key

        if is_routing_exchange(self.config.exchange):
            return self.config.queue.routing_template()

        return None

    @property
    def address(self) -> str:
        """The routing key a message published here travels by, prefixed.

        The exchange decides first, not the declaration: a fanout reaches every
        queue bound to it and ignores any routing key handed to it, so one declared
        anyway still addresses nothing. The subscriber gates on the same question,
        which is what keeps both ends of an address showing one string.
        """
        if not is_routing_exchange(self.config.exchange):
            return ""

        routing = self.routing
        return f"{self._outer_config.prefix}{routing}" if routing else ""

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        exchange_name = getattr(self.config.exchange, "name", None)

        return f"{self.routing or '_'}:{exchange_name or '_'}:Publisher"

    def get_schema(self) -> dict[str, "PublisherSpec"]:
        payloads = self.get_payloads()

        exchange_binding = amqp.Exchange.from_exchange(self.config.exchange)
        queue_binding = amqp.Queue.from_queue(self.config.queue)

        # deliberately not `self.address`: the binding hands the routing key over
        # whatever the exchange type, and the renderer is what drops it where the
        # exchange ignores one. `address` answers for the document instead, so it
        # has to make that call itself.
        r = self.config.routing_key or self.config.queue.routing_template()
        routing_key = f"{self._outer_config.prefix}{r}"

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
