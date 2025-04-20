from collections.abc import Iterable
from typing import TYPE_CHECKING, Optional

from faststream._internal.subscriber.specified import (
    SpecificationSubscriber as SpecificationSubscriberMixin,
)
from faststream.rabbit.schemas.proto import BaseRMQInformation as RMQSpecificationMixin
from faststream.rabbit.subscriber.usecase import LogicSubscriber
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    amqp,
)

if TYPE_CHECKING:
    from aio_pika import IncomingMessage
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import BrokerMiddleware
    from faststream.middlewares import AckPolicy
    from faststream.rabbit.schemas import Channel, RabbitExchange, RabbitQueue


class SpecificationSubscriber(
    SpecificationSubscriberMixin,
    RMQSpecificationMixin,
    LogicSubscriber,
):
    """AsyncAPI-compatible Rabbit Subscriber class."""

    def __init__(
        self,
        *,
        queue: "RabbitQueue",
        exchange: "RabbitExchange",
        channel: Optional["Channel"],
        consume_args: Optional["AnyDict"],
        # Subscriber args
        ack_policy: "AckPolicy",
        no_ack: bool,
        no_reply: bool,
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Iterable["BrokerMiddleware[IncomingMessage]"],
        # AsyncAPI args
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
            # propagate to RMQSpecificationMixin
            queue=queue,
            exchange=exchange,
        )

        LogicSubscriber.__init__(
            self,
            queue=queue,
            exchange=exchange,
            channel=channel,
            consume_args=consume_args,
            ack_policy=ack_policy,
            no_ack=no_ack,
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
        )

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
