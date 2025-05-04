import warnings
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Optional

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from faststream.rabbit.subscriber.specified import SpecificationSubscriber

if TYPE_CHECKING:
    from aio_pika import IncomingMessage
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import BrokerMiddleware
    from faststream.rabbit.schemas import (
        Channel,
        RabbitExchange,
        RabbitQueue,
    )


def create_subscriber(
    *,
    queue: "RabbitQueue",
    exchange: "RabbitExchange",
    consume_args: Optional["AnyDict"],
    channel: Optional["Channel"],
    # Subscriber args
    no_reply: bool,
    broker_dependencies: Iterable["Dependant"],
    broker_middlewares: Sequence["BrokerMiddleware[IncomingMessage]"],
    ack_policy: "AckPolicy",
    no_ack: bool,
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> SpecificationSubscriber:
    _validate_input_for_misconfigure(ack_policy=ack_policy, no_ack=no_ack)

    if ack_policy is EMPTY:
        ack_policy = AckPolicy.DO_NOTHING if no_ack else AckPolicy.REJECT_ON_ERROR

    consumer_no_ack = ack_policy is AckPolicy.ACK_FIRST
    if consumer_no_ack:
        ack_policy = AckPolicy.DO_NOTHING

    return SpecificationSubscriber(
        queue=queue,
        exchange=exchange,
        consume_args=consume_args,
        ack_policy=ack_policy,
        no_ack=consumer_no_ack,
        channel=channel,
        no_reply=no_reply,
        broker_dependencies=broker_dependencies,
        broker_middlewares=broker_middlewares,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )


def _validate_input_for_misconfigure(
    *,
    ack_policy: "AckPolicy",
    no_ack: bool,
) -> None:
    if no_ack is not EMPTY:
        warnings.warn(
            "`no_ack` option was deprecated in prior to `ack_policy=AckPolicy.DO_NOTHING`. Scheduled to remove in 0.7.0",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `no_ack` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)
