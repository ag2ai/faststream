import warnings
from typing import TYPE_CHECKING, Optional

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.rabbit.configs import (
    RabbitSubscriberConfigFacade,
)
from faststream.rabbit.subscriber.specified import SpecificationSubscriber

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict
    from faststream.middlewares import AckPolicy
    from faststream.rabbit.configs import RabbitBrokerConfig
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
    ack_policy: "AckPolicy",
    no_ack: bool,
    # Broker args
    config: "RabbitBrokerConfig",
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> SpecificationSubscriber:
    _validate_input_for_misconfigure(ack_policy=ack_policy, no_ack=no_ack)

    config = RabbitSubscriberConfigFacade(
        no_reply=no_reply,
        consume_args=consume_args,
        channel=channel,
        _ack_policy=ack_policy,
        _no_ack=no_ack,
        # rmq
        queue=queue,
        exchange=exchange,
        # specification
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
        # broker
        config=config,
    )

    return SpecificationSubscriber(config)


def _validate_input_for_misconfigure(
    *,
    ack_policy: "AckPolicy",
    no_ack: bool,
) -> None:
    if no_ack is not EMPTY:
        warnings.warn(
            "`no_ack` option was deprecated in prior to `ack_policy=AckPolicy.ACK_FIRST`. Scheduled to remove in 0.7.0",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `no_ack` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)
