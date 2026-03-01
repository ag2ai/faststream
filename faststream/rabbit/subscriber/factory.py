import warnings
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError

from .config import (
    RabbitSubscriberConfig,
    RabbitSubscriberSpecificationConfig,
)
from .specification import RabbitSubscriberSpecification
from .usecase import RabbitSubscriber

if TYPE_CHECKING:
    from faststream.middlewares import AckPolicy
    from faststream.rabbit.configs import RabbitBrokerConfig
    from faststream.rabbit.schemas import Channel, RabbitExchange, RabbitQueue


def create_subscriber(
    *,
    queue: "RabbitQueue",
    exchange: "RabbitExchange",
    consume_args: dict[str, Any] | None,
    channel: Optional["Channel"],
    # Subscriber args
    no_reply: bool,
    ack_policy: "AckPolicy",
    no_ack: bool,
    # Broker args
    config: "RabbitBrokerConfig",
    # Specification args
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> RabbitSubscriber:
    _validate_input_for_misconfigure(ack_policy=ack_policy, no_ack=no_ack)

    subscriber_config = RabbitSubscriberConfig(
        no_reply=no_reply,
        consume_args=consume_args,
        channel=channel,
        queue=queue,
        exchange=exchange,
        _ack_policy=ack_policy,
        _no_ack=no_ack,
        # broker
        _outer_config=config,
    )

    calls = CallsCollection[Any]()

    specification = RabbitSubscriberSpecification(
        _outer_config=config,
        specification_config=RabbitSubscriberSpecificationConfig(
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
            queue=queue,
            exchange=exchange,
        ),
        calls=calls,
    )

    return RabbitSubscriber(
        config=subscriber_config,
        specification=specification,
        calls=calls,
    )


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
