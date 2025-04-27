import warnings
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Optional

from faststream._internal.constants import EMPTY
from faststream._internal.subscriber.configs import (
    SpecificationSubscriberOptions,
)
from faststream.exceptions import SetupError
from faststream.rabbit.schemas.base import RabbitBaseConfigs
from faststream.rabbit.subscriber.configs import (
    RabbitSubscriberBaseConfigs,
)
from faststream.rabbit.subscriber.specified import SpecificationSubscriber

if TYPE_CHECKING:
    from aio_pika import IncomingMessage
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import BrokerMiddleware
    from faststream.middlewares import AckPolicy
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

    base_configs = RabbitSubscriberBaseConfigs(
        ack_policy=ack_policy,
        no_reply=no_reply,
        broker_dependencies=broker_dependencies,
        broker_middlewares=broker_middlewares,
        default_decoder=EMPTY,
        default_parser=EMPTY,
        consume_args=consume_args,
        queue=queue,
        no_ack=no_ack,
        channel=channel,
        exchange=exchange
    )

    specification_configs = SpecificationSubscriberOptions(
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    rabbit_mq_base_configs = RabbitBaseConfigs(queue=queue, exchange=exchange)


    return SpecificationSubscriber(
        channel=channel,
        base_configs=base_configs,
        specification_configs=specification_configs,
        rabbit_mq_base_configs=rabbit_mq_base_configs,
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
