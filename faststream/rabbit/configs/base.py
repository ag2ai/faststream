from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias, Union

from faststream._internal.config_value import Configurable

if TYPE_CHECKING:
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue


ConfigurableQueue: TypeAlias = Configurable[Union["RabbitQueue", str]]
"""A queue as an endpoint may declare it: a name, a prepared object, or a placeholder."""

ConfigurableExchange: TypeAlias = Configurable[Union["RabbitExchange", str, None]]
"""An exchange as an endpoint may declare it. `None` means the default exchange."""


@dataclass(kw_only=True)
class RabbitConfig:
    """How an endpoint's RabbitMQ addresses were declared, before they are read.

    A queue or an exchange arrives here as the user wrote it — a name, a prepared
    object or a Config placeholder — and is resolved and turned into a value
    object by the read layer in `faststream.rabbit.address`, not here.
    """

    queue: ConfigurableQueue
    exchange: ConfigurableExchange
