from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Union

from faststream._internal.endpoint.specification.base import (
    SpecificationEndpoint,
    T,
)
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from faststream.redis.schemas import ListSub, PubSub, StreamSub
    from faststream.specification.schema.bindings import redis


class RedisSpecificationProtocol(SpecificationEndpoint[Any, T]):
    @property
    @abstractmethod
    def channel_binding(self) -> "redis.ChannelBinding": ...

    @abstractmethod
    def get_payloads(self) -> Any: ...


def validate_options(
    *,
    channel: Union["PubSub", str, None],
    list: Union["ListSub", str, None],
    stream: Union["StreamSub", str, None],
) -> None:
    if all((channel, list)):
        msg = "You can't use `PubSub` and `ListSub` both"
        raise SetupError(msg)
    if all((channel, stream)):
        msg = "You can't use `PubSub` and `StreamSub` both"
        raise SetupError(msg)
    if all((list, stream)):
        msg = "You can't use `ListSub` and `StreamSub` both"
        raise SetupError(msg)
