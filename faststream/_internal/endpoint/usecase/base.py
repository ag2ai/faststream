from collections.abc import Sequence
from typing import TYPE_CHECKING

from faststream._internal.endpoint.base import EndpointWrapper
from faststream._internal.types import BrokerMiddleware, MsgType

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto


class Endpoint(EndpointWrapper[MsgType]):
    @property
    def _producer(self) -> "ProducerProto":
        raise NotImplementedError

    @property
    def _broker_middlewares(self) -> Sequence["BrokerMiddleware[MsgType]"]:
        raise NotImplementedError
