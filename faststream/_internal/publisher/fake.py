from abc import abstractmethod
from collections.abc import Iterable
from functools import partial
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.basic_types import SendableMessage
from faststream._internal.publisher.proto import BasePublisherProto
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from faststream._internal.basic_types import AsyncFunc
    from faststream._internal.publisher.proto import ProducerProto
    from faststream._internal.types import PublisherMiddleware
    from faststream.response.response import PublishCommand


class FakePublisher(BasePublisherProto):
    """Publisher Interface implementation to use as RPC or REPLY TO answer publisher."""

    def __init__(
        self,
        *,
        producer: "ProducerProto",
    ) -> None:
        """Initialize an object."""
        self._producer = producer

    @abstractmethod
    def patch_command(self, cmd: "PublishCommand") -> "PublishCommand":
        cmd.publish_type = PublishType.REPLY
        return cmd

    async def _publish(
        self,
        cmd: "PublishCommand",
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> Any:
        """This method should be called in subscriber flow only."""
        cmd = self.patch_command(cmd)

        call: AsyncFunc = self._producer.publish
        for m in _extra_middlewares:
            call = partial(m, call)

        return await call(cmd)

    async def publish(
        self,
        message: SendableMessage,
        /,
        *,
        correlation_id: Optional[str] = None,
    ) -> Optional[Any]:
        msg = (
            f"`{self.__class__.__name__}` can be used only to publish "
            "a response for `reply-to` or `RPC` messages."
        )
        raise NotImplementedError(msg)

    async def request(
        self,
        message: "SendableMessage",
        /,
        *,
        correlation_id: Optional[str] = None,
    ) -> Any:
        msg = (
            f"`{self.__class__.__name__}` can be used only to publish "
            "a response for `reply-to` or `RPC` messages."
        )
        raise NotImplementedError(msg)
