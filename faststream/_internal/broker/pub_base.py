from abc import abstractmethod
from collections.abc import Sequence
from functools import partial
from typing import TYPE_CHECKING, Any, Generic

from faststream._internal.subscriber.utils import process_msg
from faststream._internal.types import MsgType
from faststream.message.source_type import SourceType

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.context import ContextRepo
    from faststream._internal.publisher.proto import ProducerProto
    from faststream._internal.types import BrokerMiddleware
    from faststream.response import PublishCommand


class BrokerPublishMixin(Generic[MsgType]):
    middlewares: Sequence["BrokerMiddleware[MsgType]"]

    @property
    @abstractmethod
    def context(self) -> "ContextRepo":
        raise NotImplementedError

    @abstractmethod
    async def publish(
        self,
        message: "SendableMessage",
        queue: str,
        /,
    ) -> Any:
        raise NotImplementedError

    async def _basic_publish(
        self,
        cmd: "PublishCommand",
        *,
        producer: "ProducerProto",
    ) -> Any:
        publish = producer.publish
        context = self.context  # caches property

        for m in self.middlewares[::-1]:
            publish = partial(m(None, context=context).publish_scope, publish)

        return await publish(cmd)

    async def publish_batch(
        self,
        *messages: "SendableMessage",
        queue: str,
    ) -> Any:
        raise NotImplementedError

    async def _basic_publish_batch(
        self,
        cmd: "PublishCommand",
        *,
        producer: "ProducerProto",
    ) -> Any:
        publish = producer.publish_batch
        context = self.context  # caches property

        for m in self.middlewares[::-1]:
            publish = partial(m(None, context=context).publish_scope, publish)

        return await publish(cmd)

    @abstractmethod
    async def request(
        self,
        message: "SendableMessage",
        queue: str,
        /,
        timeout: float = 0.5,
    ) -> Any:
        raise NotImplementedError

    async def _basic_request(
        self,
        cmd: "PublishCommand",
        *,
        producer: "ProducerProto",
    ) -> Any:
        request = producer.request
        context = self.context  # caches property

        for m in self.middlewares[::-1]:
            request = partial(m(None, context=context).publish_scope, request)

        published_msg = await request(cmd)

        response_msg: Any = await process_msg(
            msg=published_msg,
            middlewares=(m(published_msg, context=context) for m in self.middlewares),
            parser=producer._parser,
            decoder=producer._decoder,
            source_type=SourceType.RESPONSE,
        )
        return response_msg
