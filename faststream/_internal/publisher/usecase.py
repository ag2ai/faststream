from collections.abc import Awaitable, Iterable, Sequence
from functools import partial
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Union,
)
from unittest.mock import MagicMock

from typing_extensions import override

from faststream._internal.publisher.proto import PublisherProto
from faststream._internal.state import BrokerState, EmptyBrokerState, Pointer
from faststream._internal.state.producer import ProducerUnset
from faststream._internal.subscriber.call_wrapper import (
    HandlerCallWrapper,
)
from faststream._internal.subscriber.utils import process_msg
from faststream._internal.types import (
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.message.source_type import SourceType

if TYPE_CHECKING:
    from faststream._internal.publisher.proto import ProducerProto
    from faststream._internal.types import (
        BrokerMiddleware,
        PublisherMiddleware,
    )
    from faststream.response.response import PublishCommand


class PublisherUsecase(PublisherProto[MsgType]):
    """A base class for publishers in an asynchronous API."""

    def __init__(
        self,
        *,
        broker_middlewares: Sequence["BrokerMiddleware[MsgType]"],
        middlewares: Sequence["PublisherMiddleware"],
    ) -> None:
        self.middlewares = middlewares
        self._broker_middlewares = broker_middlewares

        self.__producer: Optional[ProducerProto] = ProducerUnset()

        self._fake_handler = False
        self.mock: Optional[MagicMock] = None

        self._state: Pointer[BrokerState] = Pointer(
            EmptyBrokerState("You should include publisher to any broker.")
        )

    def add_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None:
        self._broker_middlewares = (*self._broker_middlewares, middleware)

    @property
    def _producer(self) -> "ProducerProto":
        return self.__producer or self._state.get().producer

    @override
    def _setup(
        self,
        *,
        state: "Pointer[BrokerState]",
        producer: Optional["ProducerProto"] = None,
    ) -> None:
        self._state = state
        self.__producer = producer

    def set_test(
        self,
        *,
        mock: MagicMock,
        with_fake: bool,
    ) -> None:
        """Turn publisher to testing mode."""
        self.mock = mock
        self._fake_handler = with_fake

    def reset_test(self) -> None:
        """Turn off publisher's testing mode."""
        self._fake_handler = False
        self.mock = None

    def __call__(
        self,
        func: Union[
            Callable[P_HandlerParams, T_HandlerReturn],
            HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn],
        ],
    ) -> HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]:
        """Decorate user's function by current publisher."""
        handler = super().__call__(func)
        handler._publishers.append(self)
        return handler

    async def _basic_publish(
        self,
        cmd: "PublishCommand",
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> Any:
        pub: Callable[..., Awaitable[Any]] = self._producer.publish

        context = self._state.get().di_state.context

        for pub_m in chain(
            self.middlewares[::-1],
            (
                _extra_middlewares
                or (
                    m(None, context=context).publish_scope
                    for m in self._broker_middlewares[::-1]
                )
            ),
        ):
            pub = partial(pub_m, pub)

        return await pub(cmd)

    async def _basic_request(
        self,
        cmd: "PublishCommand",
    ) -> Optional[Any]:
        request = self._producer.request

        context = self._state.get().di_state.context

        for pub_m in chain(
            self.middlewares[::-1],
            (
                m(None, context=context).publish_scope
                for m in self._broker_middlewares[::-1]
            ),
        ):
            request = partial(pub_m, request)

        published_msg = await request(cmd)

        response_msg: Any = await process_msg(
            msg=published_msg,
            middlewares=(
                m(published_msg, context=context)
                for m in self._broker_middlewares[::-1]
            ),
            parser=self._producer._parser,
            decoder=self._producer._decoder,
            source_type=SourceType.RESPONSE,
        )
        return response_msg

    async def _basic_publish_batch(
        self,
        cmd: "PublishCommand",
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> Any:
        pub = self._producer.publish_batch

        context = self._state.get().di_state.context

        for pub_m in chain(
            self.middlewares[::-1],
            (
                _extra_middlewares
                or (
                    m(None, context=context).publish_scope
                    for m in self._broker_middlewares[::-1]
                )
            ),
        ):
            pub = partial(pub_m, pub)

        return await pub(cmd)
