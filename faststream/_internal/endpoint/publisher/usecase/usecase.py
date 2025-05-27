from collections.abc import Awaitable, Generator, Iterable
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

from faststream._internal.endpoint.call_wrapper import (
    HandlerCallWrapper,
)
from faststream._internal.endpoint.utils import process_msg
from faststream._internal.types import (
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.message.source_type import SourceType

from .proto import PublisherProto

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerConfig
    from faststream._internal.di import FastDependsConfig
    from faststream._internal.producer import ProducerProto
    from faststream._internal.types import (
        PublisherMiddleware,
    )
    from faststream.response.response import PublishCommand

    from .config import PublisherUsecaseConfig


class PublisherUsecase(PublisherProto[MsgType]):
    """A base class for publishers in an asynchronous API."""

    def __init__(self, config: "PublisherUsecaseConfig", /) -> None:
        broker_config = config.config
        self._outer_config = broker_config

        self.middlewares = config.middlewares

        self._fake_handler = False
        self.mock: Optional[MagicMock] = None

    def register(self, config: "BrokerConfig", /) -> None:
        self._outer_config = final_config = config | self._outer_config
        self.include_in_schema = final_config.include_in_schema

    @property
    def _producer(self) -> "ProducerProto":
        return self._outer_config.producer

    @override
    def _setup(self, config: Optional["FastDependsConfig"] = None, /) -> None:
        if config:
            self._outer_config.fd_config = config | self._outer_config.fd_config

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
        for pub_m in self._build_middlewares_stack(_extra_middlewares):
            pub = partial(pub_m, pub)

        return await pub(cmd)

    async def _basic_publish_batch(
        self,
        cmd: "PublishCommand",
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> Any:
        pub = self._producer.publish_batch
        for pub_m in self._build_middlewares_stack(_extra_middlewares):
            pub = partial(pub_m, pub)

        return await pub(cmd)

    async def _basic_request(
        self,
        cmd: "PublishCommand",
    ) -> Optional[Any]:
        request = self._producer.request
        for pub_m in self._build_middlewares_stack():
            request = partial(pub_m, request)

        published_msg = await request(cmd)

        context = self._outer_config.fd_config.context

        response_msg: Any = await process_msg(
            msg=published_msg,
            middlewares=(
                m(published_msg, context=context)
                for m in self._outer_config.broker_middlewares[::-1]
            ),
            parser=self._producer._parser,
            decoder=self._producer._decoder,
            source_type=SourceType.RESPONSE,
        )
        return response_msg

    def _build_middlewares_stack(
        self,
        extra_middlewares: Iterable["PublisherMiddleware"] = (),
    ) -> Generator["PublisherMiddleware", None, None]:
        context = self._outer_config.fd_config.context

        yield from chain(
            self.middlewares[::-1],
            (
                extra_middlewares
                or (
                    m(None, context=context).publish_scope
                    for m in self._outer_config.broker_middlewares[::-1]
                )
            ),
        )
