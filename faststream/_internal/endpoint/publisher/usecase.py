from collections.abc import Callable, Generator, Iterable
from functools import partial
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
)
from unittest.mock import MagicMock

from faststream._internal.endpoint.call_wrapper import (
    HandlerCallWrapper,
)
from faststream._internal.endpoint.usecase import Endpoint
from faststream._internal.endpoint.utils import process_msg
from faststream._internal.types import (
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.message.source_type import SourceType

from .proto import PublisherProto

if TYPE_CHECKING:
    from faststream._internal.configs import PublisherUsecaseConfig
    from faststream._internal.producer import ProducerProto
    from faststream._internal.types import (
        PublisherMiddleware,
    )
    from faststream.response.response import PublishCommand
    from faststream.specification.schema import PublisherSpec

    from .specification import PublisherSpecification


class PublisherUsecase(Endpoint, PublisherProto):
    """A base class for publishers in an asynchronous API."""

    def __init__(
        self,
        config: "PublisherUsecaseConfig",
        specification: "PublisherSpecification",
    ) -> None:
        super().__init__(config._outer_config)

        self.specification = specification
        self.middlewares = config.middlewares

        self._fake_handler = False
        self.mock = MagicMock()

    async def start(self) -> None:
        pass

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
        self.mock.reset_mock()

    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn],
    ) -> HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]:
        """Decorate user's function by current publisher."""
        handler = super().__call__(func)
        handler._publishers.append(self)
        self.specification.add_call(handler._original_call)
        return handler

    async def _basic_publish(
        self,
        cmd: "PublishCommand",
        *,
        producer: "ProducerProto[Any]",
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> Any:
        pub = producer.publish
        for pub_m in self._build_middlewares_stack(_extra_middlewares):
            pub = partial(pub_m, pub)
        return await pub(cmd)

    async def _basic_publish_batch(
        self,
        cmd: "PublishCommand",
        *,
        producer: "ProducerProto[Any]",
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> Any:
        pub = producer.publish_batch
        for pub_m in self._build_middlewares_stack(_extra_middlewares):
            pub = partial(pub_m, pub)
        return await pub(cmd)

    async def _basic_request(
        self,
        cmd: "PublishCommand",
        *,
        producer: "ProducerProto[Any]",
    ) -> Any:
        request = producer.request
        for pub_m in self._build_middlewares_stack():
            request = partial(pub_m, request)

        published_msg = await request(cmd)

        context = self._outer_config.fd_config.context

        response_msg: Any = await process_msg(
            msg=published_msg,
            middlewares=(
                m(published_msg, context=context)
                for m in reversed(self._outer_config.broker_middlewares)
            ),
            parser=producer._parser,
            decoder=producer._decoder,
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
                    for m in reversed(self._outer_config.broker_middlewares)
                )
            ),
        )

    def schema(self) -> dict[str, "PublisherSpec"]:
        return self.specification.get_schema()
