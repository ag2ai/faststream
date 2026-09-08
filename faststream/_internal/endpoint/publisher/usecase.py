from collections.abc import Callable, Generator, Iterable
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
)

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.call_wrapper import (
    HandlerCallWrapper,
)
from faststream._internal.endpoint.usecase import Endpoint
from faststream._internal.endpoint.utils import process_msg
from faststream._internal.testing.calls import CallRecorder
from faststream._internal.types import (
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.exceptions import SetupError
from faststream.message.source_type import SourceType

from .proto import PublisherProto

if TYPE_CHECKING:
    from collections.abc import Mapping
    from unittest.mock import MagicMock

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

        self._fake_handler = False
        self._recorder = CallRecorder(specification.name, self._outer_config)
        self.is_test = False

    async def start(self) -> None:
        pass

    @property
    def mock(self) -> "MagicMock":
        """The mock recording the publisher's calls, available under a test broker."""
        if not self.is_test:
            msg = (
                f"`{self._recorder.name}` is not under a test broker: "
                "wrap the broker with its `Test*Broker` to access the mock."
            )
            raise SetupError(msg)
        return self._recorder.mock

    async def assert_called_once_with(
        self,
        body: Any = EMPTY,
        /,
        *,
        headers: Any = EMPTY,
        correlation_id: Any = EMPTY,
        reply_to: Any = EMPTY,
        content_type: Any = EMPTY,
        path: Any = EMPTY,
        context: "Mapping[str, Any]" = EMPTY,
    ) -> None:
        """Assert the publisher was called once, with the message described here.

        Headers match as a subset; every other field matches exactly.
        """
        self.mock.assert_called_once()
        await self._recorder.assert_called_once_with(
            body,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            content_type=content_type,
            path=path,
            context=context,
        )

    def set_test(
        self,
        *,
        recorder: CallRecorder | None = None,
        with_fake: bool,
    ) -> None:
        """Turn publisher to testing mode, sharing `recorder` when one is given."""
        self.is_test = True
        if recorder is None:
            self._recorder.reset()
        else:
            self._recorder = recorder
        self._fake_handler = with_fake

    def reset_test(self) -> None:
        """Turn off publisher's testing mode."""
        self.is_test = False
        self._recorder.reset()
        self._fake_handler = False

    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn],
    ) -> HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]:
        """Decorate user's function by current publisher."""
        handler = super().__call__(func)
        handler._publishers.append(self)
        self.specification.add_call(handler._declared_call)
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

        yield from (
            extra_middlewares
            or (
                m(None, context=context).publish_scope
                for m in reversed(self._outer_config.broker_middlewares)
            )
        )

    def schema(self) -> dict[str, "PublisherSpec"]:
        return self.specification.get_schema()
