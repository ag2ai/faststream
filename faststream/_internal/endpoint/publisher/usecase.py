import logging
from collections.abc import Callable, Generator, Iterable
from functools import partial
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
from faststream._internal.endpoint.usecase import Endpoint
from faststream._internal.endpoint.utils import process_msg
from faststream._internal.types import P_HandlerParams, T_HandlerReturn
from faststream.message.source_type import SourceType

from .proto import PublisherProto

if TYPE_CHECKING:
    from faststream._internal.configs import PublisherUsecaseConfig
    from faststream._internal.producer import ProducerProto
    from faststream._internal.types import PublisherMiddleware
    from faststream.response.response import BatchPublishCommand, PublishCommand
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

        self.skip_none = config.skip_none

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

    def _should_skip_publish(self, cmd: "PublishCommand") -> bool:
        """Check if the message with None body should be skipped."""
        has_nonetype = cmd.body is None or None in cmd.batch_bodies

        return has_nonetype and self.skip_none

    async def _basic_publish(
        self,
        cmd: "PublishCommand",
        *,
        producer: "ProducerProto[Any]",
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> Any:
        """Publish a single message through the broker producer.

        Args:
            cmd (PublishCommand): Message body, headers and metadata to publish.
            producer (ProducerProto[Any]): Broker producer used to actually publish.
            _extra_middlewares (Iterable[PublisherMiddleware]): Publisher-level
                middlewares wrapping the publish call.

        Returns:
            Any: Producer publish result.
        """
        # skip_none guard runs before the middlewares stack is built,
        # so a skipped (None) message never reaches middlewares or producer.
        if self._should_skip_publish(cmd):
            msg = "Publish skipped. NoneType body."
            self._outer_config.logger.log(msg, logging.DEBUG)
            return None

        publish_callable = producer.publish

        for middleware in self._build_middlewares_stack(_extra_middlewares):
            publish_callable = partial(middleware, publish_callable)

        return await publish_callable(cmd)

    async def _basic_publish_batch(
        self,
        cmd: "BatchPublishCommand",
        *,
        producer: "ProducerProto[Any]",
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> Any:
        """Publish a batch of messages through the broker producer.

        Args:
            cmd (BatchPublishCommand): Batch bodies, headers and metadata to publish.
            producer (ProducerProto[Any]): Broker producer used to actually publish.
            _extra_middlewares (Iterable[PublisherMiddleware]): Publisher-level
                middlewares wrapping the publish call.

        Returns:
            Any: Producer publish_batch result.
        """
        # A partially-None batch is still published: only None values are
        # excluded (`cmd.batch_bodies` guard skips useless filtering).
        if cmd.batch_bodies and self._should_skip_publish(cmd):
            cmd.batch_bodies = tuple(filter(lambda x: x is not None, cmd.batch_bodies))

        # Re-check after exclusion: an empty batch resets `cmd.body` to None
        # via the setter, so an all-None batch is skipped right here.
        if self._should_skip_publish(cmd):
            msg = "Publish skipped. Empty batch (NoneType body)."
            self._outer_config.logger.log(msg, logging.DEBUG)
            return None

        publish_callable = producer.publish_batch

        for middleware in self._build_middlewares_stack(_extra_middlewares):
            publish_callable = partial(middleware, publish_callable)

        return await publish_callable(cmd)

    async def _basic_request(
        self,
        cmd: "PublishCommand",
        *,
        producer: "ProducerProto[Any]",
    ) -> Any:
        """Send a request message and process the received response.

        The raw response is processed by the broker middlewares, producer
        parser and decoder before being returned.

        Args:
            cmd (PublishCommand): Message body, headers and metadata of the request.
            producer (ProducerProto[Any]): Broker producer used to actually request.

        Returns:
            Any: Processed response message.
        """
        # skip_none guard runs before the middlewares stack is built,
        # so a skipped (None) request never reaches middlewares or producer.
        if self._should_skip_publish(cmd):
            msg = "Request skipped. NoneType body."
            self._outer_config.logger.log(msg, logging.DEBUG)
            return None

        request_callable = producer.request

        for middleware in self._build_middlewares_stack():
            request_callable = partial(middleware, request_callable)

        response = await request_callable(cmd)
        context = self._outer_config.fd_config.context

        return await process_msg(
            msg=response,
            middlewares=(
                m(response, context=context)
                for m in reversed(self._outer_config.broker_middlewares)
            ),
            parser=producer._parser,
            decoder=producer._decoder,
            source_type=SourceType.RESPONSE,
        )

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
