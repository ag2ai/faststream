from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any

import anyio
from typing_extensions import override

from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.sqs.helpers import poll_with_backoff
from faststream.sqs.parser import SQSParser
from faststream.sqs.publisher.fake import SQSFakePublisher

if TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage
    from faststream.sqs.configs import SQSBrokerConfig
    from faststream.sqs.message import SQSMessage, SQSRawMessage

    from .config import SQSSubscriberConfig


class SQSSubscriber(TasksMixin, SubscriberUsecase["SQSRawMessage"]):
    """Polling subscriber for an SQS queue (long-poll receive loop)."""

    _outer_config: "SQSBrokerConfig"

    def __init__(
        self,
        config: "SQSSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[SQSRawMessage]",
    ) -> None:
        self._sqs_parser = SQSParser()
        self._batch = config.batch
        config.parser, config.decoder = self._parser_pair()
        super().__init__(config, specification, calls)

        self._queue = config.queue
        self._declare = config.declare
        self._wait_time_seconds = config.wait_time_seconds
        self._max_messages = config.max_messages
        self._visibility_timeout = config.visibility_timeout
        self._request_attempt_id = config.request_attempt_id
        self._queue_url: str = ""

    @property
    def queue(self) -> str:
        return f"{self._outer_config.prefix}{self._queue}"

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence["PublisherProto"]:
        return (
            SQSFakePublisher(
                producer=self._outer_config.producer,
                queue=message.reply_to,
            ),
        )

    @staticmethod
    def build_log_context(
        message: "StreamMessage[SQSRawMessage] | None",
        queue: str = "",
    ) -> dict[str, str]:
        return {
            "queue": queue,
            "message_id": getattr(message, "message_id", ""),
        }

    def get_log_context(
        self,
        message: "StreamMessage[SQSRawMessage] | None",
    ) -> dict[str, str]:
        return self.build_log_context(message=message, queue=self.queue)

    def _parser_pair(self) -> tuple[Any, Any]:
        """The (parser, decoder) pair matching this subscriber's batch mode."""
        if self._batch:
            return self._sqs_parser.parse_batch, self._sqs_parser.decode_batch
        return self._sqs_parser.parse_message, self._sqs_parser.decode_message

    async def _resolve_queue_url(self) -> str:
        if self._declare.declare:
            return await self._outer_config.declare_queue(self._declare, name=self.queue)
        return await self._outer_config.get_queue_url(self.queue)

    async def _bind_queue(self) -> None:
        if not self._queue_url:
            self._queue_url = await self._resolve_queue_url()
            self._sqs_parser.bind(self._outer_config.client, self._queue_url)

    @override
    async def start(self) -> None:
        await super().start()

        await self._bind_queue()
        self._parser, self._decoder = self._parser_pair()

        if self.calls:
            self.add_task(self._consume_loop)

        self._post_start()

    def _receive_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "QueueUrl": self._queue_url,
            "MaxNumberOfMessages": self._max_messages,
            "WaitTimeSeconds": self._wait_time_seconds,
            "MessageAttributeNames": ["All"],
            # system attributes (ApproximateReceiveCount, MessageGroupId, ...)
            "AttributeNames": ["All"],
        }
        if self._visibility_timeout is not None:
            kwargs["VisibilityTimeout"] = self._visibility_timeout
        if self._request_attempt_id is not None:
            kwargs["ReceiveRequestAttemptId"] = self._request_attempt_id
        return kwargs

    async def _receive(self) -> list["SQSRawMessage"]:
        resp = await self._outer_config.client.receive_message(**self._receive_kwargs())
        return resp.get("Messages", [])

    def _log_receive_error(self, error: Exception, backoff: float) -> None:
        self._outer_config.logger.log(
            f"SQS receive failed, retrying in {backoff:.0f}s: {error}",
            extra=self.get_log_context(None),
        )

    async def _consume_loop(self) -> None:
        async for messages in poll_with_backoff(
            self._receive,
            is_running=lambda: self.running,
            on_error=self._log_receive_error,
        ):
            await self._dispatch(messages)

    async def _dispatch(self, messages: list["SQSRawMessage"]) -> None:
        """Hand received messages to the handler.

        In batch mode the whole poll result is one ``consume`` call (the handler
        receives a list); otherwise each message is consumed individually.
        """
        # TODO: visibility-timeout heartbeat for long-running handlers.
        # If a handler runs longer than the queue's VisibilityTimeout, the message
        # becomes visible again and is redelivered (duplicate processing). For now
        # the guidance is to raise `visibility_timeout` on the subscriber. A future
        # enhancement could spawn a background task per in-flight message that calls
        # `change_message_visibility` periodically (opt-in via e.g. `extend_visibility`).
        if self._batch:
            if messages:
                await self.consume(messages)
        else:
            for message in messages:
                await self.consume(message)

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "StreamMessage[SQSRawMessage] | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        await self._bind_queue()

        get_one_kwargs = self._receive_kwargs() | {
            "MaxNumberOfMessages": 1,
            # Strictly shorter than the `move_on_after` deadline below: if the
            # long poll lasted the full client timeout, a message returned at
            # its very end would be cancelled client-side after SQS already
            # marked it in-flight — silently hiding it for the queue's whole
            # visibility timeout.
            "WaitTimeSeconds": min(max(int(timeout) - 1, 0), 20),
        }

        raw_msg: SQSRawMessage | None = None
        with anyio.move_on_after(timeout):
            resp = await self._outer_config.client.receive_message(**get_one_kwargs)
            messages = resp.get("Messages", [])
            raw_msg = messages[0] if messages else None

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()
        # In batch mode the parser expects a list of raw messages.
        msg_input: Any = [raw_msg] if (self._batch and raw_msg is not None) else raw_msg
        return await process_msg(
            msg=msg_input,
            middlewares=(m(msg_input, context=context) for m in self._broker_middlewares),
            parser=async_parser,
            decoder=async_decoder,
        )

    @override
    async def __aiter__(self) -> AsyncIterator["SQSMessage"]:  # type: ignore[override]
        await self._bind_queue()

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()
        async for received in poll_with_backoff(
            self._receive,
            is_running=lambda: True,
            on_error=self._log_receive_error,
        ):
            # In batch mode each poll result is yielded as one batch message.
            inputs: list[Any] = [received] if self._batch else list(received)
            for raw_msg in inputs:
                if self._batch and not raw_msg:
                    continue
                msg: SQSMessage = await process_msg(  # type: ignore[assignment]
                    msg=raw_msg,
                    middlewares=(
                        m(raw_msg, context=context) for m in self._broker_middlewares
                    ),
                    parser=async_parser,
                    decoder=async_decoder,
                )
                yield msg
