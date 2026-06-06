from collections.abc import AsyncIterator, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any

import anyio
from botocore.exceptions import BotoCoreError, ClientError
from typing_extensions import override

from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.utils import process_msg
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
        config.parser = self._sqs_parser.parse_message
        config.decoder = self._sqs_parser.decode_message
        super().__init__(config, specification, calls)

        self._queue = config.queue
        self._declare = config.declare
        self._wait_time_seconds = config.wait_time_seconds
        self._max_messages = config.max_messages
        self._visibility_timeout = config.visibility_timeout
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

    async def _resolve_queue_url(self) -> str:
        if self._declare.declare:
            return await self._outer_config.declare_queue(self._declare, name=self.queue)
        return await self._outer_config.get_queue_url(self.queue)

    @override
    async def start(self) -> None:
        await super().start()

        self._queue_url = await self._resolve_queue_url()
        self._sqs_parser.bind(self._outer_config.client, self._queue_url)
        self._parser = self._sqs_parser.parse_message
        self._decoder = self._sqs_parser.decode_message

        if self.calls:
            self.add_task(self._consume_loop)

        self._post_start()

    def _receive_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "QueueUrl": self._queue_url,
            "MaxNumberOfMessages": self._max_messages,
            "WaitTimeSeconds": self._wait_time_seconds,
            "MessageAttributeNames": ["All"],
        }
        if self._visibility_timeout is not None:
            kwargs["VisibilityTimeout"] = self._visibility_timeout
        return kwargs

    async def _receive(self) -> list["SQSRawMessage"]:
        resp = await self._outer_config.client.receive_message(**self._receive_kwargs())
        return resp.get("Messages", [])

    async def _consume_loop(self) -> None:
        backoff = 1.0
        while self.running:
            try:
                messages = await self._receive()
            except (ClientError, BotoCoreError) as e:
                self._outer_config.logger.log(
                    f"SQS receive failed, retrying in {backoff:.0f}s: {e}",
                    extra=self.get_log_context(None),
                )
                await anyio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue

            backoff = 1.0
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

        if not self._queue_url:
            self._queue_url = await self._resolve_queue_url()
            self._sqs_parser.bind(self._outer_config.client, self._queue_url)

        raw_msg: SQSRawMessage | None = None
        with anyio.move_on_after(timeout):
            resp = await self._outer_config.client.receive_message(
                QueueUrl=self._queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=min(int(timeout), 20),
                MessageAttributeNames=["All"],
            )
            messages = resp.get("Messages", [])
            raw_msg = messages[0] if messages else None

        context = self._outer_config.fd_config.context
        return await process_msg(
            msg=raw_msg,
            middlewares=(m(raw_msg, context=context) for m in self._broker_middlewares),
            parser=self._sqs_parser.parse_message,
            decoder=self._sqs_parser.decode_message,
        )

    @override
    async def __aiter__(self) -> AsyncIterator["SQSMessage"]:  # type: ignore[override]
        if not self._queue_url:
            self._queue_url = await self._resolve_queue_url()
            self._sqs_parser.bind(self._outer_config.client, self._queue_url)

        context = self._outer_config.fd_config.context
        while True:
            with suppress(ClientError, BotoCoreError):
                for raw_msg in await self._receive():
                    msg: SQSMessage = await process_msg(  # type: ignore[assignment]
                        msg=raw_msg,
                        middlewares=(
                            m(raw_msg, context=context)
                            for m in self._broker_middlewares
                        ),
                        parser=self._sqs_parser.parse_message,
                        decoder=self._sqs_parser.decode_message,
                    )
                    yield msg
