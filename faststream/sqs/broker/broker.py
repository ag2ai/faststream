import asyncio
import logging
from collections.abc import Iterable, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from aiobotocore.session import get_session
from fast_depends import Provider, dependency_provider
from typing_extensions import override

from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.message import gen_cor_id
from faststream.middlewares import AckPolicy
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec
from faststream.sqs.broker.config import SQSBrokerConfig
from faststream.sqs.message import SQSRawMessage
from faststream.sqs.publisher.producer import SQSFastProducer
from faststream.sqs.response import SQSBatchPublishCommand, SQSPublishCommand
from faststream.sqs.schemas import SQSQueue
from faststream.sqs.security import parse_security
from faststream.sqs.subscriber.usecase import SQSSubscriber

from .logging import make_sqs_logger_state
from .registrator import SQSRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from types_aiobotocore_sqs import SQSClient

    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict
    from faststream.sqs.message import SQSMessage


class SQSBroker(
    SQSRegistrator,
    BrokerUsecase[SQSRawMessage, "SQSClient"],
):
    """AWS SQS broker for FastStream (backed by aiobotocore)."""

    def __init__(
        self,
        *,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        use_ssl: bool = True,
        verify: bool | str | None = None,
        botocore_config: Any | None = None,
        response_queue: Union[str, "SQSQueue", None] = None,
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[SQSRegistrator] = (),
        ack_policy: AckPolicy = EMPTY,
        # AsyncAPI args
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = None,
        description: str | None = None,
        tags: Iterable["Tag | TagDict"] = (),
        security: Optional["BaseSecurity"] = None,
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends args
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
    ) -> None:
        secure_kwargs = parse_security(security)

        connection_kwargs: dict[str, Any] = {
            "region_name": region_name,
            "endpoint_url": endpoint_url,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_session_token": aws_session_token,
            "use_ssl": use_ssl,
            "verify": verify,
            "config": botocore_config,
            **secure_kwargs,
        }
        # botocore rejects unknown Nones for some keys; drop empties
        connection_kwargs = {k: v for k, v in connection_kwargs.items() if v is not None}

        if specification_url is None:
            specification_url = endpoint_url or (
                f"https://sqs.{region_name}.amazonaws.com" if region_name else "sqs"
            )

        self._response_queue = response_queue
        self._client_cm: Any = None
        self._response_task: asyncio.Task[Any] | None = None

        super().__init__(
            **connection_kwargs,
            routers=routers,
            config=SQSBrokerConfig(
                producer=SQSFastProducer(parser=parser, decoder=decoder),
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                broker_codec=codec,
                logger=make_sqs_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                ack_policy=ack_policy,
                extra_context={"broker": self},
            ),
            specification=BrokerSpec(
                description=description,
                url=[specification_url],
                protocol=protocol or "sqs",
                protocol_version=protocol_version or "custom",
                tags=tags,
                security=security,
            ),
        )

    @override
    async def _connect(self) -> "SQSClient":
        session = get_session()
        self._client_cm = session.create_client("sqs", **self._connection_kwargs)
        client: SQSClient = await self._client_cm.__aenter__()
        self.config.connect(client)
        return client

    async def _setup_response_queue(self) -> None:
        if self._response_queue is None:
            return

        if isinstance(self._response_queue, SQSQueue):
            url = await self.config.declare_queue(self._response_queue)
        else:
            url = await self.config.get_queue_url(self._response_queue)

        cast("SQSFastProducer", self.config.producer).response_queue_url = url
        self._response_task = asyncio.create_task(self._consume_responses(url))

    async def _consume_responses(self, queue_url: str) -> None:
        client = self.config.client
        producer = cast("SQSFastProducer", self.config.producer)
        while self.running:
            with suppress(Exception):
                resp = await client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=5,
                    MessageAttributeNames=["All"],
                )
                for message in resp.get("Messages", []):
                    attrs = message.get("MessageAttributes", {}) or {}
                    cid = attrs.get("correlation_id", {}).get("StringValue", "")
                    if cid:
                        producer.resolve_response(cid, message)
                    await client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message["ReceiptHandle"],
                    )

    @override
    async def start(self) -> None:
        await self.connect()
        self.running = True
        c = SQSSubscriber.build_log_context(None, "")
        self.config.logger.log("Connection established", logging.INFO, c)
        await self._setup_response_queue()
        await super().start()

    @override
    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)

        if self._response_task is not None and not self._response_task.done():
            self._response_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._response_task
            self._response_task = None

        if self._client_cm is not None:
            with suppress(Exception):
                await self._client_cm.__aexit__(exc_type, exc_val, exc_tb)
            self._client_cm = None

        self._connection = None
        self.config.disconnect()

    @override
    async def ping(self, timeout: float | None = None) -> bool:
        if self._connection is None:
            return False
        try:
            await self._connection.list_queues(MaxResults=1)
        except Exception:
            return False
        else:
            return True

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        queue: Union[str, "SQSQueue"] = "",
        *,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        group_id: str | None = None,
        deduplication_id: str | None = None,
        delay_seconds: int = 0,
        reply_to: str = "",
    ) -> Any:
        """Publish a message to an SQS queue.

        Args:
            message: Message body to send.
            queue: Target queue name or an SQSQueue declaration.
            headers: Message headers (sent as SQS MessageAttributes).
            correlation_id: Correlation ID for message tracing.
            group_id: MessageGroupId for FIFO queues.
            deduplication_id: MessageDeduplicationId for FIFO queues.
            delay_seconds: Delay before the message becomes visible (0-900).
            reply_to: Response queue (name or URL) for reply-to flows.
        """
        queue_name = queue.queue_name if isinstance(queue, SQSQueue) else queue
        cmd = SQSPublishCommand(
            message,
            queue=queue_name,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            reply_to=reply_to,
            group_id=group_id,
            deduplication_id=deduplication_id,
            delay_seconds=delay_seconds,
            _publish_type=PublishType.PUBLISH,
        )
        return await self._basic_publish(cmd, producer=self.config.producer)

    @override
    async def request(
        self,
        message: "SendableMessage" = None,
        queue: Union[str, "SQSQueue"] = "",
        /,
        timeout: float = 30.0,
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
        group_id: str | None = None,
    ) -> "SQSMessage":
        queue_name = queue.queue_name if isinstance(queue, SQSQueue) else queue
        cmd = SQSPublishCommand(
            message,
            queue=queue_name,
            correlation_id=correlation_id or gen_cor_id(),
            headers=headers,
            group_id=group_id,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
        )
        msg: SQSMessage = await self._basic_request(cmd, producer=self.config.producer)
        return msg

    @override
    async def publish_batch(
        self,
        *messages: "SendableMessage",
        queue: Union[str, "SQSQueue"] = "",
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        group_id: str | None = None,
    ) -> Any:
        queue_name = queue.queue_name if isinstance(queue, SQSQueue) else queue
        first, *rest = messages or (None,)
        cmd = SQSBatchPublishCommand(
            first,
            *rest,
            queue=queue_name,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            group_id=group_id,
            _publish_type=PublishType.PUBLISH,
        )
        return await self._basic_publish_batch(cmd, producer=self.config.producer)
