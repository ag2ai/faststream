import time
from collections.abc import Awaitable, Sequence
from typing import TYPE_CHECKING, Any, Callable, Generic, Optional

from faststream._internal.constants import EMPTY
from faststream._internal.middlewares import BaseMiddleware
from faststream._internal.types import AnyMsg, PublishCommandType
from faststream.exceptions import IgnoredException
from faststream.message import SourceType
from faststream.prometheus.consts import (
    PROCESSING_STATUS_BY_ACK_STATUS,
    PROCESSING_STATUS_BY_HANDLER_EXCEPTION_MAP,
)
from faststream.prometheus.container import MetricsContainer
from faststream.prometheus.manager import MetricsManager
from faststream.prometheus.provider import MetricsSettingsProvider
from faststream.prometheus.types import ProcessingStatus, PublishingStatus
from faststream.response import PublishType

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

    from faststream._internal.basic_types import AsyncFuncAny
    from faststream._internal.context.repository import ContextRepo
    from faststream.message.message import StreamMessage


class PrometheusMiddleware(Generic[PublishCommandType, AnyMsg]):
    __slots__ = ("_metrics_container", "_metrics_manager", "_settings_provider_factory")

    def __init__(
        self,
        *,
        settings_provider_factory: Callable[
            [Optional[AnyMsg]],
            Optional[MetricsSettingsProvider[AnyMsg, PublishCommandType]],
        ],
        registry: "CollectorRegistry",
        app_name: str = EMPTY,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Optional[Sequence[float]] = None,
    ) -> None:
        if app_name is EMPTY:
            app_name = metrics_prefix

        self._settings_provider_factory = settings_provider_factory
        self._metrics_container = MetricsContainer(
            registry,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
        )
        self._metrics_manager = MetricsManager(
            self._metrics_container,
            app_name=app_name,
        )

    def __call__(
        self,
        msg: Optional[AnyMsg],
        /,
        *,
        context: "ContextRepo",
    ) -> "BasePrometheusMiddleware[PublishCommandType]":
        return BasePrometheusMiddleware[PublishCommandType](
            msg,
            metrics_manager=self._metrics_manager,
            settings_provider_factory=self._settings_provider_factory,
            context=context,
        )


class BasePrometheusMiddleware(
    BaseMiddleware[PublishCommandType, AnyMsg],
    Generic[PublishCommandType, AnyMsg],
):
    def __init__(
        self,
        msg: Optional[AnyMsg],
        /,
        *,
        settings_provider_factory: Callable[
            [Optional[AnyMsg]],
            Optional[MetricsSettingsProvider[AnyMsg, PublishCommandType]],
        ],
        metrics_manager: MetricsManager,
        context: "ContextRepo",
    ) -> None:
        self._metrics_manager = metrics_manager
        self._settings_provider = settings_provider_factory(msg)
        super().__init__(msg, context=context)

    async def consume_scope(
        self,
        call_next: "AsyncFuncAny",
        msg: "StreamMessage[AnyMsg]",
    ) -> Any:
        if self._settings_provider is None or msg._source_type is SourceType.RESPONSE:
            return await call_next(msg)

        messaging_system = self._settings_provider.messaging_system
        consume_attrs = self._settings_provider.get_consume_attrs_from_message(msg)
        destination_name = consume_attrs["destination_name"]

        self._metrics_manager.add_received_message(
            amount=consume_attrs["messages_count"],
            broker=messaging_system,
            handler=destination_name,
        )

        self._metrics_manager.observe_received_messages_size(
            size=consume_attrs["message_size"],
            broker=messaging_system,
            handler=destination_name,
        )

        self._metrics_manager.add_received_message_in_process(
            amount=consume_attrs["messages_count"],
            broker=messaging_system,
            handler=destination_name,
        )

        err: Optional[Exception] = None
        start_time = time.perf_counter()

        try:
            result = await call_next(await self.on_consume(msg))

        except Exception as e:
            err = e

            if not isinstance(err, IgnoredException):
                self._metrics_manager.add_received_processed_message_exception(
                    exception_type=type(err).__name__,
                    broker=messaging_system,
                    handler=destination_name,
                )
            raise

        finally:
            duration = time.perf_counter() - start_time
            self._metrics_manager.observe_received_processed_message_duration(
                duration=duration,
                broker=messaging_system,
                handler=destination_name,
            )

            self._metrics_manager.remove_received_message_in_process(
                amount=consume_attrs["messages_count"],
                broker=messaging_system,
                handler=destination_name,
            )

            status = ProcessingStatus.acked

            if msg.committed or err:
                status = (
                    PROCESSING_STATUS_BY_ACK_STATUS.get(msg.committed)  # type: ignore[arg-type]
                    or PROCESSING_STATUS_BY_HANDLER_EXCEPTION_MAP.get(type(err))
                    or ProcessingStatus.error
                )

            self._metrics_manager.add_received_processed_message(
                amount=consume_attrs["messages_count"],
                status=status,
                broker=messaging_system,
                handler=destination_name,
            )

        return result

    async def publish_scope(
        self,
        call_next: Callable[[PublishCommandType], Awaitable[Any]],
        cmd: PublishCommandType,
    ) -> Any:
        if self._settings_provider is None or cmd.publish_type is PublishType.REPLY:
            return await call_next(cmd)

        destination_name = (
            self._settings_provider.get_publish_destination_name_from_cmd(cmd)
        )
        messaging_system = self._settings_provider.messaging_system

        err: Optional[Exception] = None
        start_time = time.perf_counter()

        try:
            result = await call_next(cmd)

        except Exception as e:
            err = e
            self._metrics_manager.add_published_message_exception(
                exception_type=type(err).__name__,
                broker=messaging_system,
                destination=destination_name,
            )
            raise

        finally:
            duration = time.perf_counter() - start_time

            self._metrics_manager.observe_published_message_duration(
                duration=duration,
                broker=messaging_system,
                destination=destination_name,
            )

            status = PublishingStatus.error if err else PublishingStatus.success

            self._metrics_manager.add_published_message(
                amount=len(cmd.batch_bodies),
                status=status,
                broker=messaging_system,
                destination=destination_name,
            )

        return result
