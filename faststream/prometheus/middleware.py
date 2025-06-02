import time
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence

from prometheus_client import multiprocess

from faststream import BaseMiddleware
from faststream.exceptions import IgnoredException
from faststream.prometheus.consts import (
    PROCESSING_STATUS_BY_ACK_STATUS,
    PROCESSING_STATUS_BY_HANDLER_EXCEPTION_MAP,
)
from faststream.prometheus.container import MetricsContainer
from faststream.prometheus.manager import MetricsManager
from faststream.prometheus.provider import MetricsSettingsProvider
from faststream.prometheus.types import ProcessingStatus, PublishingStatus
from faststream.types import EMPTY

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

    from faststream.broker.message import StreamMessage
    from faststream.types import AsyncFunc, AsyncFuncAny


class PrometheusMiddleware(BaseMiddleware):
    def __init__(
        self,
        msg: Optional[Any] = None,
        *,
        settings_provider_factory: Callable[
            [Any], Optional[MetricsSettingsProvider[Any]]
        ],
        metrics_manager: MetricsManager,
    ) -> None:
        self._metrics_manager = metrics_manager
        self._settings_provider = settings_provider_factory(msg)
        super().__init__(msg)

    async def consume_scope(
        self,
        call_next: "AsyncFuncAny",
        msg: "StreamMessage[Any]",
    ) -> Any:
        if self._settings_provider is None:
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
        call_next: "AsyncFunc",
        msg: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if self._settings_provider is None:
            return await call_next(msg, *args, **kwargs)

        destination_name = (
            self._settings_provider.get_publish_destination_name_from_kwargs(kwargs)
        )
        messaging_system = self._settings_provider.messaging_system

        err: Optional[Exception] = None
        start_time = time.perf_counter()

        try:
            result = await call_next(
                await self.on_publish(msg, *args, **kwargs),
                *args,
                **kwargs,
            )

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
            messages_count = len((msg, *args))

            self._metrics_manager.add_published_message(
                amount=messages_count,
                status=status,
                broker=messaging_system,
                destination=destination_name,
            )

        return result


class BasePrometheusMiddleware:
    __slots__ = ("_metrics_container", "_metrics_manager", "_settings_provider_factory")

    def __init__(
        self,
        *,
        settings_provider_factory: Callable[
            [Any], Optional[MetricsSettingsProvider[Any]]
        ],
        registry: "CollectorRegistry",
        app_name: str = EMPTY,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Optional[Sequence[float]] = None,
        is_multiprocess: bool = False,
        multiprocess_dir: Optional[str] = None,
    ):
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

        if is_multiprocess:
            if multiprocess_dir is None:
                raise ValueError("Multiprocess mode requires multiprocess_dir.")
            multiprocess.MultiProcessCollector(registry, path=multiprocess_dir)


    def __call__(self, msg: Optional[Any]) -> BaseMiddleware:
        return PrometheusMiddleware(
            msg=msg,
            metrics_manager=self._metrics_manager,
            settings_provider_factory=self._settings_provider_factory,
        )
