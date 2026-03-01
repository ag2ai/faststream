import asyncio
from typing import TYPE_CHECKING, Any, cast

import pytest
from dirty_equals import IsList, IsPositiveFloat, IsStr
from prometheus_client import CollectorRegistry

from faststream import Context
from faststream.exceptions import IgnoredException, RejectMessage
from faststream.message import AckStatus
from faststream.prometheus import MetricsSettingsProvider
from faststream.prometheus.middleware import (
    PROCESSING_STATUS_BY_ACK_STATUS,
    PROCESSING_STATUS_BY_HANDLER_EXCEPTION_MAP,
    BasePrometheusMiddleware,
    PrometheusMiddleware,
)
from faststream.prometheus.types import ProcessingStatus, PublishingStatus
from tests.brokers.base.basic import BaseTestcaseConfig
from tests.prometheus.utils import (
    get_published_messages_duration_seconds_metric,
    get_published_messages_exceptions_metric,
    get_published_messages_metric,
    get_received_messages_in_process_metric,
    get_received_messages_metric,
    get_received_messages_size_bytes_metric,
    get_received_processed_messages_duration_seconds_metric,
    get_received_processed_messages_exceptions_metric,
    get_received_processed_messages_metric,
)

if TYPE_CHECKING:
    from collections.abc import Callable


@pytest.mark.asyncio()
class LocalPrometheusTestcase(BaseTestcaseConfig):
    def get_middleware(self, **kwargs: Any) -> PrometheusMiddleware:
        raise NotImplementedError

    def get_settings_provider(self) -> MetricsSettingsProvider[Any]:
        raise NotImplementedError

    @pytest.mark.parametrize(
        (
            "status",
            "exception_class",
        ),
        (
            pytest.param(
                AckStatus.ACKED,
                RejectMessage,
                id="acked status with reject message exception",
            ),
            pytest.param(
                AckStatus.ACKED,
                Exception,
                id="acked status with not handler exception",
            ),
            pytest.param(
                AckStatus.ACKED,
                None,
                id="acked status without exception",
            ),
            pytest.param(
                AckStatus.NACKED,
                None,
                id="nacked status without exception",
            ),
            pytest.param(
                AckStatus.REJECTED,
                None,
                id="rejected status without exception",
            ),
            pytest.param(
                AckStatus.ACKED,
                IgnoredException,
                id="acked status with ignore exception",
            ),
        ),
    )
    async def test_metrics(
        self,
        queue: str,
        status: AckStatus,
        exception_class: type[Exception] | None,
    ) -> None:
        event = asyncio.Event()
        registry = CollectorRegistry()
        custom_labels: dict[str, str | Callable[[Any], str]] = {
            "static": "pupupu",
            "dynamic": lambda x: "papapa",
        }
        middleware = self.get_middleware(registry=registry, custom_labels=custom_labels)

        broker = self.get_broker(apply_types=True, middlewares=(middleware,))

        args, kwargs = self.get_subscriber_params(queue)

        message = None

        @broker.subscriber(*args, **kwargs)
        async def handler(m=Context("message")) -> None:
            event.set()

            nonlocal message
            message = m

            if exception_class:
                raise exception_class

            if status == AckStatus.ACKED:
                await message.ack()
            elif status == AckStatus.NACKED:
                await message.nack()
            elif status == AckStatus.REJECTED:
                await message.reject()

        async with broker:
            await broker.start()
            tasks = (
                asyncio.create_task(broker.publish("hello", queue)),
                asyncio.create_task(event.wait()),
            )
            await asyncio.wait(tasks, timeout=self.timeout)

        assert event.is_set()
        self.assert_metrics(
            registry=registry,
            message=message,
            exception_class=exception_class,
            custom_labels={
                "static": "pupupu",
                "dynamic": "papapa",
            },
        )

    def assert_metrics(
        self,
        *,
        registry: CollectorRegistry,
        message: Any,
        exception_class: type[Exception] | None,
        custom_labels: dict[str, str],
    ) -> None:
        settings_provider = self.get_settings_provider()
        consume_attrs = settings_provider.get_consume_attrs_from_message(message)

        received_messages_metric = get_received_messages_metric(
            metrics_prefix="faststream",
            app_name="faststream",
            broker=settings_provider.messaging_system,
            queue=consume_attrs["destination_name"],
            messages_amount=consume_attrs["messages_count"],
            custom_labels=custom_labels,
        )

        received_messages_size_bytes_metric = get_received_messages_size_bytes_metric(
            metrics_prefix="faststream",
            app_name="faststream",
            broker=settings_provider.messaging_system,
            queue=consume_attrs["destination_name"],
            buckets=(
                2.0**4,
                2.0**6,
                2.0**8,
                2.0**10,
                2.0**12,
                2.0**14,
                2.0**16,
                2.0**18,
                2.0**20,
                2.0**22,
                2.0**24,
                float("inf"),
            ),
            size=consume_attrs["message_size"],
            messages_amount=1,
            custom_labels=custom_labels,
        )

        received_messages_in_process_metric = get_received_messages_in_process_metric(
            metrics_prefix="faststream",
            app_name="faststream",
            broker=settings_provider.messaging_system,
            queue=consume_attrs["destination_name"],
            messages_amount=0,
            custom_labels=custom_labels,
        )

        received_processed_messages_duration_seconds_metric = (
            get_received_processed_messages_duration_seconds_metric(
                metrics_prefix="faststream",
                app_name="faststream",
                broker=settings_provider.messaging_system,
                queue=consume_attrs["destination_name"],
                duration=cast("float", IsPositiveFloat),
                custom_labels=custom_labels,
            )
        )

        status = ProcessingStatus.acked

        if exception_class:
            status = (
                PROCESSING_STATUS_BY_HANDLER_EXCEPTION_MAP.get(exception_class)
                or ProcessingStatus.error
            )
        elif message.committed:
            status = PROCESSING_STATUS_BY_ACK_STATUS[message.committed]

        received_processed_messages_metric = get_received_processed_messages_metric(
            metrics_prefix="faststream",
            app_name="faststream",
            broker=settings_provider.messaging_system,
            queue=consume_attrs["destination_name"],
            messages_amount=consume_attrs["messages_count"],
            status=status,
            custom_labels=custom_labels,
        )

        exception_type: str | None = None

        if exception_class and not issubclass(exception_class, IgnoredException):
            exception_type = exception_class.__name__

        received_processed_messages_exceptions_metric = (
            get_received_processed_messages_exceptions_metric(
                metrics_prefix="faststream",
                app_name="faststream",
                broker=settings_provider.messaging_system,
                queue=consume_attrs["destination_name"],
                exception_type=exception_type,
                exceptions_amount=consume_attrs["messages_count"],
                custom_labels=custom_labels,
            )
        )

        published_messages_metric = get_published_messages_metric(
            metrics_prefix="faststream",
            app_name="faststream",
            broker=settings_provider.messaging_system,
            queue=cast("str", IsStr),
            status=PublishingStatus.success,
            messages_amount=consume_attrs["messages_count"],
            custom_labels=custom_labels,
        )

        published_messages_duration_seconds_metric = (
            get_published_messages_duration_seconds_metric(
                metrics_prefix="faststream",
                app_name="faststream",
                broker=settings_provider.messaging_system,
                queue=cast("str", IsStr),
                duration=cast("float", IsPositiveFloat),
                custom_labels=custom_labels,
            )
        )

        published_messages_exceptions_metric = get_published_messages_exceptions_metric(
            metrics_prefix="faststream",
            app_name="faststream",
            broker=settings_provider.messaging_system,
            queue=cast("str", IsStr),
            exception_type=None,
            custom_labels=custom_labels,
        )

        expected_metrics = IsList(
            received_messages_metric,
            received_messages_size_bytes_metric,
            received_messages_in_process_metric,
            received_processed_messages_metric,
            received_processed_messages_duration_seconds_metric,
            received_processed_messages_exceptions_metric,
            published_messages_metric,
            published_messages_duration_seconds_metric,
            published_messages_exceptions_metric,
            check_order=False,
        )
        real_metrics = list(registry.collect())

        assert real_metrics == expected_metrics


class LocalRPCPrometheusTestcase:
    @pytest.mark.asyncio()
    async def test_rpc_request(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()
        registry = CollectorRegistry()

        middleware = self.get_middleware(registry=registry)

        broker = self.get_broker(apply_types=True, middlewares=(middleware,))

        message = None

        @broker.subscriber(queue)
        async def handle(m=Context("message")):
            event.set()

            nonlocal message
            message = m

            return ""

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait_for(
                br.request("", queue),
                timeout=3,
            )

        assert event.is_set()

        self.assert_metrics(
            registry=registry,
            message=message,
            exception_class=None,
            custom_labels={},
        )


class LocalMetricsSettingsProviderTestcase:
    messaging_system: str

    def get_middleware(self, **kwargs) -> BasePrometheusMiddleware:
        raise NotImplementedError

    @staticmethod
    def get_settings_provider() -> MetricsSettingsProvider:
        raise NotImplementedError

    def test_messaging_system(self) -> None:
        provider = self.get_settings_provider()
        assert provider.messaging_system == self.messaging_system

    def test_one_registry_for_some_middlewares(self) -> None:
        registry = CollectorRegistry()

        middleware_1 = self.get_middleware(registry=registry)
        middleware_2 = self.get_middleware(registry=registry)
        self.get_broker(middlewares=(middleware_1,))
        self.get_broker(middlewares=(middleware_2,))

        assert (
            middleware_1._metrics_container.received_messages_total
            is middleware_2._metrics_container.received_messages_total
        )
