import random
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from faststream.prometheus.container import MetricsContainer
from faststream.prometheus.manager import MetricsManager
from faststream.prometheus.types import ProcessingStatus, PublishingStatus
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


class TestCaseMetrics:
    @staticmethod
    def create_metrics_manager(
        app_name: str,
        metrics_prefix: str,
        received_messages_size_buckets: list[float] | None = None,
        custom_label_names: list[str] | None = None,
    ) -> MetricsManager:
        registry = CollectorRegistry()
        container = MetricsContainer(
            registry,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
            custom_label_names=custom_label_names or [],
        )
        return MetricsManager(container, app_name=app_name)

    @pytest.fixture()
    def app_name(self, request) -> str:
        return "youtube"

    @pytest.fixture()
    def metrics_prefix(self, request) -> str:
        return "fs"

    @pytest.fixture()
    def broker(self) -> str:
        return "rabbit"

    @pytest.fixture()
    def queue(self) -> str:
        return "default.test"

    @pytest.fixture()
    def messages_amount(self) -> int:
        return random.randint(1, 10)

    @pytest.fixture()
    def exception_type(self) -> str:
        return Exception.__name__

    def test_add_received_message(
        self,
        app_name: str,
        metrics_prefix: str,
        queue: str,
        broker: str,
        messages_amount: int,
    ) -> None:
        manager = self.create_metrics_manager(
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            custom_label_names=["foo"],
        )

        expected = get_received_messages_metric(
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            queue=queue,
            broker=broker,
            messages_amount=messages_amount,
            custom_labels={"foo": "bar"},
        )

        manager.add_received_message(
            amount=messages_amount,
            broker=broker,
            handler=queue,
            foo="bar",
        )

        metric_values = manager._container.received_messages_total.collect()

        assert metric_values == [expected]

    @pytest.mark.parametrize(
        "is_default_buckets",
        (
            pytest.param(True, id="with default buckets"),
            pytest.param(False, id="with custom buckets"),
        ),
    )
    def test_observe_received_messages_size(
        self,
        app_name: str,
        metrics_prefix: str,
        queue: str,
        broker: str,
        is_default_buckets: bool,
    ) -> None:
        manager_kwargs: dict[str, Any] = {
            "app_name": app_name,
            "metrics_prefix": metrics_prefix,
        }

        custom_buckets = [1.0, 2.0, 3.0, float("inf")]

        if not is_default_buckets:
            manager_kwargs["received_messages_size_buckets"] = custom_buckets

        manager = self.create_metrics_manager(
            **manager_kwargs,
            custom_label_names=["foo"],
        )

        size = 1
        buckets = (
            MetricsContainer.DEFAULT_SIZE_BUCKETS
            if is_default_buckets
            else custom_buckets
        )

        expected = get_received_messages_size_bytes_metric(
            metrics_prefix=metrics_prefix,
            app_name=app_name,
            broker=broker,
            queue=queue,
            buckets=buckets,
            size=size,
            messages_amount=1,
            custom_labels={"foo": "bar"},
        )

        manager.observe_received_messages_size(
            size=size, broker=broker, handler=queue, foo="bar"
        )

        metric_values = manager._container.received_messages_size_bytes.collect()

        assert metric_values == [expected]

    def test_add_received_message_in_process(
        self,
        app_name: str,
        metrics_prefix: str,
        queue: str,
        broker: str,
        messages_amount: int,
    ) -> None:
        manager = self.create_metrics_manager(
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            custom_label_names=["foo"],
        )

        expected = get_received_messages_in_process_metric(
            metrics_prefix=metrics_prefix,
            app_name=app_name,
            broker=broker,
            queue=queue,
            messages_amount=messages_amount,
            custom_labels={"foo": "bar"},
        )

        manager.add_received_message_in_process(
            amount=messages_amount,
            broker=broker,
            handler=queue,
            foo="bar",
        )

        metric_values = manager._container.received_messages_in_process.collect()

        assert metric_values == [expected]

    def test_remove_received_message_in_process(
        self,
        app_name: str,
        metrics_prefix: str,
        queue: str,
        broker: str,
        messages_amount: int,
    ) -> None:
        manager = self.create_metrics_manager(
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            custom_label_names=["foo"],
        )

        expected = get_received_messages_in_process_metric(
            metrics_prefix=metrics_prefix,
            app_name=app_name,
            broker=broker,
            queue=queue,
            messages_amount=messages_amount - 1,
            custom_labels={"foo": "bar"},
        )

        manager.add_received_message_in_process(
            amount=messages_amount, broker=broker, handler=queue, foo="bar"
        )
        manager.remove_received_message_in_process(
            amount=1,
            broker=broker,
            handler=queue,
            foo="bar",
        )

        metric_values = manager._container.received_messages_in_process.collect()

        assert metric_values == [expected]

    @pytest.mark.parametrize(
        "status",
        (
            pytest.param(ProcessingStatus.acked, id="acked status"),
            pytest.param(ProcessingStatus.nacked, id="nacked status"),
            pytest.param(ProcessingStatus.rejected, id="rejected status"),
            pytest.param(ProcessingStatus.skipped, id="skipped status"),
            pytest.param(ProcessingStatus.error, id="error status"),
        ),
    )
    def test_add_received_processed_message(
        self,
        app_name: str,
        metrics_prefix: str,
        queue: str,
        broker: str,
        messages_amount: int,
        status: ProcessingStatus,
    ) -> None:
        manager = self.create_metrics_manager(
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            custom_label_names=["foo"],
        )

        expected = get_received_processed_messages_metric(
            metrics_prefix=metrics_prefix,
            app_name=app_name,
            broker=broker,
            queue=queue,
            messages_amount=messages_amount,
            status=status,
            custom_labels={"foo": "bar"},
        )

        manager.add_received_processed_message(
            amount=messages_amount,
            status=status,
            broker=broker,
            handler=queue,
            foo="bar",
        )

        metric_values = manager._container.received_processed_messages_total.collect()

        assert metric_values == [expected]

    def test_observe_received_processed_message_duration(
        self,
        app_name: str,
        metrics_prefix: str,
        queue: str,
        broker: str,
    ) -> None:
        manager = self.create_metrics_manager(
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            custom_label_names=["foo"],
        )

        duration = 0.001

        expected = get_received_processed_messages_duration_seconds_metric(
            metrics_prefix=metrics_prefix,
            app_name=app_name,
            broker=broker,
            queue=queue,
            duration=duration,
            custom_labels={"foo": "bar"},
        )

        manager.observe_received_processed_message_duration(
            duration=duration,
            broker=broker,
            handler=queue,
            foo="bar",
        )

        metric_values = (
            manager._container.received_processed_messages_duration_seconds.collect()
        )

        assert metric_values == [expected]

    def test_add_received_processed_message_exception(
        self,
        app_name: str,
        metrics_prefix: str,
        queue: str,
        broker: str,
        exception_type: str,
    ) -> None:
        manager = self.create_metrics_manager(
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            custom_label_names=["foo"],
        )

        expected = get_received_processed_messages_exceptions_metric(
            metrics_prefix=metrics_prefix,
            app_name=app_name,
            broker=broker,
            queue=queue,
            exception_type=exception_type,
            exceptions_amount=1,
            custom_labels={"foo": "bar"},
        )

        manager.add_received_processed_message_exception(
            exception_type=exception_type,
            broker=broker,
            handler=queue,
            foo="bar",
        )

        metric_values = (
            manager._container.received_processed_messages_exceptions_total.collect()
        )

        assert metric_values == [expected]

    @pytest.mark.parametrize(
        "status",
        (
            pytest.param(PublishingStatus.success, id="success status"),
            pytest.param(PublishingStatus.error, id="error status"),
        ),
    )
    def test_add_published_message(
        self,
        app_name: str,
        metrics_prefix: str,
        queue: str,
        broker: str,
        messages_amount: int,
        status: PublishingStatus,
    ) -> None:
        manager = self.create_metrics_manager(
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            custom_label_names=["foo"],
        )

        expected = get_published_messages_metric(
            metrics_prefix=metrics_prefix,
            app_name=app_name,
            broker=broker,
            queue=queue,
            status=status,
            messages_amount=1,
            custom_labels={"foo": "bar"},
        )

        manager.add_published_message(
            status=status,
            broker=broker,
            destination=queue,
            foo="bar",
        )

        metric_values = manager._container.published_messages_total.collect()

        assert metric_values == [expected]

    def test_observe_published_message_duration(
        self,
        app_name: str,
        metrics_prefix: str,
        queue: str,
        broker: str,
    ) -> None:
        manager = self.create_metrics_manager(
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            custom_label_names=["foo"],
        )

        duration = 0.001

        expected = get_published_messages_duration_seconds_metric(
            metrics_prefix=metrics_prefix,
            app_name=app_name,
            broker=broker,
            queue=queue,
            duration=duration,
            custom_labels={"foo": "bar"},
        )

        manager.observe_published_message_duration(
            duration=duration,
            broker=broker,
            destination=queue,
            foo="bar",
        )

        metric_values = manager._container.published_messages_duration_seconds.collect()

        assert metric_values == [expected]

    def test_add_published_message_exception(
        self,
        app_name: str,
        metrics_prefix: str,
        queue: str,
        broker: str,
        exception_type: str,
    ) -> None:
        manager = self.create_metrics_manager(
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            custom_label_names=["foo"],
        )

        expected = get_published_messages_exceptions_metric(
            metrics_prefix=metrics_prefix,
            app_name=app_name,
            broker=broker,
            queue=queue,
            exception_type=exception_type,
            custom_labels={"foo": "bar"},
        )

        manager.add_published_message_exception(
            exception_type=exception_type,
            broker=broker,
            destination=queue,
            foo="bar",
        )

        metric_values = manager._container.published_messages_exceptions_total.collect()

        assert metric_values == [expected]
