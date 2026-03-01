from collections.abc import Sequence
from typing import cast

from dirty_equals import IsFloat, IsPositiveFloat, IsStr
from prometheus_client import Histogram, Metric
from prometheus_client.samples import Sample

from faststream.prometheus.types import ProcessingStatus, PublishingStatus


def get_received_messages_metric(
    *,
    metrics_prefix: str,
    app_name: str,
    broker: str,
    queue: str,
    messages_amount: int,
    custom_labels: dict[str, str],
) -> Metric:
    metric = Metric(
        name=f"{metrics_prefix}_received_messages",
        documentation="Count of received messages by broker and handler",
        unit="",
        typ="counter",
    )
    metric.samples = [
        Sample(
            name=f"{metrics_prefix}_received_messages_total",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                **custom_labels,
            },
            value=float(messages_amount),
            timestamp=None,
            exemplar=None,
        ),
        Sample(
            name=f"{metrics_prefix}_received_messages_created",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                **custom_labels,
            },
            value=cast("float", IsPositiveFloat),
            timestamp=None,
            exemplar=None,
        ),
    ]

    return metric


def get_received_messages_size_bytes_metric(
    *,
    metrics_prefix: str,
    app_name: str,
    broker: str,
    queue: str,
    buckets: Sequence[float],
    size: int,
    messages_amount: int,
    custom_labels: dict[str, str],
) -> Metric:
    metric = Metric(
        name=f"{metrics_prefix}_received_messages_size_bytes",
        documentation="Histogram of received messages size in bytes by broker and handler",
        unit="",
        typ="histogram",
    )
    metric.samples = [
        *[
            Sample(
                name=f"{metrics_prefix}_received_messages_size_bytes_bucket",
                labels={
                    "app_name": app_name,
                    "broker": broker,
                    "handler": queue,
                    "le": cast("str", IsStr),
                    **custom_labels,
                },
                value=float(messages_amount),
                timestamp=None,
                exemplar=None,
            )
            for _ in buckets
        ],
        Sample(
            name=f"{metrics_prefix}_received_messages_size_bytes_count",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                **custom_labels,
            },
            value=float(messages_amount),
            timestamp=None,
            exemplar=None,
        ),
        Sample(
            name=f"{metrics_prefix}_received_messages_size_bytes_sum",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                **custom_labels,
            },
            value=size,
            timestamp=None,
            exemplar=None,
        ),
        Sample(
            name=f"{metrics_prefix}_received_messages_size_bytes_created",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                **custom_labels,
            },
            value=cast("float", IsPositiveFloat),
            timestamp=None,
            exemplar=None,
        ),
    ]

    return metric


def get_received_messages_in_process_metric(
    *,
    metrics_prefix: str,
    app_name: str,
    broker: str,
    queue: str,
    messages_amount: int,
    custom_labels: dict[str, str],
) -> Metric:
    metric = Metric(
        name=f"{metrics_prefix}_received_messages_in_process",
        documentation="Gauge of received messages in process by broker and handler",
        unit="",
        typ="gauge",
    )
    metric.samples = [
        Sample(
            name=f"{metrics_prefix}_received_messages_in_process",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                **custom_labels,
            },
            value=float(messages_amount),
            timestamp=None,
            exemplar=None,
        ),
    ]

    return metric


def get_received_processed_messages_metric(
    *,
    metrics_prefix: str,
    app_name: str,
    broker: str,
    queue: str,
    messages_amount: int,
    status: ProcessingStatus,
    custom_labels: dict[str, str],
) -> Metric:
    metric = Metric(
        name=f"{metrics_prefix}_received_processed_messages",
        documentation="Count of received processed messages by broker, handler and status",
        unit="",
        typ="counter",
    )
    metric.samples = [
        Sample(
            name=f"{metrics_prefix}_received_processed_messages_total",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                "status": status.value,
                **custom_labels,
            },
            value=float(messages_amount),
            timestamp=None,
            exemplar=None,
        ),
        Sample(
            name=f"{metrics_prefix}_received_processed_messages_created",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                "status": status.value,
                **custom_labels,
            },
            value=cast("float", IsPositiveFloat),
            timestamp=None,
            exemplar=None,
        ),
    ]

    return metric


def get_received_processed_messages_duration_seconds_metric(
    *,
    metrics_prefix: str,
    app_name: str,
    broker: str,
    queue: str,
    duration: float,
    custom_labels: dict[str, str],
) -> Metric:
    metric = Metric(
        name=f"{metrics_prefix}_received_processed_messages_duration_seconds",
        documentation="Histogram of received processed messages duration in seconds by broker and handler",
        unit="",
        typ="histogram",
    )
    metric.samples = [
        *[
            Sample(
                name=f"{metrics_prefix}_received_processed_messages_duration_seconds_bucket",
                labels={
                    "app_name": app_name,
                    "broker": broker,
                    "handler": queue,
                    "le": cast("str", IsStr),
                    **custom_labels,
                },
                value=cast("float", IsFloat),
                timestamp=None,
                exemplar=None,
            )
            for _ in Histogram.DEFAULT_BUCKETS
        ],
        Sample(
            name=f"{metrics_prefix}_received_processed_messages_duration_seconds_count",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                **custom_labels,
            },
            value=cast("float", IsPositiveFloat),
            timestamp=None,
            exemplar=None,
        ),
        Sample(
            name=f"{metrics_prefix}_received_processed_messages_duration_seconds_sum",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                **custom_labels,
            },
            value=duration,
            timestamp=None,
            exemplar=None,
        ),
        Sample(
            name=f"{metrics_prefix}_received_processed_messages_duration_seconds_created",
            labels={
                "app_name": app_name,
                "broker": broker,
                "handler": queue,
                **custom_labels,
            },
            value=cast("float", IsPositiveFloat),
            timestamp=None,
            exemplar=None,
        ),
    ]

    return metric


def get_received_processed_messages_exceptions_metric(
    *,
    metrics_prefix: str,
    app_name: str,
    broker: str,
    queue: str,
    exception_type: str | None,
    exceptions_amount: int,
    custom_labels: dict[str, str],
) -> Metric:
    metric = Metric(
        name=f"{metrics_prefix}_received_processed_messages_exceptions",
        documentation="Count of received processed messages exceptions by broker, handler and exception_type",
        unit="",
        typ="counter",
    )
    metric.samples = (
        [
            Sample(
                name=f"{metrics_prefix}_received_processed_messages_exceptions_total",
                labels={
                    "app_name": app_name,
                    "broker": broker,
                    "handler": queue,
                    "exception_type": exception_type,
                    **custom_labels,
                },
                value=float(exceptions_amount),
                timestamp=None,
                exemplar=None,
            ),
            Sample(
                name=f"{metrics_prefix}_received_processed_messages_exceptions_created",
                labels={
                    "app_name": app_name,
                    "broker": broker,
                    "handler": queue,
                    "exception_type": exception_type,
                    **custom_labels,
                },
                value=cast("float", IsPositiveFloat),
                timestamp=None,
                exemplar=None,
            ),
        ]
        if exception_type is not None
        else []
    )

    return metric


def get_published_messages_metric(
    *,
    metrics_prefix: str,
    app_name: str,
    broker: str,
    queue: str,
    messages_amount: int,
    status: PublishingStatus,
    custom_labels: dict[str, str],
) -> Metric:
    metric = Metric(
        name=f"{metrics_prefix}_published_messages",
        documentation="Count of published messages by destination and status",
        unit="",
        typ="counter",
    )
    metric.samples = [
        Sample(
            name=f"{metrics_prefix}_published_messages_total",
            labels={
                "app_name": app_name,
                "broker": broker,
                "destination": queue,
                "status": status.value,
                **custom_labels,
            },
            value=messages_amount,
            timestamp=None,
            exemplar=None,
        ),
        Sample(
            name=f"{metrics_prefix}_published_messages_created",
            labels={
                "app_name": app_name,
                "broker": broker,
                "destination": queue,
                "status": status.value,
                **custom_labels,
            },
            value=cast("float", IsPositiveFloat),
            timestamp=None,
            exemplar=None,
        ),
    ]

    return metric


def get_published_messages_duration_seconds_metric(
    *,
    metrics_prefix: str,
    app_name: str,
    broker: str,
    queue: str,
    duration: float,
    custom_labels: dict[str, str],
) -> Metric:
    metric = Metric(
        name=f"{metrics_prefix}_published_messages_duration_seconds",
        documentation="Histogram of published messages duration in seconds by broker and destination",
        unit="",
        typ="histogram",
    )
    metric.samples = [
        *[
            Sample(
                name=f"{metrics_prefix}_published_messages_duration_seconds_bucket",
                labels={
                    "app_name": app_name,
                    "broker": broker,
                    "destination": queue,
                    "le": cast("str", IsStr),
                    **custom_labels,
                },
                value=cast("float", IsFloat),
                timestamp=None,
                exemplar=None,
            )
            for _ in Histogram.DEFAULT_BUCKETS
        ],
        Sample(
            name=f"{metrics_prefix}_published_messages_duration_seconds_count",
            labels={
                "app_name": app_name,
                "broker": broker,
                "destination": queue,
                **custom_labels,
            },
            value=cast("float", IsPositiveFloat),
            timestamp=None,
            exemplar=None,
        ),
        Sample(
            name=f"{metrics_prefix}_published_messages_duration_seconds_sum",
            labels={
                "app_name": app_name,
                "broker": broker,
                "destination": queue,
                **custom_labels,
            },
            value=duration,
            timestamp=None,
            exemplar=None,
        ),
        Sample(
            name=f"{metrics_prefix}_published_messages_duration_seconds_created",
            labels={
                "app_name": app_name,
                "broker": broker,
                "destination": queue,
                **custom_labels,
            },
            value=cast("float", IsPositiveFloat),
            timestamp=None,
            exemplar=None,
        ),
    ]

    return metric


def get_published_messages_exceptions_metric(
    *,
    metrics_prefix: str,
    app_name: str,
    broker: str,
    queue: str,
    exception_type: str | None,
    custom_labels: dict[str, str],
) -> Metric:
    metric = Metric(
        name=f"{metrics_prefix}_published_messages_exceptions",
        documentation="Count of published messages exceptions by broker, destination and exception_type",
        unit="",
        typ="counter",
    )
    metric.samples = (
        [
            Sample(
                name=f"{metrics_prefix}_published_messages_exceptions_total",
                labels={
                    "app_name": app_name,
                    "broker": broker,
                    "destination": queue,
                    "exception_type": exception_type,
                    **custom_labels,
                },
                value=1.0,
                timestamp=None,
                exemplar=None,
            ),
            Sample(
                name=f"{metrics_prefix}_published_messages_exceptions_created",
                labels={
                    "app_name": app_name,
                    "broker": broker,
                    "destination": queue,
                    "exception_type": exception_type,
                    **custom_labels,
                },
                value=cast("float", IsPositiveFloat),
                timestamp=None,
                exemplar=None,
            ),
        ]
        if exception_type is not None
        else []
    )

    return metric
