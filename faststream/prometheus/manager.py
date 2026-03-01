from .container import MetricsContainer
from .types import ProcessingStatus, PublishingStatus


class MetricsManager:
    __slots__ = ("_app_name", "_container")

    def __init__(
        self, container: MetricsContainer, *, app_name: str = "faststream"
    ) -> None:
        self._container = container
        self._app_name = app_name

    def add_received_message(
        self,
        broker: str,
        handler: str,
        amount: int = 1,
        **custom_labels: str,
    ) -> None:
        self._container.received_messages_total.labels(
            app_name=self._app_name,
            broker=broker,
            handler=handler,
            **custom_labels,
        ).inc(amount)

    def observe_received_messages_size(
        self,
        broker: str,
        handler: str,
        size: int,
        **custom_labels: str,
    ) -> None:
        self._container.received_messages_size_bytes.labels(
            app_name=self._app_name,
            broker=broker,
            handler=handler,
            **custom_labels,
        ).observe(size)

    def add_received_message_in_process(
        self,
        broker: str,
        handler: str,
        amount: int = 1,
        **custom_labels: str,
    ) -> None:
        self._container.received_messages_in_process.labels(
            app_name=self._app_name,
            broker=broker,
            handler=handler,
            **custom_labels,
        ).inc(amount)

    def remove_received_message_in_process(
        self,
        broker: str,
        handler: str,
        amount: int = 1,
        **custom_labels: str,
    ) -> None:
        self._container.received_messages_in_process.labels(
            app_name=self._app_name,
            broker=broker,
            handler=handler,
            **custom_labels,
        ).dec(amount)

    def add_received_processed_message(
        self,
        broker: str,
        handler: str,
        status: ProcessingStatus,
        amount: int = 1,
        **custom_labels: str,
    ) -> None:
        self._container.received_processed_messages_total.labels(
            app_name=self._app_name,
            broker=broker,
            handler=handler,
            status=status.value,
            **custom_labels,
        ).inc(amount)

    def observe_received_processed_message_duration(
        self,
        duration: float,
        broker: str,
        handler: str,
        **custom_labels: str,
    ) -> None:
        self._container.received_processed_messages_duration_seconds.labels(
            app_name=self._app_name,
            broker=broker,
            handler=handler,
            **custom_labels,
        ).observe(duration)

    def add_received_processed_message_exception(
        self,
        broker: str,
        handler: str,
        exception_type: str,
        **custom_labels: str,
    ) -> None:
        self._container.received_processed_messages_exceptions_total.labels(
            app_name=self._app_name,
            broker=broker,
            handler=handler,
            exception_type=exception_type,
            **custom_labels,
        ).inc()

    def add_published_message(
        self,
        broker: str,
        destination: str,
        status: PublishingStatus,
        amount: int = 1,
        **custom_labels: str,
    ) -> None:
        self._container.published_messages_total.labels(
            app_name=self._app_name,
            broker=broker,
            destination=destination,
            status=status.value,
            **custom_labels,
        ).inc(amount)

    def observe_published_message_duration(
        self,
        duration: float,
        broker: str,
        destination: str,
        **custom_labels: str,
    ) -> None:
        self._container.published_messages_duration_seconds.labels(
            app_name=self._app_name,
            broker=broker,
            destination=destination,
            **custom_labels,
        ).observe(duration)

    def add_published_message_exception(
        self,
        broker: str,
        destination: str,
        exception_type: str,
        **custom_labels: str,
    ) -> None:
        self._container.published_messages_exceptions_total.labels(
            app_name=self._app_name,
            broker=broker,
            destination=destination,
            exception_type=exception_type,
            **custom_labels,
        ).inc()
