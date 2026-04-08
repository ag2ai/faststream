import pytest

from .basic import MQMemoryTestcaseConfig


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestStartupRetry(MQMemoryTestcaseConfig):
    async def test_broker_connect_retries_retryable_error(self, monkeypatch) -> None:
        broker = self.get_broker()
        broker.config.connection_config.startup_retry_timeout = 0.1
        broker.config.connection_config.startup_retry_interval = 0.0

        monkeypatch.setattr(
            "faststream.mq.broker.broker.is_retryable_mq_exception",
            lambda exc: isinstance(exc, RetryableError),
        )

        attempts = 0

        async def fake_connect() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RetryableError
            broker.config.producer._publish_connection = object()

        monkeypatch.setattr(broker.config, "connect", fake_connect)

        connection = await broker.connect()

        assert connection is broker.config.producer.connection
        assert attempts == 2

    async def test_broker_connect_fails_fast_on_non_retryable_error(self, monkeypatch) -> None:
        broker = self.get_broker()
        broker.config.connection_config.startup_retry_timeout = 0.1
        broker.config.connection_config.startup_retry_interval = 0.0

        monkeypatch.setattr(
            "faststream.mq.broker.broker.is_retryable_mq_exception",
            lambda exc: False,
        )

        attempts = 0

        async def fake_connect() -> None:
            nonlocal attempts
            attempts += 1
            raise NonRetryableError

        monkeypatch.setattr(broker.config, "connect", fake_connect)

        with pytest.raises(NonRetryableError):
            await broker.connect()

        assert attempts == 1

    async def test_broker_connect_times_out_on_repeated_retryable_error(
        self,
        monkeypatch,
    ) -> None:
        broker = self.get_broker()
        broker.config.connection_config.startup_retry_timeout = 0.0
        broker.config.connection_config.startup_retry_interval = 0.0

        monkeypatch.setattr(
            "faststream.mq.broker.broker.is_retryable_mq_exception",
            lambda exc: isinstance(exc, RetryableError),
        )

        attempts = 0

        async def fake_connect() -> None:
            nonlocal attempts
            attempts += 1
            raise RetryableError

        monkeypatch.setattr(broker.config, "connect", fake_connect)

        with pytest.raises(RetryableError):
            await broker.connect()

        assert attempts == 1

    async def test_subscriber_start_retries_retryable_connect_error(
        self,
        queue: str,
        monkeypatch,
    ) -> None:
        broker = self.get_broker()
        broker.config.connection_config.startup_retry_timeout = 0.1
        broker.config.connection_config.startup_retry_interval = 0.0

        monkeypatch.setattr(
            "faststream.mq.subscriber.usecase.is_retryable_mq_exception",
            lambda exc: isinstance(exc, RetryableError),
        )

        class DummyConnection:
            attempts = 0

            def __init__(self, *, connection_config) -> None:
                self.connection_config = connection_config
                self.disconnected = 0

            async def connect(self) -> None:
                type(self).attempts += 1
                if type(self).attempts == 1:
                    raise RetryableError

            async def start_consumer(self, queue_name: str) -> None:
                return None

            async def stop_consumer(self) -> None:
                return None

            async def disconnect(self) -> None:
                self.disconnected += 1

        monkeypatch.setattr(
            "faststream.mq.subscriber.usecase.AsyncMQConnection",
            DummyConnection,
        )

        subscriber = broker.subscriber(queue)

        @subscriber
        async def handler(msg: str) -> None: ...

        broker._setup_logger()
        await subscriber.start()
        try:
            assert subscriber._consumer is not None
            assert DummyConnection.attempts == 2
        finally:
            await subscriber.stop()
