import logging

import pytest

from .basic import ConfluentTestcaseConfig


@pytest.mark.confluent()
class TestLogger(ConfluentTestcaseConfig):
    """A class to represent a test Kafka broker."""

    @pytest.mark.asyncio()
    async def test_custom_logger(self, queue: str) -> None:
        test_logger = logging.getLogger("test_logger")
        broker = self.get_broker(logger=test_logger)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        def subscriber(m) -> None: ...

        await broker.start()

        for sub in broker._subscribers:
            consumer_logger = sub.consumer.logger_state.logger.logger
            assert consumer_logger == test_logger

        await broker.close()
