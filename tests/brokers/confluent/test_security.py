from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from faststream.exceptions import SetupError


@contextmanager
def patch_aio_consumer_and_producer() -> tuple[MagicMock, MagicMock]:
    try:
        producer = MagicMock(return_value=AsyncMock())

        with patch(
            "faststream.confluent.broker.broker.AsyncConfluentProducer",
            new=producer,
        ):
            yield producer
    finally:
        pass


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_base_security_pass_ssl_context() -> None:
    import ssl

    from faststream.confluent import KafkaBroker
    from faststream.security import BaseSecurity

    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    basic_broker = KafkaBroker("localhost:9092", security=security)

    with (
        patch_aio_consumer_and_producer(),
        pytest.raises(
            SetupError,
            match="not supported",
        ) as e,
    ):
        async with basic_broker:
            pass

    assert (
        str(e.value)
        == "ssl_context in not supported by confluent-kafka-python, please use config instead."
    )
