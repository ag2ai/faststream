from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from faststream.exceptions import SetupError


@contextmanager
def patch_aio_consumer_and_producer() -> tuple[MagicMock, MagicMock]:
    try:
        producer = MagicMock(return_value=AsyncMock())

        with patch(
            "faststream.confluent.helpers.client.AsyncConfluentProducer",
            new=producer,
        ):
            yield producer
    finally:
        pass


@pytest.mark.asyncio()
async def test_base_security_pass_ssl_context() -> None:
    import ssl

    from faststream.confluent import KafkaBroker
    from faststream.security import BaseSecurity

    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    with pytest.raises(
        SetupError,
        match="ssl_context is not supported by confluent-kafka-python, please use config instead.",
    ):
        KafkaBroker("localhost:9092", security=security)
