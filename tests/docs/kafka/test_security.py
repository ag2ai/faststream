import ssl
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@contextmanager
def patch_aio_consumer_and_producer() -> tuple[MagicMock, MagicMock]:
    try:
        producer = MagicMock(return_value=AsyncMock())

        with patch("aiokafka.AIOKafkaProducer", new=producer):
            yield producer
    finally:
        pass


@pytest.mark.asyncio()
@pytest.mark.kafka()
async def test_base_security() -> None:
    from docs.docs_src.kafka.security.basic import broker as basic_broker

    with patch_aio_consumer_and_producer() as producer:
        async with basic_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {}
            call_kwargs["security_protocol"] = "SSL"

            assert call_kwargs.items() <= producer_call_kwargs.items()

            assert type(producer_call_kwargs["ssl_context"]) is ssl.SSLContext


@pytest.mark.asyncio()
@pytest.mark.kafka()
async def test_scram256() -> None:
    from docs.docs_src.kafka.security.sasl_scram256 import (
        broker as scram256_broker,
    )

    with patch_aio_consumer_and_producer() as producer:
        async with scram256_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {}
            call_kwargs["sasl_mechanism"] = "SCRAM-SHA-256"
            call_kwargs["sasl_plain_username"] = "admin"
            call_kwargs["sasl_plain_password"] = "password"  # pragma: allowlist secret
            call_kwargs["security_protocol"] = "SASL_SSL"

            assert call_kwargs.items() <= producer_call_kwargs.items()

            assert type(producer_call_kwargs["ssl_context"]) is ssl.SSLContext


@pytest.mark.asyncio()
@pytest.mark.kafka()
async def test_scram512() -> None:
    from docs.docs_src.kafka.security.sasl_scram512 import (
        broker as scram512_broker,
    )

    with patch_aio_consumer_and_producer() as producer:
        async with scram512_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {}
            call_kwargs["sasl_mechanism"] = "SCRAM-SHA-512"
            call_kwargs["sasl_plain_username"] = "admin"
            call_kwargs["sasl_plain_password"] = "password"  # pragma: allowlist secret
            call_kwargs["security_protocol"] = "SASL_SSL"

            assert call_kwargs.items() <= producer_call_kwargs.items()

            assert type(producer_call_kwargs["ssl_context"]) is ssl.SSLContext


@pytest.mark.asyncio()
@pytest.mark.kafka()
async def test_plaintext() -> None:
    from docs.docs_src.kafka.security.plaintext import (
        broker as plaintext_broker,
    )

    with patch_aio_consumer_and_producer() as producer:
        async with plaintext_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {}
            call_kwargs["sasl_mechanism"] = "PLAIN"
            call_kwargs["sasl_plain_username"] = "admin"
            call_kwargs["sasl_plain_password"] = "password"  # pragma: allowlist secret
            call_kwargs["security_protocol"] = "SASL_SSL"

            assert call_kwargs.items() <= producer_call_kwargs.items()

            assert type(producer_call_kwargs["ssl_context"]) is ssl.SSLContext


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_gssapi() -> None:
    from docs.docs_src.kafka.security.sasl_gssapi import (
        broker as gssapi_broker,
    )

    with patch_aio_consumer_and_producer() as producer:
        async with gssapi_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {
                "sasl_mechanism": "GSSAPI",
                "security_protocol": "SASL_SSL",
            }

            assert call_kwargs.items() <= producer_call_kwargs.items()

            assert type(producer_call_kwargs["ssl_context"]) is ssl.SSLContext
