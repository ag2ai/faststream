import pytest

from tests.brokers.confluent.test_security import patch_aio_consumer_and_producer


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_base_security() -> None:
    from docs.docs_src.confluent.security.basic import broker as basic_broker

    with patch_aio_consumer_and_producer() as producer:
        async with basic_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {}

            assert call_kwargs.items() <= producer_call_kwargs.items()


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_scram256() -> None:
    from docs.docs_src.confluent.security.sasl_scram256 import (
        broker as scram256_broker,
    )

    with patch_aio_consumer_and_producer() as producer:
        async with scram256_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {
                "security_config": {
                    "sasl.mechanism": "SCRAM-SHA-256",
                    "sasl.username": "admin",
                    "sasl.password": "password",  # pragma: allowlist secret
                },
                "security_protocol": "SASL_SSL",
            }

            assert call_kwargs.items() <= producer_call_kwargs.items()

            assert (
                producer_call_kwargs["security_protocol"]
                == call_kwargs["security_protocol"]
            )


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_scram512() -> None:
    from docs.docs_src.confluent.security.sasl_scram512 import (
        broker as scram512_broker,
    )

    with patch_aio_consumer_and_producer() as producer:
        async with scram512_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {
                "security_config": {
                    "sasl.mechanism": "SCRAM-SHA-512",
                    "sasl.username": "admin",
                    "sasl.password": "password",  # pragma: allowlist secret
                },
                "security_protocol": "SASL_SSL",
            }

            assert call_kwargs.items() <= producer_call_kwargs.items()

            assert (
                producer_call_kwargs["security_protocol"]
                == call_kwargs["security_protocol"]
            )


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_plaintext() -> None:
    from docs.docs_src.confluent.security.plaintext import (
        broker as plaintext_broker,
    )

    with patch_aio_consumer_and_producer() as producer:
        async with plaintext_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {
                "security_config": {
                    "sasl.mechanism": "PLAIN",
                    "sasl.username": "admin",
                    "sasl.password": "password",  # pragma: allowlist secret
                },
                "security_protocol": "SASL_SSL",
            }

            assert call_kwargs.items() <= producer_call_kwargs.items()

            assert (
                producer_call_kwargs["security_protocol"]
                == call_kwargs["security_protocol"]
            )


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_oathbearer() -> None:
    from docs.docs_src.confluent.security.sasl_oauthbearer import (
        broker as oauthbearer_broker,
    )

    with patch_aio_consumer_and_producer() as producer:
        async with oauthbearer_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {
                "security_config": {
                    "sasl.mechanism": "OAUTHBEARER",
                },
                "security_protocol": "SASL_SSL",
            }

            assert call_kwargs.items() <= producer_call_kwargs.items()

            assert (
                producer_call_kwargs["security_protocol"]
                == call_kwargs["security_protocol"]
            )


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_gssapi() -> None:
    from docs.docs_src.confluent.security.sasl_gssapi import (
        broker as gssapi_broker,
    )

    with patch_aio_consumer_and_producer() as producer:
        async with gssapi_broker:
            producer_call_kwargs = producer.call_args.kwargs

            call_kwargs = {
                "security_config": {
                    "sasl.mechanism": "GSSAPI",
                },
                "security_protocol": "SASL_SSL",
            }

            assert call_kwargs.items() <= producer_call_kwargs.items()

            assert (
                producer_call_kwargs["security_protocol"]
                == call_kwargs["security_protocol"]
            )
