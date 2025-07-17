import pytest
from dirty_equals import IsPartialDict


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_base_security() -> None:
    from docs.docs_src.confluent.security.basic import broker as basic_broker

    assert basic_broker.config.connection_config.producer_config == IsPartialDict({
        "security.protocol": "ssl",
    })


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_scram256() -> None:
    from docs.docs_src.confluent.security.sasl_scram256 import (
        broker as scram256_broker,
    )

    assert scram256_broker.config.connection_config.producer_config == IsPartialDict({
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": "admin",
        "sasl.password": "password",
        "security.protocol": "sasl_ssl",
    })


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_scram512() -> None:
    from docs.docs_src.confluent.security.sasl_scram512 import (
        broker as scram512_broker,
    )

    assert scram512_broker.config.connection_config.producer_config == IsPartialDict({
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "admin",
        "sasl.password": "password",
        "security.protocol": "sasl_ssl",
    })


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_plaintext() -> None:
    from docs.docs_src.confluent.security.plaintext import (
        broker as plaintext_broker,
    )

    producer_config = plaintext_broker.config.connection_config.producer_config

    assert producer_config == IsPartialDict({
        "sasl.mechanism": "PLAIN",
        "sasl.username": "admin",
        "sasl.password": "password",
        "security.protocol": "sasl_ssl",
    })


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_oathbearer() -> None:
    from docs.docs_src.confluent.security.sasl_oauthbearer import (
        broker as oauthbearer_broker,
    )

    producer_config = oauthbearer_broker.config.connection_config.producer_config

    assert producer_config == IsPartialDict({
        "sasl.mechanism": "OAUTHBEARER",
        "security.protocol": "sasl_ssl",
    })


@pytest.mark.asyncio()
@pytest.mark.confluent()
async def test_gssapi() -> None:
    from docs.docs_src.confluent.security.sasl_gssapi import (
        broker as gssapi_broker,
    )

    producer_config = gssapi_broker.config.connection_config.producer_config

    assert producer_config == IsPartialDict({
        "sasl.mechanism": "GSSAPI",
        "security.protocol": "sasl_ssl",
    })
