import pytest

from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.mq()
async def test_plaintext_security() -> None:
    from docs.docs_src.mq.security.plaintext import broker

    async with broker:
        assert await broker.ping(1.0)

    schema = get_2_6_0_schema(broker)
    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {
            "messages": {},
            "schemas": {},
            "securitySchemes": {"user-password": {"type": "userPassword"}},
        },
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "ibmmq",
                "protocolVersion": "mqi",
                "security": [{"user-password": []}],
                "url": "mq://QM1@localhost(1414)",
            },
        },
    }


@pytest.mark.mq()
def test_tls_snippets(monkeypatch) -> None:
    monkeypatch.setattr("faststream.mq.tls._build_pkcs12", lambda **kwargs: b"pkcs12")
    from docs.docs_src.mq.security.tls_key_repository import broker as keyrepo_broker
    from docs.docs_src.mq.security.tls_pem import broker as pem_broker

    assert keyrepo_broker.config.connection_config.tls is not None
    assert pem_broker.config.connection_config.tls is not None
