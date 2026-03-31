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
