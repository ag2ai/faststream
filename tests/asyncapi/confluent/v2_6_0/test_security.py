import ssl
from copy import deepcopy

from faststream.confluent import KafkaBroker
from faststream.security import (
    SASLGSSAPI,
    BaseSecurity,
    SASLOAuthBearer,
    SASLPlaintext,
    SASLScram256,
    SASLScram512,
)
from faststream.specification.asyncapi import AsyncAPI

basic_schema = {
    "asyncapi": "2.6.0",
    "channels": {
        "test_1:TestTopic": {
            "bindings": {"kafka": {"bindingVersion": "0.4.0", "topic": "test_1"}},
            "servers": ["development"],
            "publish": {
                "message": {"$ref": "#/components/messages/test_1:TestTopic:Message"},
            },
        },
        "test_2:Publisher": {
            "bindings": {"kafka": {"bindingVersion": "0.4.0", "topic": "test_2"}},
            "subscribe": {
                "message": {"$ref": "#/components/messages/test_2:Publisher:Message"},
            },
            "servers": ["development"],
        },
    },
    "components": {
        "messages": {
            "test_1:TestTopic:Message": {
                "correlationId": {"location": "$message.header#/correlation_id"},
                "payload": {"$ref": "#/components/schemas/TestTopic:Message:Payload"},
                "title": "test_1:TestTopic:Message",
            },
            "test_2:Publisher:Message": {
                "correlationId": {"location": "$message.header#/correlation_id"},
                "payload": {
                    "$ref": "#/components/schemas/test_2:Publisher:Message:Payload",
                },
                "title": "test_2:Publisher:Message",
            },
        },
        "schemas": {
            "TestTopic:Message:Payload": {
                "title": "TestTopic:Message:Payload",
                "type": "string",
            },
            "test_2:Publisher:Message:Payload": {
                "title": "test_2:Publisher:Message:Payload",
                "type": "string",
            },
        },
        "securitySchemes": {},
    },
    "defaultContentType": "application/json",
    "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
    "servers": {
        "development": {
            "protocol": "kafka-secure",
            "protocolVersion": "auto",
            "security": [],
            "url": "localhost:9092",
        },
    },
}


def test_base_security_schema() -> None:
    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    broker = KafkaBroker("localhost:9092", security=security)

    @broker.publisher("test_2")
    @broker.subscriber("test_1")
    async def test_topic(msg: str) -> str:
        pass

    schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

    assert schema == basic_schema


def test_plaintext_security_schema() -> None:
    ssl_context = ssl.create_default_context()
    security = SASLPlaintext(
        ssl_context=ssl_context,
        username="admin",
        password="password",  # pragma: allowlist secret
    )

    broker = KafkaBroker("localhost:9092", security=security)

    @broker.publisher("test_2")
    @broker.subscriber("test_1")
    async def test_topic(msg: str) -> str:
        pass

    schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

    plaintext_security_schema = deepcopy(basic_schema)
    plaintext_security_schema["servers"]["development"]["security"] = [
        {"user-password": []},
    ]
    plaintext_security_schema["components"]["securitySchemes"] = {
        "user-password": {"type": "userPassword"},
    }

    assert schema == plaintext_security_schema


def test_scram256_security_schema() -> None:
    ssl_context = ssl.create_default_context()
    security = SASLScram256(
        ssl_context=ssl_context,
        username="admin",
        password="password",  # pragma: allowlist secret
    )

    broker = KafkaBroker("localhost:9092", security=security)

    @broker.publisher("test_2")
    @broker.subscriber("test_1")
    async def test_topic(msg: str) -> str:
        pass

    schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

    sasl256_security_schema = deepcopy(basic_schema)
    sasl256_security_schema["servers"]["development"]["security"] = [{"scram256": []}]
    sasl256_security_schema["components"]["securitySchemes"] = {
        "scram256": {"type": "scramSha256"},
    }

    assert schema == sasl256_security_schema


def test_scram512_security_schema() -> None:
    ssl_context = ssl.create_default_context()
    security = SASLScram512(
        ssl_context=ssl_context,
        username="admin",
        password="password",  # pragma: allowlist secret
    )

    broker = KafkaBroker("localhost:9092", security=security)

    @broker.publisher("test_2")
    @broker.subscriber("test_1")
    async def test_topic(msg: str) -> str:
        pass

    schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

    sasl512_security_schema = deepcopy(basic_schema)
    sasl512_security_schema["servers"]["development"]["security"] = [{"scram512": []}]
    sasl512_security_schema["components"]["securitySchemes"] = {
        "scram512": {"type": "scramSha512"},
    }

    assert schema == sasl512_security_schema


def test_oauthbearer_security_schema() -> None:
    ssl_context = ssl.create_default_context()
    security = SASLOAuthBearer(
        ssl_context=ssl_context,
    )

    broker = KafkaBroker("localhost:9092", security=security)

    @broker.publisher("test_2")
    @broker.subscriber("test_1")
    async def test_topic(msg: str) -> str:
        pass

    schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

    sasl_oauthbearer_security_schema = deepcopy(basic_schema)
    sasl_oauthbearer_security_schema["servers"]["development"]["security"] = [
        {"oauthbearer": []},
    ]
    sasl_oauthbearer_security_schema["components"]["securitySchemes"] = {
        "oauthbearer": {"type": "oauth2", "$ref": ""}
    }

    assert schema == sasl_oauthbearer_security_schema


def test_gssapi_security_schema() -> None:
    ssl_context = ssl.create_default_context()
    security = SASLGSSAPI(ssl_context=ssl_context)

    broker = KafkaBroker("localhost:9092", security=security)

    @broker.publisher("test_2")
    @broker.subscriber("test_1")
    async def test_topic(msg: str) -> str:
        pass

    schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

    gssapi_security_schema = deepcopy(basic_schema)
    gssapi_security_schema["servers"]["development"]["security"] = [{"gssapi": []}]
    gssapi_security_schema["components"]["securitySchemes"] = {
        "gssapi": {"type": "gssapi"},
    }

    assert schema == gssapi_security_schema
