from faststream.rabbit.fastapi import RabbitRouter
from faststream.rabbit.testing import TestRabbitBroker
from faststream.security import SASLPlaintext
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.arguments import FastAPICompatible
from tests.asyncapi.base.v2_6_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = staticmethod(lambda: RabbitRouter().broker)
    router_class = RabbitRouter
    broker_wrapper = staticmethod(TestRabbitBroker)

    def build_app(self, router):
        return router


class TestRouterPublisher(PublisherTestcase):
    broker_class = staticmethod(lambda: RabbitRouter().broker)

    def build_app(self, router):
        return router


def test_fastapi_security_schema() -> None:
    security = SASLPlaintext(username="user", password="pass", use_ssl=False)

    router = RabbitRouter(security=security)

    schema = AsyncAPI(router.broker, schema_version="2.6.0").to_jsonable()

    assert schema["servers"]["development"] == {
        "protocol": "amqp",
        "protocolVersion": "0.9.1",
        "security": [{"user-password": []}],
        "url": "amqp://user:pass@localhost:5672/",  # pragma: allowlist secret
    }
    assert schema["components"]["securitySchemes"] == {
        "user-password": {"type": "userPassword"},
    }
