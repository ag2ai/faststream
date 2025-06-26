from faststream.kafka.fastapi import KafkaRouter
from faststream.kafka.testing import TestKafkaBroker
from faststream.security import SASLPlaintext
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.arguments import FastAPICompatible
from tests.asyncapi.base.v2_6_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = staticmethod(lambda: KafkaRouter().broker)
    router_class = KafkaRouter
    broker_wrapper = staticmethod(TestKafkaBroker)

    def build_app(self, router):
        return router


class TestRouterPublisher(PublisherTestcase):
    broker_class = staticmethod(lambda: KafkaRouter().broker)

    def build_app(self, router):
        return router


def test_fastapi_security_schema() -> None:
    security = SASLPlaintext(username="user", password="pass", use_ssl=False)

    router = KafkaRouter("localhost:9092", security=security)

    schema = AsyncAPI(router.broker, schema_version="2.6.0").to_jsonable()

    assert schema["servers"]["development"] == {
        "protocol": "kafka",
        "protocolVersion": "auto",
        "security": [{"user-password": []}],
        "url": "localhost:9092",
    }
    assert schema["components"]["securitySchemes"] == {
        "user-password": {"type": "userPassword"},
    }
