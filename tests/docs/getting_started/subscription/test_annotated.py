from typing import Any

import pytest
from fast_depends.exceptions import ValidationError
from typing_extensions import TypeAlias

from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.subscriber.usecase import SubscriberUsecase
from faststream._internal.testing.broker import TestBroker
from tests.marks import (
    python39,
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)

Setup: TypeAlias = tuple[
    BrokerUsecase[Any, Any],
    SubscriberUsecase[Any],
    type[TestBroker],
]


@pytest.mark.asyncio()
@python39
class BaseCase:
    async def test_handle(self, setup: Setup) -> None:
        broker, handle, test_class = setup

        async with test_class(broker) as br:
            await br.publish({"name": "John", "user_id": 1}, "test")
            handle.mock.assert_called_once_with({"name": "John", "user_id": 1})

        assert handle.mock is None

    async def test_validation_error(self, setup: Setup) -> None:
        broker, handle, test_class = setup

        async with test_class(broker) as br:
            with pytest.raises(ValidationError):
                await br.publish("wrong message", "test")

            handle.mock.assert_called_once_with("wrong message")


@require_aiokafka
class TestKafka(BaseCase):
    @pytest.fixture(scope="class")
    def setup(self) -> Setup:
        from docs.docs_src.getting_started.subscription.kafka.pydantic_annotated_fields import (
            broker,
            handle,
        )
        from faststream.kafka import TestKafkaBroker

        return (broker, handle, TestKafkaBroker)


@require_confluent
class TestConfluent(BaseCase):
    @pytest.fixture(scope="class")
    def setup(self) -> Setup:
        from docs.docs_src.getting_started.subscription.confluent.pydantic_annotated_fields import (
            broker,
            handle,
        )
        from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

        return (broker, handle, TestConfluentKafkaBroker)


@require_aiopika
class TestRabbit(BaseCase):
    @pytest.fixture(scope="class")
    def setup(self) -> Setup:
        from docs.docs_src.getting_started.subscription.rabbit.pydantic_annotated_fields import (
            broker,
            handle,
        )
        from faststream.rabbit import TestRabbitBroker

        return (broker, handle, TestRabbitBroker)


@require_nats
class TestNats(BaseCase):
    @pytest.fixture(scope="class")
    def setup(self) -> Setup:
        from docs.docs_src.getting_started.subscription.nats.pydantic_annotated_fields import (
            broker,
            handle,
        )
        from faststream.nats import TestNatsBroker

        return (broker, handle, TestNatsBroker)


@require_redis
class TestRedis(BaseCase):
    @pytest.fixture(scope="class")
    def setup(self) -> Setup:
        from docs.docs_src.getting_started.subscription.redis.pydantic_annotated_fields import (
            broker,
            handle,
        )
        from faststream.redis import TestRedisBroker

        return (broker, handle, TestRedisBroker)
