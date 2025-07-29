import asyncio
from typing import Any, List, Mapping
from unittest.mock import Mock, patch

import pytest
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from faststream.confluent import KafkaRouter
from faststream.confluent.fastapi import KafkaRouter as StreamRouter
from faststream.confluent.testing import TestKafkaBroker, build_message
from tests.brokers.base.fastapi import FastAPILocalTestcase, FastAPITestcase

from .basic import ConfluentTestcaseConfig


@pytest.mark.confluent
class TestConfluentRouter(ConfluentTestcaseConfig, FastAPITestcase):
    router_class = StreamRouter
    broker_router_class = KafkaRouter

    async def test_batch_real(
        self,
        mock: Mock,
        queue: str,
        event: asyncio.Event,
    ):
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: List[str]):
            event.set()
            return mock(msg)

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hi", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "kwargs",
        [
            {},  # AsyncAPIDefaultSubscriber
            {"max_workers": 2},  # AsyncAPIConcurrentDefaultSubscriber
            {"batch": True},  # AsyncAPIBatchSubscriber
        ],
    )
    async def test_fastapi_router_real_topic_auto_creation(
        self, queue: str, config: Mapping[str, Any], kwargs: Mapping[str, Any]
    ):
        num_partitions = 2
        replication_factor = 1
        router = self.router_class()

        async def handler(msg):
            return "test"

        args, kwargs = self.get_subscriber_params(
            queue,
            **kwargs,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topics_configs=config,
        )

        router.subscriber(*args, **kwargs)(handler)

        admin = AdminClient({"bootstrap.servers": "localhost:9092"})

        resources = [ConfigResource("TOPIC", queue)]

        async with router.broker:
            with patch("faststream.confluent.client.NewTopic", wraps=NewTopic) as mock:
                await router.broker.start()

                mock.assert_called_once_with(
                    queue,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                    config=config,
                )

        futures = admin.describe_configs(resources)

        for f in futures.values():
            topic_config = f.result()
            for k, v in topic_config.items():
                assert v.value.lower() == str(config[k]).lower()


class TestRouterLocal(ConfluentTestcaseConfig, FastAPILocalTestcase):
    router_class = StreamRouter
    broker_router_class = KafkaRouter
    broker_test = staticmethod(TestKafkaBroker)
    build_message = staticmethod(build_message)

    async def test_batch_testclient(
        self,
        mock: Mock,
        queue: str,
        event: asyncio.Event,
    ):
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: List[str]):
            event.set()
            return mock(msg)

        async with TestKafkaBroker(router.broker):
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hi", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])
