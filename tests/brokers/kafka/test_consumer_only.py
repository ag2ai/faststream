from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from faststream.kafka import KafkaBroker
from faststream.kafka.configs.broker import KafkaBrokerConfig


@pytest.mark.kafka()
class TestConsumerOnly:
    def test_broker_config_stores_consumer_only(self) -> None:
        assert KafkaBrokerConfig(consumer_only=True).consumer_only is True

    def test_broker_config_consumer_only_defaults_to_false(self) -> None:
        assert KafkaBrokerConfig().consumer_only is False

    def test_broker_forwards_consumer_only_to_config(self) -> None:
        broker = KafkaBroker(consumer_only=True)
        assert broker.config.broker_config.consumer_only is True

    def test_broker_consumer_only_defaults_to_false(self) -> None:
        broker = KafkaBroker()
        assert broker.config.broker_config.consumer_only is False

    @pytest.mark.asyncio()
    async def test_connect_skips_producer_and_admin_when_consumer_only(self) -> None:
        config = KafkaBrokerConfig(consumer_only=True)
        # The producer must not be touched when consumer_only is set.
        config.producer = MagicMock()
        config.producer.connect = AsyncMock()

        with (
            patch(
                "faststream.kafka.configs.broker.aiokafka.AIOKafkaProducer"
            ) as producer_cls,
            patch(
                "faststream.kafka.configs.broker.aiokafka.admin.client.AIOKafkaAdminClient"
            ) as admin_cls,
        ):
            await config.connect(bootstrap_servers="localhost:9092")

        producer_cls.assert_not_called()
        admin_cls.assert_not_called()
        config.producer.connect.assert_not_called()
        # The consumer builder still gets wired up so subscribers work.
        assert callable(config.builder)

    @pytest.mark.asyncio()
    async def test_connect_creates_producer_and_admin_by_default(self) -> None:
        config = KafkaBrokerConfig()
        config.producer = MagicMock()
        config.producer.connect = AsyncMock()

        with (
            patch(
                "faststream.kafka.configs.broker.aiokafka.AIOKafkaProducer"
            ) as producer_cls,
            patch(
                "faststream.kafka.configs.broker.aiokafka.admin.client.AIOKafkaAdminClient"
            ) as admin_cls,
        ):
            admin_cls.return_value.start = AsyncMock()
            await config.connect(bootstrap_servers="localhost:9092")

        producer_cls.assert_called_once()
        admin_cls.assert_called_once()
        config.producer.connect.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_disconnect_skips_producer_when_consumer_only(self) -> None:
        config = KafkaBrokerConfig(consumer_only=True)
        config.producer = MagicMock()
        config.producer.disconnect = AsyncMock()

        await config.disconnect()

        config.producer.disconnect.assert_not_called()
