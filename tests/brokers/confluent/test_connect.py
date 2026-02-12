import pytest
from dirty_equals import IsPartialDict

from faststream.confluent import KafkaBroker
from faststream.confluent.helpers import config
from tests.brokers.base.connection import BrokerConnectionTestcase

from .conftest import Settings


@pytest.mark.connected()
@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_correct_config_merging(queue: str) -> None:
    broker = KafkaBroker(
        connections_max_idle_ms=1000,
        config={
            "compression.codec": config.CompressionCodec.lz4,
            "message.max.bytes": 1000,
            "debug": config.Debug.broker,
        },
    )

    @broker.subscriber(queue)
    async def handler() -> None: ...

    async with broker:
        await broker.start()

        expected_config = IsPartialDict({
            "connections.max.idle.ms": 1000,
            "compression.codec": config.CompressionCodec.lz4,
            "message.max.bytes": 1000,
            "debug": config.Debug.broker,
        })

        producer_config = broker._producer._producer.producer.config

        assert producer_config == expected_config

        subscriber_config = broker.subscribers[0].consumer.config

        assert subscriber_config == expected_config


@pytest.mark.confluent()
def test_correct_config_with_dict() -> None:
    broker = KafkaBroker(
        config={
            "compression.codec": config.CompressionCodec.none,
            "compression.type": config.CompressionType.none,
            "client.dns.lookup": config.ClientDNSLookup.use_all_dns_ips,
            "offset.store.method": config.OffsetStoreMethod.broker,
            "isolation.level": config.IsolationLevel.read_uncommitted,
            "sasl.oauthbearer.method": config.SASLOAUTHBearerMethod.default,
            "security.protocol": config.SecurityProtocol.ssl,
            "broker.address.family": config.BrokerAddressFamily.any,
            "builtin.features": config.BuiltinFeatures.gzip,
            "debug": config.Debug.broker,
            "group.protocol": config.GroupProtocol.classic,
        },
    )

    assert broker.config.connection_config.consumer_config == IsPartialDict({
        "compression.codec": config.CompressionCodec.none.value,
        "compression.type": config.CompressionType.none.value,
        "client.dns.lookup": config.ClientDNSLookup.use_all_dns_ips.value,
        "offset.store.method": config.OffsetStoreMethod.broker.value,
        "isolation.level": config.IsolationLevel.read_uncommitted.value,
        "sasl.oauthbearer.method": config.SASLOAUTHBearerMethod.default.value,
        "security.protocol": config.SecurityProtocol.ssl.value,
        "broker.address.family": config.BrokerAddressFamily.any.value,
        "builtin.features": config.BuiltinFeatures.gzip.value,
        "debug": config.Debug.broker.value,
        "group.protocol": config.GroupProtocol.classic.value,
    })


@pytest.mark.connected()
@pytest.mark.confluent()
class TestConnection(BrokerConnectionTestcase):
    broker = KafkaBroker

    def _get_broker_args(self, settings: Settings) -> dict[str, str]:
        return {"bootstrap_servers": settings.url}
