import pytest

from faststream.confluent import KafkaBroker, config
from tests.brokers.base.connection import BrokerConnectionTestcase


def test_correct_config() -> None:
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

    assert broker.config.as_config_dict() == {
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
    }


@pytest.mark.confluent()
class TestConnection(BrokerConnectionTestcase):
    broker = KafkaBroker

    def get_broker_args(self, settings):
        return {"bootstrap_servers": settings.url}
