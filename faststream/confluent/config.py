from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from typing_extensions import TypedDict

if TYPE_CHECKING:
    from faststream.types import AnyDict


class BuiltinFeatures(str, Enum):
    gzip = "gzip"
    snappy = "snappy"
    ssl = "ssl"
    sasl = "sasl"
    regex = "regex"
    lz4 = "lz4"
    sasl_gssapi = "sasl_gssapi"
    sasl_plain = "sasl_plain"
    sasl_scram = "sasl_scram"
    plugins = "plugins"
    zstd = "zstd"
    sasl_oauthbearer = "sasl_oauthbearer"
    http = "http"
    oidc = "oidc"


class Debug(str, Enum):
    generic = "generic"
    broker = "broker"
    topic = "topic"
    metadata = "metadata"
    feature = "feature"
    queue = "queue"
    msg = "msg"
    protocol = "protocol"
    cgrp = "cgrp"
    security = "security"
    fetch = "fetch"
    interceptor = "interceptor"
    plugin = "plugin"
    consumer = "consumer"
    admin = "admin"
    eos = "eos"
    mock = "mock"
    assignor = "assignor"
    conf = "conf"
    all = "all"


class BrokerAddressFamily(str, Enum):
    any = "any"
    v4 = "v4"
    v6 = "v6"


class SecurityProtocol(str, Enum):
    plaintext = "plaintext"
    ssl = "ssl"
    sasl_plaintext = "sasl_plaintext"
    sasl_ssl = "sasl_ssl"


class SASLOAUTHBearerMethod(str, Enum):
    default = "default"
    oidc = "oidc"


class GroupProtocol(str, Enum):
    classic = "classic"
    consumer = "consumer"


class OffsetStoreMethod(str, Enum):
    none = "none"
    file = "file"
    broker = "broker"


class IsolationLevel(str, Enum):
    read_uncommitted = "read_uncommitted"
    read_committed = "read_committed"


class CompressionCodec(str, Enum):
    none = "none"
    gzip = "gzip"
    snappy = "snappy"
    lz4 = "lz4"
    zstd = "zstd"


class CompressionType(str, Enum):
    none = "none"
    gzip = "gzip"
    snappy = "snappy"
    lz4 = "lz4"
    zstd = "zstd"


class ClientDNSLookup(str, Enum):
    use_all_dns_ips = "use_all_dns_ips"
    resolve_canonical_bootstrap_servers_only = (
        "resolve_canonical_bootstrap_servers_only"
    )


ConfluentConfig = TypedDict(
    "ConfluentConfig",
    {
        "compression.codec": Union[CompressionCodec, str],
        "compression.type": Union[CompressionType, str],
        "client.dns.lookup": Union[ClientDNSLookup, str],
        "offset.store.method": Union[OffsetStoreMethod, str],
        "isolation.level": Union[IsolationLevel, str],
        "sasl.oauthbearer.method": Union[SASLOAUTHBearerMethod, str],
        "security.protocol": Union[SecurityProtocol, str],
        "broker.address.family": Union[BrokerAddressFamily, str],
        "builtin.features": Union[BuiltinFeatures, str],
        "debug": Union[Debug, str],
        "group.protocol": Union[GroupProtocol, str],
        "client.id": str,
        "metadata.broker.list": str,
        "bootstrap.servers": str,
        "message.max.bytes": int,
        "message.copy.max.bytes": int,
        "receive.message.max.bytes": int,
        "max.in.flight.requests.per.connection": int,
        "max.in.flight": int,
        "topic.metadata.refresh.interval.ms": int,
        "metadata.max.age.ms": int,
        "topic.metadata.refresh.fast.interval.ms": int,
        "topic.metadata.refresh.fast.cnt": int,
        "topic.metadata.refresh.sparse": bool,
        "topic.metadata.propagation.max.ms": int,
        "topic.blacklist": str,
        "socket.timeout.ms": int,
        "socket.blocking.max.ms": int,
        "socket.send.buffer.bytes": int,
        "socket.receive.buffer.bytes": int,
        "socket.keepalive.enable": bool,
        "socket.nagle.disable": bool,
        "socket.max.fails": int,
        "broker.address.ttl": int,
        "socket.connection.setup.timeout.ms": int,
        "connections.max.idle.ms": int,
        "reconnect.backoff.jitter.ms": int,
        "reconnect.backoff.ms": int,
        "reconnect.backoff.max.ms": int,
        "statistics.interval.ms": int,
        "enabled_events": int,
        "error_cb": Callable[..., Any],
        "throttle_cb": Callable[..., Any],
        "stats_cb": Callable[..., Any],
        "log_cb": Callable[..., Any],
        "log_level": int,
        "log.queue": bool,
        "log.thread.name": bool,
        "enable.random.seed": bool,
        "log.connection.close": bool,
        "background_event_cb": Callable[..., Any],
        "socket_cb": Callable[..., Any],
        "connect_cb": Callable[..., Any],
        "closesocket_cb": Callable[..., Any],
        "open_cb": Callable[..., Any],
        "resolve_cb": Callable[..., Any],
        "opaque": str,
        "default_topic_conf": str,
        "internal.termination.signal": int,
        "api.version.request": bool,
        "api.version.request.timeout.ms": int,
        "api.version.fallback.ms": int,
        "broker.version.fallback": str,
        "allow.auto.create.topics": bool,
        "ssl.cipher.suites": str,
        "ssl.curves.list": str,
        "ssl.sigalgs.list": str,
        "ssl.key.location": str,
        "ssl.key.password": str,
        "ssl.key.pem": str,
        "ssl_key": str,
        "ssl.certificate.location": str,
        "ssl.certificate.pem": str,
        "ssl_certificate": str,
        "ssl.ca.location": str,
        "ssl.ca.pem": str,
        "ssl_ca": str,
        "ssl.ca.certificate.stores": str,
        "ssl.crl.location": str,
        "ssl.keystore.location": str,
        "ssl.keystore.password": str,
        "ssl.providers": str,
        "ssl.engine.location": str,
        "ssl.engine.id": str,
        "ssl_engine_callback_data": str,
        "enable.ssl.certificate.verification": bool,
        "ssl.endpoint.identification.algorithm": str,
        "ssl.certificate.verify_cb": Callable[..., Any],
        "sasl.mechanisms": str,
        "sasl.mechanism": str,
        "sasl.kerberos.service.name": str,
        "sasl.kerberos.principal": str,
        "sasl.kerberos.kinit.cmd": str,
        "sasl.kerberos.keytab": str,
        "sasl.kerberos.min.time.before.relogin": int,
        "sasl.username": str,
        "sasl.password": str,
        "sasl.oauthbearer.config": str,
        "enable.sasl.oauthbearer.unsecure.jwt": bool,
        "oauth_cb": Callable[..., Any],
        "sasl.oauthbearer.client.id": str,
        "sasl.oauthbearer.client.secret": str,
        "sasl.oauthbearer.scope": str,
        "sasl.oauthbearer.extensions": str,
        "sasl.oauthbearer.token.endpoint.url": str,
        "plugin.library.paths": str,
        "interceptors": str,
        "group.id": str,
        "group.instance.id": str,
        "partition.assignment.strategy": str,
        "session.timeout.ms": str,
        "heartbeat.interval.ms": str,
        "group.protocol.type": str,
        "group.remote.assignor": str,
        "coordinator.query.interval.ms": int,
        "max.poll.interval.ms": int,
        "enable.auto.commit": bool,
        "auto.commit.interval.ms": int,
        "enable.auto.offset.store": bool,
        "queued.min.messages": int,
        "queued.max.messages.kbytes": int,
        "fetch.wait.max.ms": int,
        "fetch.queue.backoff.ms": int,
        "fetch.message.max.bytes": int,
        "max.partition.fetch.bytes": int,
        "fetch.max.bytes": int,
        "fetch.min.bytes": int,
        "fetch.error.backoff.ms": int,
        "consume_cb": Callable[..., Any],
        "rebalance_cb": Callable[..., Any],
        "offset_commit_cb": Callable[..., Any],
        "enable.partition.eof": bool,
        "check.crcs": bool,
        "client.rack": str,
        "transactional.id": str,
        "transaction.timeout.ms": int,
        "enable.idempotence": bool,
        "enable.gapless.guarantee": bool,
        "queue.buffering.max.messages": int,
        "queue.buffering.max.kbytes": int,
        "queue.buffering.max.ms": float,
        "linger.ms": float,
        "message.send.max.retries": int,
        "retries": int,
        "retry.backoff.ms": int,
        "retry.backoff.max.ms": int,
        "queue.buffering.backpressure.threshold": int,
        "batch.num.messages": int,
        "batch.size": int,
        "delivery.report.only.error": bool,
        "dr_cb": Callable[..., Any],
        "dr_msg_cb": Callable[..., Any],
        "sticky.partitioning.linger.ms": int,
        "on_delivery": Callable[..., Any],
    },
    total=False,
)


class ConfluentFastConfig:
    def __init__(self, config: Optional[ConfluentConfig]) -> None:
        self.config = config or {}

    def as_config_dict(self) -> "AnyDict":
        if not self.config:
            return {}

        data = dict(self.config)

        for key, enum in (
            ("compression.codec", CompressionCodec),
            ("compression.type", CompressionType),
            ("client.dns.lookup", ClientDNSLookup),
            ("offset.store.method", OffsetStoreMethod),
            ("isolation.level", IsolationLevel),
            ("sasl.oauthbearer.method", SASLOAUTHBearerMethod),
            ("security.protocol", SecurityProtocol),
            ("broker.address.family", BrokerAddressFamily),
            ("builtin.features", BuiltinFeatures),
            ("debug", Debug),
            ("group.protocol", GroupProtocol),
        ):
            if key in data:
                data[key] = enum(data[key]).value

        return data
