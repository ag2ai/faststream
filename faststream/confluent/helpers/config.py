from collections.abc import Callable, Iterable
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Optional

from typing_extensions import TypedDict

from faststream.__about__ import SERVICE_NAME
from faststream._internal.constants import EMPTY
from faststream.confluent.security import parse_security

if TYPE_CHECKING:
    from faststream.security import BaseSecurity


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
    resolve_canonical_bootstrap_servers_only = "resolve_canonical_bootstrap_servers_only"


_SharedConfig = {
    "bootstrap_servers": "bootstrap.servers",
    "client_id": "client.id",
    "allow_auto_create_topics": "allow.auto.create.topics",
    "connections_max_idle_ms": "connections.max.idle.ms",
    "metadata_max_age_ms": "metadata.max.age.ms",
}

_ProducerConfig = _SharedConfig | {
    "request_timeout_ms": "request.timeout.ms",
    "compression_type": "compression.type",
    "acks": "acks",
    "retry_backoff_ms": "retry.backoff.ms",
    "partitioner": "partitioner",
    "max_request_size": "message.max.bytes",
    "linger_ms": "linger.ms",
    "enable_idempotence": "enable.idempotence",
    "transactional_id": "transactional.id",
    "transaction_timeout_ms": "transaction.timeout.ms",
}

_ConsumerConfig = _SharedConfig

_AdminConfig = _SharedConfig | {
    "request_timeout_ms": "request.timeout.ms",
    "retry_backoff_ms": "retry.backoff.ms",
}

ConfluentConfig = TypedDict(
    "ConfluentConfig",
    {
        "compression.codec": CompressionCodec | str,
        "compression.type": CompressionType | str,
        "client.dns.lookup": ClientDNSLookup | str,
        "offset.store.method": OffsetStoreMethod | str,
        "isolation.level": IsolationLevel | str,
        "sasl.oauthbearer.method": SASLOAUTHBearerMethod | str,
        "security.protocol": SecurityProtocol | str,
        "broker.address.family": BrokerAddressFamily | str,
        "builtin.features": BuiltinFeatures | str,
        "debug": Debug | str,
        "group.protocol": GroupProtocol | str,
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
        "enable.sasl.oauthbearer.unsecure.jwt": bool,  # codespell:ignore unsecure
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
        "delivery.timeout.ms": int,
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
    def __init__(
        self,
        *,
        security: Optional["BaseSecurity"] = None,
        config: ConfluentConfig | None = None,
        # shared
        bootstrap_servers: str | Iterable[str] = "localhost",
        retry_backoff_ms: int = 100,
        client_id: str | None = SERVICE_NAME,
        allow_auto_create_topics: bool = True,
        connections_max_idle_ms: int = 9 * 60 * 1000,
        metadata_max_age_ms: int = 5 * 60 * 1000,
        # producer
        request_timeout_ms: int = 40 * 1000,
        acks: Literal[0, 1, -1, "all"] = EMPTY,
        compression_type: Literal["gzip", "snappy", "lz4", "zstd"] | None = None,
        partitioner: str
        | Callable[[bytes, list[Any], list[Any]], Any] = "consistent_random",
        max_request_size: int = 1024 * 1024,
        linger_ms: int = 0,
        enable_idempotence: bool = False,
        transactional_id: str | None = None,
        transaction_timeout_ms: int = 60 * 1000,
    ) -> None:
        self.config = parse_security(security) | (config or {})

        shared_config: dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "client_id": client_id,
            "allow_auto_create_topics": allow_auto_create_topics,
            "connections_max_idle_ms": connections_max_idle_ms,
            "metadata_max_age_ms": metadata_max_age_ms,
        }

        # extended consumer options were passed to `broker.subscriber` method
        self.raw_consumer_config = shared_config

        self.raw_producer_config = shared_config | {
            "request_timeout_ms": request_timeout_ms,
            "partitioner": partitioner,
            "retry_backoff_ms": retry_backoff_ms,
            "max_request_size": max_request_size,
            "linger_ms": linger_ms,
            "enable_idempotence": enable_idempotence,
            "transactional_id": transactional_id,
            "transaction_timeout_ms": transaction_timeout_ms,
        }

        if compression_type:
            self.raw_producer_config["compression_type"] = compression_type

        if acks is EMPTY or acks == "all":
            self.raw_producer_config["acks"] = -1
        else:
            self.raw_producer_config["acks"] = acks

        self.raw_admin_config = shared_config | {
            "request_timeout_ms": request_timeout_ms,
            "retry_backoff_ms": retry_backoff_ms,
        }

    @property
    def consumer_config(self) -> dict[str, Any]:
        return _to_confluent(
            {_ConsumerConfig[k]: v for k, v in self.raw_consumer_config.items()}
            | self.config,
        )

    @property
    def producer_config(self) -> dict[str, Any]:
        return _to_confluent(
            {_ProducerConfig[k]: v for k, v in self.raw_producer_config.items()}
            | self.config,
        )

    @property
    def admin_config(self) -> dict[str, Any]:
        return _to_confluent(
            {_AdminConfig[k]: v for k, v in self.raw_admin_config.items()} | self.config,
        )


def _to_confluent(config: dict[str, Any]) -> dict[str, Any]:
    data = config.copy()

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
        if v := data.get(key):
            data[key] = enum(v).value

    bootstrap_servers = data.get("bootstrap.servers")
    if (
        bootstrap_servers
        and isinstance(bootstrap_servers, Iterable)
        and not isinstance(bootstrap_servers, str)
    ):
        data["bootstrap.servers"] = ",".join(bootstrap_servers)

    return data
