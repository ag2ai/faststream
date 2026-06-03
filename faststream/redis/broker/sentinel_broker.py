import logging
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from fast_depends import dependency_provider
from typing_extensions import Unpack

from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.exceptions import SetupError
from faststream.redis.broker import RedisBroker
from faststream.redis.configs import RedisBrokerConfig
from faststream.redis.configs.state import RedisSentinelConnectionState
from faststream.redis.parser import BinaryMessageFormatV1
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.schemas.types import NON_CONNECTION_PARAMS, SENTINEL_PARAMS
from faststream.specification.schema import BrokerSpec

from .logging import make_redis_logger_state

if TYPE_CHECKING:
    from faststream.redis.schemas.types import RedisSentinelParams


class RedisSentinelBroker(RedisBroker):
    """A Redis broker backed by Redis Sentinel (high-availability with failover).

    The master address is discovered from the ``sentinels`` nodes through
    ``Sentinel.master_for(...)``; the underlying ``SentinelConnectionPool``
    re-discovers the current master on reconnect, so publishers and consumers
    survive a master failover transparently. Commands, Pub/Sub, Lists and
    Streams behave exactly like a plain ``RedisBroker`` — only the connection
    acquisition differs.
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        **kwargs: Unpack["RedisSentinelParams"],
    ) -> None:
        sentinels = kwargs.pop("sentinels", None)
        sentinel_master_name = kwargs.pop("sentinel_master_name", None)
        sentinel_kwargs = kwargs.pop("sentinel_kwargs", None)

        if not sentinels:
            msg = "`sentinels` is required for RedisSentinelBroker."
            raise SetupError(msg)
        if not sentinel_master_name:
            msg = "`sentinel_master_name` is required for RedisSentinelBroker."
            raise SetupError(msg)

        host = kwargs.pop("host", EMPTY)
        port = kwargs.pop("port", EMPTY)
        security = kwargs.pop("security", None)
        specification_url = kwargs.pop("specification_url", None)
        protocol = kwargs.pop("protocol", None)
        message_format = kwargs.pop("message_format", BinaryMessageFormatV1)
        self.message_format = message_format

        if specification_url is None:
            specification_url = url
        if protocol is None:
            protocol = urlparse(specification_url).scheme

        connection_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in NON_CONNECTION_PARAMS | SENTINEL_PARAMS
        }
        connection_options = self._resolve_url_options(
            url,
            security=security,
            host=host,
            port=port,
            **connection_kwargs,
        )

        connection_state = RedisSentinelConnectionState(
            connection_options,
            sentinels=list(sentinels),
            master_name=sentinel_master_name,
            sentinel_kwargs=sentinel_kwargs,
        )

        super(RedisBroker, self).__init__(
            **connection_options,
            routers=kwargs.get("routers", ()),
            config=RedisBrokerConfig(
                connection=connection_state,
                producer=RedisFastProducer(
                    connection=connection_state,
                    parser=kwargs.get("parser"),
                    decoder=kwargs.get("decoder"),
                    message_format=self.message_format,
                    serializer=kwargs.get("serializer"),
                ),
                message_format=self.message_format,
                broker_middlewares=kwargs.get("middlewares", ()),
                broker_parser=kwargs.get("parser"),
                broker_decoder=kwargs.get("decoder"),
                broker_codec=kwargs.get("codec"),
                logger=make_redis_logger_state(
                    logger=kwargs.get("logger", EMPTY),
                    log_level=kwargs.get("log_level", logging.INFO),
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=kwargs.get("apply_types", True),
                    serializer=kwargs.get("serializer", EMPTY),
                    provider=kwargs.get("provider") or dependency_provider,
                    context=kwargs.get("context") or ContextRepo(),
                ),
                broker_dependencies=kwargs.get("dependencies", ()),
                graceful_timeout=kwargs.get("graceful_timeout", 15.0),
                ack_policy=kwargs.get("ack_policy", EMPTY),
                extra_context={"broker": self},
            ),
            specification=BrokerSpec(
                description=kwargs.get("description"),
                url=[specification_url],
                protocol=protocol,
                protocol_version=kwargs.get("protocol_version", "custom"),
                security=security,
                tags=kwargs.get("tags", ()),
            ),
        )
