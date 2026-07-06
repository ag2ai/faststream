from typing import TYPE_CHECKING, Any, ClassVar

from typing_extensions import Unpack

from faststream.exceptions import SetupError
from faststream.redis.broker import RedisBroker
from faststream.redis.configs.state import RedisSentinelConnectionState
from faststream.redis.schemas.types import SENTINEL_PARAMS

if TYPE_CHECKING:
    from faststream.redis.configs.state import ConnectionState
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

    _EXTRA_NON_CONNECTION_PARAMS: ClassVar[frozenset[str]] = SENTINEL_PARAMS

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        **kwargs: Unpack["RedisSentinelParams"],
    ) -> None:
        self._init_broker(url, dict(kwargs))

    def _validate_init_params(self, kwargs: dict[str, Any]) -> None:
        if not kwargs.get("sentinels"):
            msg = "`sentinels` is required for RedisSentinelBroker."
            raise SetupError(msg)
        if not kwargs.get("sentinel_master_name"):
            msg = "`sentinel_master_name` is required for RedisSentinelBroker."
            raise SetupError(msg)

    def _make_connection_state(
        self,
        connection_options: dict[str, Any],
        kwargs: dict[str, Any],
    ) -> "ConnectionState[Any]":
        return RedisSentinelConnectionState(
            connection_options,
            sentinels=list(kwargs["sentinels"]),
            master_name=kwargs["sentinel_master_name"],
            sentinel_kwargs=kwargs.get("sentinel_kwargs"),
        )
