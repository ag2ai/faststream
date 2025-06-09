from typing import TYPE_CHECKING, Optional

from nats.js.api import KeyValueConfig

from .state import ConnectedState, ConnectionState, EmptyConnectionState

if TYPE_CHECKING:
    from nats.js import JetStreamContext
    from nats.js.api import Placement, RePublish, StorageType
    from nats.js.kv import KeyValue


class KVBucketDeclarer:
    buckets: dict[str, "KeyValue"]

    def __init__(self) -> None:
        self.buckets = {}

        self.__state: ConnectionState[JetStreamContext] = EmptyConnectionState()

    def connect(self, connection: "JetStreamContext") -> None:
        self.__state = ConnectedState(connection)

    def disconnect(self) -> None:
        self.__state = EmptyConnectionState()

    async def create_key_value(
        self,
        bucket: str,
        *,
        description: str | None = None,
        max_value_size: int | None = None,
        history: int = 1,
        ttl: float | None = None,  # in seconds
        max_bytes: int | None = None,
        storage: Optional["StorageType"] = None,
        replicas: int = 1,
        placement: Optional["Placement"] = None,
        republish: Optional["RePublish"] = None,
        direct: bool | None = None,
        # custom
        declare: bool = True,
    ) -> "KeyValue":
        if (key_value := self.buckets.get(bucket)) is None:
            if declare:
                key_value = await self.__state.connection.create_key_value(
                    config=KeyValueConfig(
                        bucket=bucket,
                        description=description,
                        max_value_size=max_value_size,
                        history=history,
                        ttl=ttl,
                        max_bytes=max_bytes,
                        storage=storage,
                        replicas=replicas,
                        placement=placement,
                        republish=republish,
                        direct=direct,
                    ),
                )
            else:
                key_value = await self.__state.connection.key_value(bucket)

            self.buckets[bucket] = key_value

        return key_value
