from typing import TYPE_CHECKING, Optional

from nats.js.api import ObjectStoreConfig

from .state import ConnectedState, ConnectionState, EmptyConnectionState

if TYPE_CHECKING:
    from nats.js import JetStreamContext
    from nats.js.api import Placement, StorageType
    from nats.js.object_store import ObjectStore


class OSBucketDeclarer:
    buckets: dict[str, "ObjectStore"]

    def __init__(self) -> None:
        self.buckets = {}

        self.__state: ConnectionState[JetStreamContext] = EmptyConnectionState()

    def connect(self, connection: "JetStreamContext") -> None:
        self.__state = ConnectedState(connection)

    def disconnect(self) -> None:
        self.__state = EmptyConnectionState()

    async def create_object_store(
        self,
        bucket: str,
        *,
        description: str | None = None,
        ttl: float | None = None,
        max_bytes: int | None = None,
        storage: Optional["StorageType"] = None,
        replicas: int = 1,
        placement: Optional["Placement"] = None,
        # custom
        declare: bool = True,
    ) -> "ObjectStore":
        if (object_store := self.buckets.get(bucket)) is None:
            if declare:
                object_store = await self.__state.connection.create_object_store(
                    bucket=bucket,
                    config=ObjectStoreConfig(
                        bucket=bucket,
                        description=description,
                        ttl=ttl,
                        max_bytes=max_bytes,
                        storage=storage,
                        replicas=replicas,
                        placement=placement,
                    ),
                )
            else:
                object_store = await self.__state.connection.object_store(bucket)

            self.buckets[bucket] = object_store

        return object_store
