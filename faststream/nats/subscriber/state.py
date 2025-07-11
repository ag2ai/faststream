from typing import TYPE_CHECKING, Protocol

from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from nats.aio.client import Client
    from nats.js import JetStreamContext

    from faststream.nats.broker.state import BrokerState
    from faststream.nats.helpers import KVBucketDeclarer, OSBucketDeclarer


class SubscriberState(Protocol):
    @property
    def client(self) -> "Client": ...

    @property
    def js(self) -> "JetStreamContext": ...

    kv_declarer: "KVBucketDeclarer"
    os_declarer: "OSBucketDeclarer"


class EmptySubscriberState(SubscriberState):
    @property
    def client(self) -> "Client":
        msg = "Connection is not available yet. Please, setup the subscriber first."
        raise IncorrectState(msg)

    @property
    def js(self) -> "JetStreamContext":
        msg = "Stream is not available yet. Please, setup the subscriber first."
        raise IncorrectState(msg)

    @property
    def kv_declarer(self) -> "KVBucketDeclarer":
        msg = "KeyValue is not available yet. Please, setup the subscriber first."
        raise IncorrectState(msg)

    @kv_declarer.setter
    def kv_declarer(self, v: "KVBucketDeclarer") -> None:
        msg = "Connection is not available yet. Please, setup the subscriber first."
        raise IncorrectState(msg)

    @property
    def os_declarer(self) -> "OSBucketDeclarer":
        msg = "ObjectStorage is not available yet. Please, setup the subscriber first."
        raise IncorrectState(msg)

    @os_declarer.setter
    def os_declarer(self, v: "OSBucketDeclarer") -> None:
        msg = "Connection is not available yet. Please, setup the subscriber first."
        raise IncorrectState(msg)


class ConnectedSubscriberState(SubscriberState):
    def __init__(
        self,
        *,
        parent_state: "BrokerState",
        kv_declarer: "KVBucketDeclarer",
        os_declarer: "OSBucketDeclarer",
    ) -> None:
        self._parent_state = parent_state
        self.kv_declarer = kv_declarer
        self.os_declarer = os_declarer

    @property
    def client(self) -> "Client":
        return self._parent_state.connection

    @property
    def js(self) -> "JetStreamContext":
        return self._parent_state.stream
