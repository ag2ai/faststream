from typing import TYPE_CHECKING, Protocol

from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from nats.aio.client import Client
    from nats.js import JetStreamContext


class BrokerState(Protocol):
    stream: "JetStreamContext"
    connection: "Client"

    def __bool__(self) -> bool: ...

    def brake(self) -> "BrokerState": ...

    def reconnect(self) -> "BrokerState": ...


class EmptyBrokerState(BrokerState):
    @property
    def connection(self) -> "Client":
        msg = "Connection is not available yet. Please, connect the broker first."
        raise IncorrectState(msg)

    @property
    def stream(self) -> "JetStreamContext":
        msg = "Stream is not available yet. Please, connect the broker first."
        raise IncorrectState(msg)

    def brake(self) -> "BrokerState":
        return self

    def reconnect(self) -> "BrokerState":
        msg = "You can't reconnect an empty state. Please, connect the broker first."
        raise IncorrectState(msg)

    def __bool__(self) -> bool:
        return False


class ConnectedState(BrokerState):
    def __init__(
        self,
        connection: "Client",
        stream: "JetStreamContext",
    ) -> None:
        self.connection = connection
        self.stream = stream

    def __bool__(self) -> bool:
        return True

    def brake(self) -> "ConnectionBrokenState":
        return ConnectionBrokenState(
            connection=self.connection,
            stream=self.stream,
        )


class ConnectionBrokenState(BrokerState):
    def __init__(
        self,
        connection: "Client",
        stream: "JetStreamContext",
    ) -> None:
        self.connection = connection
        self.stream = stream

    def __bool__(self) -> bool:
        return False

    def reconnect(self) -> "ConnectedState":
        return ConnectedState(
            connection=self.connection,
            stream=self.stream,
        )
