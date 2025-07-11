from typing import TYPE_CHECKING

from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from nats.aio.client import Client
    from nats.js import JetStreamContext


class BrokerState:
    def __init__(self) -> None:
        self._connected = False

        self._stream: JetStreamContext | None = None
        self._connection: Client | None = None

    @property
    def connection(self) -> "Client":
        if not self._connection:
            msg = "Connection is not available yet. Please, connect the broker first."
            raise IncorrectState(msg)
        return self._connection

    @property
    def stream(self) -> "JetStreamContext":
        if not self._stream:
            msg = "Stream is not available yet. Please, connect the broker first."
            raise IncorrectState(msg)
        return self._stream

    def __bool__(self) -> bool:
        return self._connected

    def connect(self, connection: "Client", stream: "JetStreamContext") -> None:
        self._connection = connection
        self._stream = stream
        self._connected = True

    def disconnect(self) -> None:
        self._connection = None
        self._stream = None
        self._connected = False
