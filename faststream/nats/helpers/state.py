from typing import Protocol, TypeVar

from nats.aio.client import Client
from nats.js import JetStreamContext

from faststream.exceptions import IncorrectState

ClientT = TypeVar("ClientT", Client, JetStreamContext)


class ConnectionState(Protocol[ClientT]):
    connection: ClientT


class EmptyConnectionState(ConnectionState[ClientT]):
    __slots__ = ()

    @property
    def connection(self) -> ClientT:
        raise IncorrectState

    @connection.setter
    def connection(self, v: ClientT) -> None:
        raise IncorrectState


class ConnectedState(ConnectionState[ClientT]):
    __slots__ = ("connection",)

    def __init__(self, connection: ClientT) -> None:
        self.connection: ClientT = connection
