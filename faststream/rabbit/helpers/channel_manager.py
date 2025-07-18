from typing import TYPE_CHECKING, Optional, Protocol, cast

from faststream.rabbit.schemas import Channel

from .state import ConnectedState, ConnectionState, EmptyConnectionState

if TYPE_CHECKING:
    import aio_pika


class ChannelManager(Protocol):
    def connect(self, connection: "aio_pika.RobustConnection") -> None: ...

    def disconnect(self) -> None: ...

    async def get_channel(
        self,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustChannel":
        """Declare a channel."""
        ...


class FakeChannelManager(ChannelManager):
    def connect(self, connection: "aio_pika.RobustConnection") -> None:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError

    async def get_channel(
        self,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustChannel":
        raise NotImplementedError


class ChannelManagerImpl(ChannelManager):
    __slots__ = ("__channels", "__connection", "__default_channel")

    def __init__(
        self,
        default_channel: Optional["Channel"] = None,
    ) -> None:
        self.__connection: ConnectionState = EmptyConnectionState()

        self.__default_channel = default_channel or Channel()

        self.__channels: dict[Channel, aio_pika.RobustChannel] = {}

    def connect(self, connection: "aio_pika.RobustConnection") -> None:
        self.__connection = ConnectedState(connection)

    def disconnect(self) -> None:
        self.__connection = EmptyConnectionState()
        self.__channels.clear()

    async def get_channel(
        self,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustChannel":
        if channel is None:
            channel = self.__default_channel

        if (ch := self.__channels.get(channel)) is None:
            self.__channels[channel] = ch = cast(
                "aio_pika.RobustChannel",
                await self.__connection.connection.channel(
                    channel_number=channel.channel_number,
                    publisher_confirms=channel.publisher_confirms,
                    on_return_raises=channel.on_return_raises,
                ),
            )

            if channel.prefetch_count:
                await ch.set_qos(
                    prefetch_count=channel.prefetch_count,
                    global_=channel.global_qos,
                )

        return ch
