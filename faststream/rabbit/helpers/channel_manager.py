from typing import TYPE_CHECKING, Dict, Optional, cast

if TYPE_CHECKING:
    import aio_pika

    from faststream.rabbit.schemas import Channel


class ChannelManager:
    __slots__ = ("__channels", "__connection", "__default_channel")

    def __init__(
        self,
        connection: "aio_pika.RobustConnection",
        *,
        default_channel: "Channel",
    ) -> None:
        self.__connection = connection
        self.__default_channel = default_channel
        self.__channels: Dict[Channel, aio_pika.RobustChannel] = {}

    async def get_channel(
        self,
        channel: Optional["Channel"] = None,
    ) -> "aio_pika.RobustChannel":
        """Declare a queue."""
        if channel is None:
            channel = self.__default_channel

        if (ch := self.__channels.get(channel)) is None:
            self.__channels[channel] = ch = cast(
                "aio_pika.RobustChannel",
                await self.__connection.channel(
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
