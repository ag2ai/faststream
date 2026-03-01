import asyncio

from aiogram import Bot, Dispatcher
from aiogram.types import Message

from faststream.nats import NatsBroker

bot = Bot("")
dispatcher = Dispatcher()
broker = NatsBroker()

@broker.subscriber("echo")
async def echo_faststream_handler(data: dict[str, str]) -> None:
    await bot.copy_message(**data)


@dispatcher.message()
async def echo_aiogram_handler(event: Message) -> None:
    await broker.publish(
        subject="echo",
        message={
            "chat_id": event.chat.id,
            "message_id": event.message_id,
            "from_chat_id": event.chat.id,
        },
    )


async def main() -> None:
    async with broker:
        await broker.start()
        await dispatcher.start_polling(bot)

asyncio.run(main())
