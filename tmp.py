from typing import Any, Awaitable, Callable

from faststream import BaseMiddleware, FastStream, Logger, PublishCommand
from faststream.redis import RedisBroker
from faststream.redis.schemas.list_sub import ListSub


class MyMiddleware(BaseMiddleware):
    async def publish_scope(
        self,
        call_next: Callable[[PublishCommand], Awaitable[Any]],
        cmd: PublishCommand,
    ) -> Any:
        print("PUBLISH_SCOPE", cmd.body)
        return await super().publish_scope(call_next, cmd)


broker = RedisBroker(middlewares=[MyMiddleware])
app = FastStream(broker)


@app.after_startup
async def t() -> None:
    await broker.publish_batch("kick1", "kick2", "kick3", list="input-queue")


@broker.subscriber(list=ListSub("input-queue", batch=True, max_records=5))
@broker.publisher("output-queue")
async def handle_message(msg: list[str], logger: Logger) -> str:
    logger.info(f"Получено сообщение: {msg}")
    return f"Обработано: {[m.upper() for m in msg]}"


@broker.subscriber("output-queue")
async def check_result(msg: str, logger: Logger) -> None:
    logger.info(f"Промежуточный результат: {msg}")
