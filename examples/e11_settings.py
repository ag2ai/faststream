import os

try:
    # pydantic V1
    from pydantic import BaseSettings
except ImportError:
    # pydantic V2
    from pydantic_settings import BaseSettings

from faststream import FastStream, Logger
from faststream.rabbit import RabbitBroker


class Settings(BaseSettings):
    url: str = "amqp://guest:guest@localhost:5672/"
    queue: str = "test_q"


settings = Settings(_env_file=os.getenv("ENV", ".env"))

broker = RabbitBroker(settings.url)
app = FastStream(broker)


@broker.subscriber(settings.queue)
async def handle(msg: str, logger: Logger) -> None:
    logger.info(msg)


@app.after_startup
async def test() -> None:
    await broker.publish("Hello!", settings.queue)


# ENV=.prod.env faststream run serve:app
# ENV=.test.env pytest
