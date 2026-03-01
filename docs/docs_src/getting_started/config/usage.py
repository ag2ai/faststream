import os

from pydantic_settings import BaseSettings

from faststream import FastStream
from faststream.rabbit import RabbitBroker


class Settings(BaseSettings):
    url: str
    queue: str = "test-queue"


settings = Settings(_env_file=os.getenv("ENV", ".env"))

broker = RabbitBroker(settings.url)
app = FastStream(broker)


@broker.subscriber(settings.queue)
async def handler(msg):
    ...
