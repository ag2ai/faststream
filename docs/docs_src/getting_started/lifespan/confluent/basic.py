from pydantic_settings import BaseSettings

from faststream import ContextRepo, FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker()
app = FastStream(broker)


class Settings(BaseSettings):
    host: str = "localhost:9092"


@app.on_startup
async def setup(context: ContextRepo, env: str = ".env"):
    settings = Settings(_env_file=env)
    context.set_global("settings", settings)
    await broker.connect(settings.host)
