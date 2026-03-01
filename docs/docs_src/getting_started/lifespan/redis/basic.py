from pydantic_settings import BaseSettings

from faststream import ContextRepo, FastStream
from faststream.redis import RedisBroker

broker = RedisBroker()
app = FastStream(broker)


class Settings(BaseSettings):
    host: str = "redis://localhost:6379"


@app.on_startup
async def setup(context: ContextRepo, env: str = ".env"):
    settings = Settings(_env_file=env)
    context.set_global("settings", settings)
    await broker.connect(settings.host)
