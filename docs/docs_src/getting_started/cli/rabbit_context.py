from faststream import FastStream, ContextRepo
from faststream.rabbit import RabbitBroker
from pydantic_settings import BaseSettings

broker = RabbitBroker()

app = FastStream(broker)

class Settings(BaseSettings):
    any_flag: bool

@app.on_startup
async def setup(context: ContextRepo, env: str = ".env"):
    settings = Settings(_env_file=env)
    context.set_global("settings", settings)
