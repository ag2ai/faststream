from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitQueue#, RabbitRouter
from faststream._internal.configs.settings import SettingsContainer, Settings

q = RabbitQueue("test")

settings = SettingsContainer(q1=q)
broker = RabbitBroker(settings=settings)
# router = RabbitRouter()
# broker.include_router(router)


app = FastStream(broker)

@broker.subscriber(queue=Settings("q1"))
async def base_handler(body: str):
    print(body)


@app.on_startup
async def pub():
    await broker.connect()
    pub_ = broker.publisher(queue=Settings("q1"))
    await pub_.publish("hi")
