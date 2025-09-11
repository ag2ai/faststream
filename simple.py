from faststream import FastStream
from faststream._internal.configs.settings import Settings, SettingsContainer
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

q = RabbitQueue("test", routing_key="rk")
ex = RabbitExchange("tt")

settings = SettingsContainer(q1=q, ex=ex, rk="rk")
broker = RabbitBroker(settings=settings)
pub_ = broker.publisher(queue=Settings("q1"))#, exchange=Settings("ex"))
app = FastStream(broker)


@broker.subscriber(queue=Settings("q1"))#, exchange=Settings("ex"))
async def base_handler(body: str) -> None:
    print(body)

@app.on_startup
async def pub() -> None:
    await broker.start()
    await pub_.publish("MAZAFAKA")
