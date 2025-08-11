from litestar import Litestar, get
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")

@broker.subscriber("queue")
async def handle(msg):
    print(msg)

@get("/")
async def index() -> str:
    return "Hello, world!"

app = Litestar(
    [index],
    on_startup=[broker.start],
    on_shutdown=[broker.stop],
)
