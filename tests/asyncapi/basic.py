
from faststream import FastStream
from faststream.asgi import make_ping_asgi
from faststream.kafka import KafkaBroker

broker = KafkaBroker()
app = FastStream(broker).as_asgi(
    asgi_routes=[
        ("/ping", make_ping_asgi(broker))
    ]
)
