from faststream.rabbit import RabbitBroker
from faststream.security import SASLPlaintext

broker = RabbitBroker(
    host="rabbit.internal",
    port=5673,
    virtualhost="my_vhost",
    security=SASLPlaintext(username="app", password="secret"),
)
