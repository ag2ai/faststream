import ssl

from faststream.kafka import KafkaBroker
from faststream.security import SASLScram512

ssl_context = ssl.create_default_context()
security = SASLScram512(
    ssl_context=ssl_context,
    username="admin",
    password="password",
)

broker = KafkaBroker("localhost:9092", security=security)
