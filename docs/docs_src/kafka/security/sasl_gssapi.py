import ssl

from faststream.kafka import KafkaBroker
from faststream.security import SASLGSSAPI

ssl_context = ssl.create_default_context()
security = SASLGSSAPI(ssl_context=ssl_context)

broker = KafkaBroker("localhost:9092", security=security)
