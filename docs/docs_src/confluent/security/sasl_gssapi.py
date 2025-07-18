import ssl

from faststream.confluent import KafkaBroker
from faststream.security import SASLGSSAPI

security = SASLGSSAPI(use_ssl=True)

broker = KafkaBroker("localhost:9092", security=security)
