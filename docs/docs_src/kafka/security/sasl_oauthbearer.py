import ssl

from faststream.kafka import KafkaBroker
from faststream.security import SASLOAuthBearer

ssl_context = ssl.create_default_context()
security = SASLOAuthBearer(
    use_ssl=True,
    ssl_context=ssl_context,
)

broker = KafkaBroker(
    "localhost:9092",
    security=security,
    sasl_oauth_token_provider=...,
)
