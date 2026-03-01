from faststream.confluent import KafkaBroker
from faststream.security import SASLOAuthBearer

security = SASLOAuthBearer(
    use_ssl=True,
)

broker = KafkaBroker("localhost:9092", security=security)
