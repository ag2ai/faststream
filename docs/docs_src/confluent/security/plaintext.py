from faststream.confluent import KafkaBroker
from faststream.security import SASLPlaintext

security = SASLPlaintext(
    username="admin",
    password="password",
    use_ssl=True,
)

broker = KafkaBroker("localhost:9092", security=security)
