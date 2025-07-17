from faststream.confluent import KafkaBroker
from faststream.security import SASLScram512

security = SASLScram512(
    username="admin",
    password="password",
    use_ssl=True,
)

broker = KafkaBroker("localhost:9092", security=security)
