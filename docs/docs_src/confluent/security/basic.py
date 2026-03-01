from faststream.confluent import KafkaBroker
from faststream.security import BaseSecurity

security = BaseSecurity(use_ssl=True)

broker = KafkaBroker("localhost:9092", security=security)
