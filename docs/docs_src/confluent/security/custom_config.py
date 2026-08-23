from faststream.confluent import KafkaBroker
from faststream.security import SASLPlaintext

security = SASLPlaintext(
    username="admin",
    password="password",
)

config = {"ssl.ca.location": "~/my_certs/CRT_cacerts.pem"}

broker = KafkaBroker("localhost:9092", security=security, config=config)
