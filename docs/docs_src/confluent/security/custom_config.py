from faststream.confluent import ConfluentConfig, KafkaBroker
from faststream.security import SASLPlaintext

security = SASLPlaintext(
    username="admin",
    password="password",
)

config: ConfluentConfig = {"ssl.ca.location": "~/my_certs/CRT_cacerts.pem"}

broker = KafkaBroker("localhost:9092", security=security, config=config)
