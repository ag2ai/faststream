from faststream.mq import MQBroker
from faststream.security import SASLPlaintext

security = SASLPlaintext(username="app", password="password", use_ssl=False)

broker = MQBroker(
    queue_manager="QM1",
    conn_name="localhost(1414)",
    security=security,
)
