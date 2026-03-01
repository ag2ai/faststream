import ssl

from faststream.rabbit import RabbitBroker
from faststream.security import BaseSecurity

ssl_context = ssl.create_default_context()
security = BaseSecurity(ssl_context=ssl_context)

broker = RabbitBroker(security=security)
