import ssl

from faststream.redis import RedisBroker
from faststream.security import BaseSecurity

ssl_context = ssl.create_default_context()
security = BaseSecurity(ssl_context=ssl_context)

broker = RedisBroker(security=security)
