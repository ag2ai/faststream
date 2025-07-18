import ssl

from faststream.redis import RedisBroker
from faststream.security import SASLPlaintext

ssl_context = ssl.create_default_context()
security = SASLPlaintext(
    ssl_context=ssl_context,
    username="admin",
    password="password",
)

broker = RedisBroker(security=security)
