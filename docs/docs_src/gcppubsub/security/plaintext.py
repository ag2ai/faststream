import ssl

from faststream.gcp import GCPBroker
from faststream.security import SASLPlaintext

ssl_context = ssl.create_default_context()
security = SASLPlaintext(
    ssl_context=ssl_context,
    username="admin",
    password="password",
)

broker = GCPBroker(project_id="test-project-id")
