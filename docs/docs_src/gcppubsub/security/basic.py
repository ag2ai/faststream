import ssl

from faststream.gcp import GCPBroker
from faststream.security import BaseSecurity

ssl_context = ssl.create_default_context()
security = BaseSecurity(ssl_context=ssl_context)

broker = GCPBroker(project_id="test-project-id")
