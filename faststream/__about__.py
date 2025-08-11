"""Simple and fast framework to create message brokers based microservices."""

from importlib.metadata import version

__version__ = version("faststream")

SERVICE_NAME = f"faststream-{__version__}"
