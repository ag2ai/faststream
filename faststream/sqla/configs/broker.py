from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine
from faststream._internal.configs.broker import BrokerConfig
from faststream.sqla.client import create_sqla_client


@dataclass(kw_only=True)
class SqlaBrokerConfig(BrokerConfig):
    engine: AsyncEngine
    validate_schema_on_start: bool = True

    async def connect(self) -> None:
        self.producer.connect(
            connection=None,
            serializer=self.fd_config._serializer,
        )
        if self.validate_schema_on_start:
            client = create_sqla_client(self.engine)
            await client.validate_schema()