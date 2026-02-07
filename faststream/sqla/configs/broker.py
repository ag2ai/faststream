from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncEngine

from faststream._internal.configs.broker import BrokerConfig
from faststream._internal.producer import ProducerUnset
from faststream.sqla.client import SqlaBaseClient, create_sqla_client
from faststream.sqla.publisher.producer import SqlaProducer


@dataclass(kw_only=True)
class SqlaBrokerConfig(BrokerConfig):
    producer: "SqlaProducer" = field(default_factory=ProducerUnset)
    validate_schema_on_start: bool = True
    client: SqlaBaseClient = field(init=False)

    async def connect(self, *, engine: AsyncEngine) -> None:
        self.producer.connect(
            connection=None,
            serializer=self.fd_config._serializer,
        )
        self.client = create_sqla_client(engine)
        if self.validate_schema_on_start:
            await self.client.validate_schema()
