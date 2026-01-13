from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine
from faststream._internal.configs.broker import BrokerConfig


@dataclass(kw_only=True)
class SqlaBrokerConfig(BrokerConfig):
    engine: AsyncEngine
    validate_schema_on_start: bool = True