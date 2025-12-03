from dataclasses import dataclass
from faststream._internal.configs.broker import BrokerConfig


@dataclass(kw_only=True)
class SqlaBrokerConfig(BrokerConfig):
    ...