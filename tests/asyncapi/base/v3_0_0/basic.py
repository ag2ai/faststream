from typing import Any

from faststream._internal.broker.broker import BrokerUsecase
from faststream.specification import AsyncAPI
from faststream.specification.base import Specification


class AsyncAPI300Factory:
    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        factory = AsyncAPI(schema_version="3.0.0")
        factory.add_broker(broker)
        return factory.to_specification()
