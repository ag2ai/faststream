from typing import Any

from faststream import FastStream
from faststream._internal.broker.broker import BrokerUsecase
from faststream.specification import AsyncAPI
from faststream.specification.base import Specification


class AsyncAPI300Factory:
    def get_spec(self, *brokers: BrokerUsecase[Any, Any]) -> Specification:
        factory = AsyncAPI(schema_version="3.0.0")
        for broker in brokers:
            factory.add_broker(broker)
        return factory.to_specification()


def get_3_0_0_spec(broker: BrokerUsecase[Any, Any], **kwargs: Any) -> Specification:
    return FastStream(
        broker,
        specification=AsyncAPI(schema_version="3.0.0", **kwargs),
    ).schema.to_specification()


def get_3_0_0_schema(broker: BrokerUsecase[Any, Any], **kwargs: Any) -> Any:
    return get_3_0_0_spec(broker, **kwargs).to_jsonable()
