from typing import Any

from faststream import FastStream
from faststream._internal.broker.broker import BrokerUsecase
from faststream.specification import AsyncAPI
from faststream.specification.base import Specification


class AsyncAPI260Factory:
    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        factory = AsyncAPI(schema_version="2.6.0")
        factory.add_broker(broker)
        return factory.to_specification()


def get_2_6_0_spec(
        broker: BrokerUsecase[Any, Any], **kwargs: Any
) -> Specification:
    return (
        FastStream(broker, specification=AsyncAPI(
            schema_version="2.6.0", **kwargs
        )).schema.to_specification()
    )


def get_2_6_0_schema(broker: BrokerUsecase[Any, Any], **kwargs: Any) -> Any:
    return get_2_6_0_spec(broker, **kwargs).to_jsonable()
