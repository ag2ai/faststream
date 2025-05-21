from faststream.confluent import KafkaBroker
from faststream.specification.asyncapi import AsyncAPI
from faststream.specification.base.specification import Specification


class AsyncAPI30Mixin:
    def get_schema(self, broker: KafkaBroker) -> Specification:
        return AsyncAPI(broker, schema_version="3.0.0")
