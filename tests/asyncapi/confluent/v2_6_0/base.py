from faststream.confluent import KafkaBroker
from faststream.specification.asyncapi import AsyncAPI
from faststream.specification.base.specification import Specification


class AsyncAPI26Mixin:
    def get_schema(self, broker: KafkaBroker) -> Specification:
        return AsyncAPI(broker, schema_version="2.6.0")
