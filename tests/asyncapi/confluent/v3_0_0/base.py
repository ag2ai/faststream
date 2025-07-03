from faststream.confluent import KafkaBroker
from faststream.specification.base.specification import Specification
from tests.asyncapi.base.v3_0_0 import get_3_0_0_spec


class AsyncAPI30Mixin:
    def get_schema(self, broker: KafkaBroker) -> Specification:
        return get_3_0_0_spec(broker)
