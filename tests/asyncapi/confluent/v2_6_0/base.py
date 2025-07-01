from faststream.confluent import KafkaBroker
from faststream.specification.base.specification import Specification
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_spec


class AsyncAPI26Mixin:
    def get_schema(self, broker: KafkaBroker) -> Specification:
        return get_2_6_0_spec(broker)
