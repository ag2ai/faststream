from faststream.mq import MQBroker
from faststream.specification.base.specification import Specification
from tests.asyncapi.base.v2_6_0 import get_2_6_0_spec


class AsyncAPI260Mixin:
    def get_schema(self, broker: MQBroker) -> Specification:
        return get_2_6_0_spec(broker)
