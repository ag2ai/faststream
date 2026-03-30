from faststream.mq import MQBroker
from faststream.specification.base.specification import Specification
from tests.asyncapi.base.v3_0_0 import get_3_0_0_spec


class AsyncAPI300Mixin:
    def get_schema(self, broker: MQBroker) -> Specification:
        return get_3_0_0_spec(broker)
