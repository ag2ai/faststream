from typing import Any

from faststream._internal.broker.broker import BrokerUsecase
from faststream.specification.base import Specification


class SpecificationFactory:
    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        raise NotImplementedError
