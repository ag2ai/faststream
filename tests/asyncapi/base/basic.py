from typing import Any

from faststream.specification.base import Specification


class SpecificationFactory:
    def get_spec(self, *brokers: Any) -> Specification:
        raise NotImplementedError
