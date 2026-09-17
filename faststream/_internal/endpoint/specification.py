from typing import Generic

from typing_extensions import TypeVar as TypeVar313

from faststream._internal.configs import BrokerConfig
from faststream._internal.configs.specification import SpecificationConfig

T_SpecificationConfig = TypeVar313(
    "T_SpecificationConfig",
    bound=SpecificationConfig,
    default=SpecificationConfig,
)
T_BrokerConfig = TypeVar313("T_BrokerConfig", bound=BrokerConfig, default=BrokerConfig)


class EndpointSpecification(Generic[T_BrokerConfig, T_SpecificationConfig]):
    """What a Subscriber's and a Publisher's Specification have in common.

    Both document an endpoint from the same two configs: the composition the
    endpoint reads through and the endpoint's own, and one rule decides whether
    the document lists it at all.
    """

    def __init__(
        self,
        _outer_config: "T_BrokerConfig",
        specification_config: "T_SpecificationConfig",
    ) -> None:
        self.config = specification_config
        self._outer_config = _outer_config

    @property
    def include_in_schema(self) -> bool:
        return bool(
            self._outer_config.include_in_schema and self.config.include_in_schema,
        )
