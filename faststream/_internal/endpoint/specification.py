from typing import Generic

from typing_extensions import TypeVar as TypeVar313

from faststream._internal.configs import BrokerConfig, SubscriberSpecificationConfig

T_SpecificationConfig = TypeVar313(
    "T_SpecificationConfig",
    bound=SubscriberSpecificationConfig,
    default=SubscriberSpecificationConfig,
)
T_BrokerConfig = TypeVar313("T_BrokerConfig", bound=BrokerConfig, default=BrokerConfig)


class EndpointSpecification(Generic[T_BrokerConfig, T_SpecificationConfig]):
    """What a Subscriber's and a Publisher's Specification have in common.

    A Specification documents its endpoint by reading the composition, and every
    such read is taken per render rather than kept, so there is nothing here to
    forget when Preparation is undone.
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
