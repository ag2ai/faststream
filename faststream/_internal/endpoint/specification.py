from typing import Generic

from typing_extensions import TypeVar as TypeVar313

from faststream._internal.configs import BrokerConfig, SubscriberSpecificationConfig
from faststream._internal.endpoint.derived import DerivedReads

T_SpecificationConfig = TypeVar313(
    "T_SpecificationConfig",
    bound=SubscriberSpecificationConfig,
    default=SubscriberSpecificationConfig,
)
T_BrokerConfig = TypeVar313("T_BrokerConfig", bound=BrokerConfig, default=BrokerConfig)


class EndpointSpecification(Generic[T_BrokerConfig, T_SpecificationConfig]):
    """What a Subscriber's and a Publisher's Specification have in common.

    Both document their endpoint by reading the composition the endpoint itself
    reads, so both keep reads from it, and both forget them at the same moment.
    """

    def __init__(
        self,
        _outer_config: "T_BrokerConfig",
        specification_config: "T_SpecificationConfig",
    ) -> None:
        self.config = specification_config
        self._outer_config = _outer_config
        self._derived = DerivedReads()

    def invalidate(self) -> None:
        """Forget every read this Specification kept from the composition.

        Schema generation prepares the Brokers it renders, so a Specification
        memoises on the same terms an endpoint does and is undone with it.
        """
        self._derived.reset()

    @property
    def include_in_schema(self) -> bool:
        return bool(
            self._outer_config.include_in_schema and self.config.include_in_schema,
        )
