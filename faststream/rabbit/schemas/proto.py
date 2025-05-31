from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from faststream.rabbit.configs.specification import RabbitSpecificationConfig

    from .exchange import RabbitExchange
    from .queue import RabbitQueue


class BaseRMQInformation:
    """Base class to store Specification RMQ bindings."""

    queue: "RabbitQueue"
    exchange: "RabbitExchange"

    def __init__(self, config: "RabbitSpecificationConfig", /, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

        self.queue = config.queue
        self.exchange = config.exchange

        self._outer_config = config.config

    @property
    def virtual_host(self) -> str:
        return self._outer_config.virtual_host

    @property
    def app_id(self) -> Optional[str]:
        return self._outer_config.app_id
