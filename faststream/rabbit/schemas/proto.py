from typing import TYPE_CHECKING, Any, Optional

from faststream.rabbit.schemas.base import RabbitBaseConfigs

if TYPE_CHECKING:
    from faststream.rabbit.schemas.exchange import RabbitExchange
    from faststream.rabbit.schemas.queue import RabbitQueue


class BaseRMQInformation:
    """Base class to store Specification RMQ bindings."""

    virtual_host: str
    queue: "RabbitQueue"
    exchange: "RabbitExchange"
    app_id: Optional[str]

    def __init__(self, *, rmq_base_configs: RabbitBaseConfigs) -> None:
        self.queue = rmq_base_configs.queue
        self.exchange = rmq_base_configs.exchange

        # Setup it later
        self.app_id = None
        self.virtual_host = ""

    def _setup(
        self,
        *,
        app_id: Optional[str],
        virtual_host: str,
        **kwargs: Any,
    ) -> None:
        self.app_id = app_id
        self.virtual_host = virtual_host

        # Setup next parent class
        super()._setup(**kwargs)  # type: ignore[misc]
