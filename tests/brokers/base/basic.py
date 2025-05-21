from abc import abstractmethod
from typing import Any

from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.broker.router import BrokerRouter


class BaseTestcaseConfig:
    timeout: float = 3.0

    @abstractmethod
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> BrokerUsecase[Any, Any]:
        raise NotImplementedError

    def patch_broker(
        self,
        broker: BrokerUsecase,
        **kwargs: Any,
    ) -> BrokerUsecase:
        return broker

    def get_subscriber_params(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[
        tuple[Any, ...],
        dict[str, Any],
    ]:
        return args, kwargs

    @abstractmethod
    def get_router(self, **kwargs: Any) -> BrokerRouter:
        raise NotImplementedError
