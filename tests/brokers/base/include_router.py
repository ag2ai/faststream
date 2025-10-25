from typing import Any

import pytest

from faststream._internal.broker import BrokerRouter, BrokerUsecase

from .basic import BaseTestcaseConfig


class IncludeTestcase(BaseTestcaseConfig):
    def get_object(self, router: BrokerRouter[Any] | BrokerUsecase[Any, Any]) -> Any:
        raise NotImplementedError

    def test_broker_middlewares(self) -> None:
        broker = self.get_broker(middlewares=(1,))

        obj = self.get_object(broker)

        broker_middlewars = obj._outer_config.broker_middlewares
        assert tuple(broker_middlewars) == (1,), broker_middlewars

    def test_router_middlewares(self) -> None:
        broker = self.get_broker(middlewares=(1,))

        router = self.get_router(middlewares=[2])

        obj = self.get_object(router)

        broker.include_router(router)

        broker_middlewars = obj._outer_config.broker_middlewares
        assert tuple(broker_middlewars) == (1, 2), broker_middlewars

    def test_nested_router_middleware(self) -> None:
        broker = self.get_broker(middlewares=(1,))

        router = self.get_router(middlewares=[2])

        router2 = self.get_router(middlewares=[3])

        obj = self.get_object(router2)

        router.include_router(router2)
        broker.include_router(router)

        broker_middlewars = obj._outer_config.broker_middlewares
        assert tuple(broker_middlewars) == (1, 2, 3), broker_middlewars

    def test_include_router_with_middlewares(self) -> None:
        broker = self.get_broker(middlewares=(1,))

        router = self.get_router(middlewares=[3])

        obj = self.get_object(router)

        broker.include_router(router, middlewares=[2])

        broker_middlewars = obj._outer_config.broker_middlewares
        assert tuple(broker_middlewars) == (1, 2, 3), broker_middlewars

    @pytest.mark.parametrize(
        ("include_router", "include", "result"),
        (
            pytest.param(False, True, False, id="visible router"),
            pytest.param(True, True, True, id="invisible include"),
            pytest.param(None, True, True, id="default router"),
            pytest.param(False, False, False, id="ignore visible router"),
            pytest.param(
                True,
                False,
                False,
                id="ignore invisible router",
            ),
            pytest.param(None, False, False, id="ignore default router"),
        ),
    )
    def test_router_include_in_schema(
        self,
        include_router: bool | None,
        include: bool,
        result: bool,
    ) -> None:
        broker = self.get_broker()
        router = self.get_router(include_in_schema=include_router)

        obj = self.get_object(router)
        broker.include_router(router, include_in_schema=include)

        assert obj.specification.include_in_schema is result


class IncludeSubscriberTestcase(IncludeTestcase):
    def get_object(self, router: BrokerRouter[Any] | BrokerUsecase[Any, Any]) -> Any:
        return router.subscriber("test")

    def test_graceful_timeout(self) -> None:
        broker = self.get_broker(graceful_timeout=10)
        router = self.get_router()
        router2 = self.get_router()

        obj = self.get_object(router2)

        router.include_router(router2)
        broker.include_router(router)

        assert obj._outer_config.graceful_timeout == 10

    def test_simple_router_prefix(self) -> None:
        broker = self.get_broker()

        router = self.get_router(prefix="1.")
        obj = self.get_object(router)

        broker.include_router(router)

        assert obj._outer_config.prefix == "1."

    def test_nested_router_prefix(self) -> None:
        broker = self.get_broker()

        router = self.get_router(prefix="1.")

        router2 = self.get_router(prefix="2.")
        obj = self.get_object(router2)

        router.include_router(router2)
        broker.include_router(router)

        assert obj._outer_config.prefix == "1.2."

    def test_complex_router_prefix(self) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="1.")

        router2 = self.get_router()
        sub2 = self.get_object(router2)

        router3 = self.get_router(prefix="5.")
        sub3 = self.get_object(router3)

        router2.include_router(router3, prefix="4.")
        router.include_router(router2)
        broker.include_router(router)

        assert sub2._outer_config.prefix == "1."
        assert sub3._outer_config.prefix == "1.4.5."

    def test_idempotent_include_twice_on_same_broker(self) -> None:
        router = self.get_router()
        broker = self.get_broker()

        broker.include_router(router)
        broker.include_router(router)
        assert len(router.config.configs) == 2
        assert router.parent is broker

    def test_reregister_on_include_in_different_brokers(self) -> None:
        router = self.get_router()
        broker1 = self.get_broker()
        broker2 = self.get_broker()

        broker1.include_router(router)
        broker2.include_router(router)

        assert len(router.config.configs) == 2
        assert router.parent is broker2
        assert router not in broker1.routers


class IncludePublisherTestcase(IncludeTestcase):
    def get_object(self, router: BrokerRouter[Any] | BrokerUsecase[Any, Any]) -> Any:
        return router.publisher("test")
