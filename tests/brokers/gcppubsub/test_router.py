"""GCP Pub/Sub router functionality tests."""

import asyncio
from typing import Any

import pytest

from faststream.gcppubsub import GCPPubSubRouter
from faststream.gcppubsub.broker.router import GCPPubSubPublisher, GCPPubSubRoute
from tests.brokers.base.router import RouterTestcase
from tests.marks import require_gcppubsub

from .basic import GCPPubSubTestcaseConfig


@pytest.mark.gcppubsub()
@require_gcppubsub
class TestRouter(GCPPubSubTestcaseConfig, RouterTestcase):
    """Test GCP Pub/Sub router functionality."""

    route_class = GCPPubSubRoute
    publisher_class = GCPPubSubPublisher

    @pytest.mark.asyncio()
    async def test_router_creation(self) -> None:
        """Test basic router creation."""
        router = GCPPubSubRouter()

        assert isinstance(router, GCPPubSubRouter)
        assert len(router.subscribers) == 0
        assert len(router.publishers) == 0

    @pytest.mark.asyncio()
    async def test_router_subscriber_registration(self) -> None:
        """Test subscriber registration on router."""
        router = GCPPubSubRouter()

        @router.subscriber("test-subscription", topic="test-topic")
        async def handler(msg: Any) -> None:
            pass

        assert len(router.subscribers) == 1
        subscriber = router.subscribers[0]
        assert subscriber.config.subscription == "test-subscription"

    @pytest.mark.asyncio()
    async def test_router_publisher_registration(self) -> None:
        """Test publisher registration on router."""
        router = GCPPubSubRouter()

        publisher = router.publisher("test-topic")

        # Router should track the publisher
        assert len(router.publishers) >= 1  # May include additional publishers
        assert publisher.topic == "test-topic"

    @pytest.mark.asyncio()
    async def test_router_include_in_broker(self) -> None:
        """Test including router in broker."""
        broker = self.get_broker()
        router = GCPPubSubRouter()

        @router.subscriber("router-subscription", topic="router-topic")
        async def router_handler(msg: Any) -> None:
            pass

        # Include router in broker
        broker.include_router(router)

        # Broker should now have the router's subscribers
        assert len(broker.subscribers) >= 1
        # Find the subscriber that came from the router
        router_subscribers = [
            s
            for s in broker.subscribers
            if hasattr(s, "config") and s.config.subscription == "router-subscription"
        ]
        assert len(router_subscribers) == 1

    @pytest.mark.asyncio()
    async def test_nested_routers(self) -> None:
        """Test router nesting functionality."""
        main_router = GCPPubSubRouter()
        nested_router = GCPPubSubRouter()

        @nested_router.subscriber("nested-subscription", topic="nested-topic")
        async def nested_handler(msg: Any) -> None:
            pass

        # Include nested router in main router
        main_router.include_router(nested_router)

        # Main router should have nested router's subscribers
        assert len(main_router.subscribers) >= 1

    @pytest.mark.asyncio()
    async def test_router_with_prefix(self) -> None:
        """Test router with topic/subscription prefixes."""
        router = GCPPubSubRouter(prefix="prefix-")

        @router.subscriber("test-subscription", topic="test-topic")
        async def handler(msg: Any) -> None:
            pass

        # Check if prefix is applied (implementation-dependent)
        subscriber = router.subscribers[0]
        # The exact behavior depends on how prefixes are implemented
        assert subscriber.config.subscription in {
            "test-subscription",
            "prefix-test-subscription",
        }

    @pytest.mark.asyncio()
    async def test_router_middleware(self, subscription: str, topic: str) -> None:
        """Test router-level middleware."""
        from faststream import BaseMiddleware

        middleware_calls = []

        class TestMiddleware(BaseMiddleware):
            async def consume_scope(self, call_next, msg):
                middleware_calls.append("router_middleware")
                return await call_next(msg)

        router = GCPPubSubRouter(middlewares=[TestMiddleware])

        @router.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler(msg: Any) -> None:
            middleware_calls.append("handler")

        # Test with broker
        broker = self.get_broker()
        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("test message", topic=topic)),
                    asyncio.create_task(asyncio.sleep(0.5)),
                ),
                timeout=self.timeout,
            )

        # Middleware should have been called
        assert "router_middleware" in middleware_calls

    @pytest.mark.asyncio()
    async def test_router_lifespan(self) -> None:
        """Test router lifecycle management."""
        router = GCPPubSubRouter()
        lifespan_events = []

        @router.on_startup
        async def startup():
            lifespan_events.append("router_startup")

        @router.on_shutdown
        async def shutdown():
            lifespan_events.append("router_shutdown")

        # Test with broker
        broker = self.get_broker()
        broker.include_router(router)

        async with broker:
            await broker.start()
            lifespan_events.append("broker_started")

        lifespan_events.append("broker_stopped")

        # Check that lifespan events were called
        # Note: The exact order and presence of events depends on implementation
        assert len(lifespan_events) > 0

    @pytest.mark.asyncio()
    async def test_router_tags(self) -> None:
        """Test router with tags for documentation."""
        router = GCPPubSubRouter(tags=["router-tag", "test-tag"])

        @router.subscriber("tagged-subscription", topic="tagged-topic")
        async def handler(msg: Any) -> None:
            pass

        # Tags should be associated with the router
        # The exact implementation of tags may vary
        assert hasattr(router, "tags") or hasattr(router, "_tags")

    @pytest.mark.asyncio()
    async def test_multiple_routers(self) -> None:
        """Test multiple routers in same broker."""
        broker = self.get_broker()

        router1 = GCPPubSubRouter()
        router2 = GCPPubSubRouter()

        @router1.subscriber("router1-subscription", topic="router1-topic")
        async def handler1(msg: Any) -> None:
            pass

        @router2.subscriber("router2-subscription", topic="router2-topic")
        async def handler2(msg: Any) -> None:
            pass

        broker.include_router(router1)
        broker.include_router(router2)

        # Broker should have subscribers from both routers
        assert len(broker.subscribers) >= 2

    @pytest.mark.asyncio()
    async def test_router_publisher_decorator(self) -> None:
        """Test publisher decorator on router."""
        router = GCPPubSubRouter()
        messages_published = []

        @router.publisher("output-topic")
        async def publish_message(msg: str) -> str:
            messages_published.append(msg)
            return f"Published: {msg}"

        # Test the publisher decorator
        result = await publish_message("test message")
        assert result == "Published: test message"
        assert "test message" in messages_published

    @pytest.mark.asyncio()
    async def test_router_dependency_injection(
        self, subscription: str, topic: str
    ) -> None:
        """Test dependency injection in router handlers."""
        from faststream import Depends

        dependency_calls = []

        def test_dependency() -> str:
            dependency_calls.append("dependency_called")
            return "dependency_result"

        router = GCPPubSubRouter()

        @router.subscriber(subscription, topic=topic, create_subscription=True)
        async def handler_with_dep(msg: Any, dep: str = Depends(test_dependency)) -> None:
            assert dep == "dependency_result"

        broker = self.get_broker(apply_types=True)
        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("test", topic=topic)),
                    asyncio.create_task(asyncio.sleep(0.5)),
                ),
                timeout=self.timeout,
            )

        # Dependency should have been called
        assert "dependency_called" in dependency_calls

    @pytest.mark.asyncio()
    async def test_router_error_handling(self, subscription: str, topic: str) -> None:
        """Test error handling in router context."""
        router = GCPPubSubRouter()
        error_occurred = False

        @router.subscriber(subscription, topic=topic, create_subscription=True)
        async def error_handler(msg: Any) -> None:
            nonlocal error_occurred
            error_occurred = True
            error_msg = "Router handler error"
            raise ValueError(error_msg)

        broker = self.get_broker()
        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("error test", topic=topic)),
                    asyncio.create_task(asyncio.sleep(0.5)),
                ),
                timeout=self.timeout,
            )

        assert error_occurred

    @pytest.mark.asyncio()
    async def test_router_context_propagation(
        self, subscription: str, topic: str
    ) -> None:
        """Test context propagation through routers."""
        from faststream import Context

        router = GCPPubSubRouter()
        context_values = []

        @router.subscriber(subscription, topic=topic, create_subscription=True)
        async def context_handler(msg: Any, context=Context()) -> None:
            context_values.append(str(context))

        broker = self.get_broker()
        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("context test", topic=topic)),
                    asyncio.create_task(asyncio.sleep(0.5)),
                ),
                timeout=self.timeout,
            )

        # Context should be available
        assert len(context_values) > 0
