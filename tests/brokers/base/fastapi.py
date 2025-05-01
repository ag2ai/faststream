import asyncio
from contextlib import asynccontextmanager
from typing import Callable, Type
from unittest.mock import Mock

import pytest
from fastapi import BackgroundTasks, Depends, FastAPI, Header
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from typing_extensions import Annotated, Any, TypeVar

from faststream import Context as FSContext
from faststream import Depends as FSDepends
from faststream import Response, context
from faststream.broker.core.usecase import BrokerUsecase
from faststream.broker.fastapi.context import Context
from faststream.broker.fastapi.router import StreamRouter
from faststream.broker.router import BrokerRouter
from faststream.exceptions import SetupError
from faststream.types import AnyCallable

from .basic import BaseTestcaseConfig

Broker = TypeVar("Broker", bound=BrokerUsecase)


@pytest.mark.asyncio
class FastAPITestcase(BaseTestcaseConfig):
    router_class: Type[StreamRouter[BrokerUsecase]]
    broker_router_class: Type[BrokerRouter[Any]]

    async def test_base_real(self, mock: Mock, queue: str, event: asyncio.Event):
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg):
            event.set()
            return mock(msg)

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hi", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with("hi")

    async def test_background(self, mock: Mock, queue: str, event: asyncio.Event):
        router = self.router_class()

        def task(msg):
            event.set()
            return mock(msg)

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg, tasks: BackgroundTasks):
            tasks.add_task(task, msg)

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hi", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with("hi")

    async def test_context(self, mock: Mock, queue: str, event: asyncio.Event):
        router = self.router_class()

        context_key = "message.headers"

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg=Context(context_key)):
            event.set()
            return mock(msg == context.resolve(context_key))

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(True)

    async def test_context_annotated(
        self, mock: Mock, queue: str, event: asyncio.Event
    ):
        router = self.router_class()

        context_key = "message.headers"

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: Annotated[Any, Context(context_key)]):
            event.set()
            return mock(msg == context.resolve(context_key))

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(True)

    async def test_faststream_context(
        self, mock: Mock, queue: str, event: asyncio.Event
    ):
        router = self.router_class()

        context_key = "message.headers"

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg=FSContext(context_key)):
            event.set()
            return mock(msg == context.resolve(context_key))

        app = FastAPI()
        app.include_router(router)

        with pytest.raises(SetupError), TestClient(app):
            ...

    async def test_faststream_context_annotated(
        self, mock: Mock, queue: str, event: asyncio.Event
    ):
        router = self.router_class()

        context_key = "message.headers"

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: Annotated[Any, FSContext(context_key)]):
            event.set()
            return mock(msg == context.resolve(context_key))

        app = FastAPI()
        app.include_router(router)

        with pytest.raises(SetupError), TestClient(app):
            ...

    async def test_initial_context(self, queue: str, event: asyncio.Event):
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: int, data=Context(queue, initial=set)):
            data.add(msg)
            if len(data) == 2:
                event.set()

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish(1, queue)),
                    asyncio.create_task(router.broker.publish(2, queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert context.get(queue) == {1, 2}
        context.reset_global(queue)

    async def test_double_real(self, mock: Mock, queue: str, event: asyncio.Event):
        event2 = asyncio.Event()
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)
        sub1 = router.subscriber(*args, **kwargs)

        args2, kwargs2 = self.get_subscriber_params(queue + "2")

        @sub1
        @router.subscriber(*args2, **kwargs2)
        async def hello(msg: str):
            if event.is_set():
                event2.set()
            else:
                event.set()
            mock()

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("hi", queue)),
                    asyncio.create_task(router.broker.publish("hi", queue + "2")),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2

    async def test_base_publisher_real(
        self,
        mock: Mock,
        queue: str,
        event: asyncio.Event,
    ):
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        @router.publisher(queue + "resp")
        async def m():
            return "hi"

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @router.subscriber(*args2, **kwargs2)
        async def resp(msg):
            event.set()
            mock(msg)

        async with router.broker:
            await router.broker.start()

            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("hi")


@pytest.mark.asyncio
class FastAPILocalTestcase(BaseTestcaseConfig):
    router_class: Type[StreamRouter[BrokerUsecase]]
    broker_test: Callable[[Broker], Broker]
    build_message: AnyCallable

    async def test_base(self, queue: str):
        router = self.router_class()

        app = FastAPI()
        app.include_router(router)

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello():
            return "hi"

        async with self.broker_test(router.broker):
            with TestClient(app) as client:
                assert client.app_state["broker"] is router.broker

                r = await router.broker.publish(
                    "hi",
                    queue,
                    rpc=True,
                    rpc_timeout=0.5,
                )
                assert r == "hi", r

    async def test_request(self, queue: str):
        """Local test due request exists in all TestClients."""
        router = self.router_class(setup_state=False)

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello():
            return Response("Hi!", headers={"x-header": "test"})

        async with self.broker_test(router.broker):
            with TestClient(app) as client:
                assert not client.app_state.get("broker")

                r = await router.broker.request(
                    "hi",
                    queue,
                    timeout=0.5,
                )
                assert await r.decode() == "Hi!"
                assert r.headers["x-header"] == "test"

    async def test_base_without_state(self, queue: str):
        router = self.router_class(setup_state=False)

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello():
            return "hi"

        async with self.broker_test(router.broker):
            with TestClient(app) as client:
                assert not client.app_state.get("broker")

                r = await router.broker.publish(
                    "hi",
                    queue,
                    rpc=True,
                    rpc_timeout=0.5,
                )
                assert r == "hi"

    async def test_invalid(self, queue: str):
        router = self.router_class()

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: int): ...

        app.include_router(router)

        async with self.broker_test(router.broker):
            with TestClient(app):
                with pytest.raises(RequestValidationError):
                    await router.broker.publish("hi", queue)

    async def test_headers(self, queue: str):
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(w=Header()):
            return w

        async with self.broker_test(router.broker):
            r = await router.broker.publish(
                "",
                queue,
                headers={"w": "hi"},
                rpc=True,
                rpc_timeout=0.5,
            )
            assert r == "hi"

    async def test_depends(self, mock: Mock, queue: str):
        router = self.router_class()

        def dep(a):
            mock(a)
            return a

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(a, w=Depends(dep)):
            return w

        async with self.broker_test(router.broker):
            r = await router.broker.publish(
                {"a": "hi"},
                queue,
                rpc=True,
                rpc_timeout=0.5,
            )
            assert r == "hi"

        mock.assert_called_once_with("hi")

    async def test_depends_from_fastdepends_default(self, queue: str):
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        subscriber = router.subscriber(*args, **kwargs)

        @subscriber
        def sub(d: Any = FSDepends(lambda: 1)) -> None: ...

        app = FastAPI()
        app.include_router(router)

        with pytest.raises(SetupError), TestClient(app):
            ...

    async def test_depends_from_fastdepends_annotated(self, queue: str):
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        subscriber = router.subscriber(*args, **kwargs)

        @subscriber
        def sub(d: Annotated[Any, FSDepends(lambda: 1)]) -> None: ...

        app = FastAPI()
        app.include_router(router)

        with pytest.raises(SetupError), TestClient(app):
            ...

    async def test_yield_depends(self, mock: Mock, queue: str):
        router = self.router_class()

        def dep(a):
            mock.start()
            yield a
            mock.close()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(a, w=Depends(dep)):
            mock.start.assert_called_once()
            assert not mock.close.call_count
            return w

        async with self.broker_test(router.broker):
            r = await router.broker.publish(
                {"a": "hi"},
                queue,
                rpc=True,
                rpc_timeout=0.5,
            )
            assert r == "hi"

        mock.start.assert_called_once()
        mock.close.assert_called_once()

    async def test_router_depends(self, mock: Mock, queue: str):
        def mock_dep():
            mock()

        router = self.router_class(dependencies=(Depends(mock_dep, use_cache=False),))

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(a):
            return a

        async with self.broker_test(router.broker):
            r = await router.broker.publish("hi", queue, rpc=True, rpc_timeout=0.5)
            assert r == "hi"

        mock.assert_called_once()

    async def test_subscriber_depends(self, mock: Mock, queue: str):
        def mock_dep():
            mock()

        router = self.router_class()

        args, kwargs = self.get_subscriber_params(
            queue,
            dependencies=(Depends(mock_dep, use_cache=False),),
        )

        @router.subscriber(*args, **kwargs)
        async def hello(a):
            return a

        async with self.broker_test(router.broker):
            r = await router.broker.publish(
                "hi",
                queue,
                rpc=True,
                rpc_timeout=0.5,
            )
            assert r == "hi"

        mock.assert_called_once()

    async def test_hooks(self, mock: Mock):
        router = self.router_class()

        app = FastAPI()
        app.include_router(router)

        @router.after_startup
        def test_sync(app):
            mock.sync_called()

        @router.after_startup
        async def test_async(app):
            mock.async_called()

        @router.on_broker_shutdown
        def test_shutdown_sync(app):
            mock.sync_shutdown_called()

        @router.on_broker_shutdown
        async def test_shutdown_async(app):
            mock.async_shutdown_called()

        async with self.broker_test(router.broker), router.lifespan_context(app):
            pass

        mock.sync_called.assert_called_once()
        mock.async_called.assert_called_once()
        mock.sync_shutdown_called.assert_called_once()
        mock.async_shutdown_called.assert_called_once()

    async def test_existed_lifespan_startup(self, mock: Mock):
        @asynccontextmanager
        async def lifespan(app):
            mock.start()
            yield {"lifespan": True}
            mock.close()

        router = self.router_class(lifespan=lifespan)

        app = FastAPI()
        app.include_router(router)

        async with self.broker_test(router.broker), router.lifespan_context(
            app
        ) as context:
            assert context["lifespan"]

        mock.start.assert_called_once()
        mock.close.assert_called_once()

    async def test_subscriber_mock(self, queue: str):
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def m():
            return "hi"

        async with self.broker_test(router.broker) as rb:
            await rb.publish("hello", queue)
            m.mock.assert_called_once_with("hello")

    async def test_publisher_mock(self, queue: str):
        router = self.router_class()

        publisher = router.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)
        sub = router.subscriber(*args, **kwargs)

        @publisher
        @sub
        async def m():
            return "response"

        async with self.broker_test(router.broker) as rb:
            await rb.publish("hello", queue)
            publisher.mock.assert_called_with("response")

    async def test_include(self, queue: str):
        router = self.router_class()
        router2 = self.broker_router_class()

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello():
            return "hi"

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @router2.subscriber(*args2, **kwargs2)
        async def hello_router2():
            return "hi"

        router.include_router(router2)
        app.include_router(router)

        async with self.broker_test(router.broker):
            with TestClient(app) as client:
                assert client.app_state["broker"] is router.broker

                r = await router.broker.publish(
                    "hi",
                    queue,
                    rpc=True,
                    rpc_timeout=0.5,
                )
                assert r == "hi"

                r = await router.broker.publish(
                    "hi",
                    queue + "1",
                    rpc=True,
                    rpc_timeout=0.5,
                )
                assert r == "hi"

    async def test_dependency_overrides(self, mock: Mock, queue: str):
        router = self.router_class()
        router2 = self.router_class()

        def dep1():
            mock.not_call()
            pass

        app = FastAPI()
        app.dependency_overrides[dep1] = lambda: mock()

        args, kwargs = self.get_subscriber_params(queue)

        @router2.subscriber(*args, **kwargs)
        async def hello_router2(dep=Depends(dep1)):
            return "hi"

        router.include_router(router2)
        app.include_router(router)

        async with self.broker_test(router.broker):
            with TestClient(app) as client:
                assert client.app_state["broker"] is router.broker

                r = await router.broker.publish(
                    "hi",
                    queue,
                    rpc=True,
                    rpc_timeout=0.5,
                )
                assert r == "hi"

        mock.assert_called_once()
        assert not mock.not_call.called
