import asyncio
from collections.abc import AsyncGenerator, Iterator
from contextlib import asynccontextmanager
from typing import Annotated, Any
from unittest.mock import Mock

import pytest
from fastapi import BackgroundTasks, Body, Depends, FastAPI, Header
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from pydantic import BaseModel

from faststream import (
    Context as FSContext,
    Depends as FSDepends,
    Response,
)
from faststream._internal.fastapi.context import Context
from faststream._internal.fastapi.route import StreamMessage
from faststream._internal.kafka.tombstone import TOMBSTONE
from faststream.exceptions import SetupError
from faststream.message import StreamMessage as NativeMessage

from .basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class FastAPITestcase(BaseTestcaseConfig[Any]):
    router_class: Any
    broker_router_class: Any

    async def test_base_real(self, mock: Mock, queue: str, event: asyncio.Event) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: Any) -> Any:
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

    async def test_background(
        self,
        mock: Mock,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        router = self.router_class()

        def task(msg: Any) -> None:
            event.set()
            mock(msg)

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: Any, tasks: BackgroundTasks) -> None:
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

        mock.assert_called_with("hi")

    async def test_context(self, mock: Mock, queue: str, event: asyncio.Event) -> None:
        router = self.router_class()
        context = router.context

        context_key = "message.headers"

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: Any = Context(context_key)) -> None:
            try:
                mock(msg == context.resolve(context_key) and msg["1"] == "1")
            finally:
                event.set()

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(
                        router.broker.publish("", queue, headers={"1": "1"}),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(True)

    async def test_context_annotated(
        self,
        mock: Mock,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        router = self.router_class()
        context = router.context

        context_key = "message.headers"

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: Annotated[Any, Context(context_key)]) -> None:
            try:
                mock(msg == context.resolve(context_key) and msg["1"] == "1")
            finally:
                event.set()

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(
                        router.broker.publish("", queue, headers={"1": "1"}),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(True)

    @pytest.mark.flaky(reruns=3, reruns_delay=1)
    async def test_initial_context(self, queue: str, event: asyncio.Event) -> None:
        router = self.router_class()
        context = router.context

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: int, data: set[int] = Context(queue, initial=set)) -> None:
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

        assert context.get(queue) == {1, 2}
        context.reset_global(queue)

    async def test_double_real(
        self, mock: Mock, queue: str, event: asyncio.Event, event2: asyncio.Event
    ) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)
        sub1 = router.subscriber(*args, **kwargs)

        args2, kwargs2 = self.get_subscriber_params(queue + "2")

        @sub1
        @router.subscriber(*args2, **kwargs2)
        async def hello(msg: str) -> None:
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
    ) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        @router.publisher(queue + "resp")
        async def m() -> str:
            return "hi"

        args2, kwargs2 = self.get_subscriber_params(queue + "resp")

        @router.subscriber(*args2, **kwargs2)
        async def resp(msg: Any) -> None:
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

    async def test_injection_fastapi(
        self,
        mock: Mock,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def subscriber(msg: StreamMessage) -> None:
            mock("app" in msg.scope)
            event.set()

        async with router.broker:
            await router.broker.start()
            await asyncio.wait(
                (
                    asyncio.create_task(router.broker.publish(None, queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        mock.assert_called_once_with(True)


@pytest.mark.asyncio()
class KafkaTombstoneFastAPILocalTestcase(BaseTestcaseConfig[Any]):
    router_class: Any

    @pytest.mark.parametrize(
        "embed", (pytest.param(False, id="direct"), pytest.param(True, id="embedded"))
    )
    async def test_optional_body_resolves_to_none_for_tombstone(
        self, queue: str, embed: bool
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/1967."""

        class _BodyModel(BaseModel):
            x: int

        router = self.router_class()
        received: list[tuple[_BodyModel | None, Any]] = []

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def handler(
            msg: Annotated[_BodyModel | None, Body(embed=embed)] = None,
            *,
            request: StreamMessage,
        ) -> None:
            received.append((msg, await request.body()))

        app = FastAPI()
        app.include_router(router)

        async with self.patch_broker(router.broker) as br:
            with TestClient(app):
                payload = b'{"msg": {"x": 5}}' if embed else b'{"x": 5}'
                await br.publish(payload, queue, key=b"k1")
                await br.publish(None, queue, key=b"k2")
                headers = {"content-type": "application/json"}
                await br.publish(None, queue, key=b"k3", headers=headers)
                with pytest.raises(RequestValidationError):
                    await br.publish(b"", queue, key=b"k4")

        assert received == [
            (_BodyModel(x=5), {"msg": {"x": 5}} if embed else {"x": 5}),
            (None, {"msg": b""}),
            (None, {"msg": b""}),
        ]

    @pytest.mark.parametrize(
        ("decoded", "expected"),
        (
            pytest.param({"msg": b"decoded"}, b"decoded", id="mapping"),
            pytest.param(b"", b"", id="empty-bytes"),
        ),
    )
    async def test_tombstone_preserves_custom_decoded_body(
        self,
        queue: str,
        mock: Mock,
        decoded: dict[str, bytes] | bytes,
        expected: bytes,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/1967."""

        class _BodyModel(BaseModel):
            msg: bytes

        async def decoder(_: NativeMessage[Any]) -> dict[str, bytes] | bytes:
            return decoded

        router = self.router_class()
        args, kwargs = self.get_subscriber_params(queue, decoder=decoder)

        @router.subscriber(*args, **kwargs)
        async def handler(msg: _BodyModel | None = None) -> None:
            mock(msg)

        async with self.patch_broker(router.broker) as br:
            await br.publish(None, queue, key=b"key")

        mock.assert_called_once_with(_BodyModel(msg=expected))

    @pytest.mark.parametrize(
        "embed",
        (pytest.param(False, id="direct"), pytest.param(True, id="embedded")),
    )
    async def test_tombstone_resolves_optional_body_in_dependency(
        self, queue: str, mock: Mock, embed: bool
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/1967."""

        class _BodyModel(BaseModel):
            x: int

        async def parse_body(
            msg: Annotated[_BodyModel | None, Body(embed=embed)] = None,
        ) -> _BodyModel | None:
            return msg

        router = self.router_class()
        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def handler(
            msg: Annotated[_BodyModel | None, Depends(parse_body)],
        ) -> None:
            mock(msg)

        async with self.patch_broker(router.broker) as br:
            await br.publish(None, queue, key=b"key")

        mock.assert_called_once_with(None)

    async def test_tombstone_preserves_explicit_embedded_bytes_parameter(
        self, queue: str, mock: Mock
    ) -> None:
        router = self.router_class()
        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def handler(msg: Annotated[bytes, Body(embed=True)]) -> None:
            mock(msg)

        async with self.patch_broker(router.broker) as br:
            await br.publish(None, queue, key=b"key")

        mock.assert_called_once_with(b"")

    @pytest.mark.parametrize(
        "custom_decoder",
        (pytest.param(False, id="default"), pytest.param(True, id="custom")),
    )
    async def test_tombstone_batch_binds_nullable_models(
        self, queue: str, mock: Mock, custom_decoder: bool
    ) -> None:
        class Model(BaseModel):
            x: int

        async def decoder(_: NativeMessage[Any]) -> list[dict[str, int]]:
            return [{"x": 7}, {"x": 8}]

        router = self.router_class()
        args, kwargs = self.get_subscriber_params(
            queue, batch=True, decoder=decoder if custom_decoder else None
        )

        @router.subscriber(*args, **kwargs)
        async def handler(msg: list[Model | None]) -> None:
            mock(msg)

        publisher = router.broker.publisher(queue, batch=True, key=b"key")
        async with self.patch_broker(router.broker):
            await publisher.publish(b'{"x": 5}', TOMBSTONE)

        expected = [Model(x=7), Model(x=8)] if custom_decoder else [Model(x=5), None]
        mock.assert_called_once_with(expected)

    async def test_tombstone_batch_preserves_bytes(self, queue: str, mock: Mock) -> None:
        router = self.router_class()
        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @router.subscriber(*args, **kwargs)
        async def handler(msg: list[bytes]) -> None:
            mock(msg)

        publisher = router.broker.publisher(queue, batch=True, key=b"key")
        async with self.patch_broker(router.broker):
            await publisher.publish(b"keep", TOMBSTONE)

        mock.assert_called_once_with([b"keep", b""])


@pytest.mark.asyncio()
class FastAPILocalTestcase(BaseTestcaseConfig[Any]):
    router_class: Any
    broker_router_class: Any

    async def test_base(self, queue: str) -> None:
        router = self.router_class()

        app = FastAPI()
        app.include_router(router)

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello() -> str:
            return "hi"

        async with self.patch_broker(router.broker) as br:
            with TestClient(app) as client:
                assert client.app_state["broker"] is br

                r = await br.request(
                    "hi",
                    queue,
                    timeout=0.5,
                )
                assert await r.decode() == "hi", r

    async def test_request(self, queue: str) -> None:
        """Local test due request exists in all TestClients."""
        router = self.router_class(setup_state=False)

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello() -> Any:
            return Response("Hi!", headers={"x-header": "test"})

        async with self.patch_broker(router.broker) as br:
            with TestClient(app) as client:
                assert not client.app_state.get("broker")

                r = await br.request(
                    "hi",
                    queue,
                    timeout=0.5,
                )
                assert await r.decode() == "Hi!"
                assert r.headers["x-header"] == "test"

    async def test_base_without_state(self, queue: str) -> None:
        router = self.router_class(setup_state=False)

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello() -> str:
            return "hi"

        async with self.patch_broker(router.broker) as br:
            with TestClient(app) as client:
                assert not client.app_state.get("broker")

                r = await br.request(
                    "hi",
                    queue,
                    timeout=0.5,
                )
                assert await r.decode() == "hi", r

    async def test_invalid(self, queue: str) -> None:
        router = self.router_class()

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: int) -> None: ...

        app.include_router(router)

        async with self.patch_broker(router.broker) as br:
            with TestClient(app), pytest.raises(RequestValidationError):
                await br.publish("hi", queue)

    async def test_headers(self, queue: str) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(w: Any = Header()) -> Any:
            return w

        async with self.patch_broker(router.broker) as br:
            r = await br.request(
                "",
                queue,
                headers={"w": "hi"},
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

    async def test_depends(self, mock: Mock, queue: str) -> None:
        router = self.router_class()

        def dep(a: Any) -> Any:
            mock(a)
            return a

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(a: Any, w: Any = Depends(dep)) -> Any:
            return w

        async with self.patch_broker(router.broker) as br:
            r = await br.request(
                {"a": "hi"},
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

        mock.assert_called_once_with("hi")

    async def test_mixed_depends(self, mock: Mock, queue: str) -> None:
        router = self.router_class()

        def dep(a: str) -> str:
            mock(a)
            return a

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(
            a: str,
            w: Annotated[
                str,
                Depends(dep),
                FSDepends(dep),  # will be ignored
            ],
        ) -> str:
            return w

        async with self.patch_broker(router.broker) as br:
            r = await br.request(
                {"a": "hi"},
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

        mock.assert_called_once_with("hi")

    async def test_depends_from_fastdepends_default(self, queue: str) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        subscriber = router.subscriber(*args, **kwargs)

        @subscriber
        def sub(d: Any = FSDepends(lambda: 1)) -> None: ...

        app = FastAPI()
        app.include_router(router)

        with pytest.raises(SetupError):  # ruff: ignore[pytest-raises-with-multiple-statements]
            async with self.patch_broker(router.broker):
                with TestClient(app):
                    ...

    async def test_depends_from_fastdepends_annotated(self, queue: str) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        subscriber = router.subscriber(*args, **kwargs)

        @subscriber
        def sub(d: Annotated[Any, FSDepends(lambda: 1)]) -> None: ...

        app = FastAPI()
        app.include_router(router)

        with pytest.raises(SetupError):  # ruff: ignore[pytest-raises-with-multiple-statements]
            async with self.patch_broker(router.broker):
                with TestClient(app):
                    ...

    async def test_depends_combined_annotated(self, queue: str) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        subscriber = router.subscriber(*args, **kwargs)

        @subscriber
        def sub(
            d: Annotated[Any, FSDepends(lambda: 1), Depends(lambda: 1)],
        ) -> None: ...

        app = FastAPI()
        app.include_router(router)

        async with self.patch_broker(router.broker):
            with TestClient(app):
                ...

    async def test_faststream_context(self, queue: str) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: Any = FSContext()) -> None: ...

        app = FastAPI()
        app.include_router(router)

        with pytest.raises(SetupError):  # ruff: ignore[pytest-raises-with-multiple-statements]
            async with self.patch_broker(router.broker):
                with TestClient(app):
                    ...

    async def test_faststream_context_annotated(self, queue: str) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(msg: Annotated[Any, FSContext()]) -> None: ...

        app = FastAPI()
        app.include_router(router)

        with pytest.raises(SetupError):  # ruff: ignore[pytest-raises-with-multiple-statements]
            async with self.patch_broker(router.broker):
                with TestClient(app):
                    ...

    async def test_combined_context_annotated(self, queue: str) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(
            msg: Annotated[
                Any,
                Context("message.headers"),
                FSContext("message.headers"),
            ],
        ) -> None: ...

        app = FastAPI()
        app.include_router(router)

        async with self.patch_broker(router.broker):
            with TestClient(app):
                ...

    async def test_nested_combined_context_annotated(self, queue: str) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(
            msg: Annotated[
                Annotated[Any, FSContext("message.headers")],
                Context("message.headers"),
            ],
        ) -> None: ...

        app = FastAPI()
        app.include_router(router)

        async with self.patch_broker(router.broker):
            with TestClient(app):
                ...

    async def test_yield_depends(self, mock: Mock, queue: str) -> None:
        router = self.router_class()

        def dep(a: Any) -> Iterator[Any]:
            mock.start()
            yield a
            mock.close()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(a: Any, w: Any = Depends(dep)) -> Any:
            mock.inside(started=mock.start.call_count, closed=mock.close.call_count)
            return w

        async with self.patch_broker(router.broker) as br:
            r = await br.request(
                {"a": "hi"},
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

        # the dependency is open while the handler runs and closed after it
        mock.inside.assert_called_once_with(started=1, closed=0)
        mock.start.assert_called_once()
        mock.close.assert_called_once()

    async def test_router_depends(self, mock: Mock, queue: str) -> None:
        def mock_dep() -> None:
            mock()

        router = self.router_class(dependencies=(Depends(mock_dep, use_cache=False),))

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(a: Any) -> Any:
            return a

        async with self.patch_broker(router.broker) as br:
            r = await br.request("hi", queue, timeout=0.5)
            assert await r.decode() == "hi", r

        mock.assert_called_once()

    async def test_subscriber_depends(self, mock: Mock, queue: str) -> None:
        def mock_dep() -> None:
            mock()

        router = self.router_class()

        args, kwargs = self.get_subscriber_params(
            queue,
            dependencies=(Depends(mock_dep, use_cache=False),),
        )

        @router.subscriber(*args, **kwargs)
        async def hello(a: Any) -> Any:
            return a

        async with self.patch_broker(router.broker) as br:
            r = await br.request(
                "hi",
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

        mock.assert_called_once()

    async def test_hooks(self, mock: Mock) -> None:
        router = self.router_class()

        app = FastAPI()
        app.include_router(router)

        @router.after_startup
        def test_sync(app: Any) -> None:
            mock.sync_called()

        @router.after_startup
        async def test_async(app: Any) -> None:
            mock.async_called()

        @router.on_broker_shutdown
        def test_shutdown_sync(app: Any) -> None:
            mock.sync_shutdown_called()

        @router.on_broker_shutdown
        async def test_shutdown_async(app: Any) -> None:
            mock.async_shutdown_called()

        async with self.patch_broker(router.broker), router.lifespan_context(app):
            pass

        mock.sync_called.assert_called_once()
        mock.async_called.assert_called_once()
        mock.sync_shutdown_called.assert_called_once()
        mock.async_shutdown_called.assert_called_once()

    async def test_existed_lifespan_startup(self, mock: Mock) -> None:
        @asynccontextmanager
        async def lifespan(app: Any) -> AsyncGenerator[Any, None]:
            mock.start()
            try:
                yield {"lifespan": True}
            finally:
                mock.close()

        router = self.router_class(lifespan=lifespan)

        app = FastAPI()
        app.include_router(router)

        async with (
            self.patch_broker(router.broker),
            router.lifespan_context(
                app,
            ) as context,
        ):
            assert context["lifespan"]

        mock.start.assert_called_once()
        mock.close.assert_called_once()

    async def test_subscriber_mock(self, queue: str) -> None:
        router = self.router_class()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def m() -> str:
            return "hi"

        async with self.patch_broker(router.broker) as rb:
            await rb.publish("hello", queue)
            m.mock.assert_called_once_with("hello")

    async def test_publisher_mock(self, queue: str) -> None:
        router = self.router_class()

        publisher = router.publisher(queue + "resp")

        args, kwargs = self.get_subscriber_params(queue)
        sub = router.subscriber(*args, **kwargs)

        @publisher
        @sub
        async def m() -> str:
            return "response"

        async with self.patch_broker(router.broker) as rb:
            await rb.publish("hello", queue)
            publisher.mock.assert_called_with("response")

    async def test_include(self, queue: str) -> None:
        router = self.router_class()
        router2 = self.broker_router_class()

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello() -> str:
            return "hi"

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @router2.subscriber(*args2, **kwargs2)
        async def hello_router2() -> str:
            return "hi"

        router.include_router(router2)
        app.include_router(router)

        async with self.patch_broker(router.broker) as br:
            with TestClient(app) as client:
                assert client.app_state["broker"] is br

                r = await br.request(
                    "hi",
                    queue,
                    timeout=0.5,
                )
                assert await r.decode() == "hi", r

                r = await br.request(
                    "hi",
                    queue + "1",
                    timeout=0.5,
                )
                assert await r.decode() == "hi", r

    async def test_dependency_overrides(self, mock: Mock, queue: str) -> None:
        router = self.router_class()

        def dep1() -> None:
            raise AssertionError

        def dep2() -> None:
            mock()

        app = FastAPI()
        app.dependency_overrides[dep1] = dep2

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello_router2(dep: None = Depends(dep1)) -> str:
            return "hi"

        app.include_router(router)

        async with self.patch_broker(router.broker) as br:
            with TestClient(app) as client:
                assert client.app_state["broker"] is br

                r = await br.request(
                    "hi",
                    queue,
                    timeout=0.5,
                )
                assert await r.decode() == "hi", r

        mock.assert_called_once()

    async def test_nested_stream_router_raises(self) -> None:
        """Including a StreamRouter into another StreamRouter must raise TypeError.

        This pattern is unsupported (issue #2657).  Users should include a regular
        broker router (e.g. KafkaRouter) into the StreamRouter instead.
        """
        router = self.router_class()
        router2 = self.router_class()

        with pytest.raises(
            TypeError,
            match="Including a StreamRouter into another StreamRouter is not supported",
        ):
            router.include_router(router2)
