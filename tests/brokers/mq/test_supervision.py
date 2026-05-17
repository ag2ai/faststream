import pytest

from .basic import MQMemoryTestcaseConfig


class DummyConnection:
    def __init__(self, *, connection_config) -> None:
        self.connection_config = connection_config

    async def connect(self) -> None:
        return None

    async def start_consumer(self, queue_name: str) -> None:
        return None

    async def stop_consumer(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestSupervisor(MQMemoryTestcaseConfig):
    async def test_start_uses_supervised_consume_task(
        self,
        queue: str,
        monkeypatch,
    ) -> None:
        broker = self.get_broker()
        subscriber = broker.subscriber(queue)

        @subscriber
        async def handler(msg) -> None: ...

        broker._setup_logger()

        monkeypatch.setattr(
            "faststream.mq.subscriber.usecase.AsyncMQConnection",
            DummyConnection,
        )

        captured: list[object] = []

        def fake_add_task(func, func_args=None, func_kwargs=None):
            captured.append(func)
            return None

        monkeypatch.setattr(subscriber, "add_task", fake_add_task)

        await subscriber.start()
        try:
            assert captured == [subscriber._consume_loop]
        finally:
            await subscriber.stop()
