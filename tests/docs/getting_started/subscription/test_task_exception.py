import pytest


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_task_exception() -> None:
    from docs.docs_src.getting_started.subscription.task_exception import subscriber

    async def task() -> None:
        pass

    subscriber.running = True
    task_count = len(subscriber.tasks)

    subscriber._handle_task_exception(RuntimeError(), task, (), {})

    assert not subscriber.running
    assert len(subscriber.tasks) == task_count
