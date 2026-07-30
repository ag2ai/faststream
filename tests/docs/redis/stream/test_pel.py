import pytest


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_pending_message_stays_without_a_claimer() -> None:
    from docs.docs_src.redis.stream.pel_testing import (
        test_pending_message_stays_without_a_claimer as run,
    )

    await run()


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_each_group_gets_its_own_pel_entry() -> None:
    from docs.docs_src.redis.stream.pel_testing import (
        test_each_group_gets_its_own_pel_entry as run,
    )

    await run()


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_no_ack_worker_never_touches_the_pel() -> None:
    from docs.docs_src.redis.stream.pel_testing import (
        test_no_ack_worker_never_touches_the_pel as run,
    )

    await run()
