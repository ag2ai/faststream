from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream.sqla.client import create_sqla_client
from faststream.sqla.message import SqlaInnerMessage, SqlaMessageState


@pytest.mark.sqla()
@pytest.mark.connected()
@pytest.mark.slow()
@pytest.mark.asyncio()
async def test_archive_on_conflict_do_nothing(
    engine: AsyncEngine, recreate_tables: None
) -> None:
    client = create_sqla_client(
        engine,
        message_table_name="message",
        message_archive_table_name="message_archive",
    )
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    message = SqlaInnerMessage(
        id=1,
        queue="default1",
        headers={},
        payload=b"payload",
        state=SqlaMessageState.COMPLETED,
        attempts_count=2,
        deliveries_count=1,
        created_at=now,
        first_attempt_at=now,
        next_attempt_at=now,
        last_attempt_at=now,
        acquired_at=None,
    )

    async with engine.begin() as conn:
        await conn.execute(
            client._message_table.insert().values(
                id=message.id,
                queue=message.queue,
                headers=message.headers,
                payload=message.payload,
                state=message.state,
                attempts_count=message.attempts_count,
                deliveries_count=message.deliveries_count,
                created_at=message.created_at,
                first_attempt_at=message.first_attempt_at,
                next_attempt_at=message.next_attempt_at,
                last_attempt_at=message.last_attempt_at,
                acquired_at=message.acquired_at,
            )
        )

    await client.archive([message])
    await client.archive([message])

    async with engine.begin() as conn:
        archived = await conn.execute(
            client._message_archive_table.select().where(
                client._message_archive_table.c.id == message.id
            )
        )
        remaining = await conn.execute(client._message_table.select())

    assert len(archived.mappings().all()) == 1
    assert len(remaining.mappings().all()) == 0
