import os
from typing import Any

import asyncpg

DSN = os.environ.get(
    "BENCHMARK_PG_DSN",
    "postgresql://postgres:postgres@localhost:5432/postgres",  # pragma: allowlist secret
)


async def find_user_by_name(name: str, pool: asyncpg.Pool) -> dict[str, Any] | None:

    row = await pool.fetchrow(
        "SELECT id, name, age, fullname FROM users WHERE name = $1 LIMIT 1",
        name,
    )

    return dict(row) if row is not None else None
