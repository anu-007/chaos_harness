"""Dedup + fence-monotonicity invariants (enforced by DB constraints).

These map directly to chaos_harness.py::_verify:
  * concurrent same-key submit -> exactly one job_id   (jobs.idempotency_key UNIQUE)
  * fence tokens strictly increasing                    (nextval('fence_seq'))
"""

import asyncio

import asyncpg

DSN = None  # set from env in conftest via the db fixture; here we open raw conns for concurrency


async def _submit_same_key(pool, key: str) -> str:
    """One racing submitter: the exact dedup insert handle_create_job uses. Returns the
    resulting job_id whether this caller created the row or lost the race to a peer."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO jobs (idempotency_key, payload)
            VALUES ($1, '{}'::jsonb)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING job_id
            """,
            key,
        )
        if row is not None:
            return str(row["job_id"])
        return str(
            await conn.fetchval(
                "SELECT job_id FROM jobs WHERE idempotency_key = $1", key
            )
        )


async def test_concurrent_same_key_submit_yields_one_id(db):
    """20 concurrent submits of the SAME idempotency_key -> all resolve to ONE job_id, and
    exactly one row exists. This is the dedup guarantee even when same-key requests land on
    different coordinators in the same instant."""
    import os

    key = "dup-key-shared"
    # Open an independent pool so the 20 inserts truly run concurrently (own connections),
    # racing on the UNIQUE index exactly like requests fanned across coordinators.
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=8, max_size=20)
    try:
        ids = await asyncio.gather(*[_submit_same_key(pool, key) for _ in range(20)])
    finally:
        await pool.close()

    assert len(set(ids)) == 1, f"expected one job_id, got {set(ids)}"
    count = await db.fetchval(
        "SELECT count(*) FROM jobs WHERE idempotency_key = $1", key
    )
    assert count == 1


async def test_fence_sequence_strictly_increasing(db):
    """nextval('fence_seq') is strictly increasing — the basis for global fence monotonicity.
    Draw many values concurrently and assert every one is unique and the sorted order is
    strictly ascending (no duplicates, no reuse)."""
    import os

    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=8, max_size=20)
    try:
        vals = await asyncio.gather(
            *[pool.fetchval("SELECT nextval('fence_seq')") for _ in range(200)]
        )
    finally:
        await pool.close()

    assert len(set(vals)) == len(vals), "fence values must be unique (no reuse)"
    s = sorted(vals)
    assert all(s[i] < s[i + 1] for i in range(len(s) - 1)), "fences must strictly increase"
