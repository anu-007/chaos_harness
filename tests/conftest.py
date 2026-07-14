"""Shared fixtures for the invariant suite.

These are INTEGRATION tests: they run against the same compose Postgres the coordinators use
(DATABASE_URL), because the dedup / fencing / exactly-once invariants live in real DB
constraints (unique indexes, the fence sequence) and in the real handle_commit / Reaper code
paths — not in anything worth mocking. Each test drives those real code paths and then asserts
on the durable rows the chaos harness's _verify would inspect.

To exercise handle_commit and Reaper without HTTP/WebSocket we build a minimal "app" dict with
exactly the keys they read (db, coord_id, rates, lease_ttl_ms, dispatcher) plus a fake worker
ConnState whose WS records the messages the handler would have sent (ack / commit_rejected).
"""

import os
import uuid

import asyncpg
import pytest_asyncio

from db import Database

DSN = os.environ["DATABASE_URL"]


@pytest_asyncio.fixture
async def db():
    """A connected Database (the coordinator's real DB wrapper) with a clean set of tables.

    Truncating before each test keeps them isolated and order-independent; the schema is
    already applied by the running coordinators, so we only reset data here."""
    database = Database(DSN)
    await database.connect(min_size=1, max_size=8)
    await _truncate(database)
    try:
        yield database
    finally:
        await database.close()


async def _truncate(database: Database) -> None:
    # commits/leases/transitions reference jobs by id (no FK), so order is cosmetic; jobs last.
    await database.execute("DELETE FROM commits")
    await database.execute("DELETE FROM job_transitions")
    await database.execute("DELETE FROM leases")
    await database.execute("DELETE FROM jobs")


class FakeWS:
    """Stand-in for a worker WebSocket: records every frame the handler sends so a test can
    assert the worker was told 'ack' vs 'commit_rejected'."""

    def __init__(self):
        self.sent: list[dict] = []

    async def send_json(self, msg: dict) -> None:
        self.sent.append(msg)


class FakeConnState:
    """Minimal worker state handle_commit / dispatch read: worker_id, ws, inflight, limit."""

    def __init__(self, worker_id: str, limit: int = 4):
        self.worker_id = worker_id
        self.ws = FakeWS()
        self.inflight: set = set()
        self.limit = limit

    @property
    def has_capacity(self) -> bool:
        return len(self.inflight) < self.limit


class _NoopRates:
    """handle_commit calls app['rates'].incr(...); rates are cosmetic, so no-op here."""

    async def incr(self, *_a, **_k) -> None:
        return


class _NoopDispatcher:
    """Reaper calls app['dispatcher'].wake() after a requeue; irrelevant to invariants."""

    def wake(self) -> None:
        return


class _NoopRedis:
    """Reaper best-effort publishes a wakeup; swallow it in tests (Redis is optional)."""

    async def publish(self, *_a, **_k) -> None:
        return


def make_app(db: Database, coord_id: str = "test-c1") -> dict:
    """A dict with exactly the keys handle_commit / Reaper / Dispatcher read from the app."""
    from ws import WorkerRegistry

    return {
        "db": db,
        "coord_id": coord_id,
        "rates": _NoopRates(),
        "lease_ttl_ms": int(os.environ.get("LEASE_TTL_MS", "10000")),
        "dispatcher": _NoopDispatcher(),
        "redis": _NoopRedis(),
        "drop_acks_remaining": 0,
        # Dispatcher.__init__ reads app["workers"]; a real (empty) registry suffices since the
        # tests drive _default_issue directly rather than running the dispatch loop.
        "workers": WorkerRegistry(),
    }


async def new_pending_job(db: Database, key: str | None = None) -> str:
    """Insert one pending job (with its none->pending transition) and return its job_id."""
    from app import record_transition

    key = key or f"k-{uuid.uuid4().hex[:12]}"
    async with db.transaction() as conn:
        row = await conn.fetchrow(
            "INSERT INTO jobs (idempotency_key, payload) VALUES ($1, '{}'::jsonb) "
            "RETURNING job_id",
            key,
        )
        job_id = row["job_id"]
        await record_transition(conn, job_id, None, "pending", "test")
    return str(job_id)


async def lease_job(db: Database, app: dict, job_id: str, state: FakeConnState) -> int:
    """Claim+lease a pending job through the REAL dispatch lease issuer, returning its fence.

    Uses Dispatcher._default_issue so the fence/issued_at come from nextval('fence_seq') +
    db_now_ms() exactly as production does — the leases these tests assert on are genuine."""
    from dispatch import Dispatcher

    dispatcher = Dispatcher(app)
    async with db.transaction() as conn:
        await conn.execute(
            "UPDATE jobs SET state='leased' WHERE job_id = $1::uuid", job_id
        )
        fence = await dispatcher._default_issue(conn, job_id, state)
    state.inflight.add(job_id)
    return fence
