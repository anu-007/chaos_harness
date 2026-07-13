"""Coordinator database layer.

Wraps an asyncpg pool. Postgres is the single source of truth for jobs, idempotency,
the global fence sequence, and the append-only audit logs.

Every query path passes through a partition gate. While the `partition_db` chaos fault is
active this coordinator's DB access fails closed (raises DBPartitioned) instead of silently
proceeding, so dispatch/commit/reaper stop rather than corrupt state.
"""

import contextlib
import os
import time

import asyncpg

# Resolve db/schema.sql relative to the repo root by default (repo/coordinator/db.py).
_DEFAULT_SCHEMA = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "db", "schema.sql"
)


class DBPartitioned(Exception):
    """Raised while the partition_db chaos fault has gated this coordinator's DB."""


class Database:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None
        # Real-time (monotonic) deadline until which the DB is treated as partitioned.
        self._partition_until_ms: float = 0.0

    @classmethod
    def from_env(cls) -> "Database":
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            raise RuntimeError(
                "DATABASE_URL is not set. Point it at Postgres, e.g. "
                "'postgres://postgres:postgres@localhost:5432/farlabs'. "
                "The docker-compose stack (make up) sets this automatically."
            )
        return cls(dsn)

    async def connect(self, *, min_size: int = 2, max_size: int = 10) -> None:
        self._pool = await asyncpg.create_pool(
            self._dsn, min_size=min_size, max_size=max_size
        )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def apply_schema(self, schema_path: str | None = None) -> None:
        path = schema_path or os.environ.get("SCHEMA_PATH", _DEFAULT_SCHEMA)
        with open(path) as f:
            sql = f.read()
        async with self._pool.acquire() as conn:
            await conn.execute(sql)

    # --- partition_db fault control -------------------------------------------------

    def partition(self, ms: int) -> None:
        """Refuse all DB access for the next `ms` milliseconds of real time."""
        self._partition_until_ms = self._monotonic_ms() + ms

    def is_partitioned(self) -> bool:
        return self._monotonic_ms() < self._partition_until_ms

    @staticmethod
    def _monotonic_ms() -> float:
        return time.monotonic() * 1000.0

    def _gate(self) -> None:
        if self._pool is None:
            raise RuntimeError("database pool not connected")
        if self.is_partitioned():
            raise DBPartitioned("database connection partitioned by chaos fault")

    # --- query helpers (each gated) -------------------------------------------------

    async def execute(self, query: str, *args, timeout: float | None = None):
        self._gate()
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args, timeout=timeout)

    async def fetch(self, query: str, *args, timeout: float | None = None):
        self._gate()
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args, timeout=timeout)

    async def fetchrow(self, query: str, *args, timeout: float | None = None):
        self._gate()
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args, timeout=timeout)

    async def fetchval(self, query: str, *args, timeout: float | None = None):
        self._gate()
        async with self._pool.acquire() as conn:
            return await conn.fetchval(query, *args, timeout=timeout)

    @contextlib.asynccontextmanager
    async def transaction(self):
        """Acquire a connection and open a transaction for multi-statement atomic ops
        (claim+lease, fenced commit). Gated once at entry: a coordinator whose DB is
        partitioned cannot start new work."""
        self._gate()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                yield conn
