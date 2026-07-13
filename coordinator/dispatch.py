"""Per-coordinator dispatch loop.

One async task per coordinator. It wakes on a Redis pub/sub wakeup (best-effort) or a
short poll fallback, then tries to hand pending work to locally-connected workers that
have spare capacity. Claiming is done with FOR UPDATE SKIP LOCKED so two coordinators can
never grab the same job.

Step 10 implements the claim (pending -> leased) atomically with its transition. Issuing
the fenced lease row and pushing the dispatch over the worker WS is layered in via the
`lease_issuer` callback (wired in Step 11) so claim + lease live in ONE transaction: a
SIGKILLed coordinator leaves a job either fully-pending or fully-leased, never orphaned
mid-claim.
"""

import asyncio
import logging

from db import DBPartitioned

# Poll fallback so losing Redis only adds latency, never loses jobs.
POLL_INTERVAL_S = 0.2
WAKEUP_CHANNEL = "jobs:wakeup"


class Dispatcher:
    def __init__(self, app, lease_issuer=None):
        self.app = app
        self.coord_id = app["coord_id"]
        self.db = app["db"]
        self.registry = app["workers"]
        # Callback(conn, job_id, payload, worker_state) -> awaitable, run inside the claim
        # transaction to issue the lease + push dispatch. Defaults to transition-only.
        self._lease_issuer = lease_issuer or self._default_issue
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._sub_task: asyncio.Task | None = None
        # pause_dispatch fault sets this; loop no-ops while now < deadline (Step 16).
        self.dispatch_paused_until_ms = 0.0

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"dispatch-{self.coord_id}")
        self._sub_task = asyncio.create_task(
            self._subscribe(), name=f"dispatch-sub-{self.coord_id}"
        )

    async def stop(self) -> None:
        for t in (self._task, self._sub_task):
            if t is not None:
                t.cancel()
        for t in (self._task, self._sub_task):
            if t is not None:
                await asyncio.gather(t, return_exceptions=True)

    def wake(self) -> None:
        self._wake.set()

    def _paused(self) -> bool:
        import time

        return time.monotonic() * 1000.0 < self.dispatch_paused_until_ms

    async def _subscribe(self) -> None:
        """Best-effort Redis wakeup subscription; failures fall back to polling."""
        try:
            pubsub = self.app["redis"].pubsub()
            await pubsub.subscribe(WAKEUP_CHANNEL)
            async for _msg in pubsub.listen():
                self.wake()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - Redis optional; poll still runs
            logging.warning(
                "coordinator %s: dispatch redis subscribe failed (polling only): %s",
                self.coord_id,
                e,
            )

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=POLL_INTERVAL_S)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            if self._paused():
                continue
            try:
                await self._dispatch_round()
            except DBPartitioned:
                # Fail-closed: skip this round; the DB is partitioned by chaos.
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - keep the loop alive
                logging.warning(
                    "coordinator %s: dispatch round error: %s", self.coord_id, e
                )

    async def _dispatch_round(self) -> None:
        # Give each worker with spare capacity one job per round; a wakeup re-runs quickly.
        for state in self.registry.all():
            while state.has_capacity:
                claimed = await self._claim_one(state)
                if not claimed:
                    break

    async def _claim_one(self, state) -> bool:
        """Atomically claim one pending job for `state` and issue its lease. Returns True
        if a job was claimed, False if none were available."""
        async with self.db.transaction() as conn:
            row = await conn.fetchrow(
                """
                UPDATE jobs SET state='leased', updated_at=now()
                WHERE job_id = (
                    SELECT job_id FROM jobs
                    WHERE state='pending'
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING job_id, payload
                """
            )
            if row is None:
                return False
            job_id = row["job_id"]
            await self._lease_issuer(conn, job_id, row["payload"], state)
            logging.info(
                "coordinator %s: claimed job %s for worker %s",
                self.coord_id,
                job_id,
                state.worker_id,
            )
        return True

    async def _default_issue(self, conn, job_id, payload, state) -> None:
        """Step 10 default: record the pending->leased transition. Replaced in Step 11
        with fence issuance + WS dispatch push."""
        from app import record_transition

        await record_transition(conn, job_id, "pending", "leased", self.coord_id)
