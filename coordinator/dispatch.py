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
# How long to wait before re-attempting the Redis wakeup subscription after it drops, so a
# Redis outage degrades to poll-only latency and RESTORING Redis makes wakeups fast again
# without a process restart.
SUB_RETRY_S = 1.0


class Dispatcher:
    def __init__(self, app, lease_issuer=None):
        self.app = app
        self.coord_id = app["coord_id"]
        self.db = app["db"]
        self.registry = app["workers"]
        # Callback(conn, job_id, worker_state) -> awaitable[int fence], run inside the claim
        # transaction to insert the fenced lease row + record the transition. The WS push
        # happens in _claim_one after commit. Overridable for tests.
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
        """Best-effort Redis wakeup subscription, retried forever on failure.

        A wakeup only makes dispatch FASTER — the 200ms poll in _run() guarantees no job is
        ever lost if Redis is down. But losing Redis must be latency-only AND recoverable:
        if the subscription drops (Redis killed), we log once, wait SUB_RETRY_S, and try
        again, so RESTORING Redis re-establishes fast wakeups without a process restart.
        """
        while True:
            try:
                pubsub = self.app["redis"].pubsub()
                await pubsub.subscribe(WAKEUP_CHANNEL)
                logging.info(
                    "coordinator %s: dispatch wakeup subscription active", self.coord_id
                )
                async for _msg in pubsub.listen():
                    self.wake()
                # listen() returned without error (channel closed): fall through to retry.
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - Redis optional; poll still runs
                logging.warning(
                    "coordinator %s: dispatch redis subscribe dropped (polling only, "
                    "retrying in %.1fs): %s",
                    self.coord_id,
                    SUB_RETRY_S,
                    e,
                )
            await asyncio.sleep(SUB_RETRY_S)

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
        """Atomically claim one pending job for `state`, issue its fenced lease, then push
        the dispatch to the worker. Returns True if a job was claimed, False if none were
        available.

        Claim + lease-row INSERT + transition all commit in ONE transaction, so a SIGKILLed
        coordinator leaves a job either fully-pending or fully-leased, never orphaned
        mid-claim. The WS push happens only AFTER that transaction commits, so a worker can
        never receive a dispatch for a lease that later rolled back.
        """
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
            payload = row["payload"]
            fence = await self._lease_issuer(conn, job_id, state)

        # Committed. Reserve capacity and push the dispatch to the worker. If the send
        # fails the worker is gone; drop the reservation and let the reaper re-lease.
        # inflight is keyed by the string job_id so the commit handler (which sees the id
        # as a string over JSON) can discard the same key.
        job_id_str = str(job_id)
        state.inflight.add(job_id_str)
        try:
            await state.ws.send_json(
                {
                    "type": "dispatch",
                    "job_id": job_id_str,
                    "payload": payload,
                    "fence": fence,
                }
            )
        except Exception as e:  # noqa: BLE001 - worker vanished after commit
            state.inflight.discard(job_id_str)
            logging.warning(
                "coordinator %s: dispatch push to worker %s failed for job %s "
                "(lease %d will be reaped): %s",
                self.coord_id,
                state.worker_id,
                job_id,
                fence,
                e,
            )
            return True
        logging.info(
            "coordinator %s: leased job %s to worker %s (fence=%d)",
            self.coord_id,
            job_id,
            state.worker_id,
            fence,
        )
        return True

    async def _default_issue(self, conn, job_id, state) -> int:
        """Issue the fenced lease for a just-claimed job, inside the claim transaction.

        Inserts the lease row (fence from nextval('fence_seq'), issued/expiry stamped from
        the DB clock via db_now_ms) and records the pending->leased transition. Returns the
        allocated fence token so the caller can push it to the worker after commit.
        """
        from app import record_transition

        ttl_ms = self.app["lease_ttl_ms"]
        fence = await conn.fetchval(
            """
            INSERT INTO leases (job_id, fence, worker, issued_at_ms, expires_at_ms)
            VALUES ($1, nextval('fence_seq'), $2, db_now_ms(), db_now_ms() + $3)
            RETURNING fence
            """,
            job_id,
            state.worker_id,
            ttl_ms,
        )
        await record_transition(conn, job_id, "pending", "leased", self.coord_id)
        return fence
