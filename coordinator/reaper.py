"""Per-coordinator lease reaper.

A periodic task (~1s) that finds leases whose deadline has passed on the DB clock and are
still live, and returns their jobs to the queue so another worker can pick them up. This is
what makes a SIGKILLed worker's in-flight job recover: its lease stops being renewed, the
deadline passes, the reaper expires the lease and flips the job leased->pending, and the
next dispatch round issues a NEW lease with a strictly higher fence. The dead worker's late
commit then carries a stale fence and is rejected by the commit handler (no double-commit).

Every coordinator runs its own reaper; the reap is a single atomic UPDATE per lease guarded
so two coordinators (or a reaper racing an in-flight commit) can never both act on the same
lease. All time comparisons use db_now_ms() so clock_skew cannot make a live lease look
expired or an expired one look live.

Fail-closed: while partition_db is active db.transaction()/db.fetch raise DBPartitioned and
the reap round is skipped rather than acting on a stale view.
"""

import asyncio
import logging

from db import DBPartitioned

REAP_INTERVAL_S = 1.0
WAKEUP_CHANNEL = "jobs:wakeup"


class Reaper:
    def __init__(self, app):
        self.app = app
        self.coord_id = app["coord_id"]
        self.db = app["db"]
        self._task: asyncio.Task | None = None
        # Largest observed (db_now_ms - expires_at_ms) at reap time, in ms. Exposed via
        # /stats as an operator-visible re-lease latency signal.
        self.max_rels_latency_ms = 0

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"reaper-{self.coord_id}")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(REAP_INTERVAL_S)
            try:
                await self._reap_round()
            except DBPartitioned:
                # Fail-closed: skip this round; chaos has partitioned the DB.
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - keep the reaper alive
                logging.warning(
                    "coordinator %s: reap round error: %s", self.coord_id, e
                )

    async def _reap_round(self) -> None:
        # Candidate live-but-overdue leases. SKIP LOCKED so concurrent reapers on other
        # coordinators divide the work instead of blocking each other.
        rows = await self.db.fetch(
            """
            SELECT id, job_id, fence, expires_at_ms
            FROM leases
            WHERE expired_at_ms IS NULL AND expires_at_ms < db_now_ms()
            ORDER BY expires_at_ms
            LIMIT 100
            """
        )
        for row in rows:
            await self._reap_one(row["id"], row["job_id"], row["fence"])

    async def _reap_one(self, lease_id, job_id, fence) -> None:
        """Expire one overdue lease and requeue its job, atomically. Returns quietly if the
        lease was already handled (committed, renewed, or reaped by a peer)."""
        from app import record_transition

        async with self.db.transaction() as conn:
            # Re-check under a row lock: only expire if STILL live and STILL overdue. A
            # heartbeat that renewed it between the scan and now, or a peer reaper, makes
            # this a no-op.
            lease = await conn.fetchrow(
                """
                UPDATE leases SET expired_at_ms = db_now_ms()
                WHERE id = $1 AND expired_at_ms IS NULL AND expires_at_ms < db_now_ms()
                RETURNING db_now_ms() - expires_at_ms AS overdue_ms
                """,
                lease_id,
            )
            if lease is None:
                return

            # Requeue the job ONLY if it is still leased and this is its latest fence, and
            # it has no accepted commit. Guards against flipping a job that already
            # succeeded (commit won the race) or was superseded by a newer lease.
            requeued = await conn.fetchval(
                """
                UPDATE jobs SET state='pending', updated_at=now()
                WHERE job_id = $1::uuid AND state='leased'
                  AND $2 = (SELECT max(fence) FROM leases WHERE job_id = $1::uuid)
                  AND NOT EXISTS (
                      SELECT 1 FROM commits WHERE job_id = $1::uuid AND accepted
                  )
                RETURNING job_id
                """,
                job_id,
                fence,
            )
            if requeued is not None:
                await record_transition(conn, job_id, "leased", "pending", self.coord_id)
                overdue = int(lease["overdue_ms"] or 0)
                if overdue > self.max_rels_latency_ms:
                    self.max_rels_latency_ms = overdue
                logging.info(
                    "coordinator %s: reaped lease %s job %s (fence=%s, overdue=%dms) -> pending",
                    self.coord_id,
                    lease_id,
                    job_id,
                    fence,
                    overdue,
                )

        # Committed the expiry+requeue. Wake dispatchers (local + peers) to re-lease fast.
        if requeued is not None:
            self.app["dispatcher"].wake()
            try:
                await self.app["redis"].publish(WAKEUP_CHANNEL, str(job_id))
            except Exception as e:  # noqa: BLE001 - Redis is best-effort
                logging.debug(
                    "coordinator %s: reaper wakeup publish failed: %s", self.coord_id, e
                )
