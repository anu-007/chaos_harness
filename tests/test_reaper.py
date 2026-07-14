"""Reaper recovery invariant — driving the REAL Reaper._reap_round.

Maps to chaos_harness.py::_verify: a SIGKILLed worker's overdue lease is reaped, the job
returns to pending, and the NEXT lease carries a strictly higher fence. The dead worker's
late commit (old fence) is then rejected — the mechanism behind no-double-commit on recovery.
"""

from commit import handle_commit
from conftest import FakeConnState, lease_job, make_app, new_pending_job
from reaper import Reaper


async def test_reaper_requeues_with_strictly_higher_fence(db):
    app = make_app(db)
    worker_a = FakeConnState("w-dead")
    job_id = await new_pending_job(db)
    fence1 = await lease_job(db, app, job_id, worker_a)

    # Force this lease overdue on the DB clock (worker died, no heartbeats renewing it).
    await db.execute(
        "UPDATE leases SET expires_at_ms = db_now_ms() - 1 "
        "WHERE job_id = $1::uuid AND fence = $2",
        job_id,
        fence1,
    )

    # Run one real reap round: expire the lease + requeue the job leased->pending.
    reaper = Reaper(app)
    await reaper._reap_round()

    assert await db.fetchval(
        "SELECT state FROM jobs WHERE job_id = $1::uuid", job_id
    ) == "pending"
    assert await db.fetchval(
        "SELECT expired_at_ms IS NOT NULL FROM leases WHERE fence = $1", fence1
    ) is True

    # Re-lease to a fresh worker: the new fence MUST be strictly greater.
    worker_b = FakeConnState("w-new")
    fence2 = await lease_job(db, app, job_id, worker_b)
    assert fence2 > fence1, f"re-lease fence {fence2} must exceed reaped fence {fence1}"

    # The DEAD worker's late commit carries the stale fence1 -> rejected, no accept.
    await handle_commit(
        app, worker_a, {"job_id": job_id, "fence": fence1, "result": {"stale": True}}
    )
    assert worker_a.ws.sent[-1] == {"type": "commit_rejected", "job_id": job_id}
    assert await db.fetchval(
        "SELECT count(*) FROM commits WHERE job_id = $1::uuid AND accepted", job_id
    ) == 0

    # The NEW worker's commit on fence2 is accepted -> exactly one accept overall.
    await handle_commit(
        app, worker_b, {"job_id": job_id, "fence": fence2, "result": {"ok": True}}
    )
    assert worker_b.ws.sent[-1] == {"type": "ack", "job_id": job_id}
    assert await db.fetchval(
        "SELECT count(*) FROM commits WHERE job_id = $1::uuid AND accepted", job_id
    ) == 1
    assert await db.fetchval(
        "SELECT state FROM jobs WHERE job_id = $1::uuid", job_id
    ) == "succeeded"
