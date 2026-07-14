"""Fenced, exactly-once commit invariants — driving the REAL handle_commit.

Maps to chaos_harness.py::_verify:
  * exactly-once accept: a second accepted commit is blocked (one_accept unique index)
  * no stale commit: a commit on an expired/reaped lease is rejected
  * drop_acks retry stays single-accepted: suppressed ack + worker retry = one accepted row
"""

from commit import handle_commit
from conftest import FakeConnState, lease_job, make_app, new_pending_job


async def _accepted_count(db, job_id: str) -> int:
    return await db.fetchval(
        "SELECT count(*) FROM commits WHERE job_id = $1::uuid AND accepted", job_id
    )


async def test_second_accepted_commit_is_blocked(db):
    """A valid commit succeeds and turns the job 'succeeded'; a SECOND commit (same live
    fence) is an idempotent replay — it re-acks but records no new accepted row, so exactly
    one accepted commit ever exists (the one_accept partial unique index)."""
    app = make_app(db)
    state = FakeConnState("w-A")
    job_id = await new_pending_job(db)
    fence = await lease_job(db, app, job_id, state)

    await handle_commit(app, state, {"job_id": job_id, "fence": fence, "result": {"n": 1}})
    assert state.ws.sent[-1] == {"type": "ack", "job_id": job_id}
    assert await _accepted_count(db, job_id) == 1
    assert await db.fetchval(
        "SELECT state FROM jobs WHERE job_id = $1::uuid", job_id
    ) == "succeeded"

    # Second commit with the same (still-latest) fence: replay -> re-ack, still ONE accept.
    await handle_commit(app, state, {"job_id": job_id, "fence": fence, "result": {"n": 2}})
    assert state.ws.sent[-1] == {"type": "ack", "job_id": job_id}
    assert await _accepted_count(db, job_id) == 1
    # The result of the FIRST accept is preserved; the replay wrote nothing.
    result = await db.fetchval(
        "SELECT result FROM jobs WHERE job_id = $1::uuid", job_id
    )
    assert '"n": 1' in result or result == {"n": 1} or result == '{"n": 1}'


async def test_commit_on_expired_lease_is_rejected(db):
    """A worker whose lease was expired (reaped) commits with its now-stale fence: the commit
    must be REJECTED (accepted=false) and the job must NOT become succeeded. This is the
    no-double-commit guard for a resurrected worker."""
    app = make_app(db)
    state = FakeConnState("w-B")
    job_id = await new_pending_job(db)
    fence = await lease_job(db, app, job_id, state)

    # Simulate the reaper having expired this lease (deadline passed, expired_at_ms stamped).
    await db.execute(
        "UPDATE leases SET expired_at_ms = db_now_ms(), expires_at_ms = db_now_ms() - 1 "
        "WHERE job_id = $1::uuid AND fence = $2",
        job_id,
        fence,
    )

    await handle_commit(app, state, {"job_id": job_id, "fence": fence, "result": {"x": 1}})

    assert state.ws.sent[-1] == {"type": "commit_rejected", "job_id": job_id}
    assert await _accepted_count(db, job_id) == 0
    # A rejected attempt is still logged (for /audit) but the job is not terminal via commit.
    rejected = await db.fetchval(
        "SELECT count(*) FROM commits WHERE job_id = $1::uuid AND NOT accepted", job_id
    )
    assert rejected == 1
    assert await db.fetchval(
        "SELECT state FROM jobs WHERE job_id = $1::uuid", job_id
    ) == "leased"


async def test_drop_acks_retry_stays_single_accepted(db):
    """drop_acks fault: the coordinator persists the accept but SUPPRESSES the ack, so the
    worker (hearing nothing) retries the same commit. The retry is an idempotent replay, so
    the job is committed exactly once and the worker finally gets its ack."""
    app = make_app(db)
    app["drop_acks_remaining"] = 1  # suppress exactly the next accepted ack
    state = FakeConnState("w-C")
    job_id = await new_pending_job(db)
    fence = await lease_job(db, app, job_id, state)

    # First commit: persisted + accepted, but the ack is dropped -> worker sees no frame.
    before = len(state.ws.sent)
    await handle_commit(app, state, {"job_id": job_id, "fence": fence, "result": {"ok": 1}})
    assert len(state.ws.sent) == before, "ack should have been suppressed by drop_acks"
    assert app["drop_acks_remaining"] == 0
    assert await _accepted_count(db, job_id) == 1  # already durably committed

    # Worker retries the identical commit: idempotent replay -> re-ack, STILL one accept.
    await handle_commit(app, state, {"job_id": job_id, "fence": fence, "result": {"ok": 1}})
    assert state.ws.sent[-1] == {"type": "ack", "job_id": job_id}
    assert await _accepted_count(db, job_id) == 1
