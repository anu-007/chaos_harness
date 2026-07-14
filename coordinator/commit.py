"""Fenced, idempotent, fail-closed commit handler.

A worker sends commit{job_id,fence,result} when it finishes a job. This is the ONLY place
a job becomes terminal, and it must hold three invariants from chaos_harness.py::_verify:

  * no stale commit — accept only if `fence` is the LATEST lease for the job and that lease
    is still live (not expired, deadline not passed). A worker resurrected after its lease
    was reaped and re-leased carries a stale fence and is rejected.
  * exactly-once accept — the `one_accept` partial unique index (commits WHERE accepted)
    means at most one accepted row per job. A replayed commit (drop_acks fault) re-acks
    without writing a second result or a second leased->succeeded transition.
  * fail-closed — while partition_db is active db.transaction() raises DBPartitioned; we
    send NO ack, so the worker retries once the partition clears rather than losing the job.

All timing uses db_now_ms() (DB clock), so clock_skew on a worker or coordinator cannot
make an expired lease look live or vice versa.
"""

import json
import logging


async def handle_commit(app, state, data: dict) -> None:
    db = app["db"]
    coord_id = app["coord_id"]
    job_id = data.get("job_id")
    fence = data.get("fence")
    result = data.get("result")

    if job_id is None or not isinstance(fence, int):
        logging.warning(
            "coordinator %s: malformed commit from %s: %r",
            coord_id,
            state.worker_id,
            data,
        )
        return

    result_json = json.dumps(result if result is not None else {})

    # Fail-closed: a DBPartitioned here propagates to the WS route, which swallows it and
    # sends nothing, so the worker retries the commit after the partition lifts.
    async with db.transaction() as conn:
        # Latest lease for this job + its liveness, evaluated on the DB clock.
        latest = await conn.fetchrow(
            """
            SELECT fence, expired_at_ms, expires_at_ms, db_now_ms() AS now_ms
            FROM leases
            WHERE job_id = $1::uuid
            ORDER BY fence DESC
            LIMIT 1
            """,
            job_id,
        )

        # Idempotent replay: an accepted commit already exists for this job. Re-ack the
        # same (valid) fence without writing anything again; reject a different fence.
        already = await conn.fetchval(
            "SELECT fence FROM commits WHERE job_id = $1::uuid AND accepted",
            job_id,
        )
        if already is not None:
            accepted = already == fence
            await _finish(app, state, conn, job_id, fence, accepted, replay=True)
            return

        valid = (
            latest is not None
            and latest["fence"] == fence
            and latest["expired_at_ms"] is None
            and latest["now_ms"] < latest["expires_at_ms"]
        )

        if valid:
            # accepted=true guarded by the one_accept unique index. DO NOTHING makes a
            # racing duplicate in the same instant a no-op rather than an error.
            inserted = await conn.fetchval(
                """
                INSERT INTO commits (job_id, fence, worker, accepted, at_ms)
                VALUES ($1::uuid, $2, $3, true, db_now_ms())
                ON CONFLICT (job_id) WHERE accepted DO NOTHING
                RETURNING id
                """,
                job_id,
                fence,
                state.worker_id,
            )
            if inserted is None:
                # Lost the accept race to a concurrent commit; treat as replay re-ack.
                await _finish(app, state, conn, job_id, fence, True, replay=True)
                return
            await conn.execute(
                "UPDATE jobs SET result = $2::jsonb, state='succeeded', updated_at=now() "
                "WHERE job_id = $1::uuid",
                job_id,
                result_json,
            )
            from app import record_transition

            await record_transition(conn, job_id, "leased", "succeeded", coord_id)
            await _finish(app, state, conn, job_id, fence, True, replay=False)
        else:
            # Stale or expired fence: record the rejected attempt for /audit and tell the
            # worker to stop. The job stays leased; the reaper will re-lease it.
            await conn.execute(
                """
                INSERT INTO commits (job_id, fence, worker, accepted, at_ms)
                VALUES ($1::uuid, $2, $3, false, db_now_ms())
                """,
                job_id,
                fence,
                state.worker_id,
            )
            await _finish(app, state, conn, job_id, fence, False, replay=False)


async def _finish(app, state, conn, job_id, fence, accepted, replay: bool) -> None:
    """Free the worker's capacity slot and reply ack / commit_rejected."""
    state.inflight.discard(job_id)
    msg = {"type": "ack" if accepted else "commit_rejected", "job_id": str(job_id)}
    try:
        await state.ws.send_json(msg)
    except Exception as e:  # noqa: BLE001 - worker gone; state is already durable
        logging.warning(
            "coordinator %s: could not send %s for job %s to %s: %s",
            app["coord_id"],
            msg["type"],
            job_id,
            state.worker_id,
            e,
        )
    logging.info(
        "coordinator %s: commit %s job %s fence=%s from %s%s",
        app["coord_id"],
        "accepted" if accepted else "rejected",
        job_id,
        fence,
        state.worker_id,
        " (replay)" if replay else "",
    )
