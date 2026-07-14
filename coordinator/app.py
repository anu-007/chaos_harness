"""Coordinator HTTP application.

Skeleton for one coordinator instance (c1/c2/c3). Reads its identity and dependencies
from the environment, brings up Postgres (source of truth) and Redis (optimization only)
on startup, applies the schema idempotently, and serves a plain-text /stats endpoint.

/stats is also the harness liveness probe — it must return 200 as soon as the process is up.
Dispatch, leases, commits, audit, and the other endpoints are added in later steps.
"""

import asyncio
import json
import logging
import os
import time

import redis.asyncio as aioredis
from aiohttp import web

from chaos import chaos_subscriber, handle_chaos
from db import Database, DBPartitioned
from dispatch import Dispatcher
from reaper import Reaper
from ws import WorkerRegistry, handle_ws

# Redis pub/sub channel the dispatch loop listens on for immediate wakeups.
WAKEUP_CHANNEL = "jobs:wakeup"
# Cap idempotency keys so a client can't stuff arbitrarily large values.
MAX_IDEMPOTENCY_KEY_LEN = 128


async def record_transition(conn, job_id, from_state, to_state, coordinator) -> None:
    """Append one row to the job_transitions log, stamped with DB-clock time.

    Central helper reused everywhere a job changes state so every transition is recorded
    consistently (DB time, not coordinator time) and the /audit + no-lost checks hold.
    Must run inside a caller-provided transaction/connection so it commits atomically with
    the state change that triggered it.
    """
    await conn.execute(
        """
        INSERT INTO job_transitions (job_id, from_state, to_state, at_ms, coordinator)
        VALUES ($1, $2, $3, db_now_ms(), $4)
        """,
        job_id,
        from_state,
        to_state,
        coordinator,
    )


def format_uptime(seconds: float) -> str:
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m}m"
    if m:
        return f"{m}m{s}s"
    return f"{s}s"


async def handle_stats(request: web.Request) -> web.Response:
    app = request.app
    uptime = time.monotonic() - app["start_monotonic"]
    reaper = app.get("reaper")
    max_release_ms = reaper.max_rels_latency_ms if reaper is not None else 0
    lines = [
        f"coordinator: {app['coord_id']}  uptime: {format_uptime(uptime)}  leader_term: n/a",
        f"workers: {len(app['workers'])}  max_release_latency_ms: {max_release_ms}",
    ]
    return web.Response(text="\n".join(lines) + "\n", content_type="text/plain")


async def handle_create_job(request: web.Request) -> web.Response:
    app = request.app
    db: Database = app["db"]

    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - malformed body
        return web.json_response({"error": "invalid JSON body"}, status=400)

    idempotency_key = body.get("idempotency_key")
    if not isinstance(idempotency_key, str) or not idempotency_key:
        return web.json_response(
            {"error": "idempotency_key is required and must be a non-empty string"},
            status=400,
        )
    if len(idempotency_key.encode("utf-8")) > MAX_IDEMPOTENCY_KEY_LEN:
        return web.json_response(
            {"error": f"idempotency_key exceeds {MAX_IDEMPOTENCY_KEY_LEN} bytes"},
            status=400,
        )

    payload = body.get("payload", {})
    if not isinstance(payload, dict):
        return web.json_response(
            {"error": "payload must be a JSON object"}, status=400
        )
    payload_json = json.dumps(payload)

    try:
        async with db.transaction() as conn:
            # Dedup insert. RETURNING gives job_id only when a new row is created; a
            # same-key resubmit (even concurrently on another coordinator) conflicts.
            row = await conn.fetchrow(
                """
                INSERT INTO jobs (idempotency_key, payload)
                VALUES ($1, $2::jsonb)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING job_id
                """,
                idempotency_key,
                payload_json,
            )
            created = row is not None
            if created:
                job_id = row["job_id"]
                # First transition (none)->pending, in the same transaction so the job
                # and its history commit atomically.
                await record_transition(
                    conn, job_id, None, "pending", app["coord_id"]
                )
            else:
                job_id = await conn.fetchval(
                    "SELECT job_id FROM jobs WHERE idempotency_key = $1",
                    idempotency_key,
                )
    except DBPartitioned:
        return web.json_response(
            {"error": "database partitioned; retry later"}, status=503
        )

    # Wake the dispatch loop only for genuinely new work. Wake this coordinator's loop
    # directly (immediate) and publish to Redis so peers wake too; Redis is best-effort,
    # if it is down the periodic dispatch poll still picks the job up.
    if created:
        app["dispatcher"].wake()
        try:
            await app["redis"].publish(WAKEUP_CHANNEL, str(job_id))
        except Exception as e:  # noqa: BLE001 - Redis is best-effort
            logging.warning("redis wakeup publish failed (continuing): %s", e)

    return web.json_response(
        {"job_id": str(job_id)}, status=201 if created else 200
    )


async def handle_get_job(request: web.Request) -> web.Response:
    app = request.app
    db: Database = app["db"]
    job_id = request.match_info["job_id"]

    try:
        row = await db.fetchrow(
            "SELECT job_id, state, result FROM jobs WHERE job_id = $1::uuid",
            job_id,
        )
    except DBPartitioned:
        return web.json_response(
            {"error": "database partitioned; retry later"}, status=503
        )
    except Exception:  # noqa: BLE001 - e.g. malformed uuid
        return web.json_response({"error": "invalid job_id"}, status=400)

    if row is None:
        return web.json_response({"error": "job not found"}, status=404)

    result = row["result"]
    if isinstance(result, str):
        result = json.loads(result)
    return web.json_response(
        {"job_id": str(row["job_id"]), "state": row["state"], "result": result}
    )


async def handle_audit(request: web.Request) -> web.Response:
    """Return the full audit trail for one job: its transition log, commit-attempt log, and
    lease history. This is the harness's window into correctness — it reads the three
    append-only tables (never derived/in-memory state) so every claim it verifies (no-lost,
    no-double-commit, fence monotonicity, no stale-commit) is backed by durable rows.

    Each of the three queries is independent; a DBPartitioned on any of them fails the whole
    request closed with 503 rather than returning a partial trail.
    """
    app = request.app
    db: Database = app["db"]
    job_id = request.query.get("job_id")
    if not job_id:
        return web.json_response({"error": "job_id query param required"}, status=400)

    try:
        transitions = await db.fetch(
            """
            SELECT from_state, to_state, at_ms, coordinator
            FROM job_transitions
            WHERE job_id = $1::uuid
            ORDER BY at_ms, id
            """,
            job_id,
        )
        commits = await db.fetch(
            """
            SELECT accepted, fence, worker, at_ms
            FROM commits
            WHERE job_id = $1::uuid
            ORDER BY at_ms, id
            """,
            job_id,
        )
        leases = await db.fetch(
            """
            SELECT fence, worker, issued_at_ms, expired_at_ms
            FROM leases
            WHERE job_id = $1::uuid
            ORDER BY issued_at_ms, fence
            """,
            job_id,
        )
    except DBPartitioned:
        return web.json_response(
            {"error": "database partitioned; retry later"}, status=503
        )
    except Exception:  # noqa: BLE001 - e.g. malformed uuid
        return web.json_response({"error": "invalid job_id"}, status=400)

    return web.json_response(
        {
            "transitions": [
                {
                    "from": r["from_state"],
                    "to": r["to_state"],
                    "at_ms": r["at_ms"],
                    "coordinator": r["coordinator"],
                }
                for r in transitions
            ],
            "commits": [
                {
                    "accepted": r["accepted"],
                    "fence": r["fence"],
                    "worker": r["worker"],
                    "at_ms": r["at_ms"],
                }
                for r in commits
            ],
            "lease_history": [
                {
                    "fence": r["fence"],
                    "worker": r["worker"],
                    "issued_at_ms": r["issued_at_ms"],
                    "expired_at_ms": r["expired_at_ms"],
                }
                for r in leases
            ],
        }
    )


async def on_startup(app: web.Application) -> None:
    # Postgres is required — failing to connect or apply the schema should crash the boot.
    db = Database.from_env()
    await db.connect()
    await db.apply_schema()
    app["db"] = db

    # Redis is an optimization layer only; a failed ping must not stop the coordinator.
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    app["redis"] = aioredis.from_url(redis_url, decode_responses=True)
    try:
        await app["redis"].ping()
        logging.info("redis connected: %s", redis_url)
    except Exception as e:  # noqa: BLE001 - Redis is best-effort
        logging.warning("redis ping failed at startup (continuing): %s", e)

    # Start the dispatch loop now that DB, Redis, and the worker registry are ready.
    dispatcher = Dispatcher(app)
    dispatcher.start()
    app["dispatcher"] = dispatcher

    # Start the reaper (needs dispatcher wired so it can wake dispatch after requeueing).
    reaper = Reaper(app)
    reaper.start()
    app["reaper"] = reaper

    # Subscribe to peer chaos broadcasts so a fault injected at any coordinator degrades the
    # whole system (worker WS is pinned to one coordinator; faults must reach all).
    app["chaos_sub_task"] = asyncio.create_task(chaos_subscriber(app))

    logging.info("coordinator %s started", app["coord_id"])


async def on_cleanup(app: web.Application) -> None:
    chaos_sub = app.get("chaos_sub_task")
    if chaos_sub is not None:
        chaos_sub.cancel()
        await asyncio.gather(chaos_sub, return_exceptions=True)
    reaper = app.get("reaper")
    if reaper is not None:
        await reaper.stop()
    dispatcher = app.get("dispatcher")
    if dispatcher is not None:
        await dispatcher.stop()
    db = app.get("db")
    if db is not None:
        await db.close()
    r = app.get("redis")
    if r is not None:
        await r.aclose()


def make_app() -> web.Application:
    app = web.Application()
    app["coord_id"] = os.environ.get("COORD_ID", "c?")
    app["lease_ttl_ms"] = int(os.environ.get("LEASE_TTL_MS", "10000"))
    app["start_monotonic"] = time.monotonic()
    # drop_acks fault counter: acks to suppress before resuming normal replies (Step 17).
    app["drop_acks_remaining"] = 0
    # Per-process registry of workers connected to THIS coordinator.
    app["workers"] = WorkerRegistry()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.add_routes(
        [
            web.get("/stats", handle_stats),
            web.post("/jobs", handle_create_job),
            web.get("/jobs/{job_id}", handle_get_job),
            web.get("/audit", handle_audit),
            web.post("/chaos", handle_chaos),
            web.get("/ws", handle_ws),
        ]
    )
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    web.run_app(make_app(), host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
