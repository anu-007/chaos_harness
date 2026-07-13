"""Coordinator HTTP application.

Skeleton for one coordinator instance (c1/c2/c3). Reads its identity and dependencies
from the environment, brings up Postgres (source of truth) and Redis (optimization only)
on startup, applies the schema idempotently, and serves a plain-text /stats endpoint.

/stats is also the harness liveness probe — it must return 200 as soon as the process is up.
Dispatch, leases, commits, audit, and the other endpoints are added in later steps.
"""

import logging
import os
import time

import redis.asyncio as aioredis
from aiohttp import web

from db import Database


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
    lines = [
        f"coordinator: {app['coord_id']}  uptime: {format_uptime(uptime)}  leader_term: n/a",
    ]
    return web.Response(text="\n".join(lines) + "\n", content_type="text/plain")


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

    logging.info("coordinator %s started", app["coord_id"])


async def on_cleanup(app: web.Application) -> None:
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
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.add_routes([web.get("/stats", handle_stats)])
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    web.run_app(make_app(), host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
