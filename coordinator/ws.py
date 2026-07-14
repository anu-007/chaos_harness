"""Coordinator-side worker WebSocket registry.

Each worker holds one persistent WS to a single coordinator (pinned by the LB for the
life of the connection). This module accepts that connection, reads the initial
`register`, and tracks the worker in a per-process registry the dispatch loop consults to
find locally-connected workers with spare capacity.

Inbound `heartbeat`/`commit` messages are routed here but their handlers are added in
later steps (lease renewal, fenced commit). On disconnect the worker is removed from the
registry; eager lease expiry for fast re-lease is wired once leases exist (Steps 11-15).
"""

import json
import logging

from aiohttp import web, WSMsgType

from commit import handle_commit, handle_heartbeat
from db import DBPartitioned

# Redis pub/sub channel for runtime per-worker concurrency changes. A worker's WS is pinned
# to ONE coordinator, but POST /workers/{id}/concurrency can land on ANY coordinator via the
# LB, so the request is broadcast; only the coordinator holding that worker acts on it.
CONCURRENCY_CHANNEL = "workers:concurrency"


class ConnState:
    """One connected worker's live state on this coordinator."""

    __slots__ = ("ws", "worker_id", "limit", "inflight")

    def __init__(self, ws: web.WebSocketResponse, worker_id: str, limit: int):
        self.ws = ws
        self.worker_id = worker_id
        self.limit = limit
        # job_ids currently dispatched to this worker (populated by the dispatch loop).
        self.inflight: set = set()

    @property
    def has_capacity(self) -> bool:
        return len(self.inflight) < self.limit


class WorkerRegistry:
    """Per-coordinator map of worker_id -> ConnState for locally-connected workers."""

    def __init__(self):
        self._by_id: dict[str, ConnState] = {}

    def add(self, state: ConnState) -> None:
        # A reconnect with the same id replaces the stale entry.
        self._by_id[state.worker_id] = state

    def remove(self, worker_id: str) -> None:
        self._by_id.pop(worker_id, None)

    def get(self, worker_id: str) -> ConnState | None:
        return self._by_id.get(worker_id)

    def all(self) -> list[ConnState]:
        return list(self._by_id.values())

    def __len__(self) -> int:
        return len(self._by_id)


async def apply_set_concurrency(app: web.Application, worker_id: str, n: int) -> bool:
    """If `worker_id` is connected to THIS coordinator, forward set_concurrency{n} over its
    WS and update its registry limit so the dispatch loop respects the new ceiling at once.
    Returns True if this coordinator holds the worker (and the message was sent).

    The worker's WS is pinned to a single coordinator, so a runtime concurrency change made
    at any coordinator is broadcast to all; only the one holding the worker acts.
    """
    registry: WorkerRegistry = app["workers"]
    state = registry.get(worker_id)
    if state is None:
        return False
    state.limit = n
    try:
        await state.ws.send_json({"type": "set_concurrency", "n": n})
    except Exception as e:  # noqa: BLE001 - worker WS dying; it will re-register
        logging.warning(
            "coordinator %s: set_concurrency send to %s failed: %s",
            app["coord_id"],
            worker_id,
            e,
        )
        return True
    logging.info(
        "coordinator %s: set worker %s concurrency to %d (runtime)",
        app["coord_id"],
        worker_id,
        n,
    )
    return True


async def handle_set_concurrency(request: web.Request) -> web.Response:
    """POST /workers/{id}/concurrency {n} — reconfigure a worker's concurrency at runtime.

    The worker's WS is pinned to a single coordinator, but this request can hit any
    coordinator via the LB. So apply locally (a no-op unless THIS coordinator holds the
    worker) and broadcast over Redis; the coordinator holding the worker forwards
    set_concurrency{n} over its WS and updates the worker's registry limit, which the
    dispatch loop respects immediately.
    """
    app = request.app
    worker_id = request.match_info.get("id")
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - malformed JSON
        return web.json_response({"error": "invalid JSON body"}, status=400)

    n = body.get("n")
    if not isinstance(n, int) or n < 0:
        return web.json_response(
            {"error": "n is required and must be a non-negative integer"}, status=400
        )

    # Apply locally first so it works even if Redis is down (and returns True if this
    # coordinator happens to hold the worker).
    local = await apply_set_concurrency(app, worker_id, n)

    # Broadcast so whichever coordinator holds the worker acts. The originator applies
    # locally above and skips its own message in the subscriber.
    try:
        await app["redis"].publish(
            CONCURRENCY_CHANNEL,
            json.dumps({"worker_id": worker_id, "n": n, "from": app["coord_id"]}),
        )
    except Exception as e:  # noqa: BLE001 - Redis is best-effort
        logging.warning(
            "coordinator %s: concurrency broadcast failed (applied locally only): %s",
            app["coord_id"],
            e,
        )

    return web.json_response({"ok": True, "worker_id": worker_id, "n": n, "local": local})


async def concurrency_subscriber(app: web.Application) -> None:
    """Background task: apply concurrency changes broadcast by peer coordinators. The
    originator already applied it locally and also receives its own publish; skip
    self-originated messages to avoid a redundant re-send over the worker's WS."""
    try:
        pubsub = app["redis"].pubsub()
        await pubsub.subscribe(CONCURRENCY_CHANNEL)
        async for msg in pubsub.listen():
            if msg.get("type") != "message":
                continue
            try:
                payload = json.loads(msg["data"])
            except Exception:  # noqa: BLE001 - ignore malformed peer message
                continue
            if payload.get("from") == app["coord_id"]:
                continue
            worker_id = payload.get("worker_id")
            n = payload.get("n")
            if isinstance(worker_id, str) and isinstance(n, int) and n >= 0:
                await apply_set_concurrency(app, worker_id, n)
    except Exception as e:  # noqa: BLE001 - Redis optional; local changes still work
        logging.warning(
            "coordinator %s: concurrency subscribe failed (local changes only): %s",
            app["coord_id"],
            e,
        )


async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    app = request.app
    coord_id = app["coord_id"]
    registry: WorkerRegistry = app["workers"]

    ws = web.WebSocketResponse(heartbeat=30.0)
    await ws.prepare(request)

    state: ConnState | None = None
    try:
        # First frame must be register{worker_id, concurrency}.
        first = await ws.receive_json()
        if first.get("type") != "register":
            logging.warning(
                "coordinator %s: first WS frame was %r, not register; closing",
                coord_id,
                first.get("type"),
            )
            await ws.close()
            return ws

        worker_id = first["worker_id"]
        limit = int(first.get("concurrency", 1))
        state = ConnState(ws, worker_id, limit)
        registry.add(state)
        logging.info(
            "coordinator %s: worker %s registered (concurrency=%d, total=%d)",
            coord_id,
            worker_id,
            limit,
            len(registry),
        )

        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                await _route(app, state, msg.json())
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                break
    except Exception as e:  # noqa: BLE001 - any WS/IO failure ends this connection
        logging.warning("coordinator %s: worker WS error: %s", coord_id, e)
    finally:
        if state is not None:
            registry.remove(state.worker_id)
            logging.info(
                "coordinator %s: worker %s disconnected (total=%d)",
                coord_id,
                state.worker_id,
                len(registry),
            )
    return ws


async def _route(app: web.Application, state: ConnState, data: dict) -> None:
    """Dispatch an inbound worker message. heartbeat handling lands in Step 14."""
    mtype = data.get("type")
    if mtype == "commit":
        try:
            await handle_commit(app, state, data)
        except DBPartitioned:
            # Fail-closed: the DB is partitioned by chaos. Send no ack so the worker
            # retries the commit once the partition clears; the job stays leased.
            logging.warning(
                "coordinator %s: commit from %s dropped (db partitioned); worker will retry",
                app["coord_id"],
                state.worker_id,
            )
    elif mtype == "heartbeat":
        try:
            await handle_heartbeat(app, state, data)
        except DBPartitioned:
            # Fail-closed: skip the renewal while partitioned. The reaper's TTL still
            # bounds correctness; the worker will heartbeat again shortly.
            logging.debug(
                "coordinator %s: heartbeat from %s dropped (db partitioned)",
                app["coord_id"],
                state.worker_id,
            )
    else:
        logging.warning(
            "coordinator %s: unknown WS message type %r from %s",
            app["coord_id"],
            mtype,
            state.worker_id,
        )
