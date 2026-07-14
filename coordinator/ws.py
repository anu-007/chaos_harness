"""Coordinator-side worker WebSocket registry.

Each worker holds one persistent WS to a single coordinator (pinned by the LB for the
life of the connection). This module accepts that connection, reads the initial
`register`, and tracks the worker in a per-process registry the dispatch loop consults to
find locally-connected workers with spare capacity.

Inbound `heartbeat`/`commit` messages are routed here but their handlers are added in
later steps (lease renewal, fenced commit). On disconnect the worker is removed from the
registry; eager lease expiry for fast re-lease is wired once leases exist (Steps 11-15).
"""

import logging

from aiohttp import web, WSMsgType

from commit import handle_commit
from db import DBPartitioned


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
        # Placeholder until Step 14 adds lease renewal.
        logging.debug(
            "coordinator %s: heartbeat from %s (not yet handled)",
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
