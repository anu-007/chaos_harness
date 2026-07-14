"""Chaos fault router for POST /chaos.

The harness injects faults at a RANDOM coordinator (see chaos_harness.py::_faulter), but a
fault must degrade the whole system's behaviour even though a worker's WS — and therefore
its commits/heartbeats — is pinned to whichever single coordinator the LB chose. So faults
that affect worker-facing behaviour (pause_dispatch, drop_acks) are BROADCAST to every
coordinator over a Redis pub/sub channel; each coordinator applies the fault to its own
in-process state. The coordinator that received the HTTP request also applies it locally
right away, so the fault still takes effect if Redis is down (best-effort broadcast).

This module owns Step 17 faults:
  * pause_dispatch{ms} — set dispatcher.dispatch_paused_until_ms so the dispatch loop no-ops
    for the window; workers stay connected and the pending queue grows, then drains.
  * drop_acks{n} — bump a per-coordinator counter; the commit handler still PERSISTS the
    commit (result + leased->succeeded) but suppresses the next N ack sends, forcing the
    worker to retry. The retry is an idempotent replay: still exactly one accepted commit.

clock_skew and partition_db are added in Step 18; unknown faults return 400.
"""

import json
import logging
import time

from aiohttp import web

CHAOS_CHANNEL = "chaos:faults"


def apply_fault_local(app: web.Application, fault: str, params: dict) -> bool:
    """Apply one fault to THIS coordinator's in-process state. Returns True if the fault
    name is recognized (params already validated by the caller / peer publisher)."""
    if fault == "pause_dispatch":
        ms = int(params.get("ms", 0))
        dispatcher = app.get("dispatcher")
        if dispatcher is not None:
            dispatcher.dispatch_paused_until_ms = time.monotonic() * 1000.0 + ms
        logging.info(
            "coordinator %s: pause_dispatch for %dms", app["coord_id"], ms
        )
        return True
    if fault == "drop_acks":
        n = int(params.get("n", 0))
        # Additive so overlapping faults accumulate rather than clobber.
        app["drop_acks_remaining"] = app.get("drop_acks_remaining", 0) + max(0, n)
        logging.info(
            "coordinator %s: drop_acks +%d (now %d)",
            app["coord_id"],
            n,
            app["drop_acks_remaining"],
        )
        return True
    return False


async def handle_chaos(request: web.Request) -> web.Response:
    """POST /chaos {fault, params}. Applies the fault locally and broadcasts it to peers."""
    app = request.app
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - malformed JSON
        return web.json_response({"error": "invalid JSON body"}, status=400)

    fault = body.get("fault")
    params = body.get("params") or {}
    if not isinstance(fault, str) or not isinstance(params, dict):
        return web.json_response(
            {"error": "body must be {fault: str, params: object}"}, status=400
        )

    # Step 17 handles pause_dispatch + drop_acks. clock_skew/partition_db land in Step 18;
    # accept-and-ignore here would hide bugs, so reject unknown faults explicitly.
    if fault not in ("pause_dispatch", "drop_acks"):
        return web.json_response(
            {"error": f"unknown or not-yet-implemented fault: {fault}"}, status=400
        )

    # Apply locally first so the fault holds even if Redis is unavailable.
    apply_fault_local(app, fault, params)

    # Broadcast to peer coordinators so the fault degrades the whole system regardless of
    # which coordinator a given worker is pinned to. Best-effort: Redis down => local only.
    try:
        await app["redis"].publish(
            CHAOS_CHANNEL,
            json.dumps({"fault": fault, "params": params, "from": app["coord_id"]}),
        )
    except Exception as e:  # noqa: BLE001 - Redis is best-effort
        logging.warning(
            "coordinator %s: chaos broadcast failed (applied locally only): %s",
            app["coord_id"],
            e,
        )

    return web.json_response({"ok": True, "fault": fault, "params": params})


async def chaos_subscriber(app: web.Application) -> None:
    """Background task: apply faults broadcast by peer coordinators. The coordinator that
    originated a fault already applied it locally and also receives its own publish; skip
    self-originated messages to avoid double-applying (matters for the additive drop_acks
    counter)."""
    try:
        pubsub = app["redis"].pubsub()
        await pubsub.subscribe(CHAOS_CHANNEL)
        async for msg in pubsub.listen():
            if msg.get("type") != "message":
                continue
            try:
                payload = json.loads(msg["data"])
            except Exception:  # noqa: BLE001 - ignore malformed peer message
                continue
            if payload.get("from") == app["coord_id"]:
                continue
            fault = payload.get("fault")
            params = payload.get("params") or {}
            if isinstance(fault, str) and isinstance(params, dict):
                apply_fault_local(app, fault, params)
    except Exception as e:  # noqa: BLE001 - Redis optional; local /chaos still works
        logging.warning(
            "coordinator %s: chaos subscribe failed (local faults only): %s",
            app["coord_id"],
            e,
        )
