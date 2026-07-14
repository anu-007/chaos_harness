"""Chaos fault router for POST /chaos.

The harness injects faults at a RANDOM coordinator (see chaos_harness.py::_faulter), but a
fault must degrade the whole system's behaviour even though a worker's WS — and therefore
its commits/heartbeats — is pinned to whichever single coordinator the LB chose. So faults
that affect worker-facing behaviour (pause_dispatch, drop_acks) are BROADCAST to every
coordinator over a Redis pub/sub channel; each coordinator applies the fault to its own
in-process state. The coordinator that received the HTTP request also applies it locally
right away, so the fault still takes effect if Redis is down (best-effort broadcast).

Two fault classes, deliberately handled differently:

  BROADCAST (worker-facing) — a worker's WS is pinned to ONE coordinator, so a fault meant to
  disrupt that worker must reach whichever coordinator holds it, not just the one the HTTP
  request hit. These are published to every coordinator:
    * pause_dispatch{ms} — set dispatcher.dispatch_paused_until_ms so the dispatch loop
      no-ops for the window; workers stay connected and the pending queue grows, then drains.
    * drop_acks{n} — bump a per-coordinator counter; the commit handler still PERSISTS the
      commit (result + leased->succeeded) but suppresses the next N ack sends, forcing the
      worker to retry. The retry is an idempotent replay: still exactly one accepted commit.

  LOCAL-ONLY (coordinator-degrading) — the harness targets a SPECIFIC coordinator precisely
  to prove that ONE coordinator failing/skewing does not break the system. Broadcasting
  these would defeat the test. Applied only to the coordinator that received the request:
    * partition_db{ms} — set the DB gate's partition window (§3) so every query on THIS
      coordinator fails closed (DBPartitioned) for the window; dispatch/commit/reaper skip
      rather than corrupt state, and recover automatically after. Peers keep serving.
    * clock_skew{seconds} — shift an in-process logical_clock_offset used ONLY for /stats
      uptime + logs, NEVER for fence/lease/commit timestamps (those come from db_now_ms()),
      so skew changes nothing an /audit or invariant check can observe.

Unknown faults return 400.
"""

import json
import logging
import time

from aiohttp import web

CHAOS_CHANNEL = "chaos:faults"

# Faults broadcast to every coordinator (worker-facing). Everything else is applied only to
# the coordinator that received the /chaos request (coordinator-degrading).
_BROADCAST_FAULTS = {"pause_dispatch", "drop_acks"}
_ALL_FAULTS = _BROADCAST_FAULTS | {"partition_db", "clock_skew"}


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
    if fault == "partition_db":
        ms = int(params.get("ms", 0))
        # Fail-closed the DB gate for the window. All queries on this coordinator raise
        # DBPartitioned; dispatch/commit/reaper skip and recover after. Peers keep serving.
        app["db"].partition(max(0, ms))
        logging.info(
            "coordinator %s: partition_db for %dms (queries fail closed)",
            app["coord_id"],
            ms,
        )
        return True
    if fault == "clock_skew":
        seconds = int(params.get("seconds", 0))
        # Cosmetic ONLY: shifts /stats uptime + logs. Fence/lease/commit times come from
        # db_now_ms() (Postgres clock), so this cannot affect any invariant or /audit value.
        app["logical_clock_offset_s"] = seconds
        logging.info(
            "coordinator %s: clock_skew set to %+ds (display/logs only)",
            app["coord_id"],
            seconds,
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

    # Reject unknown faults explicitly — accept-and-ignore would hide bugs.
    if fault not in _ALL_FAULTS:
        return web.json_response(
            {"error": f"unknown fault: {fault}"}, status=400
        )

    # Apply locally first so the fault holds even if Redis is unavailable.
    apply_fault_local(app, fault, params)

    # Only worker-facing faults broadcast. partition_db/clock_skew are deliberately
    # coordinator-local — the harness targets one coordinator to test that its degradation
    # doesn't break the system, so broadcasting them would defeat the test.
    if fault in _BROADCAST_FAULTS:
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
            # Guard: only worker-facing faults are ever broadcast. Never apply a peer's
            # partition_db/clock_skew — those are strictly local to the receiving coordinator.
            if (
                isinstance(fault, str)
                and isinstance(params, dict)
                and fault in _BROADCAST_FAULTS
            ):
                apply_fault_local(app, fault, params)
    except Exception as e:  # noqa: BLE001 - Redis optional; local /chaos still works
        logging.warning(
            "coordinator %s: chaos subscribe failed (local faults only): %s",
            app["coord_id"],
            e,
        )
