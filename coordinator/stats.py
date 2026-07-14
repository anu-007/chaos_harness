"""/stats rendering + rolling rate counters.

Two data sources, each chosen deliberately:

  * Queue + lease numbers come from Postgres (the source of truth), computed with the DB
    clock (db_now_ms()) so clock_skew on any coordinator can't distort "stuck>30s" or
    "expiring<5s". A single query returns the whole snapshot.
  * Per-worker inflight/limit come from THIS coordinator's in-process WS registry (the only
    place that knows live socket state); a worker at its limit is flagged with '*'.
  * last-60s submitted/completed/failed are per-second counters in Redis (so the rate spans
    the whole cluster, not just one coordinator). Redis is optimization-only: if it's down
    we fall back to a per-coordinator in-memory ring buffer, so /stats still answers.

'lost' is structurally 0 — the no-lost invariant means a job never vanishes — so it's
rendered as 0 rather than derived from a fragile heuristic.
"""

import logging
import time

# How long a rate bucket lives in Redis before expiring (well over the 60s window so a
# read never races a just-expired bucket).
_BUCKET_TTL_S = 90
# Rolling window the rate line reports.
_WINDOW_S = 60
_EVENTS = ("submitted", "completed", "failed")


class RateCounters:
    """Cluster-wide last-60s rates via Redis per-second buckets, with an in-memory fallback.

    Each event bumps the bucket for the current wall-clock second (key
    stats:<event>:<epoch_sec>) and refreshes its TTL. A read sums the 60 most recent
    buckets. Wall-clock seconds are fine here: this is a cosmetic operator metric, never a
    correctness timestamp, and all coordinators share the same Redis so their buckets align.
    """

    def __init__(self, redis):
        self._redis = redis
        # Fallback: event -> {epoch_sec: count}, pruned to the window on access.
        self._mem: dict[str, dict[int, int]] = {e: {} for e in _EVENTS}

    async def incr(self, event: str, n: int = 1) -> None:
        if event not in self._EVENTS_SET:
            return
        sec = int(time.time())
        try:
            pipe = self._redis.pipeline()
            key = f"stats:{event}:{sec}"
            pipe.incrby(key, n)
            pipe.expire(key, _BUCKET_TTL_S)
            await pipe.execute()
        except Exception as e:  # noqa: BLE001 - Redis optional; keep an in-memory tally
            logging.debug("stats incr fell back to memory (%s): %s", event, e)
            self._mem[event][sec] = self._mem[event].get(sec, 0) + n

    async def window_counts(self) -> dict[str, int]:
        now = int(time.time())
        secs = range(now - _WINDOW_S + 1, now + 1)
        out: dict[str, int] = {}
        for event in _EVENTS:
            total = 0
            try:
                keys = [f"stats:{event}:{s}" for s in secs]
                vals = await self._redis.mget(keys)
                total = sum(int(v) for v in vals if v is not None)
            except Exception as e:  # noqa: BLE001 - Redis down: use in-memory buffer
                logging.debug("stats read fell back to memory (%s): %s", event, e)
                buf = self._mem[event]
                # Prune anything older than the window so memory stays bounded.
                for s in list(buf):
                    if s < now - _WINDOW_S:
                        del buf[s]
                total = sum(c for s, c in buf.items() if s in secs)
            out[event] = total
        return out


RateCounters._EVENTS_SET = frozenset(_EVENTS)


# Single snapshot of queue + lease numbers, all on the DB clock so chaos clock_skew can't
# distort the age-based buckets. in_flight = leased jobs with no accepted commit yet.
_SNAPSHOT_SQL = """
SELECT
  (SELECT count(*) FROM jobs WHERE state='pending')                        AS pending,
  (SELECT count(*) FROM jobs WHERE state='leased')                         AS in_flight,
  (SELECT count(*) FROM leases l
     WHERE l.expired_at_ms IS NULL
       AND db_now_ms() - l.issued_at_ms > 30000
       AND EXISTS (SELECT 1 FROM jobs j
                   WHERE j.job_id=l.job_id AND j.state='leased'))          AS stuck,
  (SELECT count(*) FROM leases WHERE expired_at_ms IS NULL)                AS active_leases,
  (SELECT count(*) FROM leases
     WHERE expired_at_ms IS NULL
       AND expires_at_ms - db_now_ms() < 5000)                            AS expiring
"""


async def db_snapshot(db) -> dict:
    """Fetch the queue/lease counts. Raises DBPartitioned if the DB is gated (caller renders
    a degraded line rather than failing the whole /stats)."""
    row = await db.fetchrow(_SNAPSHOT_SQL)
    return {
        "pending": row["pending"],
        "in_flight": row["in_flight"],
        "stuck": row["stuck"],
        "active_leases": row["active_leases"],
        "expiring": row["expiring"],
    }


def render_workers_line(registry) -> str:
    """workers: <n> connected ( w1:3/8 w2:1/8 w4:8/8* ) — '*' when a worker is at its limit.
    Sorted by worker_id for stable, readable output."""
    states = sorted(registry.all(), key=lambda s: s.worker_id)
    parts = []
    for s in states:
        used = len(s.inflight)
        star = "*" if used >= s.limit else ""
        parts.append(f"{s.worker_id}:{used}/{s.limit}{star}")
    inner = (" " + " ".join(parts) + " ") if parts else " "
    return f"workers: {len(states)} connected ({inner})"
