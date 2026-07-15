"""Worker process.

Dials the coordinator cluster outbound over a single persistent WebSocket through the
nginx LB (ws://lb:8080/ws) and registers itself: connect, register, read loop, and
reconnect with exponential backoff on any drop. On a `dispatch` it runs the job (a bounded
sleep) under a local concurrency semaphore, sends `heartbeat{job_id,fence}` every ~3s while
it runs so the coordinator can renew the lease, and finally sends
`commit{job_id,fence,result}`. Commit-retry is added in a later step.

All timing here uses a MONOTONIC clock (backoff + heartbeat intervals) — never wall-clock —
so a clock_skew fault cannot affect reconnect or heartbeat cadence.
"""

import asyncio
import json
import logging
import os
import random

import aiohttp

# Backoff bounds for outbound reconnect (seconds).
_BACKOFF_MIN = 0.5
_BACKOFF_MAX = 10.0
# How often a running job pings its lease. Must be well under LEASE_TTL_MS (10s) so a lease
# is renewed several times before it could expire.
_HEARTBEAT_INTERVAL_S = 3.0
# Commit-ack retry: after sending a commit, wait this long for ack/commit_rejected before
# resending. The drop_acks fault suppresses acks; the worker must retry so the (already
# persisted) commit is confirmed. Idempotent replay => still exactly one accepted commit.
_COMMIT_ACK_TIMEOUT_S = 2.0
_COMMIT_MAX_ATTEMPTS = 8


class ResizableSemaphore:
    """A counting semaphore whose limit can change at runtime, unlike asyncio.Semaphore.

    Bounds concurrent job execution to `limit`. set_limit(n) raises the ceiling immediately
    (waking as many waiters as new slots allow) or lowers it (new acquires block until
    running jobs release below the new limit — never cancels in-flight work). Used so the
    coordinator's set_concurrency{n} message can reconfigure a worker live without a restart.
    """

    def __init__(self, limit: int):
        self._limit = max(0, int(limit))
        self._in_use = 0
        self._cond = asyncio.Condition()

    @property
    def limit(self) -> int:
        return self._limit

    async def acquire(self) -> None:
        async with self._cond:
            while self._in_use >= self._limit:
                await self._cond.wait()
            self._in_use += 1

    async def release(self) -> None:
        async with self._cond:
            self._in_use -= 1
            self._cond.notify_all()

    async def set_limit(self, n: int) -> None:
        async with self._cond:
            self._limit = max(0, int(n))
            # Wake waiters; those that now fit proceed, the rest re-block on the new ceiling.
            self._cond.notify_all()

    async def __aenter__(self) -> "ResizableSemaphore":
        await self.acquire()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.release()


class Worker:
    def __init__(self, worker_id: str, concurrency: int, lb_url: str):
        self.worker_id = worker_id
        self.concurrency = concurrency
        # ws://lb:8080 -> ws://lb:8080/ws
        self.ws_url = lb_url.rstrip("/") + "/ws"
        # Local slot limiter so this worker never runs more than `concurrency` jobs at once.
        # Resizable so the coordinator can change the limit at runtime (set_concurrency).
        self._slots = ResizableSemaphore(concurrency)
        # In-flight executor tasks, so a disconnect can cancel them cleanly.
        self._jobs: set[asyncio.Task] = set()
        # Per-job commit-ack signals. A commit waits on its event; the read loop sets it on
        # ack/commit_rejected. Lets a commit retry when drop_acks suppresses the ack.
        self._acks: dict[str, asyncio.Event] = {}

    async def run(self) -> None:
        """Connect/register/read forever, reconnecting with exponential backoff."""
        backoff = _BACKOFF_MIN
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    await self._connect_once(session)
                    # Clean return (server closed) -> reset backoff before redialing.
                    backoff = _BACKOFF_MIN
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001 - any dial/IO failure -> retry
                    logging.warning(
                        "worker %s connection failed: %s", self.worker_id, e
                    )
                # Jittered exponential backoff (monotonic sleep) before the next dial.
                sleep_s = min(backoff, _BACKOFF_MAX) * (0.5 + random.random())
                logging.info(
                    "worker %s reconnecting in %.2fs", self.worker_id, sleep_s
                )
                await asyncio.sleep(sleep_s)
                backoff = min(backoff * 2, _BACKOFF_MAX)

    async def _connect_once(self, session: aiohttp.ClientSession) -> None:
        logging.info("worker %s dialing %s", self.worker_id, self.ws_url)
        async with session.ws_connect(
            self.ws_url, heartbeat=None, autoping=True
        ) as ws:
            logging.info("worker %s connected", self.worker_id)
            await ws.send_json(
                {
                    "type": "register",
                    "worker_id": self.worker_id,
                    "concurrency": self.concurrency,
                }
            )
            logging.info(
                "worker %s registered (concurrency=%d)",
                self.worker_id,
                self.concurrency,
            )
            try:
                await self._read_loop(ws)
            finally:
                # Connection ended: cancel any jobs still running against this socket so
                # they don't try to commit over a dead WS. The coordinator's reaper will
                # re-lease anything left leased.
                await self._cancel_jobs()
        logging.info("worker %s disconnected", self.worker_id)

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    logging.warning(
                        "worker %s got non-JSON frame: %r", self.worker_id, msg.data
                    )
                    continue
                await self._handle(ws, data)
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING):
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logging.warning(
                    "worker %s ws error: %s", self.worker_id, ws.exception()
                )
                break

    async def _handle(self, ws: aiohttp.ClientWebSocketResponse, data: dict) -> None:
        mtype = data.get("type")
        if mtype == "dispatch":
            # Run the job in the background so the read loop keeps servicing the socket
            # (heartbeats, further dispatches). The semaphore bounds real concurrency.
            task = asyncio.create_task(self._execute(ws, data))
            self._jobs.add(task)
            task.add_done_callback(self._jobs.discard)
        elif mtype in ("ack", "commit_rejected"):
            # Confirmation for a commit we sent — release its retry loop. Both outcomes are
            # terminal for the worker: ack = accepted, commit_rejected = stale fence (stop).
            job_id = data.get("job_id")
            ev = self._acks.get(job_id)
            if ev is not None:
                ev.set()
            logging.info("worker %s received: %s", self.worker_id, mtype)
        elif mtype == "set_concurrency":
            # Runtime reconfiguration: resize the slot limiter live. Raising the limit lets
            # queued jobs start at once; lowering it never cancels in-flight work — new
            # acquires just block until running jobs drain below the new ceiling.
            n = data.get("n")
            if isinstance(n, int) and n >= 0:
                self.concurrency = n
                await self._slots.set_limit(n)
                logging.info(
                    "worker %s concurrency set to %d (runtime)", self.worker_id, n
                )
            else:
                logging.warning(
                    "worker %s ignoring bad set_concurrency: %r", self.worker_id, n
                )
        else:
            logging.info("worker %s received: %s", self.worker_id, mtype)

    async def _execute(self, ws: aiohttp.ClientWebSocketResponse, data: dict) -> None:
        job_id = data.get("job_id")
        fence = data.get("fence")
        payload = data.get("payload") or {}
        # The coordinator relays payload straight from Postgres jsonb, which asyncpg hands
        # back as a JSON string; decode it here so either shape works.
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        # Bounded sleep models the work; clamp so a bad payload can't stall a slot forever.
        sleep_ms = max(0, min(int(payload.get("sleep_ms", 0)), 60000))
        async with self._slots:
            try:
                await self._run_with_heartbeats(ws, job_id, fence, sleep_ms / 1000.0)
            except asyncio.CancelledError:
                # Disconnected mid-job; drop it (no commit) and let the reaper re-lease.
                raise
            result = {"ok": True, "worker": self.worker_id, "slept_ms": sleep_ms}
            await self._commit_with_retry(ws, job_id, fence, result)

    async def _commit_with_retry(self, ws, job_id, fence, result) -> None:
        """Send commit{job_id,fence,result} and wait for ack/commit_rejected, resending on
        timeout. The drop_acks fault deliberately suppresses acks AFTER persisting the
        commit; the resend is an idempotent replay (the coordinator's one_accept index makes
        it a re-ack, never a second result), so retrying keeps exactly-once while ensuring
        the worker eventually confirms and frees its own bookkeeping."""
        ev = asyncio.Event()
        self._acks[job_id] = ev
        msg = {"type": "commit", "job_id": job_id, "fence": fence, "result": result}
        try:
            for attempt in range(1, _COMMIT_MAX_ATTEMPTS + 1):
                try:
                    await ws.send_json(msg)
                except Exception as e:  # noqa: BLE001 - WS died before commit landed
                    logging.warning(
                        "worker %s could not send commit for job %s (fence=%s): %s",
                        self.worker_id,
                        job_id,
                        fence,
                        e,
                    )
                    return
                logging.info(
                    "worker %s committed job %s (fence=%s, attempt=%d)",
                    self.worker_id,
                    job_id,
                    fence,
                    attempt,
                )
                try:
                    await asyncio.wait_for(ev.wait(), timeout=_COMMIT_ACK_TIMEOUT_S)
                    return  # ack or commit_rejected received
                except asyncio.TimeoutError:
                    # No confirmation (ack suppressed by drop_acks, or in flight) — resend.
                    continue
            logging.warning(
                "worker %s gave up waiting for ack on job %s (fence=%s) after %d attempts",
                self.worker_id,
                job_id,
                fence,
                _COMMIT_MAX_ATTEMPTS,
            )
        finally:
            self._acks.pop(job_id, None)

    async def _run_with_heartbeats(self, ws, job_id, fence, duration_s: float) -> None:
        """Sleep `duration_s` while emitting heartbeat{job_id,fence} so the coordinator
        keeps renewing the lease. Sends one heartbeat IMMEDIATELY (before the first sleep)
        so a just-issued lease — possibly delayed behind the concurrency semaphore — is
        confirmed alive at once and can't lapse before the first renewal, then every ~3s
        after. All timing is monotonic (loop.time), so a clock_skew fault can't shorten or
        stretch the interval. A heartbeat send failure is non-fatal: the socket is likely
        dying and the read loop will tear the job down.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + duration_s
        if not await self._heartbeat(ws, job_id, fence):
            return
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return
            await asyncio.sleep(min(_HEARTBEAT_INTERVAL_S, remaining))
            if loop.time() >= deadline:
                return
            if not await self._heartbeat(ws, job_id, fence):
                return

    async def _heartbeat(self, ws, job_id, fence) -> bool:
        """Send one heartbeat. Returns False if the socket is dying so the caller stops."""
        try:
            await ws.send_json(
                {"type": "heartbeat", "job_id": job_id, "fence": fence}
            )
            return True
        except Exception as e:  # noqa: BLE001 - socket dying; let read loop tear down
            logging.warning(
                "worker %s heartbeat send failed for job %s (fence=%s): %s",
                self.worker_id,
                job_id,
                fence,
                e,
            )
            return False

    async def _cancel_jobs(self) -> None:
        if not self._jobs:
            return
        tasks = list(self._jobs)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _load_dotenv() -> None:
    """For non-Docker (local) runs: populate os.environ from the repo-root .env.

    A no-op under Docker (compose injects the same vars via env_file, and load_dotenv does
    NOT override already-set vars, so container values always win) and when python-dotenv is
    not installed. find_dotenv walks up from the CWD so it works regardless of where the
    process is launched from.
    """
    try:
        from dotenv import find_dotenv, load_dotenv
    except ImportError:
        return
    load_dotenv(find_dotenv(usecwd=True))


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    _load_dotenv()
    worker_id = os.environ.get("WORKER_ID", "w?")
    concurrency = int(os.environ.get("WORKER_CONCURRENCY", "2"))
    lb_url = os.environ.get("LB_WS_URL", "ws://lb:8080")
    worker = Worker(worker_id, concurrency, lb_url)
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
