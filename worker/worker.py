"""Worker process.

Dials the coordinator cluster outbound over a single persistent WebSocket through the
nginx LB (ws://lb:8080/ws) and registers itself: connect, register, read loop, and
reconnect with exponential backoff on any drop. On a `dispatch` it runs the job (a bounded
sleep) under a local concurrency semaphore and sends back `commit{job_id,fence,result}`.
Heartbeat renewal and commit-retry are added in later steps.

All timing here uses a MONOTONIC clock (backoff intervals only) — never wall-clock — so a
clock_skew fault cannot affect reconnect behaviour.
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


class Worker:
    def __init__(self, worker_id: str, concurrency: int, lb_url: str):
        self.worker_id = worker_id
        self.concurrency = concurrency
        # ws://lb:8080 -> ws://lb:8080/ws
        self.ws_url = lb_url.rstrip("/") + "/ws"
        # Local slot limiter so this worker never runs more than `concurrency` jobs at once.
        self._slots = asyncio.Semaphore(concurrency)
        # In-flight executor tasks, so a disconnect can cancel them cleanly.
        self._jobs: set[asyncio.Task] = set()

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
                await asyncio.sleep(sleep_ms / 1000.0)
            except asyncio.CancelledError:
                # Disconnected mid-job; drop it (no commit) and let the reaper re-lease.
                raise
            result = {"ok": True, "worker": self.worker_id, "slept_ms": sleep_ms}
            try:
                await ws.send_json(
                    {
                        "type": "commit",
                        "job_id": job_id,
                        "fence": fence,
                        "result": result,
                    }
                )
                logging.info(
                    "worker %s committed job %s (fence=%s)",
                    self.worker_id,
                    job_id,
                    fence,
                )
            except Exception as e:  # noqa: BLE001 - WS died before commit landed
                logging.warning(
                    "worker %s could not send commit for job %s (fence=%s): %s",
                    self.worker_id,
                    job_id,
                    fence,
                    e,
                )

    async def _cancel_jobs(self) -> None:
        if not self._jobs:
            return
        tasks = list(self._jobs)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    worker_id = os.environ.get("WORKER_ID", "w?")
    concurrency = int(os.environ.get("WORKER_CONCURRENCY", "2"))
    lb_url = os.environ.get("LB_WS_URL", "ws://lb:8080")
    worker = Worker(worker_id, concurrency, lb_url)
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
