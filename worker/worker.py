"""Worker process.

Dials the coordinator cluster outbound over a single persistent WebSocket through the
nginx LB (ws://lb:8080/ws) and registers itself. This step establishes the connection
lifecycle only: connect, register, read loop, and reconnect with exponential backoff on
any drop. The job executor, heartbeat renewal, and commit/commit-retry are added in later
steps; inbound messages are logged for now.

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
            await self._read_loop(ws)
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
                await self._handle(data)
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING):
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logging.warning(
                    "worker %s ws error: %s", self.worker_id, ws.exception()
                )
                break

    async def _handle(self, data: dict) -> None:
        # Executor / heartbeat / commit handling arrive in later steps. Log for now.
        logging.info("worker %s received: %s", self.worker_id, data.get("type"))


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    worker_id = os.environ.get("WORKER_ID", "w?")
    concurrency = int(os.environ.get("WORKER_CONCURRENCY", "2"))
    lb_url = os.environ.get("LB_WS_URL", "ws://lb:8080")
    worker = Worker(worker_id, concurrency, lb_url)
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
