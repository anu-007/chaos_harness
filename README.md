# chaos_harness.py

This is the harness referenced in Section 4 of the take-home. You do not modify it. You run it against your stack and commit its output.

## Demo video
[Video](https://youtu.be/R1KQL_uOC2c?si=ECeJhN5npSkP-dcj)


## Running the stack

The whole FarLabs dispatcher (Postgres, Redis, three coordinators `c1/c2/c3`, the nginx LB,
and five workers `w1..w5`) runs via `docker compose`. Convenience targets live in the `Makefile`.

### Prerequisites

- Docker with the `docker compose` plugin
- Python 3.11+ with `aiohttp` (only if you run `chaos_harness.py` directly instead of `make chaos`)

### Steps

1. Create your `.env` from the template (gitignored; loaded by every service via `env_file`):
   ```
   cp .env.example .env
   ```
2. Build and start the stack in the background:
   ```
   make up
   ```
3. Check container status (all should be healthy/up):
   ```
   make ps
   ```
   The LB is published on `http://localhost:8080` — e.g. `curl http://localhost:8080/stats`.
4. Follow logs if needed:
   ```
   make logs
   ```
5. Run the pytest invariant suite against the running stack:
   ```
   make test
   ```
6. Run the chaos harness (duration/rate overridable; writes `chaos_report.txt` at the repo root):
   ```
   make chaos                      # defaults: DURATION=600, RATE=50, seed 1729
   make chaos DURATION=60          # shorter smoke run
   make chaos HARNESS_SEED=42      # fresh, non-seed-tuned run
   ```
7. Tear everything down and drop volumes (fresh Postgres/Redis next time):
   ```
   make down
   ```

`make chaos` runs the harness container on the compose network so it can reach the LB (`lb:8080`)
and `docker kill`/`start` the coordinators and workers by name. To invoke the harness manually
against a stack, see **Invocation** below.

## Requirements

- Python 3.11+
- `pip install aiohttp`
- A running `docker` CLI with permission to `kill` and `start` your containers by name

## Invocation

```
python3 chaos_harness.py \
  --base   http://localhost:8080 \
  --coords c1,c2,c3 \
  --workers w1,w2,w3,w4,w5 \
  --duration 600 \
  --rate 50 \
  --report chaos_report.txt
```

All flags also accept environment variables: `HARNESS_BASE`, `HARNESS_COORDS`, `HARNESS_WORKERS`, `HARNESS_DURATION`, `HARNESS_RATE`, `HARNESS_REPORT`, `HARNESS_SEED`.

The container names you pass via `--coords` and `--workers` must match the `container_name:` fields in your `docker-compose.yml`. The harness uses `docker kill` and `docker start` against them by name.

## What it does

It submits jobs at the given rate, deliberately duplicates ~5% of them, SIGKILLs workers and coordinators on a randomized cadence, and POSTs to your `/chaos` endpoint with a deterministic sequence of faults (`pause_dispatch`, `drop_acks`, `clock_skew`, `partition_db`). After the run window expires, it waits 45 seconds for in-flight work to drain, then audits every submitted job via `GET /audit?job_id=<id>` and checks invariants from the recorded log.

## What you must implement on your side

The harness assumes the following endpoints exist on your coordinator (any instance, behind the LB):

- `POST /jobs` — body `{"idempotency_key": "...", "payload": {...}}` — returns `{"job_id": "..."}` with status 200, 201, or 202.
- `GET  /stats` — any 200 response. Used as a liveness check at start.
- `POST /chaos` — body `{"fault": "<name>", "params": {...}}`. The four fault names are defined in Section 3.6 of the take-home.
- `GET  /audit?job_id=<id>` — returns:
  ```
  {
    "transitions":    [{"from": "...", "to": "...", "at_ms": 0, "coordinator": "c1"}, ...],
    "commits":        [{"accepted": true/false, "fence": <int>, "worker": "w1", "at_ms": 0}, ...],
    "lease_history":  [{"fence": <int>, "worker": "w1", "issued_at_ms": 0, "expired_at_ms": 0}, ...]
  }
  ```

The `/audit` endpoint is **the** endpoint we will read after the run. It is how the harness detects double-commits, non-monotonic fencing tokens, and silently lost jobs. Implement it deliberately. A system that maintains only the current state of a job in a single database row cannot answer `/audit` correctly. Plan for this.

## What it produces

- `chaos_report.txt` — the human-readable summary. Commit this file.
- `chaos_events.jsonl` — the full event log of the harness run, one event per line. Useful for your own debugging. Commit it if you want; not required.

## Exit codes

- `0` — no invariant violations detected.
- `1` — one or more invariant violations recorded. The run still produces a report; the report is still graded.
- `2` — could not reach your stack at all.
- `130` — interrupted.

## A note on the seed

The harness is deterministic given a seed. The default seed is `1729`. We will re-run your submission with a seed you have never seen.
