# DECISIONS

Five load-bearing design decisions for the FarLabs distributed job dispatcher. Each is
stated as **Chose / Over / Because / Cost** so the trade-off is legible in both directions.

---

## D1 — Postgres sequence (coupled to a monotonic clock) for global fencing

- **Chose:** A single Postgres `SEQUENCE` (`fence_seq`) as the global fencing token, allocated
  together with `issued_at_ms` inside one row-locked function `issue_fence()`.
- **Over:** An app-level counter or a Redis `INCR` for fence tokens.
- **Because:** The harness requires fences to be strictly increasing **globally** when leases
  are sorted by `issued_at_ms`. A sequence is monotonic across every coordinator, restart and
  failover with zero coordination code. The subtlety we hit: `nextval()` and `clock_timestamp()`
  are each monotonic alone, but across concurrent transactions their orderings can disagree, so
  a lower fence could get a later timestamp — a real violation. `issue_fence()` allocates the
  fence **and** the timestamp under one `UPDATE` of a single `fence_clock` row; that row lock
  fully serializes issuers, so whoever commits first gets both the lower fence and the lower
  `issued_at_ms`. `GREATEST(db_now_ms(), last+1)` keeps the timestamp real wall-clock (nudged
  forward ≥1ms only under contention). A Redis/app counter could not give this joint atomicity.
- **Cost:** The database sits on the fence-issue hot path, and every lease serializes on one
  `fence_clock` row. Measured fine at 50 jobs/s (submit p99 ≈ 5ms), but it is a deliberate
  throughput ceiling in exchange for a correctness guarantee that is impossible to violate.

---

## D2 — WebSocket for worker↔coordinator transport

- **Chose:** One persistent, worker-initiated **WebSocket** per worker (dialed outbound through
  the nginx LB), carrying `dispatch`/`set_concurrency`/ack downstream and
  `register`/`heartbeat`/`commit` upstream.
- **Over:** A gRPC bidirectional stream or a raw TCP protocol.
- **Because:** Workers must receive pushed dispatches and renew leases over a long-lived
  bidirectional channel; WebSocket gives that with trivial infra (upgrades cleanly through
  nginx, no extra proto toolchain) and a natural failure signal — a dropped socket is an
  immediate, observable worker-gone event that drives reconnect-with-backoff.
- **Cost:** No schema/codegen rigor of gRPC; we hand-roll JSON framing, heartbeats, and
  commit-ack retries, and must be disciplined about message shapes.

---

## D3 — Postgres source-of-truth, Redis as optimization only

- **Chose:** Postgres is the **only** correctness authority (jobs, dedup, append-only
  transition/commit/lease logs, fencing). Redis carries only a dispatch-wakeup pub/sub and
  `/stats` counters.
- **Over:** A Redis-authoritative queue (e.g. streams/lists) as the primary job store.
- **Because:** The invariants (dedup, exactly-once accept, fence monotonicity, no-lost) are all
  enforced by durable Postgres constraints — `UNIQUE(idempotency_key)`, the partial unique index
  `commits(job_id) WHERE accepted`, and the fence machinery. Redis is best-effort: killing it
  only adds dispatch latency (the loop still polls every 200ms) and never loses a job. The
  dispatcher re-subscribes with backoff so restoring Redis makes wakeups fast again with no
  restart.
- **Cost:** An extra moving part that contributes nothing to correctness — an honest over-scope
  risk. Justified only because it is strictly a latency optimization with a proven no-loss
  fallback.

---

## D4 — Lease expiry on DB time + worker heartbeat (never a worker wall clock)

- **Chose:** Leases carry a DB-time `expires_at_ms`; long jobs stay alive via `heartbeat`
  renewals (`UPDATE ... expires_at_ms = db_now_ms() + TTL`), and a per-coordinator **reaper**
  requeues any lease past its DB-time deadline. The worker never judges its own expiry by wall
  clock. Commit validity is decided by the coordinator/DB using `db_now_ms()`.
- **Over:** Coordinator-local-clock leases, or trusting the worker's clock for expiry.
- **Because:** All correctness timestamps come from Postgres, so the `clock_skew` fault (which
  skews a coordinator's *logical* clock) cannot make a live lease look expired or vice versa —
  the global monotonic check and reaper both keep working. Heartbeats let 5–30s jobs survive a
  ~10s TTL without inflating it.
- **Cost:** Steady heartbeat traffic (~every 3s per running job) and a TTL that must be tuned
  against re-lease latency: too low re-leases healthy work, too high slows recovery after a
  worker death.

---

## D5 — Drop-and-defer when a lease is invalidated after execution

- **Chose:** If a worker finishes a job but its `commit` is **rejected** (stale fence / expired
  lease — e.g. it was reaped and re-leased elsewhere), the worker **discards its result** and
  does not force a commit; the new lease owner redoes the work and commits under the current
  fence.
- **Over:** Letting the original worker force-commit with its old (stale) fence.
- **Because:** The exactly-once *accept* guarantee comes from refusing any commit whose fence
  isn't the job's current one. Honoring a stale-fence commit would risk two accepted results and
  duplicated external side effects. Drop-and-defer keeps a single authoritative outcome per job.
- **Cost:** Work executed under an expired lease is wasted and re-run by the new owner — this is
  an **at-least-once execution / exactly-once accept** model, not exactly-once *execution*.
  Acceptable because the harness (and real side-effect safety) demands one accepted commit, not
  one execution.

---

### Chaos result & honesty note

Full `make chaos` (600s, rate 50) passes with **0 invariant violations** on the default seed
`1729` and on a fresh seed `42` (17 worker kills, 6 coordinator kills, all fault types injected;
`chaos_report.txt` committed). Two operational — not correctness — fixes were needed to reach a
clean verdict: sizing worker concurrency so the fixed 50 jobs/s submit rate can actually drain
(otherwise un-dispatched jobs read as `lost`), and enabling nginx `proxy_next_upstream ...
non_idempotent` so a POST landing on a killed/partitioned coordinator transparently retries a
healthy peer — safe precisely because job creation is idempotent (`ON CONFLICT (idempotency_key)
DO NOTHING` returns the same `job_id`). No residual invariant violations remain.
