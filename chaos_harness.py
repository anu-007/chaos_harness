import asyncio, aiohttp, argparse, asyncio.subprocess as sp, collections, contextlib, dataclasses, hashlib, itertools, json, logging, os, random, signal, statistics, string, sys, time, uuid
from typing import Any

_C = collections.Counter()
_E = collections.defaultdict(list)
_J: dict[str, dict] = {}
_K: dict[str, str] = {}
_X: list[tuple[float, str, dict]] = []
_R = random.Random(int(os.environ.get("HARNESS_SEED", "1729")))
_T0 = time.monotonic()

def _t() -> float: return time.monotonic() - _T0
def _id() -> str: return uuid.uuid4().hex[:12]
def _key() -> str: return hashlib.sha256(f"{_id()}{_R.random()}".encode()).hexdigest()[:32]
def _log(k: str, **kw): _X.append((_t(), k, kw)); _C[k] += 1

@dataclasses.dataclass
class Cfg:
    base: str
    coord_containers: list[str]
    worker_containers: list[str]
    duration_s: int = 600
    submit_rate: float = 50.0
    dup_rate: float = 0.05
    worker_kill_period_s: float = 30.0
    coord_kill_period_s: float = 90.0
    chaos_period_s: float = 20.0
    submit_timeout_s: float = 3.0
    audit_concurrency: int = 32
    final_wait_s: float = 45.0
    report_path: str = "chaos_report.txt"

class _Profiles:
    @staticmethod
    def short() -> dict: return {"sleep_ms": _R.randint(100, 500), "kind": "short"}
    @staticmethod
    def long() -> dict: return {"sleep_ms": _R.randint(5000, 30000), "kind": "long"}
    @staticmethod
    def mixed() -> dict: return _Profiles.long() if _R.random() < 0.05 else _Profiles.short()

async def _post(s: aiohttp.ClientSession, url: str, body: dict, **kw) -> tuple[int, Any]:
    try:
        async with s.post(url, json=body, timeout=aiohttp.ClientTimeout(total=kw.get("to", 3.0))) as r:
            try: return r.status, await r.json()
            except Exception: return r.status, await r.text()
    except Exception as e:
        return 0, repr(e)

async def _get(s: aiohttp.ClientSession, url: str, **kw) -> tuple[int, Any]:
    try:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=kw.get("to", 3.0))) as r:
            try: return r.status, await r.json()
            except Exception: return r.status, await r.text()
    except Exception as e:
        return 0, repr(e)

async def _submit_one(s: aiohttp.ClientSession, cfg: Cfg, key: str, payload: dict) -> str | None:
    t0 = _t()
    st, body = await _post(s, f"{cfg.base}/jobs", {"idempotency_key": key, "payload": payload}, to=cfg.submit_timeout_s)
    dt = _t() - t0
    _E["submit_latency"].append(dt)
    if st in (200, 201, 202) and isinstance(body, dict) and "job_id" in body:
        jid = body["job_id"]
        if key in _K and _K[key] != jid:
            _log("dedup_violation_distinct_ids", key=key, a=_K[key], b=jid)
        _K[key] = jid
        if jid not in _J:
            _J[jid] = {"key": key, "submitted_at": _t(), "payload": payload, "first_status_code": st}
        _log("submit_ok", code=st)
        return jid
    _log("submit_fail", code=st, err=str(body)[:120])
    return None

async def _submitter(s: aiohttp.ClientSession, cfg: Cfg, stop: asyncio.Event):
    interval = 1.0 / cfg.submit_rate
    keyspace: list[str] = []
    while not stop.is_set():
        t0 = time.monotonic()
        if keyspace and _R.random() < cfg.dup_rate:
            k = _R.choice(keyspace)
            asyncio.create_task(_submit_one(s, cfg, k, _J.get(_K.get(k, ""), {}).get("payload") or _Profiles.mixed()))
            _log("submit_dup")
        else:
            k = _key()
            keyspace.append(k)
            asyncio.create_task(_submit_one(s, cfg, k, _Profiles.mixed()))
        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0.0, interval - elapsed))

async def _docker(*args: str) -> tuple[int, str]:
    p = await sp.create_subprocess_exec("docker", *args, stdout=sp.PIPE, stderr=sp.PIPE)
    out, err = await p.communicate()
    return p.returncode or 0, (out + err).decode(errors="ignore")

async def _kill(target: str) -> bool:
    rc, _ = await _docker("kill", "-s", "KILL", target)
    if rc == 0: _log("killed", t=target); return True
    _log("kill_failed", t=target); return False

async def _start(target: str) -> bool:
    rc, _ = await _docker("start", target)
    if rc == 0: _log("started", t=target); return True
    _log("start_failed", t=target); return False

async def _worker_killer(cfg: Cfg, stop: asyncio.Event):
    pool = itertools.cycle(_R.sample(cfg.worker_containers, len(cfg.worker_containers)))
    while not stop.is_set():
        await asyncio.sleep(cfg.worker_kill_period_s * (0.7 + _R.random() * 0.6))
        if stop.is_set(): break
        t = next(pool)
        if await _kill(t):
            await asyncio.sleep(_R.uniform(2.0, 8.0))
            await _start(t)

async def _coord_killer(cfg: Cfg, stop: asyncio.Event):
    pool = itertools.cycle(_R.sample(cfg.coord_containers, len(cfg.coord_containers)))
    while not stop.is_set():
        await asyncio.sleep(cfg.coord_kill_period_s * (0.7 + _R.random() * 0.6))
        if stop.is_set(): break
        t = next(pool)
        if await _kill(t):
            await asyncio.sleep(_R.uniform(5.0, 12.0))
            await _start(t)

class _FaultPlan:
    def __init__(self, cfg: Cfg):
        self.cfg = cfg
        self.script = [
            ("pause_dispatch", {"ms": 1500}),
            ("drop_acks", {"n": _R.randint(3, 8)}),
            ("clock_skew", {"seconds": _R.choice([-180, -45, 30, 120])}),
            ("partition_db", {"ms": _R.randint(800, 2200)}),
            ("pause_dispatch", {"ms": 400}),
            ("drop_acks", {"n": 1}),
            ("clock_skew", {"seconds": -300}),
            ("partition_db", {"ms": 3000}),
            ("clock_skew", {"seconds": 0}),
        ]
    def __iter__(self):
        while True:
            for f in self.script: yield f

async def _faulter(s: aiohttp.ClientSession, cfg: Cfg, stop: asyncio.Event):
    plan = iter(_FaultPlan(cfg))
    while not stop.is_set():
        await asyncio.sleep(cfg.chaos_period_s * (0.5 + _R.random()))
        if stop.is_set(): break
        target = _R.choice(cfg.coord_containers)
        addr = f"http://{target}:8080" if target.startswith("c") and ":" not in target else cfg.base
        fault, params = next(plan)
        st, body = await _post(s, f"{addr}/chaos", {"fault": fault, "params": params}, to=2.0)
        _log("fault_injected", fault=fault, params=params, code=st, target=target)

async def _audit_one(s: aiohttp.ClientSession, cfg: Cfg, jid: str) -> dict:
    st, body = await _get(s, f"{cfg.base}/audit?job_id={jid}", to=5.0)
    if st != 200 or not isinstance(body, dict):
        return {"job_id": jid, "ok": False, "code": st, "err": str(body)[:200]}
    return {"job_id": jid, "ok": True, "transitions": body.get("transitions", []), "commits": body.get("commits", []), "lease_history": body.get("lease_history", [])}

async def _audit_all(s: aiohttp.ClientSession, cfg: Cfg) -> dict[str, dict]:
    sem = asyncio.Semaphore(cfg.audit_concurrency)
    out: dict[str, dict] = {}
    async def _w(jid: str):
        async with sem:
            r = await _audit_one(s, cfg, jid)
            out[jid] = r
    await asyncio.gather(*[_w(j) for j in _J.keys()])
    return out

def _verify(audits: dict[str, dict]) -> list[str]:
    violations: list[str] = []
    seen_keys: dict[str, set[str]] = collections.defaultdict(set)
    for jid, meta in _J.items():
        seen_keys[meta["key"]].add(jid)
    for k, ids in seen_keys.items():
        if len(ids) > 1:
            violations.append(f"dedup: idempotency_key {k} produced {len(ids)} job_ids: {sorted(ids)}")
    for jid, audit in audits.items():
        if not audit.get("ok"):
            violations.append(f"audit_unreachable: {jid} ({audit.get('err')})")
            continue
        commits = audit.get("commits") or []
        successful = [c for c in commits if c.get("accepted") is True]
        if len(successful) == 0:
            transitions = audit.get("transitions") or []
            terminal = any(t.get("to") in ("succeeded", "failed", "cancelled") for t in transitions)
            if not terminal:
                violations.append(f"lost: {jid} has no terminal state and no accepted commit")
        if len(successful) > 1:
            violations.append(f"double_commit: {jid} accepted {len(successful)} commits")
        leases = audit.get("lease_history") or []
        tokens = [l.get("fence") for l in leases if isinstance(l.get("fence"), int)]
        if tokens != sorted(tokens):
            violations.append(f"fencing_nonmonotonic_per_job: {jid} tokens={tokens}")
        for c in commits:
            if not c.get("accepted"): continue
            f = c.get("fence")
            if isinstance(f, int) and tokens and f < max(tokens):
                violations.append(f"stale_commit_accepted: {jid} commit fence={f} but max issued={max(tokens)}")
    all_fences: list[tuple[float, int, str]] = []
    for jid, audit in audits.items():
        if not audit.get("ok"): continue
        for l in (audit.get("lease_history") or []):
            f = l.get("fence")
            ts = l.get("issued_at_ms")
            if isinstance(f, int) and isinstance(ts, (int, float)):
                all_fences.append((float(ts), f, jid))
    all_fences.sort(key=lambda x: x[0])
    last = -1
    for ts, f, jid in all_fences:
        if f <= last:
            violations.append(f"fencing_global_nonmonotonic: at ts={ts} job={jid} fence={f} <= prev={last}")
            break
        last = f
    return violations

def _percentile(xs: list[float], p: float) -> float:
    if not xs: return 0.0
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, int(round((p / 100.0) * (len(xs) - 1)))))
    return xs[k]

def _format_report(cfg: Cfg, audits: dict[str, dict], violations: list[str], wall_s: float) -> str:
    submitted = _C["submit_ok"]
    failed = _C["submit_fail"]
    dups = _C["submit_dup"]
    completed = sum(1 for a in audits.values() if a.get("ok") and any(c.get("accepted") for c in (a.get("commits") or [])))
    unreachable = sum(1 for a in audits.values() if not a.get("ok"))
    lat = _E["submit_latency"]
    lines = []
    lines.append("FarLabs Chaos Harness — Run Report")
    lines.append("=" * 60)
    lines.append(f"wall_time:                  {wall_s:.1f}s")
    lines.append(f"seed:                       {os.environ.get('HARNESS_SEED', '1729')}")
    lines.append(f"submitted (ok):             {submitted}")
    lines.append(f"submitted (failed):         {failed}")
    lines.append(f"duplicate submissions:      {dups}")
    lines.append(f"jobs tracked:               {len(_J)}")
    lines.append(f"jobs with accepted commit:  {completed}")
    lines.append(f"jobs audit-unreachable:     {unreachable}")
    lines.append(f"unique idempotency keys:    {len(set(m['key'] for m in _J.values()))}")
    lines.append("")
    lines.append("Submit latency:")
    lines.append(f"  p50: {_percentile(lat, 50) * 1000:.1f}ms   p95: {_percentile(lat, 95) * 1000:.1f}ms   p99: {_percentile(lat, 99) * 1000:.1f}ms")
    lines.append("")
    lines.append("Chaos events:")
    lines.append(f"  worker kills attempted:   {_C['killed'] and 'see below' or 0}")
    lines.append(f"  workers killed:           {sum(1 for _, k, kw in _X if k == 'killed' and kw.get('t', '').startswith('w'))}")
    lines.append(f"  coordinators killed:      {sum(1 for _, k, kw in _X if k == 'killed' and kw.get('t', '').startswith('c'))}")
    lines.append(f"  faults injected:          {_C['fault_injected']}")
    fault_breakdown = collections.Counter(kw['fault'] for _, k, kw in _X if k == 'fault_injected')
    for f, n in sorted(fault_breakdown.items()):
        lines.append(f"    {f}: {n}")
    lines.append("")
    lines.append(f"Invariant violations: {len(violations)}")
    lines.append("-" * 60)
    if not violations:
        lines.append("(none)")
    else:
        for v in violations: lines.append(f"  ✗ {v}")
    lines.append("")
    lines.append("Verdict:")
    if not violations and unreachable == 0 and failed < submitted * 0.01:
        lines.append("  PASS — system survived chaos with no observable invariant violations.")
    elif not violations:
        lines.append("  PASS-WITH-NOTES — no invariant violations, but elevated failures or unreachable audits.")
    else:
        lines.append("  FAIL — invariant violations recorded. See DECISIONS for required acknowledgement.")
    lines.append("")
    lines.append("Event log written to: chaos_events.jsonl")
    return "\n".join(lines)

def _dump_events():
    with open("chaos_events.jsonl", "w") as f:
        for t, k, kw in _X:
            f.write(json.dumps({"t": round(t, 4), "k": k, **kw}) + "\n")

async def _main(cfg: Cfg):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    connector = aiohttp.TCPConnector(limit=256, ttl_dns_cache=30)
    async with aiohttp.ClientSession(connector=connector) as s:
        st, _ = await _get(s, f"{cfg.base}/stats", to=5.0)
        if st == 0:
            print(f"[harness] cannot reach {cfg.base}/stats — is the stack up?", file=sys.stderr)
            sys.exit(2)
        _log("harness_start", base=cfg.base)
        tasks = [
            asyncio.create_task(_submitter(s, cfg, stop)),
            asyncio.create_task(_worker_killer(cfg, stop)),
            asyncio.create_task(_coord_killer(cfg, stop)),
            asyncio.create_task(_faulter(s, cfg, stop)),
        ]
        try:
            await asyncio.sleep(cfg.duration_s)
        finally:
            stop.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        _log("harness_drain", wait_s=cfg.final_wait_s)
        await asyncio.sleep(cfg.final_wait_s)
        audits = await _audit_all(s, cfg)
        violations = _verify(audits)
        wall = _t()
        report = _format_report(cfg, audits, violations, wall)
        with open(cfg.report_path, "w") as f:
            f.write(report)
        _dump_events()
        print(report)
        sys.exit(0 if not violations else 1)

def _parse() -> Cfg:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--base", default=os.environ.get("HARNESS_BASE", "http://localhost:8080"))
    p.add_argument("--coords", default=os.environ.get("HARNESS_COORDS", "c1,c2,c3"))
    p.add_argument("--workers", default=os.environ.get("HARNESS_WORKERS", "w1,w2,w3,w4,w5"))
    p.add_argument("--duration", type=int, default=int(os.environ.get("HARNESS_DURATION", "600")))
    p.add_argument("--rate", type=float, default=float(os.environ.get("HARNESS_RATE", "50")))
    p.add_argument("--report", default=os.environ.get("HARNESS_REPORT", "chaos_report.txt"))
    a = p.parse_args()
    return Cfg(
        base=a.base.rstrip("/"),
        coord_containers=[x.strip() for x in a.coords.split(",") if x.strip()],
        worker_containers=[x.strip() for x in a.workers.split(",") if x.strip()],
        duration_s=a.duration,
        submit_rate=a.rate,
        report_path=a.report,
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    cfg = _parse()
    try:
        asyncio.run(_main(cfg))
    except KeyboardInterrupt:
        sys.exit(130)
