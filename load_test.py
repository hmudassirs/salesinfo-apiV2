#!/usr/bin/env python3
"""
Post-restructure verification for the PrepareData API.

Three phases:
  1. Functional smoke tests — one request per route, checking each piece
     touched by the restructure actually still works end to end (auth
     bootstrap, health, query, tables, the new API-key validation cache).
  2. The original concurrent load test (unchanged behavior), so you can
     confirm throughput/correctness didn't regress alongside the
     structural changes.
  3. Report assembly — every result from phases 1-2, plus (optionally)
     the server's own `/debug/performance` snapshot taken right after
     the load test, is written to one timestamped JSON file for
     API-optimization analysis. See `build_report` / `--output`.

Requires: httpx   ->   pip install httpx

Usage:
    python load_test.py                       # both phases, --auto-auth
    python load_test.py --smoke-only           # just phase 1
    python load_test.py --load-only --api-key YOUR_KEY
    python load_test.py --concurrency 200 --sql "SELECT 1"
    python load_test.py --output my_run.json
    python load_test.py --dashboard-api-key ADMIN_KEY   # attach server-side
                                                         # /debug/performance
                                                         # snapshot to the report
"""

import argparse
import asyncio
import json
import secrets
import signal
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx


@dataclass
class RequestResult:
    status_code: int
    elapsed_ms: float
    error: Optional[str] = None
    body: Optional[dict] = None
    # Split of elapsed_ms into two phases, to tell apart "the client's
    # own asyncio loop hadn't gotten around to this request yet" from
    # "the request was actually in flight" -- see run_one_query's
    # docstring for why this distinction exists and what it can and
    # can't prove.
    scheduling_delay_ms: float = 0.0
    request_ms: float = 0.0
    # Further split of request_ms via httpcore's trace extension (see
    # _make_trace_extension below) -- None for any phase httpcore never
    # reported an event for (e.g. connect_ms is None whenever a pooled
    # keep-alive connection was reused instead of opening a new one, so
    # a wide split between requests that show a connect phase and ones
    # that don't is itself informative, not missing data).
    connect_ms: Optional[float] = None
    send_ms: Optional[float] = None
    response_wait_ms: Optional[float] = None
    receive_body_ms: Optional[float] = None
    # request_ms minus whichever of the four phases above are known
    # (None phases don't subtract anything, since they represent "no
    # event fired", not "zero time"). A large, consistent
    # unaccounted_ms across requests that DO have a connect_ms (so
    # aren't just "reused connection, nothing to report") points at
    # time httpcore's trace extension doesn't cover at all -- e.g.
    # waiting for a free slot in httpx's own connection pool before
    # connect_tcp is even allowed to start. See
    # _phases_from_trace_events's docstring.
    unaccounted_ms: Optional[float] = None


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


# ---------------------------------------------------------------------
# Sub-request timing via httpcore's trace extension
# ---------------------------------------------------------------------


def _make_trace_extension(events: dict) -> dict:
    """Build the `extensions={"trace": ...}` httpx accepts per-request.

    httpx has no high-level "connect/send/receive timing" API of its
    own (unlike e.g. aiohttp's `TraceConfig`); the trace extension is
    httpcore's -- it fires named `(event_name, info)` pairs as the
    request actually progresses through the connection pool and wire
    protocol. `events` is filled in place with wall-clock
    `time.perf_counter()` readings keyed by event name, for
    `_phases_from_trace_events` to turn into named phase durations
    afterward. One `events` dict per request -- these are NOT safe to
    share across concurrent requests, since the extension is attached
    per-`Request`, not per-`Client`.
    """

    async def trace(name: str, info: dict) -> None:
        events[name] = time.perf_counter()

    return {"trace": trace}


def _phases_from_trace_events(events: dict, task_start: float, task_end: float) -> dict:
    """Turn raw trace-event timestamps into four named phase durations (ms).

    Every phase is `None` if httpcore never reported the corresponding
    event pair -- most commonly `connect_ms` on a request that reused
    an already-open pooled connection instead of opening a new one
    (see `RequestResult`'s `connect_ms` field comment), but the same
    applies to any phase if the request errored before reaching it.

        connect_ms        -- TCP connect (absent on a reused connection)
        send_ms            -- writing the request headers/body
        response_wait_ms   -- time-to-first-byte after the request was
                               fully sent: this is the phase that
                               actually contains "the server was slow"
                               plus any network RTT, as opposed to
                               client-side work in the other three
        receive_body_ms    -- streaming/reading the response body

    Deliberately does NOT try to force `connect_ms + send_ms +
    response_wait_ms + receive_body_ms` to equal `task_end -
    task_start` (`request_ms`): the trace extension's events don't
    necessarily cover the entire span end-to-end -- most notably,
    connection-pool queueing time (waiting for a free pooled
    connection slot before `connect_tcp` is even allowed to start)
    shows up nowhere in these events at all. Whatever's left over after
    subtracting the four named phases from the total is returned
    separately as `unaccounted_ms` rather than silently folded into
    one of the four, so pool-queueing time (or any other client-side
    cost this trace extension doesn't name) is visible as its own
    number instead of inflating, say, `connect_ms` for requests that
    happened to also need a new connection.
    """

    def _span(start_key: str, end_key: str) -> Optional[float]:
        start = events.get(start_key)
        end = events.get(end_key)
        if start is None or end is None:
            return None
        return (end - start) * 1000

    def _first(*spans: Optional[float]) -> Optional[float]:
        # Plain `a or b` would wrongly fall through on a legitimate
        # 0.0ms span (falsy but not missing) -- this treats only
        # `None` as "try the next fallback".
        for span in spans:
            if span is not None:
                return span
        return None

    connect_ms = _span(
        "connection.connect_tcp.started", "connection.connect_tcp.complete"
    )
    send_ms = _first(
        _span(
            "http11.send_request_headers.started",
            "http11.send_request_body.complete",
        ),
        _span(
            "http11.send_request_headers.started",
            "http11.send_request_headers.complete",
        ),
    )
    response_wait_ms = _first(
        _span(
            "http11.send_request_body.complete",
            "http11.receive_response_headers.complete",
        ),
        _span(
            "http11.send_request_headers.complete",
            "http11.receive_response_headers.complete",
        ),
    )
    receive_body_ms = _span(
        "http11.receive_response_headers.complete",
        "http11.receive_response_body.complete",
    )
    known_ms = sum(
        v
        for v in (connect_ms, send_ms, response_wait_ms, receive_body_ms)
        if v is not None
    )
    unaccounted_ms = max(0.0, (task_end - task_start) * 1000 - known_ms)

    return {
        "connect_ms": connect_ms,
        "send_ms": send_ms,
        "response_wait_ms": response_wait_ms,
        "receive_body_ms": receive_body_ms,
        "unaccounted_ms": unaccounted_ms,
    }


# ---------------------------------------------------------------------
# Phase 1: functional smoke tests
# ---------------------------------------------------------------------


async def obtain_api_key(
    client: httpx.AsyncClient, base_url: str, username: str, password: str, email: str
) -> tuple[str, str]:
    """Register (fine if it already exists), log in, mint an API key.

    Returns (api_key, user_id).
    """
    await client.post(
        f"{base_url}/api/auth/users/register",
        json={"username": username, "password": password, "email": email},
    )

    login_resp = await client.post(
        f"{base_url}/api/auth/users/login",
        json={"username": username, "password": password},
    )
    login_resp.raise_for_status()
    login_data = login_resp.json()
    if not login_data.get("success"):
        raise RuntimeError(f"Login failed: {login_data.get('message')}")

    token = login_data["token"]
    user_id = login_data["user"]["user_id"]

    # Exercises the JWT-bootstrap auth path (core/auth/middleware.py) —
    # this call has no x-api-key yet, only the login JWT as bearer auth.
    key_resp = await client.post(
        f"{base_url}/api/auth/keys",
        json={"owner_id": user_id, "scopes": "read,write"},
        headers={"Authorization": f"Bearer {token}"},
    )
    key_resp.raise_for_status()
    return key_resp.json()["api_key"], user_id


async def run_smoke_tests(
    client: httpx.AsyncClient, base_url: str, api_key: str, user_id: str
) -> list[CheckResult]:
    results: list[CheckResult] = []
    headers = {"x-api-key": api_key}

    # --- health: core/app/health.py + core/db pool metrics ---
    try:
        r = await client.get(f"{base_url}/api/health", headers=headers)
        ok = r.status_code == 200 and r.json().get("status") in ("healthy", "degraded")
        results.append(
            CheckResult(
                "GET /api/health", ok, f"status={r.status_code} body={r.text[:150]}"
            )
        )
    except Exception as e:
        results.append(CheckResult("GET /api/health", False, str(e)))

    # --- query: core/app/api/routes/query.py + warehouse pool ---
    try:
        r = await client.post(
            f"{base_url}/api/query",
            json={"sql": "SELECT 1 AS ok", "params": None},
            headers=headers,
        )
        ok = r.status_code == 200 and r.json().get("success") is True
        results.append(
            CheckResult(
                "POST /api/query (SELECT 1)",
                ok,
                f"status={r.status_code} body={r.text[:150]}",
            )
        )
    except Exception as e:
        results.append(CheckResult("POST /api/query (SELECT 1)", False, str(e)))

    # --- query result cache: same SELECT again, expect cached=True ---
    try:
        r = await client.post(
            f"{base_url}/api/query",
            json={"sql": "SELECT 1 AS ok", "params": None},
            headers=headers,
        )
        cached = r.status_code == 200 and r.json().get("cached") is True
        results.append(
            CheckResult(
                "POST /api/query (repeat -> cached)",
                cached,
                f"cached={r.json().get('cached') if r.status_code == 200 else 'n/a'}",
            )
        )
    except Exception as e:
        results.append(CheckResult("POST /api/query (repeat -> cached)", False, str(e)))

    # --- tables: core/app/api/routes/query.py ---
    try:
        r = await client.get(f"{base_url}/api/tables", headers=headers)
        ok = r.status_code == 200 and "tables" in r.json()
        results.append(
            CheckResult(
                "GET /api/tables", ok, f"status={r.status_code} body={r.text[:150]}"
            )
        )
    except Exception as e:
        results.append(CheckResult("GET /api/tables", False, str(e)))

    # --- api key listing: core/app/api/routes/auth.py + auth ownership check ---
    try:
        r = await client.get(f"{base_url}/api/auth/keys/{user_id}", headers=headers)
        ok = r.status_code == 200 and r.json().get("count", 0) >= 1
        results.append(
            CheckResult(
                "GET /api/auth/keys/{owner_id}",
                ok,
                f"status={r.status_code} body={r.text[:150]}",
            )
        )
    except Exception as e:
        results.append(CheckResult("GET /api/auth/keys/{owner_id}", False, str(e)))

    # --- repeated auth with the same key in quick succession: exercises
    # the new APIKeyService validation cache (core/auth/api_key_service.py)
    # without a way to directly observe cache hits from outside, this at
    # least confirms the cache doesn't silently break auth correctness.
    try:
        oks = []
        for _ in range(5):
            r = await client.post(
                f"{base_url}/api/query",
                json={"sql": "SELECT 1 AS ok", "params": None},
                headers=headers,
            )
            oks.append(r.status_code == 200)
        results.append(
            CheckResult(
                "5x rapid requests, same key (validation cache)",
                all(oks),
                f"statuses_ok={oks}",
            )
        )
    except Exception as e:
        results.append(
            CheckResult("5x rapid requests, same key (validation cache)", False, str(e))
        )

    # --- bad key rejected ---
    try:
        r = await client.post(
            f"{base_url}/api/query",
            json={"sql": "SELECT 1", "params": None},
            headers={"x-api-key": "not_a_real_key"},
        )
        ok = r.status_code == 401
        results.append(
            CheckResult(
                "POST /api/query (bad key -> 401)", ok, f"status={r.status_code}"
            )
        )
    except Exception as e:
        results.append(CheckResult("POST /api/query (bad key -> 401)", False, str(e)))

    # --- no key at all rejected ---
    try:
        r = await client.post(
            f"{base_url}/api/query", json={"sql": "SELECT 1", "params": None}
        )
        ok = r.status_code == 401
        results.append(
            CheckResult(
                "POST /api/query (no key -> 401)", ok, f"status={r.status_code}"
            )
        )
    except Exception as e:
        results.append(CheckResult("POST /api/query (no key -> 401)", False, str(e)))

    return results


def print_smoke_report(results: list[CheckResult]):
    print("\n" + "=" * 60)
    print("Phase 1: functional smoke tests")
    print("=" * 60)
    for r in results:
        mark = "PASS" if r.passed else "FAIL"
        print(f"  [{mark}] {r.name}")
        if not r.passed:
            print(f"         {r.detail}")
    passed = sum(1 for r in results if r.passed)
    print("-" * 60)
    print(f"{passed}/{len(results)} checks passed")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------
# Phase 2: concurrent load test (unchanged behavior from before)
# ---------------------------------------------------------------------


async def run_one_query(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    sql: str,
    params: list,
    fire_time: float,
) -> RequestResult:
    """Run one query, timestamping both ends of the request separately.

    `fire_time` is `time.perf_counter()` taken once, by the caller,
    right before it created every task for this batch — i.e. the
    instant we *asked* for `concurrency` requests to start. This
    coroutine's own first line then re-reads the clock (`task_start`):
    the gap between the two (`scheduling_delay_ms`) is how long this
    particular coroutine sat queued on this process's single asyncio
    event loop before it got its first turn to run, *before it had
    sent a single byte*. `request_ms` is the actual request/response
    round trip after that, same measurement `elapsed_ms` always was.

    This distinguishes "the client hadn't started this request yet"
    from "the request was in flight"; within `request_ms` itself, an
    httpcore trace extension (see `_make_trace_extension` /
    `_phases_from_trace_events` above) further splits out
    `connect_ms`/`send_ms`/`response_wait_ms`/`receive_body_ms` --
    `response_wait_ms` in particular is the phase that actually
    contains network RTT plus "the server was slow", as opposed to
    client-side connection setup or body transfer.
    """
    task_start = time.perf_counter()
    scheduling_delay_ms = (task_start - fire_time) * 1000
    trace_events: dict = {}
    try:
        resp = await client.post(
            f"{base_url}/api/query",
            json={"sql": sql, "params": params},
            headers={"x-api-key": api_key},
            extensions=_make_trace_extension(trace_events),
        )
        task_end = time.perf_counter()
        request_ms = (task_end - task_start) * 1000
        try:
            body = resp.json()
        except Exception:
            body = None
        return RequestResult(
            status_code=resp.status_code,
            elapsed_ms=scheduling_delay_ms + request_ms,
            body=body,
            scheduling_delay_ms=scheduling_delay_ms,
            request_ms=request_ms,
            **_phases_from_trace_events(trace_events, task_start, task_end),
        )
    except Exception as e:
        task_end = time.perf_counter()
        request_ms = (task_end - task_start) * 1000
        return RequestResult(
            status_code=0,
            elapsed_ms=scheduling_delay_ms + request_ms,
            error=str(e) or repr(e),
            scheduling_delay_ms=scheduling_delay_ms,
            request_ms=request_ms,
            **_phases_from_trace_events(trace_events, task_start, task_end),
        )


def percentile(data: list, pct: float) -> float:
    if not data:
        return 0.0
    data = sorted(data)
    k = (len(data) - 1) * (pct / 100)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    if f == c:
        return data[f]
    return data[f] + (data[c] - data[f]) * (k - f)


def build_load_report(
    results: list[RequestResult], total_wall_time: float, concurrency: int, sql: str
) -> dict:
    """Turn raw per-request results into the JSON-serializable summary
    that both `print_load_report` and `build_report` (the on-disk JSON)
    read from — one computation, two consumers, so the printed numbers
    and the saved numbers can never drift apart.
    """
    latencies = [r.elapsed_ms for r in results]
    scheduling_delays = [r.scheduling_delay_ms for r in results]
    request_times = [r.request_ms for r in results]
    ok = [r for r in results if r.status_code == 200 and not r.error]
    failed = [r for r in results if r not in ok]

    status_counts: dict[str, int] = {}
    for r in results:
        key = str(r.status_code) if r.status_code else "connection_error"
        status_counts[key] = status_counts.get(key, 0) + 1

    def _stats(values: list) -> dict:
        return {
            "min": min(values) if values else 0.0,
            "avg": statistics.mean(values) if values else 0.0,
            "max": max(values) if values else 0.0,
            "p50": percentile(values, 50),
            "p95": percentile(values, 95),
            "p99": percentile(values, 99),
        }

    def _optional_stats(field: str) -> dict:
        # connect_ms/etc. are None on any request the trace extension
        # never reported that phase for (see
        # RequestResult.connect_ms's field comment) -- most often
        # because a pooled keep-alive connection was reused, not
        # because something went wrong. `sample_size` makes that
        # visible directly in the report (e.g. "43 of 200 requests
        # opened a new connection") instead of leaving a reader to
        # guess whether a low count means "rare" or "broken".
        values = [
            getattr(r, field) for r in results if getattr(r, field) is not None
        ]
        return {"sample_size": len(values), **_stats(values)}

    return {
        "sql": sql,
        "concurrency": concurrency,
        "total_requests": len(results),
        "wall_time_seconds": total_wall_time,
        "throughput_req_per_sec": len(results) / total_wall_time if total_wall_time else 0.0,
        "succeeded": len(ok),
        "failed": len(failed),
        "status_code_counts": status_counts,
        "latency_ms": _stats(latencies),
        # Client-side breakdown of latency_ms (see run_one_query's
        # docstring): scheduling_delay_ms is time this coroutine sat
        # queued on the client's own event loop before it could even
        # start; request_ms is the actual request/response round trip
        # after that. scheduling_delay_ms + request_ms == elapsed_ms
        # for every individual request (not just in aggregate -- these
        # two histograms don't have to line up percentile-for-percentile
        # with latency_ms's own, since different requests hit their
        # peak in different phases).
        "scheduling_delay_ms": _stats(scheduling_delays),
        "request_ms": _stats(request_times),
        # Sub-request split of request_ms via httpcore's trace
        # extension -- see run_one_query / _phases_from_trace_events.
        # response_wait_ms is the phase to look at for "is the server
        # actually slow, or is something client-side eating the time":
        # it's network RTT + server processing, with connection setup
        # and body transfer broken out separately.
        "connect_ms": _optional_stats("connect_ms"),
        "send_ms": _optional_stats("send_ms"),
        "response_wait_ms": _optional_stats("response_wait_ms"),
        "receive_body_ms": _optional_stats("receive_body_ms"),
        "unaccounted_ms": _optional_stats("unaccounted_ms"),
        "sample_failures": [
            {
                "status_code": r.status_code,
                "detail": r.error
                or (r.body.get("error") if isinstance(r.body, dict) else r.body),
            }
            for r in failed[:10]
        ],
        "raw_requests": [
            {
                "status_code": r.status_code,
                "elapsed_ms": r.elapsed_ms,
                "scheduling_delay_ms": r.scheduling_delay_ms,
                "request_ms": r.request_ms,
                "connect_ms": r.connect_ms,
                "send_ms": r.send_ms,
                "response_wait_ms": r.response_wait_ms,
                "receive_body_ms": r.receive_body_ms,
                "unaccounted_ms": r.unaccounted_ms,
                "error": r.error,
            }
            for r in results
        ],
    }


def print_load_report(load_report: dict):
    print("\n" + "=" * 60)
    print(
        f"Phase 2: load test — {load_report['total_requests']} requests, "
        f"concurrency={load_report['concurrency']}"
    )
    print("=" * 60)
    print(f"Wall time:           {load_report['wall_time_seconds']:.2f}s")
    print(f"Throughput:          {load_report['throughput_req_per_sec']:.1f} req/s")
    print(f"Succeeded (200):     {load_report['succeeded']}")
    print(f"Failed:              {load_report['failed']}")
    print("-" * 60)
    lat = load_report["latency_ms"]
    print(f"Latency min/avg/max: {lat['min']:.1f} / {lat['avg']:.1f} / {lat['max']:.1f} ms")
    print(f"Latency p50/p95/p99: {lat['p50']:.1f} / {lat['p95']:.1f} / {lat['p99']:.1f} ms")
    print("-" * 60)
    sched = load_report["scheduling_delay_ms"]
    req = load_report["request_ms"]
    print("Client-side breakdown (scheduling delay + request time = total latency):")
    print(
        f"  Scheduling delay p50/p95/p99: {sched['p50']:.1f} / {sched['p95']:.1f} / "
        f"{sched['p99']:.1f} ms   (queued on this client's own event loop)"
    )
    print(
        f"  Request time     p50/p95/p99: {req['p50']:.1f} / {req['p95']:.1f} / "
        f"{req['p99']:.1f} ms   (connection + wire + server + response)"
    )
    print("-" * 60)
    print("Sub-request breakdown of request time (via httpcore trace):")
    for label, key, note in [
        ("Connect", "connect_ms", "TCP connect; absent = reused pooled connection"),
        ("Send", "send_ms", "writing the request"),
        ("Response wait", "response_wait_ms", "network RTT + server processing"),
        ("Receive body", "receive_body_ms", "reading the response"),
        ("Unaccounted", "unaccounted_ms", "not covered by the four phases above"),
    ]:
        stat = load_report[key]
        n = stat["sample_size"]
        if n == 0:
            print(f"  {label:<14} (no samples)")
            continue
        print(
            f"  {label:<14} p50/p95/p99: {stat['p50']:.1f} / {stat['p95']:.1f} / "
            f"{stat['p99']:.1f} ms   (n={n}/{load_report['total_requests']}, {note})"
        )

    if load_report["sample_failures"]:
        print("-" * 60)
        print("Sample failures (up to 10):")
        for f in load_report["sample_failures"]:
            print(f"  status={f['status_code']}  detail={f['detail']}")
    print("=" * 60 + "\n")


async def run_load_test(
    client: httpx.AsyncClient, base_url: str, api_key: str, concurrency: int, sql: str
) -> dict:
    print(f"Firing {concurrency} concurrent requests: {sql!r}")
    start = time.perf_counter()
    # fire_time is the one clock reading every task's scheduling_delay_ms
    # is measured against -- see run_one_query's docstring. Deliberately
    # the same value passed to all `concurrency` tasks (not re-read per
    # task) so that value means "how long after we asked for all of
    # these to start did this particular one actually start", not "how
    # long after the previous task in this list happened to run".
    fire_time = start
    tasks = [
        run_one_query(client, base_url, api_key, sql, [], fire_time)
        for _ in range(concurrency)
    ]
    results = await asyncio.gather(*tasks)
    total_wall_time = time.perf_counter() - start
    load_report = build_load_report(results, total_wall_time, concurrency, sql)
    print_load_report(load_report)
    return load_report


# ---------------------------------------------------------------------
# Phase 3: server-side performance snapshot + JSON report assembly
# ---------------------------------------------------------------------


async def fetch_dashboard_snapshot(
    client: httpx.AsyncClient, base_url: str, dashboard_api_key: str
) -> dict:
    """Pull the server's own `/debug/performance` JSON right after the
    load test: per-stage trace timing, pool/gauge state, and recent
    request history straight from `core.performance`'s registry — the
    piece client-side wall-clock timing alone can't show (which stage
    inside the request actually cost the time). Requires an *admin*
    key (`install_performance_dashboard`'s `require_admin_user` gate),
    which the auto-registered load-test user is not, so this is opt-in
    via `--dashboard-api-key` rather than reusing `api_key`.
    """
    try:
        r = await client.get(
            f"{base_url}/debug/performance",
            headers={"x-api-key": dashboard_api_key},
        )
        if r.status_code != 200:
            return {
                "available": False,
                "reason": f"status={r.status_code} body={r.text[:300]}",
            }
        return {"available": True, "data": r.json()}
    except Exception as e:
        return {"available": False, "reason": str(e)}


def build_report(
    *,
    args: argparse.Namespace,
    smoke_results: Optional[list[CheckResult]],
    load_report: Optional[dict],
    dashboard_snapshot: Optional[dict],
) -> dict:
    """Assemble everything collected this run into one JSON-serializable dict."""
    report: dict = {
        "run_metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "base_url": args.base_url,
            "concurrency": args.concurrency,
            "sql": args.sql,
            "smoke_only": args.smoke_only,
            "load_only": args.load_only,
        },
    }
    if smoke_results is not None:
        report["smoke_tests"] = {
            "checks": [asdict(r) for r in smoke_results],
            "passed": sum(1 for r in smoke_results if r.passed),
            "total": len(smoke_results),
        }
    if load_report is not None:
        if args.no_raw_requests:
            load_report = {k: v for k, v in load_report.items() if k != "raw_requests"}
        report["load_test"] = load_report
    if dashboard_snapshot is not None:
        report["server_performance_snapshot"] = dashboard_snapshot
    return report


def save_report(report: dict, output_path: Path) -> None:
    output_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"Saved full report to: {output_path.resolve()}")


# ---------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------


def _install_graceful_sigint_handler() -> asyncio.Event:
    """Turn a raw Ctrl+C into a cooperative shutdown signal instead of a
    KeyboardInterrupt landing wherever the event loop happens to be.

    Without this, hitting Ctrl+C mid-request tears the TCP connection
    out from under whatever `await client.post(...)` was in flight --
    the server sees an abrupt client disconnect and has to cancel that
    request server-side (see core/db/session.py's CancelledError
    handling for what that used to leave behind). That's now handled
    safely on the server, but severing the connection mid-request is
    still not something *this* script should court needlessly on every
    Ctrl+C: it's cleaner to let the in-flight request finish, close the
    httpx.AsyncClient normally, and still write out whatever partial
    report was collected.

    Sets `shutdown_event` on the *first* SIGINT and returns it for
    callers to check between phases/checks; a *second* SIGINT re-raises
    KeyboardInterrupt immediately for anyone who really wants to bail
    right now rather than wait for the current request.

    Returns:
        The asyncio.Event that gets set on the first Ctrl+C.
    """
    shutdown_event = asyncio.Event()
    sigint_count = 0
    loop = asyncio.get_running_loop()

    def _handle_sigint() -> None:
        nonlocal sigint_count
        sigint_count += 1
        if sigint_count == 1:
            print(
                "\n⚠️  Shutting down gracefully — finishing the current "
                "request/phase, then closing the connection and saving "
                "whatever results were collected. Press Ctrl+C again to "
                "quit immediately instead."
            )
            shutdown_event.set()
        else:
            print("\n⚠️  Forcing immediate exit.")
            raise KeyboardInterrupt

    try:
        loop.add_signal_handler(signal.SIGINT, _handle_sigint)
    except NotImplementedError:
        # add_signal_handler isn't available on every platform/loop
        # (e.g. Windows' default ProactorEventLoop) -- fall back to the
        # old raw-KeyboardInterrupt behavior there rather than fail.
        pass

    return shutdown_event


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------


async def main():
    parser = argparse.ArgumentParser(
        description="Verify the restructured API end to end, load test it, "
        "and save a JSON report for API-optimization analysis."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--api-key", default=None, help="x-api-key to use. Omit if using --auto-auth."
    )
    parser.add_argument(
        "--auto-auth",
        action="store_true",
        default=True,
        help="Register + log in + mint a fresh API key before testing (default: on).",
    )
    parser.add_argument(
        "--username",
        default=None,
        help="Defaults to a unique per-run name to avoid register conflicts on repeat runs.",
    )
    parser.add_argument("--password", default="RestructureCheck123!")
    parser.add_argument(
        "--email",
        default=None,
        help="Defaults to a unique per-run address, same reasoning as --username.",
    )
    parser.add_argument(
        "--concurrency", type=int, default=500, help="Load test concurrency."
    )
    parser.add_argument(
        "--sql", default="SELECT 1 AS ok", help="SQL for the load test."
    )
    parser.add_argument(
        "--smoke-only", action="store_true", help="Run only the functional checks."
    )
    parser.add_argument(
        "--load-only", action="store_true", help="Run only the concurrent load test."
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Path to save the JSON report. Defaults to "
        "load_test_report_<UTC timestamp>.json in the current directory.",
    )
    parser.add_argument(
        "--dashboard-api-key",
        default=None,
        help="Admin-scoped x-api-key. If given, GET /debug/performance is "
        "fetched after the load test and attached to the JSON report as "
        "'server_performance_snapshot' (per-stage timing, pool/gauge state). "
        "Requires PERF_ENABLED=1 on the server and an admin API key — see "
        "docs/performance/configuration.md and bootstrap_admin.py.",
    )
    parser.add_argument(
        "--no-raw-requests",
        action="store_true",
        help="Exclude the full per-request array from the saved JSON, keeping "
        "only aggregate stats (smaller file for very high concurrency runs).",
    )
    args = parser.parse_args()

    if not args.username or not args.email:
        suffix = secrets.token_hex(4)
        args.username = args.username or f"restructure_check_{suffix}"
        args.email = args.email or f"restructure_check_{suffix}@example.com"

    limits = httpx.Limits(
        max_connections=args.concurrency + 50,
        max_keepalive_connections=args.concurrency,
    )
    timeout = httpx.Timeout(60.0)

    smoke_results: Optional[list[CheckResult]] = None
    load_report: Optional[dict] = None
    dashboard_snapshot: Optional[dict] = None
    interrupted = False

    shutdown_event = _install_graceful_sigint_handler()

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        api_key = args.api_key
        user_id = None
        if not api_key or not args.load_only:
            print("Registering/logging in/minting API key...")
            api_key, user_id = await obtain_api_key(
                client, args.base_url, args.username, args.password, args.email
            )
            print(f"Using API key: {api_key}")

        if not api_key:
            raise SystemExit(
                "No API key available. Pass --api-key or leave --auto-auth on."
            )

        # Checked between phases (not mid-phase): each phase is either
        # one fast sequential run (smoke tests) or one already-fired
        # batch of concurrent requests (load test) that's cleanest to
        # let finish rather than sever partway through. Ctrl+C during a
        # phase takes effect right after it completes, not instantly --
        # a second Ctrl+C still bails out immediately for anyone who
        # doesn't want to wait even that long.
        if not args.load_only and not shutdown_event.is_set():
            smoke_results = await run_smoke_tests(
                client, args.base_url, api_key, user_id or ""
            )
            print_smoke_report(smoke_results)

        if not args.smoke_only and not shutdown_event.is_set():
            load_report = await run_load_test(
                client, args.base_url, api_key, args.concurrency, args.sql
            )

        if args.dashboard_api_key and not shutdown_event.is_set():
            print("Fetching server-side /debug/performance snapshot...")
            dashboard_snapshot = await fetch_dashboard_snapshot(
                client, args.base_url, args.dashboard_api_key
            )
            if not dashboard_snapshot["available"]:
                print(f"  (unavailable: {dashboard_snapshot['reason']})")

        interrupted = shutdown_event.is_set()
        # httpx.AsyncClient closes normally here (`async with` exit) --
        # no in-flight request was severed to get here.

    if interrupted:
        print("Stopped early (Ctrl+C) — saving results collected so far.\n")

    report = build_report(
        args=args,
        smoke_results=smoke_results,
        load_report=load_report,
        dashboard_snapshot=dashboard_snapshot,
    )
    report["run_metadata"]["interrupted"] = interrupted
    if args.output:
        output_path = Path(args.output)
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = Path(f"load_test_report_{ts}.json")
    save_report(report, output_path)


if __name__ == "__main__":
    asyncio.run(main())
