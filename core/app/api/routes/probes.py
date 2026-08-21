"""Liveness/readiness probes for container/orchestrator health checks
(framework review item "split health/readiness/metrics/debug").

Deliberately NOT under `/api` or `/debug`: an orchestrator's liveness/
readiness probe can't attach an `x-api-key`/`Bearer` header, so these
two routes must sit outside `core.auth.middleware`'s path-prefix check
(that middleware only inspects paths starting with "/api" or "/debug")
to be reachable at all. They also return the absolute minimum
information -- no pool/executor/cache metrics, no schema details --
since, unlike `GET /api/health` (which IS behind auth and free to be
as detailed as it likes for an operator), these two are reachable by
anyone who can route to the process at all.

Two separate endpoints, not one, because they answer two different
questions with two different correct responses to "no":

- `/live`: is this process itself up and able to serve HTTP at all?
  "No" means the process is wedged/deadlocked -- the right response is
  for the orchestrator to kill and restart the container.
- `/ready`: can this instance serve *real* traffic right now (i.e. can
  it reach the database)? "No" means route traffic elsewhere until it
  recovers -- restarting the container does nothing to fix a database
  outage, and restarting every replica at once over a shared
  dependency being briefly unreachable is its own outage.

Collapsing these into one endpoint (as the previous single `/api/health`
effectively was, being both the only health signal available and
gated behind auth so no orchestrator could use it anyway) means an
orchestrator has no way to tell "restart me" from "just don't send me
traffic yet" apart, and typically errs toward restarting -- which is
exactly wrong for a transient database blip.
"""

from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from core.db.session import DatabaseSession

probes_router = APIRouter(tags=["probes"])


@probes_router.get("/live")
async def liveness() -> JSONResponse:
    """Liveness probe: process is up and able to serve HTTP.

    Deliberately checks nothing else -- no database, no pool, no
    downstream dependency of any kind. See this module's docstring for
    why liveness and readiness must stay separate checks.
    """
    return JSONResponse({"status": "ok"}, status_code=200)


@probes_router.get("/ready")
async def readiness(request: Request) -> JSONResponse:
    """Readiness probe: can this instance serve real traffic right now?

    Checks database reachability -- the one dependency every data
    route needs -- so a load balancer can stop sending traffic to an
    instance that's up but can't reach Postgres (e.g. mid-failover),
    without the orchestrator restarting it the way a failed liveness
    probe would.

    Returns 503, not a raised exception, when not ready: "not ready
    right now" is an expected, recoverable state for a load balancer to
    observe, not a server error worth logging as one.
    """
    db_session: Optional[DatabaseSession] = getattr(
        request.app.state, "db_session", None
    )
    if db_session is None:
        return JSONResponse(
            {"status": "not_ready", "reason": "database not configured"},
            status_code=503,
        )

    try:
        is_healthy = await db_session.health_check()
    except Exception:
        is_healthy = False

    if not is_healthy:
        return JSONResponse(
            {"status": "not_ready", "reason": "database unreachable"},
            status_code=503,
        )

    return JSONResponse({"status": "ready"}, status_code=200)
