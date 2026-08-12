"""Typed exceptions for the repository layer.

Repositories used to catch every exception and collapse it to
`False`/`None`/`[]` — the same shape a legitimate "no such row" result
has. That makes "the user doesn't exist" and "PostgreSQL is down"
indistinguishable to callers, which is dangerous: a route mapping
`None` to a 404 would just as happily return 404 for an outage.

Repositories should let these propagate; the service/route layer
decides the right HTTP status by catching the specific subclass it
cares about (e.g. `DatabaseUnavailableError` -> 503,
`DuplicateRecordError` -> 409) rather than a blanket `except Exception`
that treats every failure mode the same way.
"""


class RepositoryError(Exception):
    """Base class for repository-layer failures."""


class DatabaseUnavailableError(RepositoryError):
    """The application state store could not complete the operation --
    connection lost, pool exhausted, or the query failed for reasons
    unrelated to the data itself. Distinct from a legitimate "not
    found" result (which repositories still return as None/[]/False,
    not as an exception).
    """


class DuplicateRecordError(RepositoryError):
    """The operation violated a uniqueness constraint (e.g. a username
    or API key that already exists). The caller supplied well-formed
    data; the *record* already existed -- a different situation from a
    generic database failure, and usually a 409 rather than a 503 or a
    plain 400 at the route layer.
    """
