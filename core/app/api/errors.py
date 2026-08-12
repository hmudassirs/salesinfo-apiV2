"""Maps `core.storage.exceptions.RepositoryError` subclasses to HTTP
responses, so every route that touches a repository doesn't need to
hand-roll the same `except DatabaseUnavailableError: ... except
DuplicateRecordError: ...` chain.
"""

from fastapi import HTTPException

from core.storage.exceptions import DatabaseUnavailableError, DuplicateRecordError


def http_exception_for(
    error: Exception, *, default_status: int = 400, default_detail: str = "Request failed"
) -> HTTPException:
    """Translate a caught exception into the right `HTTPException`.

    `DatabaseUnavailableError` -> 503 (the request was fine; the store
    wasn't reachable -- worth a retry). `DuplicateRecordError` -> 409
    (the request was fine; the record already existed). Anything else,
    including other `RepositoryError` subclasses, falls back to
    `default_status`/`default_detail` -- callers still log the original
    exception themselves before calling this, so nothing about the
    real cause is lost, this only decides what the client sees.
    """
    if isinstance(error, DatabaseUnavailableError):
        return HTTPException(status_code=503, detail="Service temporarily unavailable")
    if isinstance(error, DuplicateRecordError):
        return HTTPException(status_code=409, detail=str(error))
    return HTTPException(status_code=default_status, detail=default_detail)
