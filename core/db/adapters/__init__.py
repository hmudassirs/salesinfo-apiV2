# core/db/adapters/__init__.py
"""Database adapters for different database engines.

PostgreSQL only -- see core.db.config.DatabaseType's docstring. This
import is eager (not the lazy __getattr__ pattern used elsewhere in
this codebase) deliberately: importing this package is what triggers
PostgreSQLAdapter's self-registration with the AdapterRegistry (see
the bottom of core/db/adapters/postgresql.py). core/db/session.py
imports this package specifically for that side effect — it never
names PostgreSQLAdapter directly; see session.py's _create_adapter().
"""

from core.db.adapters.postgresql import PostgreSQLAdapter

__all__ = [
    "PostgreSQLAdapter",
]
