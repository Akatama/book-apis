import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional
from urllib.parse import quote_plus

from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

# Global async connection pool
pool: Optional[AsyncConnectionPool] = None


def _build_dsn_from_env() -> str:
    """
    Build a PostgreSQL DSN from environment variables.

    Uses only:
      - DB_NAME
      - DB_USER
      - DB_PASSWORD
      - DB_HOST (defaults to "localhost" if unset)
      - DB_PORT (defaults to "5432" if unset)
    """
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST") or "localhost"
    db_port = os.getenv("DB_PORT") or "5432"

    if not all([db_name, db_user, db_password]):
        raise RuntimeError(
            "Database configuration missing. "
            "Required environment variables: DB_NAME, DB_USER, DB_PASSWORD. "
            "Optional: DB_HOST (default 'localhost'), DB_PORT (default '5432')."
        )

    # URL-encode user and password to safely handle special characters
    user_enc = quote_plus(db_user)
    password_enc = quote_plus(db_password)

    # Plain psycopg3 DSN (Data Source Name)
    dsn = f"postgresql://{user_enc}:{password_enc}@{db_host}:{db_port}/{db_name}"

    return dsn


async def init_pool() -> None:
    """Initialize the global async connection pool."""
    global pool
    if pool is not None:
        return

    dsn = _build_dsn_from_env()
    pool = AsyncConnectionPool(
        conninfo=dsn,
        min_size=1,
        max_size=10,
        kwargs={"row_factory": dict_row},
    )


async def close_pool() -> None:
    """Close the global async connection pool."""
    global pool
    if pool is not None:
        await pool.close()
        pool = None


@asynccontextmanager
async def get_connection() -> AsyncIterator:
    """
    Async context manager helper to get a connection from the pool.

    Usage:
        async with get_connection() as conn:
            ...
    """
    if pool is None:
        raise RuntimeError("Database connection pool is not initialized")

    async with pool.connection() as conn:
        yield conn
