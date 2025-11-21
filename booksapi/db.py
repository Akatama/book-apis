import os
from typing import AsyncIterator, Optional

from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

# Global async connection pool
pool: Optional[AsyncConnectionPool] = None


def _build_dsn_from_env() -> str:
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    if not all([db_name, db_user, db_password]):
        raise RuntimeError(
            "Database configuration missing. "
            "Ensure DB_NAME, DB_USER, and DB_PASSWORD are set."
        )

    # Plain psycopg3 DSN
    return (
        f"postgresql://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}"
    )


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
