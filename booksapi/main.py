import os
from typing import Any, List, Optional

from fastapi import FastAPI, HTTPException
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

app = FastAPI(title="Books API")

# Global async connection pool (could be moved to its own module, e.g. booksapi/db.py)
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

    return (
        f"postgresql+psycopg://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}"
    )


@app.on_event("startup")
async def on_startup() -> None:
    global pool
    dsn = _build_dsn_from_env()
    # Configure the async connection pool
    pool = AsyncConnectionPool(
        conninfo=dsn,
        min_size=1,
        max_size=10,
        kwargs={"row_factory": dict_row},
    )


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global pool
    if pool is not None:
        await pool.close()
        pool = None


async def _call_search_function(
    func_name: str,
    first_arg: str,
    publish_by_date: Optional[str],
) -> List[dict]:
    """
    Helper to call a PostgreSQL function that takes (text, text | null)
    and returns a set of rows.
    """
    if pool is None:
        raise RuntimeError("Database connection pool is not initialized")

    # Normalize user input a bit; psycopg will handle escaping safely.
    first_arg = first_arg.strip()

    query = f"SELECT * FROM {func_name}(%s, %s);"
    params = (first_arg, publish_by_date)

    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                rows: List[dict] = await cur.fetchall()
                return rows
    except Exception as exc:  # noqa: BLE001
        # In a real app, log the exception details.
        raise HTTPException(
            status_code=500,
            detail=f"Error querying database using {func_name}",
        ) from exc


@app.get("/author/{author_name}")
async def get_books_by_author(
    author_name: str,
    publish_by_date: Optional[str] = None,
) -> List[dict]:
    """
    Get books by author using the PostgreSQL function:
        search_author(author_name, publish_by_date)
    """
    rows = await _call_search_function(
        func_name="search_author",
        first_arg=author_name,
        publish_by_date=publish_by_date,
    )
    return rows


@app.get("/books/{book_name}")
async def get_books_by_title(
    book_name: str,
    publish_by_date: Optional[str] = None,
) -> List[dict]:
    """
    Get books by title using the PostgreSQL function:
        search_books(book_name, publish_by_date)
    """
    rows = await _call_search_function(
        func_name="search_books",
        first_arg=book_name,
        publish_by_date=publish_by_date,
    )
    return rows
