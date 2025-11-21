from typing import List, Optional

from fastapi import FastAPI, HTTPException

import db  # import as a plain module, since "booksapi" is not a package/module

app = FastAPI(title="Books API")


@app.on_event("startup")
async def on_startup() -> None:
    await db.init_pool()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await db.close_pool()


def _make_like_pattern(value: str) -> str:
    """
    Build a case-insensitive LIKE pattern for partial matches.

    - Strips leading/trailing whitespace.
    - Wraps the value in %...% so we can match substrings.
    """
    value = value.strip()
    # If the user passes an empty string, keep it as-is; the DB function
    # can decide how to handle that (e.g., return nothing or everything).
    if not value:
        return value
    return f"%{value}%"


async def _search_author(
    author_name: str,
    publish_by_date: Optional[str],
) -> List[dict]:
    """
    Call PostgreSQL function:
        search_author(author_name text, publish_by_date text default ...)
    If publish_by_date is None, omit it so the DB default is used.

    The author_name is passed as a pattern (e.g. %name%) so that the
    database function can perform partial / fuzzy matching using LIKE/ILIKE.
    """
    author_pattern = _make_like_pattern(author_name)

    async with db.get_connection() as conn:
        async with conn.cursor() as cur:
            if publish_by_date is None:
                query = "SELECT * FROM search_author(%s);"
                params = (author_pattern,)
            else:
                query = "SELECT * FROM search_author(%s, %s);"
                params = (author_pattern, publish_by_date)

            try:
                await cur.execute(query, params)
                rows: List[dict] = await cur.fetchall()
                return rows
            except Exception as exc:  # noqa: BLE001
                # In a real app, log the exception details.
                raise HTTPException(
                    status_code=500,
                    detail="Error querying database using search_author",
                ) from exc


async def _search_books(
    book_name: str,
    publish_by_date: Optional[str],
) -> List[dict]:
    """
    Call PostgreSQL function:
        search_books(book_name text, publish_by_date text default ...)
    If publish_by_date is None, omit it so the DB default is used.

    The book_name is passed as a pattern (e.g. %name%) so that the
    database function can perform partial / fuzzy matching using LIKE/ILIKE.
    """
    book_pattern = _make_like_pattern(book_name)

    async with db.get_connection() as conn:
        async with conn.cursor() as cur:
            if publish_by_date is None:
                query = "SELECT * FROM search_books(%s);"
                params = (book_pattern,)
            else:
                query = "SELECT * FROM search_books(%s, %s);"
                params = (book_pattern, publish_by_date)

            try:
                await cur.execute(query, params)
                rows: List[dict] = await cur.fetchall()
                return rows
            except Exception as exc:  # noqa: BLE001
                # In a real app, log the exception details.
                raise HTTPException(
                    status_code=500,
                    detail="Error querying database using search_books",
                ) from exc


@app.get("/author/{author_name}")
async def get_books_by_author(
    author_name: str,
    publish_by_date: Optional[str] = None,
) -> List[dict]:
    """
    Get books by author using the PostgreSQL function:
        search_author(author_name, publish_by_date)

    The author_name path parameter is treated as a partial match pattern.
    """
    return await _search_author(
        author_name=author_name,
        publish_by_date=publish_by_date,
    )


@app.get("/books/{book_name}")
async def get_books_by_title(
    book_name: str,
    publish_by_date: Optional[str] = None,
) -> List[dict]:
    """
    Get books by title using the PostgreSQL function:
        search_books(book_name, publish_by_date)

    The book_name path parameter is treated as a partial match pattern.
    """
    return await _search_books(
        book_name=book_name,
        publish_by_date=publish_by_date,
    )
