from typing import List, Optional

from fastapi import FastAPI, HTTPException

from . import db

app = FastAPI()


@app.on_event("startup")
async def on_startup() -> None:
    await db.init_pool()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await db.close_pool()


async def _search_author(
    author_name: str,
    publish_by_date: Optional[str],
) -> List[dict]:
    """
    Call PostgreSQL function:
        search_author(author_name text, publish_by_date text default ...)
    If publish_by_date is None, omit it so the DB default is used.
    """
    author_name = author_name.strip()

    async with db.get_connection() as conn:
        async with conn.cursor() as cur:
            if publish_by_date is None:
                query = "SELECT * FROM search_author(%s);"
                params = (author_name,)
            else:
                query = "SELECT * FROM search_author(%s, %s);"
                params = (author_name, publish_by_date)

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
    """
    book_name = book_name.strip()

    async with db.get_connection() as conn:
        async with conn.cursor() as cur:
            if publish_by_date is None:
                query = "SELECT * FROM search_books(%s);"
                params = (book_name,)
            else:
                query = "SELECT * FROM search_books(%s, %s);"
                params = (book_name, publish_by_date)

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
    """
    return await _search_books(
        book_name=book_name,
        publish_by_date=publish_by_date,
    )
