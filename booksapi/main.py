from typing import List, Optional

from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

import db  # import as a plain module, since "booksapi" is not a package/module


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await db.init_pool()
    try:
        yield
    finally:
        # Shutdown
        await db.close_pool()


app = FastAPI(title="Books API", lifespan=lifespan)


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
    try:
        return await db.search_author(
            author_name=author_name,
            publish_by_date=publish_by_date,
        )
    except Exception as exc:  # noqa: BLE001
        # In a real app, log the exception details.
        raise HTTPException(
            status_code=500,
            detail="Error querying database using search_author",
        ) from exc


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
    try:
        return await db.search_books(
            book_name=book_name,
            publish_by_date=publish_by_date,
        )
    except Exception as exc:  # noqa: BLE001
        # In a real app, log the exception details.
        raise HTTPException(
            status_code=500,
            detail="Error querying database using search_books",
        ) from exc
