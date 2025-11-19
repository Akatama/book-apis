import os
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends
from psycopg_pool import AsyncConnectionPool


def get_conn_str():
    return f"""
    dbname={os.getenv("DB_NAME")}
    user={os.getenv("DB_USER")}
    password={os.getenv("DB_PASSWORD")}
    host={os.getenv("DB_HOST")}
    port={os.getenv("DB_PORT")}
    """


pool = AsyncConnectionPool(get_conn_str(), open=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await pool.open()
    yield
    await pool.close()


app = FastAPI(lifespan=lifespan)


async def get_conn():
    async with pool.connection() as conn:
        yield conn


@app.get("/author/{author_name}/{publish_by_date}")
async def get_books_by_author(
    author_name: str,
    request: Request,
    conn=Depends(get_conn),
    publish_by_date: Optional[str] = None,
):
    async with conn.cursor() as cur:
        if publish_by_date:
            await cur.execute(f"""
                        SELECT * FROM search_author('%{author_name}%', '{publish_by_date}')
                        """)
        else:
            await cur.execute(f"""
                        SELECT * FROM search_author('%{author_name}%')
                                   """)
        results = await cur.fetchall()
        return results


@app.get("/book/{book_title}/{publish_by_date}")
async def get_books_by_title(
    book_title: str,
    request: Request,
    conn=Depends(get_conn),
    publish_by_date: Optional[str] = None,
):
    async with conn.cursor() as cur:
        if publish_by_date:
            await cur.execute(f"""
                        SELECT * FROM search_books('%{book_title}%', '{publish_by_date}')
                        """)
        else:
            await cur.execute(f"""
                        SELECT * FROM search_books('%{book_title}%')
                                   """)
        results = await cur.fetchall()
        return results
