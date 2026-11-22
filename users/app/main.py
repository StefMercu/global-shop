import os
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DATABASE_URL = os.environ["DATABASE_URL"]


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


@asynccontextmanager
async def lifespan(app: FastAPI):
    with get_conn() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS users (
                   id SERIAL PRIMARY KEY,
                   name TEXT NOT NULL,
                   email TEXT NOT NULL)"""
        )
    yield


app = FastAPI(title="Users service", version="1.0.0", lifespan=lifespan)


class UserIn(BaseModel):
    name: str
    email: str


class User(UserIn):
    id: int


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/users", response_model=User, status_code=201)
def create_user(data: UserIn):
    with get_conn() as conn:
        return conn.execute(
            "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id, name, email",
            (data.name, data.email),
        ).fetchone()


@app.get("/users", response_model=list[User])
def list_users():
    with get_conn() as conn:
        return conn.execute("SELECT id, name, email FROM users ORDER BY id").fetchall()


@app.get("/users/{user_id}", response_model=User)
def get_user(user_id: int):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, name, email FROM users WHERE id = %s", (user_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    return row