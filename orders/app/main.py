import os
from contextlib import asynccontextmanager
import logging
import random
import time

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DATABASE_URL = os.environ["DATABASE_URL"]

USERS_URL = os.environ.get("USERS_URL", "http://users:8000")
USERS_TIMEOUT = float(os.environ.get("USERS_TIMEOUT", "1.0"))
USERS_RETRIES = int(os.environ.get("USERS_RETRIES", "3"))
BACKOFF_BASE = 0.2

log = logging.getLogger("uvicorn.error")


class UsersUnavailable(Exception):
    pass


def user_exists(user_id: int) -> bool:
    last_error = None
    for attempt in range(1, USERS_RETRIES + 1):
        try:
            r = httpx.get(f"{USERS_URL}/users/{user_id}", timeout=USERS_TIMEOUT)
            if r.status_code == 200:
                return True
            if r.status_code == 404:
                return False
            last_error = f"HTTP {r.status_code}"
        except httpx.HTTPError as e:
            last_error = repr(e)

        log.warning("Users: tentativo %d/%d fallito (%s)", attempt, USERS_RETRIES, last_error)
        if attempt < USERS_RETRIES:
            # backoff esponenziale con jitter: attesa casuale tra 0 e base * 2^(n-1)
            time.sleep(random.uniform(0, BACKOFF_BASE * 2 ** (attempt - 1)))

    raise UsersUnavailable(last_error)


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


@asynccontextmanager
async def lifespan(app: FastAPI):
    with get_conn() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS orders (
                   id SERIAL PRIMARY KEY,
                   user_id INTEGER NOT NULL,
                   item TEXT NOT NULL,
                   quantity INTEGER NOT NULL,
                   status TEXT NOT NULL DEFAULT 'created')"""
        )
    yield


app = FastAPI(title="Orders service", version="1.0.0", lifespan=lifespan)


class OrderIn(BaseModel):
    user_id: int
    item: str
    quantity: int = Field(gt=0)


class Order(OrderIn):
    id: int
    status: str = "created"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", response_model=Order, status_code=201)
def create_order(data: OrderIn):
    try:
        exists = user_exists(data.user_id)
    except UsersUnavailable:
        raise HTTPException(status_code=503, detail="Users service unavailable")
    if not exists:
        raise HTTPException(status_code=422, detail="User does not exist")

    with get_conn() as conn:
        return conn.execute(
            """INSERT INTO orders (user_id, item, quantity)
               VALUES (%s, %s, %s)
               RETURNING id, user_id, item, quantity, status""",
            (data.user_id, data.item, data.quantity),
        ).fetchone()

@app.get("/orders", response_model=list[Order])
def list_orders():
    with get_conn() as conn:
        return conn.execute(
            "SELECT id, user_id, item, quantity, status FROM orders ORDER BY id"
        ).fetchall()


@app.get("/orders/{order_id}", response_model=Order)
def get_order(order_id: int):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, user_id, item, quantity, status FROM orders WHERE id = %s",
            (order_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return row