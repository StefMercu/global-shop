import os
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DATABASE_URL = os.environ["DATABASE_URL"]


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