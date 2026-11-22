import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager

import httpx
import pika
import psycopg
from fastapi import FastAPI, HTTPException
from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

DATABASE_URL = os.environ["DATABASE_URL"]
USERS_URL = os.environ.get("USERS_URL", "http://users:8000")
RABBIT_URL = os.environ.get("RABBIT_URL", "amqp://app:app_pw@rabbitmq:5672/%2F")
EXCHANGE = "users.events"
QUEUE = "orders.users-projection"

log = logging.getLogger("uvicorn.error")
tracer = trace.get_tracer("orders")


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


# ---------- proiezione locale degli utenti ----------
def upsert_user(u: dict):
    """Idempotente: applica solo versioni più nuove di quella già presente."""
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO users_projection (id, name, email, version)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (id) DO UPDATE
               SET name = EXCLUDED.name, email = EXCLUDED.email,
                   version = EXCLUDED.version, updated_at = now()
               WHERE EXCLUDED.version > users_projection.version""",
            (u["id"], u["name"], u["email"], u["version"]),
        )


def load_snapshot():
    with tracer.start_as_current_span("users.snapshot") as span:
        r = httpx.get(f"{USERS_URL}/users", timeout=5.0)
        r.raise_for_status()
        users = r.json()
        for u in users:
            upsert_user(u)
        span.set_attribute("snapshot.count", len(users))
        log.info("Snapshot utenti caricato: %d utenti", len(users))


def on_message(ch, method, properties, body):
    ctx = extract(properties.headers or {})
    with tracer.start_as_current_span(
        "user.event.apply", context=ctx, kind=SpanKind.CONSUMER
    ) as span:
        event = json.loads(body)
        span.set_attribute("event.type", properties.type or "")
        span.set_attribute("user.id", event["id"])
        span.set_attribute("user.version", event["version"])
        upsert_user(event)
    ch.basic_ack(delivery_tag=method.delivery_tag)  # ack solo dopo aver applicato


def projection_worker():
    snapshot_done = False
    while True:
        mq = None
        try:
            mq = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
            ch = mq.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)
            ch.queue_declare(queue=QUEUE, durable=True)
            ch.queue_bind(queue=QUEUE, exchange=EXCHANGE)
            if not snapshot_done:  # prima la coda, poi lo snapshot: nessun evento perso
                load_snapshot()
                snapshot_done = True
            ch.basic_qos(prefetch_count=10)
            ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
            ch.start_consuming()
        except Exception as e:
            log.warning("projection worker: %r, riprovo tra 2 s", e)
        finally:
            if mq is not None and mq.is_open:
                try:
                    mq.close()
                except Exception:
                    pass
        time.sleep(2)


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
        conn.execute(
            """CREATE TABLE IF NOT EXISTS users_projection (
                   id INTEGER PRIMARY KEY,
                   name TEXT NOT NULL,
                   email TEXT NOT NULL,
                   version INTEGER NOT NULL,
                   updated_at TIMESTAMPTZ NOT NULL DEFAULT now())"""
        )
    threading.Thread(target=projection_worker, daemon=True).start()
    yield


app = FastAPI(title="Orders service", version="2.0.0", lifespan=lifespan)


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
        known = conn.execute(
            "SELECT 1 FROM users_projection WHERE id = %s", (data.user_id,)
        ).fetchone()
        if known is None:
            raise HTTPException(
                status_code=422, detail="User unknown (not in local projection)"
            )
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