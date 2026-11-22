import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager

import pika
import psycopg
from fastapi import FastAPI, HTTPException
from opentelemetry import trace
from opentelemetry.instrumentation.utils import suppress_instrumentation
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import SpanKind
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel

DATABASE_URL = os.environ["DATABASE_URL"]
RABBIT_URL = os.environ.get("RABBIT_URL", "amqp://app:app_pw@rabbitmq:5672/%2F")
EXCHANGE = "users.events"
POLL_INTERVAL = float(os.environ.get("OUTBOX_POLL_INTERVAL", "0.5"))

log = logging.getLogger("uvicorn.error")
tracer = trace.get_tracer("users")


def get_conn(**kwargs):
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, **kwargs)


# ---------- outbox relay: legge gli eventi non pubblicati e li manda al broker ----------
def publish_pending(conn, ch) -> int:
    # il polling non deve riempire Jaeger di trace vuote: lo escludiamo dalla strumentazione
    with suppress_instrumentation():
        with conn.transaction():
            rows = conn.execute(
                """SELECT id, event_type, payload, headers FROM outbox
                   WHERE NOT published ORDER BY id LIMIT 50
                   FOR UPDATE SKIP LOCKED"""
            ).fetchall()
            for r in rows:
                # la trace riparte dal contesto salvato quando l'evento è stato creato
                parent = extract(r["headers"] or {})
                with tracer.start_as_current_span(
                    "outbox.publish", context=parent, kind=SpanKind.PRODUCER
                ) as span:
                    span.set_attribute("event.type", r["event_type"])
                    msg_headers = {}
                    inject(msg_headers)
                    ch.basic_publish(
                        exchange=EXCHANGE,
                        routing_key="",
                        body=json.dumps(r["payload"]),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type="application/json",
                            message_id=str(r["id"]),
                            type=r["event_type"],
                            headers=msg_headers,
                        ),
                    )
                    conn.execute(
                        "UPDATE outbox SET published = TRUE WHERE id = %s", (r["id"],)
                    )
    return len(rows)


def relay_loop():
    conn = None
    mq = None
    ch = None
    while True:
        try:
            if conn is None or conn.closed or conn.broken:
                conn = get_conn(autocommit=True)
            if mq is None or mq.is_closed:
                mq = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
                ch = mq.channel()
                ch.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)
                ch.confirm_delivery()
            if publish_pending(conn, ch) == 0:
                mq.sleep(POLL_INTERVAL)
        except Exception as e:
            log.warning("outbox relay: %r, riprovo tra 2 s", e)
            time.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    with get_conn() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS users (
                   id SERIAL PRIMARY KEY,
                   name TEXT NOT NULL,
                   email TEXT NOT NULL)"""
        )
        conn.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1"
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS outbox (
                   id SERIAL PRIMARY KEY,
                   event_type TEXT NOT NULL,
                   payload JSONB NOT NULL,
                   headers JSONB NOT NULL DEFAULT '{}',
                   published BOOLEAN NOT NULL DEFAULT FALSE,
                   created_at TIMESTAMPTZ NOT NULL DEFAULT now())"""
        )
    threading.Thread(target=relay_loop, daemon=True).start()
    yield


app = FastAPI(title="Users service", version="2.0.0", lifespan=lifespan)


class UserIn(BaseModel):
    name: str
    email: str


class User(UserIn):
    id: int
    version: int


def add_outbox(conn, event_type: str, user: dict):
    headers: dict = {}
    inject(headers)  # salva il contesto della trace insieme all'evento
    conn.execute(
        "INSERT INTO outbox (event_type, payload, headers) VALUES (%s, %s, %s)",
        (event_type, Jsonb(user), Jsonb(headers)),
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/users", response_model=User, status_code=201)
def create_user(data: UserIn):
    with get_conn() as conn:  # una sola transazione: utente + evento
        user = conn.execute(
            "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id, name, email, version",
            (data.name, data.email),
        ).fetchone()
        add_outbox(conn, "user.created", user)
    return user


@app.put("/users/{user_id}", response_model=User)
def update_user(user_id: int, data: UserIn):
    with get_conn() as conn:
        user = conn.execute(
            """UPDATE users SET name = %s, email = %s, version = version + 1
               WHERE id = %s RETURNING id, name, email, version""",
            (data.name, data.email, user_id),
        ).fetchone()
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")
        add_outbox(conn, "user.updated", user)
    return user


@app.get("/users", response_model=list[User])
def list_users():
    with get_conn() as conn:
        return conn.execute(
            "SELECT id, name, email, version FROM users ORDER BY id"
        ).fetchall()


@app.get("/users/{user_id}", response_model=User)
def get_user(user_id: int):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, name, email, version FROM users WHERE id = %s", (user_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    return row