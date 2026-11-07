"""Layanan B - Inventory API (REST) + Notification/Warehouse worker (RabbitMQ consumer).

Idempoten: tiap event punya event_id; event_id yang sudah diproses dilewati
(tetap di-ack) sehingga pengiriman ulang (at-least-once) tidak menggandakan efek.
"""
import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager

import pika
from fastapi import FastAPI, HTTPException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [B] %(message)s")
log = logging.getLogger("service-b")

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
DB_PATH = os.getenv("DB_PATH", "inventory.db")
QUEUE = "order_events"
DLQ = "order_events.dlq"
QUEUE_ARGS = {"x-dead-letter-exchange": "", "x-dead-letter-routing-key": DLQ}

SEED = [("A1", "Buku", 50_000, 100), ("B2", "Pulpen", 5_000, 500), ("C3", "Tas", 250_000, 20)]


def db():
    return sqlite3.connect(DB_PATH, timeout=10)


def init_db():
    c = db()
    with c:
        c.execute("CREATE TABLE IF NOT EXISTS stock(sku TEXT PRIMARY KEY, name TEXT, price INT, stock INT)")
        c.execute("CREATE TABLE IF NOT EXISTS processed(event_id TEXT PRIMARY KEY)")
        c.execute(
            "CREATE TABLE IF NOT EXISTS notifications("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT, order_id INT, email TEXT, subject TEXT)"
        )
        c.executemany("INSERT OR IGNORE INTO stock VALUES (?,?,?,?)", SEED)
    c.close()


def process_event(ev: dict) -> str:
    """Satu transaksi atomik: tandai diproses + ubah stok + catat notifikasi."""
    c = db()
    try:
        with c:  # commit/rollback otomatis
            try:
                c.execute("INSERT INTO processed VALUES (?)", (ev["event_id"],))
            except sqlite3.IntegrityError:
                return "duplicate"
            sign = -1 if ev["type"] == "order.created" else 1
            for it in ev["items"]:
                c.execute("UPDATE stock SET stock = stock + ? WHERE sku = ?", (sign * it["qty"], it["sku"]))
            subject = (
                f"Pesanan #{ev['order_id']} dikonfirmasi"
                if ev["type"] == "order.created"
                else f"Pesanan #{ev['order_id']} dibatalkan"
            )
            c.execute(
                "INSERT INTO notifications(event_id, order_id, email, subject) VALUES (?,?,?,?)",
                (ev["event_id"], ev["order_id"], ev["email"], subject),
            )
    finally:
        c.close()
    log.info("EMAIL ke %s: %s", ev["email"], subject)
    return "processed"


def on_message(ch, method, props, body):
    try:
        ev = json.loads(body)
        result = process_event(ev)
        if result == "duplicate":
            log.info("duplikat dilewati: %s", ev["event_id"])
        ch.basic_ack(delivery_tag=method.delivery_tag)  # ack SETELAH selesai
    except Exception as e:  # noqa: BLE001
        log.error("pesan gagal diproses -> DLQ: %s", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def consume_forever():
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
            ch = conn.channel()
            ch.queue_declare(queue=DLQ, durable=True)
            ch.queue_declare(queue=QUEUE, durable=True, arguments=QUEUE_ARGS)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
            log.info("consumer siap menunggu pesan")
            ch.start_consuming()
        except Exception as e:  # noqa: BLE001
            log.warning("consumer terputus, coba lagi 3 dtk: %s", e)
            time.sleep(3)


@asynccontextmanager
async def lifespan(_app):
    init_db()
    threading.Thread(target=consume_forever, daemon=True).start()
    yield


app = FastAPI(title="Layanan B - Inventory & Notification", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/stock/{sku}")
def get_stock(sku: str):
    c = db()
    row = c.execute("SELECT sku, name, price, stock FROM stock WHERE sku=?", (sku,)).fetchone()
    c.close()
    if not row:
        raise HTTPException(404, "SKU tidak ada")
    return {"sku": row[0], "name": row[1], "price": row[2], "stock": row[3]}


@app.get("/report")
def report():
    c = db()
    stock = {r[0]: r[1] for r in c.execute("SELECT sku, stock FROM stock")}
    notifs = [
        {"order_id": r[0], "email": r[1], "subject": r[2]}
        for r in c.execute("SELECT order_id, email, subject FROM notifications ORDER BY id")
    ]
    n = c.execute("SELECT COUNT(*) FROM processed").fetchone()[0]
    c.close()
    return {"events_processed": n, "stock": stock, "notifications": notifs}
