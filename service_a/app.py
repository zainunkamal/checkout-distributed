"""Layanan A - Order/Checkout API.

Komunikasi:
  * SINKRON  : REST (httpx) ke Layanan B untuk cek stok, dengan timeout + retry (GET idempoten).
  * ASINKRON : publish event ke RabbitMQ (durable queue, persistent message).
"""
import json
import logging
import os
import threading
import time

import httpx
import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [A] %(message)s")
log = logging.getLogger("service-a")

INVENTORY_URL = os.getenv("INVENTORY_URL", "http://localhost:8001")
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
TIMEOUT = float(os.getenv("INVENTORY_TIMEOUT", "2"))
QUEUE = "order_events"
DLQ = "order_events.dlq"
QUEUE_ARGS = {"x-dead-letter-exchange": "", "x-dead-letter-routing-key": DLQ}
PAYMENT_LIMIT = 10_000_000  # simulasi otorisasi pembayaran

app = FastAPI(title="Layanan A - Order Service")
orders: dict[int, dict] = {}
_next_id = 1
_lock = threading.Lock()


class ItemIn(BaseModel):
    sku: str
    qty: int = Field(gt=0)


class CheckoutIn(BaseModel):
    customer: str
    email: str
    items: list[ItemIn] = Field(min_length=1)


# ---------- komunikasi sinkron ----------
def check_stock(sku: str, qty: int) -> dict:
    """GET /stock/{sku} ke Layanan B. Aman di-retry karena idempoten (at-least-once)."""
    kind = None
    for attempt in (1, 2):
        try:
            r = httpx.get(f"{INVENTORY_URL}/stock/{sku}", timeout=TIMEOUT)
            break
        except httpx.TimeoutException:
            kind = "timeout"
            log.warning("timeout cek stok %s (percobaan %d)", sku, attempt)
        except httpx.TransportError as e:
            kind = "down"
            log.warning("Layanan B tidak terjangkau (percobaan %d): %s", attempt, e)
    else:
        if kind == "timeout":
            raise HTTPException(504, "Layanan inventory timeout")
        raise HTTPException(503, "Layanan inventory tidak tersedia")

    if r.status_code == 404:
        raise HTTPException(404, f"SKU {sku} tidak ditemukan")
    if r.status_code >= 500:
        raise HTTPException(502, "Layanan inventory error")
    data = r.json()
    if data["stock"] < qty:
        raise HTTPException(409, f"Stok {sku} tidak cukup (tersedia {data['stock']})")
    return data


def authorize_payment(total: int) -> None:
    if total > PAYMENT_LIMIT:
        raise HTTPException(402, "Otorisasi pembayaran ditolak (melebihi limit)")


# ---------- komunikasi asinkron ----------
def publish_event(event: dict) -> bool:
    """Titip event ke broker lalu lanjut. Gagal publish tidak menggagalkan order."""
    try:
        params = pika.URLParameters(RABBIT_URL)
        params.socket_timeout = 3
        params.connection_attempts = 1
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.queue_declare(queue=DLQ, durable=True)
        ch.queue_declare(queue=QUEUE, durable=True, arguments=QUEUE_ARGS)
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=json.dumps(event).encode(),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent (disimpan ke disk)
                content_type="application/json",
                message_id=event["event_id"],
            ),
        )
        conn.close()
        log.info("event terkirim: %s", event["event_id"])
        return True
    except Exception as e:  # noqa: BLE001
        log.error("gagal publish %s: %s", event["event_id"], e)
        return False


def make_event(kind: str, order: dict) -> dict:
    return {
        "event_id": f"{order['id']}:{kind}",  # kunci idempotensi
        "type": kind,
        "order_id": order["id"],
        "customer": order["customer"],
        "email": order["email"],
        "items": order["items"],
        "total": order["total"],
        "ts": time.time(),
    }


# ---------- endpoint ----------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/checkout", status_code=201)
def checkout(body: CheckoutIn):
    global _next_id
    total = 0
    for it in body.items:                       # 1) sinkron: cek stok
        info = check_stock(it.sku, it.qty)
        total += info["price"] * it.qty
    authorize_payment(total)                    # 2) sinkron: otorisasi bayar

    with _lock:
        order_id = _next_id
        _next_id += 1
    order = {
        "id": order_id,
        "customer": body.customer,
        "email": body.email,
        "items": [i.model_dump() for i in body.items],
        "total": total,
        "status": "CONFIRMED",
    }
    orders[order_id] = order
    published = publish_event(make_event("order.created", order))  # 3) asinkron
    return {**order, "event_published": published}


@app.get("/orders/{order_id}")
def get_order(order_id: int):
    if order_id not in orders:
        raise HTTPException(404, "Order tidak ada")
    return orders[order_id]


@app.post("/orders/{order_id}/cancel")
def cancel_order(order_id: int):
    """Murni asinkron: tidak butuh Layanan B hidup. Event menunggu di broker."""
    order = orders.get(order_id)
    if not order:
        raise HTTPException(404, "Order tidak ada")
    if order["status"] == "CANCELLED":
        raise HTTPException(409, "Order sudah dibatalkan")
    order["status"] = "CANCELLED"
    published = publish_event(make_event("order.cancelled", order))
    return {**order, "event_published": published}
