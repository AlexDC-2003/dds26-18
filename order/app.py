import logging
import os
import atexit
import random
import uuid
import time
import json
from kafka_bus import KafkaBus
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

INTERNAL_TRANSPORT = os.environ.get("INTERNAL_TRANSPORT", "rest")
KAFKA_TIMEOUT_SEC = float(os.environ.get("KAFKA_REQUEST_TIMEOUT_SEC", "2"))

kafka_bus = KafkaBus()

if INTERNAL_TRANSPORT == "kafka":
    kafka_bus.start()
    atexit.register(kafka_bus.stop)

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

# Saga transaction storage

ORDER_TX_KEY_PREFIX = "order_tx:"   # maps order_id -> tx_id
TX_KEY_PREFIX = "tx:"              # maps tx_id -> tx record

# minimal saga states for the coordinator
TX_STARTED = "STARTED"
TX_STOCK_RESERVED = "STOCK_RESERVED"
TX_PAYMENT_DONE = "PAYMENT_DONE"
TX_COMPLETED = "COMPLETED"
TX_ABORTED = "ABORTED"


class OrderTxValue(Struct):
    tx_id: str
    order_id: str
    user_id: str
    total_cost: int

    # snapshot of what we intended to buy (helps idempotency / debugging)
    items: list[tuple[str, int]]

    # saga progress
    state: str

    # what stock we've successfully subtracted so far (for idempotency + compensation)
    reserved_items: list[tuple[str, int]]

    # has payment been executed successfully?
    payment_done: bool

    # timestamps + error info
    created_at: float
    updated_at: float
    error: str | None

def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry

def _order_tx_key(order_id: str) -> str:
    return f"{ORDER_TX_KEY_PREFIX}{order_id}"


def _tx_key(tx_id: str) -> str:
    return f"{TX_KEY_PREFIX}{tx_id}"


def _get_or_create_tx_id(order_id: str) -> str:
    """
    Idempotency anchor:
    Ensures every order_id gets exactly one tx_id (even if /checkout is called multiple times).
    """
    try:
        existing = db.get(_order_tx_key(order_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    if existing:
        return existing.decode()

    new_tx_id = str(uuid.uuid4())
    try:
        # NX = only set if not exists (prevents two concurrent checkouts creating two tx_ids)
        ok = db.set(_order_tx_key(order_id), new_tx_id, nx=True)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    if ok:
        return new_tx_id

    # someone else set it between our GET and SET NX; read it now
    try:
        existing2 = db.get(_order_tx_key(order_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    return existing2.decode() if existing2 else new_tx_id


def _get_tx(tx_id: str) -> OrderTxValue | None:
    try:
        raw = db.get(_tx_key(tx_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return msgpack.decode(raw, type=OrderTxValue) if raw else None


def _save_tx(tx: OrderTxValue) -> None:
    tx.updated_at = time.time()
    try:
        db.set(_tx_key(tx.tx_id), msgpack.encode(tx))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


def _get_or_create_tx_record(tx_id: str, order_id: str, order_entry: OrderValue) -> OrderTxValue:
    """
    Durable tx record creator.
    If record exists, returns it; otherwise creates STARTED record.
    """
    existing = _get_tx(tx_id)
    if existing:
        return existing

    now = time.time()
    tx = OrderTxValue(
        tx_id=tx_id,
        order_id=order_id,
        user_id=order_entry.user_id,
        total_cost=order_entry.total_cost,
        items=list(order_entry.items),
        state=TX_STARTED,
        reserved_items=[],
        payment_done=False,
        created_at=now,
        updated_at=now,
        error=None,
    )

    # try to create only if missing (helps if two requests race)
    try:
        db.set(_tx_key(tx_id), msgpack.encode(tx), nx=True)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    # if NX failed because someone else created it, read back
    return _get_tx(tx_id) or tx

@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url, timeout=3)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url, timeout=3)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response

def reserve_stock(tx_id: str, item_id: str, quantity: int):
    if INTERNAL_TRANSPORT != "kafka":
        return send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")

    cmd = {
        "msg_id": str(uuid.uuid4()),
        "tx_id": tx_id,
        "type": "reserve_stock",
        "payload": {"item_id": item_id, "quantity": quantity},
    }
    reply = kafka_bus.request(
        os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC
    )
    return reply  # later interpret ok/error 


def release_stock(tx_id: str, item_id: str, quantity: int):
    if INTERNAL_TRANSPORT != "kafka":
        return send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")

    cmd = {
        "msg_id": str(uuid.uuid4()),
        "tx_id": tx_id,
        "type": "release_stock",
        "payload": {"item_id": item_id, "quantity": quantity},
    }
    reply = kafka_bus.request(
        os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC
    )
    return reply


def charge_user(tx_id: str, user_id: str, amount: int):
    if INTERNAL_TRANSPORT != "kafka":
        return send_post_request(f"{GATEWAY_URL}/payment/pay/{user_id}/{amount}")

    cmd = {
        "msg_id": str(uuid.uuid4()),
        "tx_id": tx_id,
        "type": "charge_user",
        "payload": {"user_id": user_id, "amount": amount},
    }
    reply = kafka_bus.request(
        os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC
    )
    return reply

@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    if order_entry.paid:
        abort(400, "Order already paid; cannot add items")
    if db.get(_order_tx_key(order_id)):
        abort(400, "Checkout already started; cannot add items")
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(tx_id: str, removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        release_stock(tx_id, item_id, quantity)

def _reserved_as_dict(reserved_items: list[tuple[str, int]]) -> dict[str, int]:
    d: dict[str, int] = defaultdict(int)
    for item_id, qty in reserved_items:
        d[item_id] += qty
    return d


def _add_reserved(tx: OrderTxValue, item_id: str, qty: int) -> None:
    # merge into tx.reserved_items
    for i, (iid, q) in enumerate(tx.reserved_items):
        if iid == item_id:
            tx.reserved_items[i] = (iid, q + qty)
            return
    tx.reserved_items.append((item_id, qty))

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")

    order_entry: OrderValue = get_order_from_db(order_id)

    # Idempotency shortcut: if already paid, do nothing
    if order_entry.paid:
        return Response("Checkout successful", status=200)

    # 1) Get/create tx_id for this order + load/create durable tx record
    tx_id = _get_or_create_tx_id(order_id)
    tx = _get_or_create_tx_record(tx_id, order_id, order_entry)

    # If tx already finished, behave idempotently
    if tx.state == TX_COMPLETED:
        return Response("Checkout successful", status=200)
    if tx.state == TX_ABORTED:
        abort(400, tx.error or "Checkout failed")

    # 2) Aggregate quantities per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in tx.items:
        items_quantities[item_id] += quantity

    # 3) STOCK step (idempotent):
    # reserve missing quantities only; persist after each success
    if tx.state == TX_STARTED:
        reserved_now = _reserved_as_dict(tx.reserved_items)

        for item_id, quantity in items_quantities.items():
            already = reserved_now.get(item_id, 0)
            if already >= quantity:
                continue

            to_reserve = quantity - already
            stock_reply = reserve_stock(tx.tx_id, item_id, to_reserve)
            if stock_reply.status_code != 200:
                # abort whole saga; release anything we reserved so far
                rollback_stock(tx.tx_id, tx.reserved_items)
                tx.state = TX_ABORTED
                tx.error = f"Out of stock on item_id: {item_id}"
                _save_tx(tx)
                abort(400, tx.error)

            _add_reserved(tx, item_id, to_reserve)
            _save_tx(tx)  # durable progress

        tx.state = TX_STOCK_RESERVED
        _save_tx(tx)

    # 4) PAYMENT step (idempotent)
    if tx.state == TX_STOCK_RESERVED and not tx.payment_done:
        user_reply = charge_user(tx.tx_id, tx.user_id, tx.total_cost)
        if user_reply.status_code != 200:
            rollback_stock(tx.tx_id, tx.reserved_items)
            tx.state = TX_ABORTED
            tx.error = "User out of credit"
            _save_tx(tx)
            abort(400, tx.error)

        tx.payment_done = True
        tx.state = TX_PAYMENT_DONE
        _save_tx(tx)

    # 5) Finalize order (idempotent-safe)
    if tx.state == TX_PAYMENT_DONE:
        order_entry.paid = True
        try:
            db.set(order_id, msgpack.encode(order_entry))
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)

        tx.state = TX_COMPLETED
        _save_tx(tx)

    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
