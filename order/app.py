import logging
import os
import atexit
import random
import uuid
import time
import redis
import requests
import asyncio
from kafka_bus import KafkaBus
from collections import defaultdict
from lock_manager import Transaction, LockManager, LockTimeout, WaitDieAbort
from msgspec import msgpack, Struct
from types import SimpleNamespace
from contextlib import asynccontextmanager
from quart import Quart, jsonify, abort, Response


app = Quart("order-service")

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']


db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

lock_manager = LockManager(db=db)
INTERNAL_TRANSPORT = os.environ.get("INTERNAL_TRANSPORT", "rest")
KAFKA_TIMEOUT_SEC = float(os.environ.get("KAFKA_REQUEST_TIMEOUT_SEC", "2"))

kafka_bus = KafkaBus()

async def rget(key: str):
    return await asyncio.to_thread(db.get, key)

async def rset(key: str, value: bytes, **kwargs):
    return await asyncio.to_thread(db.set, key, value, **kwargs)

async def rmset(mapping: dict[str, bytes]):
    return await asyncio.to_thread(db.mset, mapping)

async def rclose():
    return await asyncio.to_thread(db.close)

async def http_get(url: str):
    return await asyncio.to_thread(send_get_request, url)

async def http_post(url: str):
    return await asyncio.to_thread(send_post_request, url)

@app.before_serving
async def startup():
    if INTERNAL_TRANSPORT == "kafka":
        await kafka_bus.start()

@app.after_serving
async def shutdown():
    if INTERNAL_TRANSPORT == "kafka":
        await kafka_bus.stop()
    await asyncio.to_thread(db.close)





class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: float

# 2PC transaction storage

ORDER_TX_KEY_PREFIX = "order_tx:"   # maps order_id -> tx_id
TX_KEY_PREFIX = "tx:"              # maps tx_id -> tx record

# coordinator states
TX_STARTED = "STARTED"
TX_PREPARING = "PREPARING"
TX_PREPARED = "PREPARED"
TX_COMMITTING = "COMMITTING"
TX_COMPLETED = "COMPLETED"
TX_ABORTED = "ABORTED"


class OrderTxValue(Struct):
    tx_id: str
    order_id: str
    user_id: str
    total_cost: float

    # snapshot of what we intended to buy (helps idempotency / debugging)
    items: list[tuple[str, int]]

    # 2PC progress
    state: str

    # what stock quantities are already prepared (deducted) for this tx
    prepared_items: list[tuple[str, int]]
    stock_prepared: bool
    payment_prepared: bool
    stock_committed: bool
    payment_committed: bool

    # timestamps + error info
    created_at: float
    updated_at: float
    error: str | None

def _reply_status_code(reply: dict, *, service: str) -> int:
    # Payment worker replies with "status_code"
    if "status_code" in reply:
        return int(reply["status_code"])
    # Stock dispatcher replies with "ok"
    if "ok" in reply:
        return 200 if reply["ok"] else 400
    # Fallback
    return 400


@asynccontextmanager
async def async_2pl(resources: list[str], *, tx_id: str | None = None, ts: float | None = None):
    txn = Transaction(tx_id=tx_id, ts=ts)
    for r in sorted(set(resources)):
        await asyncio.to_thread(lock_manager.acquire, txn, r)
    try:
        yield txn
    finally:
        await asyncio.to_thread(lock_manager.release_all, txn)

def _lock_resources_for_order(order_id: str) -> list[str]:
    # lock order state + “checkout started” marker
    return [f"order:{order_id}", f"order_tx:{order_id}"]

def _as_response_like(reply: dict, *, service: str):
    sc = _reply_status_code(reply, service=service)
    # mimic the two things you use most: status_code and json()
    return SimpleNamespace(
        status_code=sc,
        _raw=reply,
        json=lambda: reply.get("payload", reply)
    )

async def get_order_from_db(order_id: str) -> OrderValue:
    try:
        entry: bytes | None = await rget(order_id)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    order: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if order is None:
        abort(400, f"Order: {order_id} not found!")
    return order

def _order_tx_key(order_id: str) -> str:
    return f"{ORDER_TX_KEY_PREFIX}{order_id}"


def _tx_key(tx_id: str) -> str:
    return f"{TX_KEY_PREFIX}{tx_id}"


async def _get_or_create_tx_id(order_id: str) -> str:
    try:
        existing = await rget(_order_tx_key(order_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    if existing:
        return existing.decode()

    new_tx_id = str(uuid.uuid4())
    try:
        ok = await rset(_order_tx_key(order_id), new_tx_id, nx=True)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    if ok:
        return new_tx_id

    try:
        existing2 = await rget(_order_tx_key(order_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    return existing2.decode() if existing2 else new_tx_id


async def _get_tx(tx_id: str) -> OrderTxValue | None:
    try:
        raw = await rget(_tx_key(tx_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return msgpack.decode(raw, type=OrderTxValue) if raw else None


async def _save_tx(tx: OrderTxValue) -> None:
    tx.updated_at = time.time()
    try:
        await rset(_tx_key(tx.tx_id), msgpack.encode(tx))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


async def _get_or_create_tx_record(tx_id: str, order_id: str, order_entry: OrderValue) -> OrderTxValue:
    existing = await _get_tx(tx_id)
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
        prepared_items=[],
        stock_prepared=False,
        payment_prepared=False,
        stock_committed=False,
        payment_committed=False,
        created_at=now,
        updated_at=now,
        error=None,
    )

    try:
        ok = await rset(_tx_key(tx_id), msgpack.encode(tx), nx=True)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    if ok:
        return tx
    return (await _get_tx(tx_id)) or tx

@app.post('/create/<user_id>')
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        await rset(key, value)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        return OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
        )

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry()) for i in range(n)}
    try:
        await rmset(kv_pairs)  # <-- changed
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    return jsonify({"msg": "Batch init for orders successful"})

@app.get('/find/<order_id>')
async def find_order(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }
    )
def send_post_request(url: str):
    try:
        return requests.post(url, timeout=3)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(REQ_ERROR_STR) from e

def send_get_request(url: str):
    try:
        return requests.get(url, timeout=3)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(REQ_ERROR_STR) from e

async def prepare_stock(tx_id: str, item_id: str, quantity: int, *, tx_ts: float):
    if INTERNAL_TRANSPORT != "kafka":
        return SimpleNamespace(status_code=400, json=lambda: {"error": "2PC requires Kafka transport"})

    cmd = {
        "msg_id": str(uuid.uuid4()),
        "tx_id": tx_id,
        "tx_ts": tx_ts,
        "type": "prepare_stock",
        "payload": {"item_id": item_id, "quantity": quantity},
    }
    reply = await kafka_bus.request(os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC)
    return _as_response_like(reply, service="stock")

async def commit_stock(tx_id: str, *, tx_ts: float):
    if INTERNAL_TRANSPORT != "kafka":
        return SimpleNamespace(status_code=400, json=lambda: {"error": "2PC requires Kafka transport"})

    cmd = {
        "msg_id": str(uuid.uuid4()),
        "tx_id": tx_id,
        "tx_ts": tx_ts,
        "type": "commit_stock",
        "payload": {},
    }
    reply = await kafka_bus.request(os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC)
    return _as_response_like(reply, service="stock")

async def abort_stock(tx_id: str, *, tx_ts: float):
    if INTERNAL_TRANSPORT != "kafka":
        return SimpleNamespace(status_code=400, json=lambda: {"error": "2PC requires Kafka transport"})

    cmd = {
        "msg_id": str(uuid.uuid4()),
        "tx_id": tx_id,
        "tx_ts": tx_ts,
        "type": "abort_stock",
        "payload": {},
    }
    reply = await kafka_bus.request(os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC)
    return _as_response_like(reply, service="stock")

async def prepare_payment(tx_id: str, user_id: str, amount: int | float, *, tx_ts: float):
    if INTERNAL_TRANSPORT != "kafka":
        return SimpleNamespace(status_code=400, json=lambda: {"error": "2PC requires Kafka transport"})

    cmd = {
        "msg_id": str(uuid.uuid4()),
        "tx_id": tx_id,
        "tx_ts": tx_ts,
        "type": "prepare_payment",
        "payload": {"user_id": user_id, "amount": amount},
    }
    reply = await kafka_bus.request(os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC)
    return _as_response_like(reply, service="payment")

async def commit_payment(tx_id: str, *, tx_ts: float):
    if INTERNAL_TRANSPORT != "kafka":
        return SimpleNamespace(status_code=400, json=lambda: {"error": "2PC requires Kafka transport"})

    cmd = {
        "msg_id": str(uuid.uuid4()),
        "tx_id": tx_id,
        "tx_ts": tx_ts,
        "type": "commit_payment",
        "payload": {},
    }
    reply = await kafka_bus.request(os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC)
    return _as_response_like(reply, service="payment")

async def abort_payment(tx_id: str, *, tx_ts: float):
    if INTERNAL_TRANSPORT != "kafka":
        return SimpleNamespace(status_code=400, json=lambda: {"error": "2PC requires Kafka transport"})

    cmd = {
        "msg_id": str(uuid.uuid4()),
        "tx_id": tx_id,
        "tx_ts": tx_ts,
        "type": "abort_payment",
        "payload": {},
    }
    reply = await kafka_bus.request(os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC)
    return _as_response_like(reply, service="payment")


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: int):
    quantity = int(quantity)

    try:
        item_reply = await http_get(f"{GATEWAY_URL}/stock/find/{item_id}")
    except Exception:
        abort(400, REQ_ERROR_STR)

    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")

    item_price = item_reply.json()["price"]

    resources = _lock_resources_for_order(order_id)

    try:
        async with async_2pl(resources, ts=time.time()):
            raw = await rget(order_id)
            if not raw:
                abort(400, f"Order: {order_id} not found!")

            order_entry: OrderValue = msgpack.decode(raw, type=OrderValue)

            if order_entry.paid:
                abort(400, "Order already paid; cannot add items")
            started = await rget(_order_tx_key(order_id))
            if started:
                existing = await _get_tx(started.decode())
                if existing and existing.state == TX_COMPLETED:
                    abort(400, "Checkout already paid and completed; cannot add items")
                elif existing and existing.state != TX_ABORTED:
                    abort(400, "Checkout already in progress; cannot add items")

            order_entry.items.append((item_id, quantity))
            order_entry.total_cost += quantity * item_price

            await rset(order_id, msgpack.encode(order_entry))

    except WaitDieAbort as e:
        abort(409, f"Transaction aborted (wait-die): {e}")
    except LockTimeout as e:
        abort(503, f"Could not acquire lock in time: {e}")
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    return Response(
        f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        status=200,
    )

def _prepared_as_dict(prepared_items: list[tuple[str, int]]) -> dict[str, int]:
    d: dict[str, int] = defaultdict(int)
    for item_id, qty in prepared_items:
        d[item_id] += qty
    return d


def _set_prepared_qty(tx: OrderTxValue, item_id: str, qty: int) -> None:
    for i, (iid, _) in enumerate(tx.prepared_items):
        if iid == item_id:
            tx.prepared_items[i] = (iid, qty)
            return
    tx.prepared_items.append((item_id, qty))


async def _abort_participants(tx: OrderTxValue) -> None:
    if tx.payment_prepared and not tx.payment_committed:
        try:
            await abort_payment(tx.tx_id, tx_ts=tx.created_at)
        except Exception:
            pass
    if tx.stock_prepared and not tx.stock_committed:
        try:
            await abort_stock(tx.tx_id, tx_ts=tx.created_at)
        except Exception:
            pass


@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    if order_entry.paid:
        return Response("Checkout successful", status=200)

    tx_id = await _get_or_create_tx_id(order_id)
    resources = _lock_resources_for_order(order_id)

    try:
        # Acquire locks first (2PL discipline)
        async with async_2pl(resources, tx_id=tx_id, ts=None):
            # Create/read tx inside lock

            tx = await _get_or_create_tx_record(tx_id, order_id, order_entry)
            if tx.state == TX_COMPLETED:
                return Response("Checkout successful", status=200)
            if tx.state == TX_ABORTED:
                tx_id = str(uuid.uuid4())
                await rset(_order_tx_key(order_id), tx_id)
                now = time.time()
                tx = OrderTxValue(
                    tx_id=tx_id,
                    order_id=order_id,
                    user_id=order_entry.user_id,
                    total_cost=order_entry.total_cost,
                    items=list(order_entry.items),
                    state=TX_STARTED,
                    prepared_items=[],
                    stock_prepared=False,
                    payment_prepared=False,
                    stock_committed=False,
                    payment_committed=False,
                    created_at=now,
                    updated_at=now,
                    error=None,
                )
                await _save_tx(tx)

            items_quantities: dict[str, int] = defaultdict(int)
            for item_id, quantity in tx.items:
                items_quantities[item_id] += quantity

            if tx.state in (TX_STARTED, TX_PREPARING):
                tx.state = TX_PREPARING
                await _save_tx(tx)

                prepared_now = _prepared_as_dict(tx.prepared_items)
                for item_id, quantity in items_quantities.items():
                    already = prepared_now.get(item_id, 0)
                    if already >= quantity:
                        continue
                    stock_reply = await prepare_stock(tx.tx_id, item_id, quantity, tx_ts=tx.created_at)
                    if stock_reply.status_code != 200:
                        tx.state = TX_ABORTED
                        tx.error = f"Out of stock on item_id: {item_id}"
                        await _abort_participants(tx)
                        await _save_tx(tx)
                        abort(400, tx.error)
                    _set_prepared_qty(tx, item_id, quantity)
                    tx.stock_prepared = True
                    await _save_tx(tx)

                if not tx.payment_prepared:
                    user_reply = await prepare_payment(tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at)
                    if user_reply.status_code != 200:
                        tx.state = TX_ABORTED
                        tx.error = "User out of credit"
                        await _abort_participants(tx)
                        await _save_tx(tx)
                        abort(400, tx.error)
                    tx.payment_prepared = True
                    await _save_tx(tx)

                tx.state = TX_PREPARED
                await _save_tx(tx)

            if tx.state in (TX_PREPARED, TX_COMMITTING):
                tx.state = TX_COMMITTING
                await _save_tx(tx)

                if not tx.stock_committed:
                    stock_commit_reply = await commit_stock(tx.tx_id, tx_ts=tx.created_at)
                    if stock_commit_reply.status_code != 200:
                        tx.error = "Failed to commit stock"
                        await _save_tx(tx)
                        abort(503, tx.error)
                    tx.stock_committed = True
                    await _save_tx(tx)

                if not tx.payment_committed:
                    payment_commit_reply = await commit_payment(tx.tx_id, tx_ts=tx.created_at)
                    if payment_commit_reply.status_code != 200:
                        tx.error = "Failed to commit payment"
                        await _save_tx(tx)
                        abort(503, tx.error)
                    tx.payment_committed = True
                    await _save_tx(tx)

                order_entry = await get_order_from_db(order_id)
                order_entry.paid = True
                await rset(order_id, msgpack.encode(order_entry))

                tx.state = TX_COMPLETED
                await _save_tx(tx)

            return Response("Checkout successful", status=200)

    except WaitDieAbort as e:
        abort(409, f"Transaction aborted (wait-die): {e}")
    except LockTimeout as e:
        abort(503, f"Could not acquire lock in time: {e}")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
