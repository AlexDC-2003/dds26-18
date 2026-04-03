import logging
import os
import asyncio
import random
import uuid
import time
import aiohttp
import redis
import requests
from kafka_bus import KafkaBus
from lock_manager import Transaction, LockManager, LockTimeout, WaitDieAbort
from msgspec import msgpack, Struct
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
orchestrator_session: aiohttp.ClientSession | None = None

async def rget(key: str):
    for attempt in range(5):
        try:
            return await asyncio.to_thread(db.get, key)
        except redis.exceptions.RedisError:
            if attempt == 4:
                raise
            await asyncio.sleep(0.2 * (2 ** attempt))

async def rset(key: str, value: bytes, **kwargs):
    for attempt in range(5):
        try:
            return await asyncio.to_thread(db.set, key, value, **kwargs)
        except redis.exceptions.RedisError:
            if attempt == 4:
                raise
            await asyncio.sleep(0.2 * (2 ** attempt))

async def rmset(mapping: dict[str, bytes]):
    return await asyncio.to_thread(db.mset, mapping)

async def http_get(url: str):
    return await asyncio.to_thread(send_get_request, url)

@app.before_serving
async def startup():
    global orchestrator_session
    if INTERNAL_TRANSPORT == "kafka":
        await kafka_bus.start()
    orchestrator_session = aiohttp.ClientSession()

@app.after_serving
async def shutdown():
    global orchestrator_session
    if INTERNAL_TRANSPORT == "kafka":
        await kafka_bus.stop()
    if orchestrator_session is not None:
        await orchestrator_session.close()
        orchestrator_session = None
    await asyncio.to_thread(db.close)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: float

# Saga transaction storage keys – read by add_item to check checkout state
ORDER_TX_KEY_PREFIX = "order_tx:"
TX_KEY_PREFIX = "tx:"

TX_STARTED = "STARTED"
TX_STOCK_RESERVED = "STOCK_RESERVED"
TX_PAYMENT_DONE = "PAYMENT_DONE"
TX_COMPLETED = "COMPLETED"
TX_ABORTED = "ABORTED"


class OrderTxValue(Struct):
    tx_id: str
    order_id: str
    user_id: str
    total_cost: float
    items: list[tuple[str, int]]
    state: str
    reserved_items: list[tuple[str, int]]
    payment_done: bool
    payment_refunded: bool
    stock_released: bool
    created_at: float
    updated_at: float
    error: str | None


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
    return [f"order:{order_id}", f"order_tx:{order_id}"]

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

async def _get_tx(tx_id: str) -> OrderTxValue | None:
    try:
        raw = await rget(_tx_key(tx_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return msgpack.decode(raw, type=OrderTxValue) if raw else None


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
        await rmset(kv_pairs)
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

def send_get_request(url: str):
    try:
        return requests.get(url, timeout=3)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(REQ_ERROR_STR) from e


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


ORCHESTRATOR_URL = os.environ["ORCHESTRATOR_URL"]
CHECKOUT_TIMEOUT_SEC = float(os.environ.get("CHECKOUT_TIMEOUT_SEC", "30"))


async def _post_checkout(url: str, cmd: dict):
    if orchestrator_session is None:
        raise RuntimeError("Orchestrator HTTP session not initialized")

    timeout = aiohttp.ClientTimeout(total=CHECKOUT_TIMEOUT_SEC)
    async with orchestrator_session.post(url, json=cmd, timeout=timeout) as reply:
        try:
            payload = await reply.json()
        except aiohttp.ContentTypeError:
            payload = {"error": await reply.text()}
        return reply.status, payload


@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    if order_entry.paid:
        return Response("Checkout successful", status=200)

    cmd = {
        "msg_id": f"checkout:{order_id}:{uuid.uuid4()}",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": list(order_entry.items),
    }
    try:
        status_code, payload = await _post_checkout(f"{ORCHESTRATOR_URL}/checkout", cmd)
    except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
        abort(503, "Checkout timed out, please retry")

    if status_code == 200:
        return Response("Checkout successful", status=200)
    error = payload.get("error", "Checkout failed") if isinstance(payload, dict) else "Checkout failed"
    abort(status_code, error)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
