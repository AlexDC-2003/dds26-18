import logging
import os
import random
import uuid
import time
import redis
import requests
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from lock_manager import Transaction, LockManager, LockTimeout, WaitDieAbort
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response
from kafka_bus import KafkaBus


app = Quart("order-service")

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
KAFKA_CHECKOUT_TIMEOUT_SEC = float(os.environ.get("KAFKA_REQUEST_TIMEOUT_SEC", "120"))

db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']),
)

lock_manager = LockManager(db=db)
kafka_bus = KafkaBus()

ORDER_TX_KEY_PREFIX = "order_tx:"   # maps order_id -> tx_id


async def rget(key: str):
    return await asyncio.to_thread(db.get, key)

async def rset(key: str, value: bytes, **kwargs):
    return await asyncio.to_thread(db.set, key, value, **kwargs)

async def rmset(mapping: dict[str, bytes]):
    return await asyncio.to_thread(db.mset, mapping)


@app.before_serving
async def startup():
    await kafka_bus.start()


@app.after_serving
async def shutdown():
    await kafka_bus.stop()
    await asyncio.to_thread(db.close)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: float


def _order_tx_key(order_id: str) -> str:
    return f"{ORDER_TX_KEY_PREFIX}{order_id}"


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


def send_get_request(url: str):
    try:
        return requests.get(url, timeout=3)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(REQ_ERROR_STR) from e


async def http_get(url: str):
    return await asyncio.to_thread(send_get_request, url)


async def call_orchestrator(tx_id: str, order_id: str, order_entry: OrderValue, tx_ts: float) -> dict:
    try:
        return await kafka_bus.request(
            os.environ["KAFKA_ORCHESTRATOR_COMMANDS_TOPIC"],
            {
                "msg_id": f"execute:{tx_id}",
                "type": "execute",
                "tx_id": tx_id,
                "order_id": order_id,
                "user_id": order_entry.user_id,
                "total_cost": order_entry.total_cost,
                "items": list(order_entry.items),
                "tx_ts": tx_ts,
            },
            timeout_sec=KAFKA_CHECKOUT_TIMEOUT_SEC,
        )
    except Exception as e:
        return {"status": "timeout", "error": str(e)}


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


class _LockContention(Exception):
    pass


@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    MAX_RETRIES = 5
    backoff = 0.2
    for attempt in range(MAX_RETRIES + 1):
        order_entry: OrderValue = await get_order_from_db(order_id)
        if order_entry.paid:
            return Response("Checkout successful", status=200)

        tx_id = await _get_or_create_tx_id(order_id)
        resources = _lock_resources_for_order(order_id)

        try:
            async with async_2pl(resources, tx_id=tx_id, ts=None):
                order_entry = await get_order_from_db(order_id)
                if order_entry.paid:
                    return Response("Checkout successful", status=200)

                tx_ts = time.time()
                result = await call_orchestrator(tx_id, order_id, order_entry, tx_ts)
                status = result.get("status")

                if status == "committed":
                    order_entry.paid = True
                    await rset(order_id, msgpack.encode(order_entry))
                    return Response("Checkout successful", status=200)

                elif status == "lock_contention":
                    # Orchestrator aborted due to lock conflict — create fresh tx_id and retry
                    new_tx_id = str(uuid.uuid4())
                    await rset(_order_tx_key(order_id), new_tx_id)
                    raise _LockContention(result.get("error", "lock contention"))

                elif status == "aborted":
                    new_tx_id = str(uuid.uuid4())
                    await rset(_order_tx_key(order_id), new_tx_id)
                    abort(400, result.get("error", "Checkout failed"))

                else:  # timeout or unexpected
                    abort(503, result.get("error", "Orchestrator error"))

        except (_LockContention, WaitDieAbort) as e:
            if attempt < MAX_RETRIES:
                print(f"[Checkout] Retrying attempt {attempt + 1}/{MAX_RETRIES + 1}: {e}", flush=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 2.0)
            else:
                abort(409, f"Checkout aborted after {MAX_RETRIES} retries: {e}")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
