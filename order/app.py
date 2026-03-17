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
    await _recover_in_flight_transactions()

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
    total_cost: float

    # snapshot of what we intended to buy (helps idempotency / debugging)
    items: list[tuple[str, int]]

    # saga progress
    state: str

    # what stock we've successfully subtracted so far (for idempotency + compensation)
    reserved_items: list[tuple[str, int]]

    # has payment been executed successfully?
    payment_done: bool

    # did we already refund after a failed finalize?
    payment_refunded: bool
    stock_released: bool

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

    # Use a deterministic tx_id based on order_id as fallback when the pointer
    # key is missing (e.g. lost in AOF fsync window after order-db crash).
    # This ensures Phase 3 retries always map to the same tx_id and can
    # therefore find any existing TX record or stock idempotency keys instead
    # of silently creating a new identity and orphaning an old stock reserve.
    stable_tx_id = f"tx:{order_id}"
    try:
        ok = await rset(_order_tx_key(order_id), stable_tx_id, nx=True)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    if ok:
        return stable_tx_id

    try:
        existing2 = await rget(_order_tx_key(order_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    return existing2.decode() if existing2 else stable_tx_id


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
        reserved_items=[],
        payment_done=False,
        payment_refunded=False,
        stock_released=False,
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

async def _recover_one_tx(tx: OrderTxValue, order_id: str) -> bool:
    """Complete or roll back a single in-flight transaction. Returns True when done."""
    # --- TX_STARTED: complete stock reservation then fall through ---
    if tx.state == TX_STARTED:
        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in tx.items:
            items_quantities[item_id] += quantity

        reserved_now = _reserved_as_dict(tx.reserved_items)
        for item_id, quantity in items_quantities.items():
            already = reserved_now.get(item_id, 0)
            if already >= quantity:
                continue
            to_reserve = quantity - already
            try:
                stock_reply = await reserve_stock(tx.tx_id, item_id, to_reserve)
            except Exception as e:
                logging.warning("[RECOVERY:RESERVE] tx=%s item=%s error=%s", tx.tx_id, item_id, e)
                return False
            if stock_reply.status_code == 200:
                _add_reserved(tx, item_id, to_reserve)
                await _save_tx(tx)
            else:
                await rollback_stock(tx)
                if tx.reserved_items:
                    return False
                tx.stock_released = True
                tx.state = TX_ABORTED
                tx.error = f"Recovery: out of stock on {item_id}"
                await _save_tx(tx)
                return True

        tx.state = TX_STOCK_RESERVED
        await _save_tx(tx)

    # --- TX_ABORTED: finish any incomplete rollback / refund ---
    if tx.state == TX_ABORTED:
        if tx.reserved_items:
            await rollback_stock(tx)
            if tx.reserved_items:
                return False  # stock still down
            tx.stock_released = True
            await _save_tx(tx)
        # Extra-release for phantom reservations (crash between stock-db write and
        # saving reserved_items).  Stock guards prevent double-release.
        for item_id, qty in tx.items:
            try:
                await release_stock(tx.tx_id, item_id, qty)
            except Exception as e:
                logging.warning("[RECOVERY:EXTRA-RELEASE] tx=%s item=%s error=%s", tx.tx_id, item_id, e)
        if tx.payment_done and not tx.payment_refunded:
            try:
                refund_reply = await refund_user(tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at)
                if refund_reply.status_code in (200, 400):
                    tx.payment_refunded = True
                    await _save_tx(tx)
                else:
                    return False
            except Exception as e:
                logging.warning("[RECOVERY:REFUND] tx=%s error=%s", tx.tx_id, e)
                return False
        return True

    # --- TX_STOCK_RESERVED: forward-recover by charging payment ---
    if tx.state == TX_STOCK_RESERVED and not tx.payment_done:
        logging.info("[RECOVERY:CHARGE] tx=%s order=%s", tx.tx_id, order_id)
        try:
            user_reply = await charge_user(tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at)
        except Exception as e:
            logging.warning("[RECOVERY:CHARGE-FAIL] tx=%s error=%s", tx.tx_id, e)
            return False
        if user_reply.status_code == 200:
            tx.payment_done = True
            tx.state = TX_PAYMENT_DONE
            await _save_tx(tx)
        else:
            await rollback_stock(tx)
            if tx.reserved_items:
                return False
            tx.stock_released = True
            tx.state = TX_ABORTED
            tx.error = "Recovery: payment failed"
            await _save_tx(tx)
            return True

    # --- TX_PAYMENT_DONE: mark order as paid and complete ---
    if tx.state == TX_PAYMENT_DONE:
        logging.info("[RECOVERY:COMPLETE] tx=%s order=%s", tx.tx_id, order_id)
        try:
            raw_order = await rget(order_id)
            if raw_order:
                order_entry = msgpack.decode(raw_order, type=OrderValue)
                if not order_entry.paid:
                    order_entry.paid = True
                    await rset(order_id, msgpack.encode(order_entry))
            tx.state = TX_COMPLETED
            await _save_tx(tx)
            logging.info("[RECOVERY:COMPLETED] tx=%s order=%s", tx.tx_id, order_id)
        except Exception as e:
            logging.warning("[RECOVERY:COMPLETE-FAIL] tx=%s error=%s", tx.tx_id, e)
            return False

    return True


async def _recover_in_flight_transactions():
    """On startup: find and complete/rollback any transactions that were in-flight
    when the order service previously crashed.
    Uses a Redis NX lock so only one gunicorn worker runs recovery."""
    lock_key = "order_service:recovery_lock"
    try:
        acquired = await rset(lock_key, "1", nx=True, ex=300)
    except Exception as e:
        logging.error("[RECOVERY] Could not acquire recovery lock: %s", e)
        return
    if not acquired:
        logging.info("[RECOVERY] Another worker is handling recovery, skipping.")
        return

    logging.info("[RECOVERY] Scanning for in-flight transactions...")
    try:
        raw_keys = await asyncio.to_thread(db.keys, "order_tx:*")
    except Exception as e:
        logging.error("[RECOVERY] Failed to scan order_tx keys: %s", e)
        await asyncio.to_thread(db.delete, lock_key)
        return

    recovered = failed = skipped = 0
    for raw_key in raw_keys:
        key_str = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
        order_id = key_str[len("order_tx:"):]
        try:
            raw_tx_id = await rget(key_str)
            if not raw_tx_id:
                skipped += 1
                continue
            tx_id = raw_tx_id.decode() if isinstance(raw_tx_id, bytes) else raw_tx_id
            tx = await _get_tx(tx_id)
            if tx is None or tx.state == TX_COMPLETED:
                skipped += 1
                continue
            # Skip cleanly-aborted transactions (nothing left to do)
            if (tx.state == TX_ABORTED
                    and not tx.reserved_items
                    and (not tx.payment_done or tx.payment_refunded)):
                skipped += 1
                continue
            logging.info("[RECOVERY] tx=%s order=%s state=%s", tx_id, order_id, tx.state)
            ok = await _recover_one_tx(tx, order_id)
            if ok:
                recovered += 1
            else:
                failed += 1
        except Exception as e:
            logging.warning("[RECOVERY] Error on order=%s: %s", order_id, e)
            failed += 1

    logging.info("[RECOVERY] Done: recovered=%d failed=%d skipped=%d", recovered, failed, skipped)
    await asyncio.to_thread(db.delete, lock_key)


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

async def reserve_stock(tx_id: str, item_id: str, quantity: int):
    if INTERNAL_TRANSPORT != "kafka":
        return await http_post(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")

    cmd = {
        "msg_id": str(f"reserve:{tx_id}:{item_id}"),
        "tx_id": tx_id,
        "type": "reserve_stock",
        "payload": {"item_id": item_id, "quantity": quantity},
    }
    reply = await kafka_bus.request(os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC)
    return _as_response_like(reply, service="stock")

async def release_stock(tx_id: str, item_id: str, quantity: int):
    if INTERNAL_TRANSPORT != "kafka":
        return await http_post(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")

    cmd = {
        "msg_id": str(f"release:{tx_id}:{item_id}"),
        "tx_id": tx_id,
        "type": "release_stock",
        "payload": {"item_id": item_id, "quantity": quantity},
    }
    reply = await kafka_bus.request(os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC)
    return _as_response_like(reply, service="stock")

async def charge_user(tx_id: str, user_id: str, amount: int | float, tx_ts: float = None):
    if INTERNAL_TRANSPORT != "kafka":
        return await http_post(f"{GATEWAY_URL}/payment/pay/{user_id}/{amount}")

    cmd = {
        "msg_id": f"charge:{tx_id}:{user_id}",
        "tx_id": tx_id,
        "tx_ts": tx_ts,
        "type": "charge_user",
        "payload": {"user_id": user_id, "amount": amount},
    }
    reply = await kafka_bus.request(os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC)
    return _as_response_like(reply, service="payment")

async def refund_user(tx_id: str, user_id: str, amount: int | float, tx_ts: float = None):
    if INTERNAL_TRANSPORT != "kafka":
        return await http_post(f"{GATEWAY_URL}/payment/add_funds/{user_id}/{amount}")

    cmd = {
        "msg_id": f"refund:{tx_id}:{user_id}",
        "tx_id": tx_id,
        "tx_ts": tx_ts,
        "type": "refund_user",
        "payload": {"user_id": user_id, "amount": amount},
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

async def rollback_stock(tx: OrderTxValue) -> None:
    still_reserved = list(tx.reserved_items)
    for item_id, quantity in still_reserved:
        try:
            reply = await release_stock(tx.tx_id, item_id, quantity)
            if reply.status_code == 200:
                tx.reserved_items = [(i, q) for i, q in tx.reserved_items if i != item_id]
                await _save_tx(tx)
                logging.info("[ROLLBACK:RELEASED] tx=%s item=%s qty=%s", tx.tx_id, item_id, quantity)
            else:
                logging.warning("[ROLLBACK:RELEASE-FAILED] tx=%s item=%s qty=%s status=%s err=%s",
                                tx.tx_id, item_id, quantity, reply.status_code, reply._raw.get("error"))
        except Exception as e:
            logging.warning("[ROLLBACK:EXCEPTION] tx=%s item=%s qty=%s error=%s", tx.tx_id, item_id, quantity, e)
        # sunt de acord

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
                # Re-apply paid=True in case the order-db write was lost
                # (tx state survived the crash but order.paid did not)
                order_entry = await get_order_from_db(order_id)
                if not order_entry.paid:
                    logging.warning("[TX:REPAID] order=%s tx=%s — paid flag was lost, re-writing", order_id, tx.tx_id)
                    order_entry.paid = True
                    await rset(order_id, msgpack.encode(order_entry))
                return Response("Checkout successful", status=200)
            if tx.state == TX_ABORTED:
                # Retry any rollback that failed in a previous attempt before discarding this tx
                logging.info("[TX:ABORTED-REENTRY] order=%s old_tx=%s error=%s reserved=%s", order_id, tx.tx_id, tx.error, tx.reserved_items)
                if tx.reserved_items:
                    await rollback_stock(tx)
                    await _save_tx(tx)
                    if tx.reserved_items:
                        # Rollback still incomplete (service down) — force client to retry
                        abort(503, "Compensating transaction in progress, please retry")
                    tx.stock_released = True
                    await _save_tx(tx)

                # Always re-attempt release for all items using the old tx_id, even when
                # reserved_items=[] and stock_released=True (order-db may have saved the
                # cleared state but stock-db lost the commit due to AOF loss within the
                # everysec fsync window). Stock service guards against releasing un-reserved
                # items (checks for a committed reserve key before adding stock back).
                for item_id_old, qty_old in tx.items:
                    try:
                        r = await release_stock(tx.tx_id, item_id_old, qty_old)
                        logging.info("[ABORTED:EXTRA-RELEASE] order=%s old_tx=%s item=%s qty=%s status=%s",
                                     order_id, tx.tx_id, item_id_old, qty_old, r.status_code)
                    except Exception as e:
                        logging.warning("[ABORTED:EXTRA-RELEASE-FAIL] order=%s old_tx=%s item=%s error=%s",
                                        order_id, tx.tx_id, item_id_old, e)

                # Refund in case the charge committed before the abort
                # (e.g. Kafka reply timeout or Redis connection error after EXEC).
                # _refund_user is a no-op if no charge record exists.
                if not tx.payment_refunded:
                    refund_reply = await refund_user(tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at)
                    if refund_reply.status_code not in (200, 400):
                        # Transient error — force client to retry so refund can complete
                        abort(503, "Compensating refund in progress, please retry")
                    tx.payment_refunded = True
                    await _save_tx(tx)
                    logging.info("[TX:ABORTED-REFUNDED] order=%s old_tx=%s", order_id, tx.tx_id)

                # Reset the existing TX record back to TX_STARTED so the same
                # tx_id is reused for the retry.  Keeping a stable tx_id means:
                # • Stock idempotency key (tied to tx_id+item) prevents a
                #   double-reserve even if the order-tx pointer was lost.
                # • Payment service can detect the prior refund and re-charge
                #   correctly (see _charge_user refund-bypass logic).
                new_tx_id = f"tx:{uuid.uuid4()}"
                tx.tx_id = new_tx_id
                tx.state = TX_STARTED
                tx.reserved_items = []
                tx.payment_done = False
                tx.payment_refunded = False
                tx.stock_released = False
                tx.error = None
                await _save_tx(tx)
                await rset(_order_tx_key(order_id), new_tx_id)
                logging.info("[TX:RESET] order=%s tx=%s new tx_id generated for retry", order_id, tx.tx_id)

            items_quantities: dict[str, int] = defaultdict(int)
            for item_id, quantity in tx.items:
                items_quantities[item_id] += quantity

            if tx.state == TX_STARTED:
                reserved_now = _reserved_as_dict(tx.reserved_items)

                for item_id, quantity in items_quantities.items():
                    already = reserved_now.get(item_id, 0)
                    if already >= quantity:
                        continue

                    to_reserve = quantity - already
                    try:
                        stock_reply = await reserve_stock(tx.tx_id, item_id, to_reserve)
                    except Exception as e:
                        logging.warning("[TX:RESERVE-TIMEOUT] order=%s tx=%s item=%s error=%s",
                                        order_id, tx.tx_id, item_id, e)
                        _add_reserved(tx, item_id, to_reserve)
                        await rollback_stock(tx)
                        if not tx.reserved_items:
                            tx.stock_released = True
                        tx.state = TX_ABORTED
                        tx.error = f"Reserve timed out on item_id: {item_id}"
                        await _save_tx(tx)
                        abort(503, tx.error)
                    if stock_reply.status_code != 200:
                        if not tx.stock_released:
                            await rollback_stock(tx)
                            if not tx.reserved_items:
                                tx.stock_released = True
                            await _save_tx(tx)

                        tx.state = TX_ABORTED
                        tx.error = f"Out of stock on item_id: {item_id}"
                        await _save_tx(tx)
                        abort(400, tx.error)

                    _add_reserved(tx, item_id, to_reserve)
                    await _save_tx(tx)

                tx.state = TX_STOCK_RESERVED
                await _save_tx(tx)

            if tx.state == TX_STOCK_RESERVED and not tx.payment_done:
                logging.info("[TX:CHARGING] order=%s tx=%s user=%s amount=%s", order_id, tx.tx_id, tx.user_id, tx.total_cost)
                user_reply = await charge_user(tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at)
                if user_reply.status_code != 200:
                    logging.warning("[TX:CHARGE-FAILED] order=%s tx=%s user=%s status=%s", order_id, tx.tx_id, tx.user_id, user_reply.status_code)
                    if not tx.stock_released:
                        await rollback_stock(tx)
                        if not tx.reserved_items:
                            tx.stock_released = True
                        await _save_tx(tx)

                    tx.state = TX_ABORTED
                    tx.error = "User out of credit"
                    await _save_tx(tx)
                    abort(400, tx.error)

                logging.info("[TX:CHARGED] order=%s tx=%s user=%s amount=%s", order_id, tx.tx_id, tx.user_id, tx.total_cost)
                tx.payment_done = True
                tx.state = TX_PAYMENT_DONE
                await _save_tx(tx)

            if tx.state == TX_PAYMENT_DONE:
                # optionally re-read order under lock
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
