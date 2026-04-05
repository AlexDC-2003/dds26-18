"""
Checkout Orchestrator

Consumes checkout.commands, runs the stock-payment saga (logic extracted
verbatim from order/app.py checkout()), and publishes results to checkout.replies.

Shares order-db Redis with the order service so it can read/write OrderValue
and OrderTxValue records and honor the same distributed lock keys.
"""

import asyncio
import logging
import os
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from types import SimpleNamespace

import redis
import redis.sentinel
from msgspec import Struct, msgpack
from quart import Quart, abort, jsonify
from quart import request

from kafka_bus import KafkaBus
from lock_manager import LockManager, LockTimeout, Transaction, WaitDieAbort

logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Redis – shared with order-service (same host/port/password/db)
# ---------------------------------------------------------------------------

app = Quart("order-service")

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

# GATEWAY_URL = os.environ['GATEWAY_URL']


def _create_redis_client() -> redis.Redis:
    password = os.environ['REDIS_PASSWORD']
    db_num = int(os.environ.get('REDIS_DB', '0'))
    sentinel_hosts = os.environ.get('REDIS_SENTINEL_HOSTS', '')
    if sentinel_hosts:
        hosts = [(h.rsplit(':', 1)[0], int(h.rsplit(':', 1)[1])) for h in sentinel_hosts.split(',')]
        s = redis.sentinel.Sentinel(hosts, password=password, db=db_num)
        return s.master_for(os.environ['REDIS_MASTER_NAME'])
    return redis.Redis(host=os.environ['REDIS_HOST'], port=int(os.environ['REDIS_PORT']),
                       password=password, db=db_num)

db: redis.Redis = _create_redis_client()

lock_manager = LockManager(db=db)
INTERNAL_TRANSPORT = os.environ.get("INTERNAL_TRANSPORT", "rest")
KAFKA_TIMEOUT_SEC = float(os.environ.get("KAFKA_REQUEST_TIMEOUT_SEC", "2"))

kafka_bus = KafkaBus()

# ---------------------------------------------------------------------------
# Data structures (identical to order/app.py)
# ---------------------------------------------------------------------------


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: float


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


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------


async def rget(key: str):
    for attempt in range(5):
        try:
            return await asyncio.to_thread(db.get, key)
        except redis.exceptions.RedisError as e:
            if attempt == 4:
                raise
            await asyncio.sleep(0.2 * (2 ** attempt))


async def rset(key: str, value: bytes, **kwargs):
    for attempt in range(5):
        try:
            return await asyncio.to_thread(db.set, key, value, **kwargs)
        except redis.exceptions.RedisError as e:
            if attempt == 4:
                raise
            await asyncio.sleep(0.2 * (2 ** attempt))


# ---------------------------------------------------------------------------
# Lock helpers (identical to order/app.py)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# TX key helpers (identical to order/app.py)
# ---------------------------------------------------------------------------


def _order_tx_key(order_id: str) -> str:
    return f"{ORDER_TX_KEY_PREFIX}{order_id}"


def _tx_key(tx_id: str) -> str:
    return f"{TX_KEY_PREFIX}{tx_id}"


async def _get_or_create_tx_id(order_id: str) -> str:
    existing = await rget(_order_tx_key(order_id))
    if existing:
        return existing.decode()

    stable_tx_id = f"tx:{order_id}"
    ok = await rset(_order_tx_key(order_id), stable_tx_id, nx=True)
    if ok:
        return stable_tx_id

    existing2 = await rget(_order_tx_key(order_id))
    return existing2.decode() if existing2 else stable_tx_id


async def _get_tx(tx_id: str) -> OrderTxValue | None:
    raw = await rget(_tx_key(tx_id))
    return msgpack.decode(raw, type=OrderTxValue) if raw else None


async def _save_tx(tx: OrderTxValue) -> None:
    tx.updated_at = time.time()
    await rset(_tx_key(tx.tx_id), msgpack.encode(tx))


async def _get_or_create_tx_record(
    tx_id: str,
    order_id: str,
    user_id: str,
    total_cost: float,
    items: list,
) -> OrderTxValue:
    existing = await _get_tx(tx_id)
    if existing:
        return existing

    now = time.time()
    tx = OrderTxValue(
        tx_id=tx_id,
        order_id=order_id,
        user_id=user_id,
        total_cost=total_cost,
        items=list(items),
        state=TX_STARTED,
        reserved_items=[],
        payment_done=False,
        payment_refunded=False,
        stock_released=False,
        created_at=now,
        updated_at=now,
        error=None,
    )
    ok = await rset(_tx_key(tx_id), msgpack.encode(tx), nx=True)
    if ok:
        return tx
    return (await _get_tx(tx_id)) or tx


# ---------------------------------------------------------------------------
# Service call helpers (identical to order/app.py)
# ---------------------------------------------------------------------------


def _reply_status_code(reply: dict, *, service: str) -> int:
    if "DB connection lost" in str(reply.get("error") or ""):
        return 503
    if "status_code" in reply:
        return int(reply["status_code"])
    if "ok" in reply:
        return 200 if reply["ok"] else 400
    return 400


def _as_response_like(reply: dict, *, service: str):
    sc = _reply_status_code(reply, service=service)
    return SimpleNamespace(
        status_code=sc,
        _raw=reply,
        json=lambda: reply.get("payload", reply),
    )


async def reserve_stock(tx_id: str, item_id: str, quantity: int):
    cmd = {
        "msg_id": str(f"reserve:{tx_id}:{item_id}"),
        "tx_id": tx_id,
        "type": "reserve_stock",
        "payload": {"item_id": item_id, "quantity": quantity},
    }
    reply = await kafka_bus.request(
        os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC
    )
    return _as_response_like(reply, service="stock")


async def release_stock(tx_id: str, item_id: str, quantity: int):
    cmd = {
        "msg_id": str(f"release:{tx_id}:{item_id}"),
        "tx_id": tx_id,
        "type": "release_stock",
        "payload": {"item_id": item_id, "quantity": quantity},
    }
    reply = await kafka_bus.request(
        os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC
    )
    return _as_response_like(reply, service="stock")


async def charge_user(tx_id: str, user_id: str, amount: int | float, tx_ts: float = None):
    cmd = {
        "msg_id": f"charge:{tx_id}:{user_id}",
        "tx_id": tx_id,
        "tx_ts": tx_ts,
        "type": "charge_user",
        "payload": {"user_id": user_id, "amount": amount},
    }
    reply = await kafka_bus.request(
        os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC
    )
    return _as_response_like(reply, service="payment")


async def refund_user(tx_id: str, user_id: str, amount: int | float, tx_ts: float = None):
    cmd = {
        "msg_id": f"refund:{tx_id}:{user_id}",
        "tx_id": tx_id,
        "tx_ts": tx_ts,
        "type": "refund_user",
        "payload": {"user_id": user_id, "amount": amount},
    }
    reply = await kafka_bus.request(
        os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"], cmd, timeout_sec=KAFKA_TIMEOUT_SEC
    )
    return _as_response_like(reply, service="payment")


# ---------------------------------------------------------------------------
# Rollback / bookkeeping helpers (identical to order/app.py)
# ---------------------------------------------------------------------------


def _reserved_as_dict(reserved_items: list[tuple[str, int]]) -> dict[str, int]:
    d: dict[str, int] = defaultdict(int)
    for item_id, qty in reserved_items:
        d[item_id] += qty
    return d


def _add_reserved(tx: OrderTxValue, item_id: str, qty: int) -> None:
    for i, (iid, q) in enumerate(tx.reserved_items):
        if iid == item_id:
            tx.reserved_items[i] = (iid, q + qty)
            return
    tx.reserved_items.append((item_id, qty))


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
                logging.warning(
                    "[ROLLBACK:RELEASE-FAILED] tx=%s item=%s qty=%s status=%s err=%s",
                    tx.tx_id, item_id, quantity, reply.status_code, reply._raw.get("error"),
                )
        except Exception as e:
            logging.warning(
                "[ROLLBACK:EXCEPTION] tx=%s item=%s qty=%s error=%s", tx.tx_id, item_id, quantity, e
            )


# ---------------------------------------------------------------------------
# Recovery (identical to order/app.py _recover_one_tx + _recover_in_flight_transactions)
# ---------------------------------------------------------------------------


async def _recover_one_tx(tx: OrderTxValue, order_id: str) -> bool:
    """Complete or roll back a single in-flight transaction. Returns True when done."""
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

    if tx.state == TX_ABORTED:
        if tx.reserved_items:
            await rollback_stock(tx)
            if tx.reserved_items:
                return False
            tx.stock_released = True
            await _save_tx(tx)
        for item_id, qty in tx.items:
            try:
                await release_stock(tx.tx_id, item_id, qty)
            except Exception as e:
                logging.warning(
                    "[RECOVERY:EXTRA-RELEASE] tx=%s item=%s error=%s", tx.tx_id, item_id, e
                )
        if tx.payment_done and not tx.payment_refunded:
            try:
                refund_reply = await refund_user(
                    tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at
                )
                if refund_reply.status_code in (200, 400):
                    tx.payment_refunded = True
                    await _save_tx(tx)
                else:
                    return False
            except Exception as e:
                logging.warning("[RECOVERY:REFUND] tx=%s error=%s", tx.tx_id, e)
                return False
        return True

    if tx.state == TX_STOCK_RESERVED and not tx.payment_done:
        logging.info("[RECOVERY:CHARGE] tx=%s order=%s", tx.tx_id, order_id)
        try:
            user_reply = await charge_user(
                tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at
            )
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
    when the orchestrator previously crashed.
    Uses a Redis NX lock so only one orchestrator instance runs recovery."""
    lock_key = "orchestrator:recovery_lock"
    try:
        acquired = await rset(lock_key, "1", nx=True, ex=300)
    except Exception as e:
        logging.error("[RECOVERY] Could not acquire recovery lock: %s", e)
        return
    if not acquired:
        logging.info("[RECOVERY] Another instance is handling recovery, skipping.")
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
            if (
                tx.state == TX_ABORTED
                and not tx.reserved_items
                and (not tx.payment_done or tx.payment_refunded)
            ):
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


# ---------------------------------------------------------------------------
# Saga execution (the checkout() logic, extracted from order/app.py)
# ---------------------------------------------------------------------------

@app.post("/checkout")
async def checkout_http():
    cmd = await request.get_json(force=True, silent=True)
    if not cmd:
        abort(400, "Invalid JSON")
    result = await run_checkout_saga(cmd)
    return jsonify(result), result["status_code"]


async def run_checkout_saga(cmd: dict) -> dict:
    """Run the full checkout saga for one order and return the result."""
    msg_id = cmd.get("msg_id", str(uuid.uuid4()))
    order_id = cmd["order_id"]
    user_id = cmd["user_id"]
    total_cost = cmd["total_cost"]
    items = [tuple(i) for i in cmd["items"]]

    def reply(status_code: int, error: str | None = None, state: str = TX_ABORTED) -> dict:
        return {
            "msg_id": msg_id,
            "order_id": order_id,
            "status_code": status_code,
            "error": error,
            "state": state,
        }

    raw_order = await rget(order_id)
    if raw_order:
        order_entry = msgpack.decode(raw_order, type=OrderValue)
        if order_entry.paid:
            return reply(200, state=TX_COMPLETED)

    tx_id = await _get_or_create_tx_id(order_id)
    resources = _lock_resources_for_order(order_id)

    try:
        async with async_2pl(resources, tx_id=tx_id, ts=None):
            tx = await _get_or_create_tx_record(tx_id, order_id, user_id, total_cost, items)

            if tx.state == TX_COMPLETED:
                raw_order = await rget(order_id)
                if raw_order:
                    order_entry = msgpack.decode(raw_order, type=OrderValue)
                    if not order_entry.paid:
                        logging.warning(
                            "[TX:REPAID] order=%s tx=%s — paid flag was lost, re-writing",
                            order_id, tx.tx_id,
                        )
                        order_entry.paid = True
                        await rset(order_id, msgpack.encode(order_entry))
                return reply(200, state=TX_COMPLETED)

            if tx.state == TX_ABORTED:
                logging.info(
                    "[TX:ABORTED-REENTRY] order=%s old_tx=%s error=%s reserved=%s",
                    order_id, tx.tx_id, tx.error, tx.reserved_items,
                )
                if tx.reserved_items:
                    await rollback_stock(tx)
                    await _save_tx(tx)
                    if tx.reserved_items:
                        return reply(503, "Compensating transaction in progress, please retry")
                    tx.stock_released = True
                    await _save_tx(tx)

                for item_id_old, qty_old in tx.items:
                    try:
                        r = await release_stock(tx.tx_id, item_id_old, qty_old)
                        logging.info(
                            "[ABORTED:EXTRA-RELEASE] order=%s old_tx=%s item=%s qty=%s status=%s",
                            order_id, tx.tx_id, item_id_old, qty_old, r.status_code,
                        )
                    except Exception as e:
                        logging.warning(
                            "[ABORTED:EXTRA-RELEASE-FAIL] order=%s old_tx=%s item=%s error=%s",
                            order_id, tx.tx_id, item_id_old, e,
                        )

                if not tx.payment_refunded:
                    refund_reply = await refund_user(
                        tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at
                    )
                    if refund_reply.status_code not in (200, 400):
                        return reply(503, "Compensating refund in progress, please retry")
                    tx.payment_refunded = True
                    await _save_tx(tx)
                    logging.info("[TX:ABORTED-REFUNDED] order=%s old_tx=%s", order_id, tx.tx_id)

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
                logging.info(
                    "[TX:RESET] order=%s tx=%s new tx_id generated for retry", order_id, tx.tx_id
                )

            items_quantities: dict[str, int] = defaultdict(int)
            for item_id, quantity in tx.items:
                items_quantities[item_id] += quantity

            if tx.state == TX_STARTED:
                reserved_now = _reserved_as_dict(tx.reserved_items)

                to_reserve_list = [
                    (item_id, quantity - reserved_now.get(item_id, 0))
                    for item_id, quantity in items_quantities.items()
                    if reserved_now.get(item_id, 0) < quantity
                ]

                if to_reserve_list:
                    results = await asyncio.gather(
                        *[reserve_stock(tx.tx_id, item_id, qty) for item_id, qty in to_reserve_list],
                        return_exceptions=True,
                    )

                    first_failure: str | None = None
                    is_timeout = False
                    for (item_id, qty), result in zip(to_reserve_list, results):
                        if isinstance(result, Exception):
                            logging.warning(
                                "[TX:RESERVE-TIMEOUT] order=%s tx=%s item=%s error=%s",
                                order_id, tx.tx_id, item_id, result,
                            )
                            if first_failure is None:
                                first_failure = item_id
                                is_timeout = True
                            # Do NOT pessimistically add — the idempotency key on the stock
                            # worker (saga:reserve:{tx_id}:{item_id}) deduplicates if the
                            # first attempt succeeded. Pessimistic add causes paid orders with
                            # no stock decremented when the first attempt never committed.
                        elif result.status_code == 200:
                            _add_reserved(tx, item_id, qty)
                        elif result.status_code == 503:
                            if first_failure is None:
                                first_failure = item_id
                                is_timeout = True
                            # Same reasoning: don't pessimistically add on transient failure.
                        else:
                            if first_failure is None:
                                first_failure = item_id

                    if first_failure:
                        if is_timeout:
                            # Don't roll back — the stock worker may still be retrying.
                            # Leave tx in TX_STARTED with partial reservations intact so
                            # recovery or a client retry can complete or cleanly roll back.
                            await _save_tx(tx)
                            return reply(503, f"Reserve timed out on item_id: {first_failure}")
                        await rollback_stock(tx)
                        if not tx.reserved_items:
                            tx.stock_released = True
                        tx.state = TX_ABORTED
                        tx.error = f"Out of stock on item_id: {first_failure}"
                        await _save_tx(tx)
                        return reply(400, tx.error)

                tx.state = TX_STOCK_RESERVED
                await _save_tx(tx)

            if tx.state == TX_STOCK_RESERVED and not tx.payment_done:
                logging.info(
                    "[TX:CHARGING] order=%s tx=%s user=%s amount=%s",
                    order_id, tx.tx_id, tx.user_id, tx.total_cost,
                )
                user_reply = await charge_user(
                    tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at
                )
                if user_reply.status_code == 503:
                    logging.warning(
                        "[TX:CHARGE-DB-ERROR] order=%s tx=%s user=%s — transient DB error, returning 503",
                        order_id, tx.tx_id, tx.user_id,
                    )
                    return reply(503, "Payment DB unavailable, please retry")
                if user_reply.status_code != 200:
                    logging.warning(
                        "[TX:CHARGE-FAILED] order=%s tx=%s user=%s status=%s",
                        order_id, tx.tx_id, tx.user_id, user_reply.status_code,
                    )
                    if not tx.stock_released:
                        await rollback_stock(tx)
                        if not tx.reserved_items:
                            tx.stock_released = True
                        await _save_tx(tx)
                    tx.state = TX_ABORTED
                    tx.error = "User out of credit"
                    await _save_tx(tx)
                    return reply(400, tx.error)

                logging.info(
                    "[TX:CHARGED] order=%s tx=%s user=%s amount=%s",
                    order_id, tx.tx_id, tx.user_id, tx.total_cost,
                )
                tx.payment_done = True
                tx.state = TX_PAYMENT_DONE
                await _save_tx(tx)

            if tx.state == TX_PAYMENT_DONE:
                raw_order = await rget(order_id)
                if raw_order:
                    order_entry = msgpack.decode(raw_order, type=OrderValue)
                    order_entry.paid = True
                    await rset(order_id, msgpack.encode(order_entry))
                tx.state = TX_COMPLETED
                await _save_tx(tx)

            return reply(200, state=TX_COMPLETED)

    except WaitDieAbort as e:
        return reply(409, f"Transaction aborted (wait-die): {e}")
    except LockTimeout as e:
        return reply(503, f"Could not acquire lock in time: {e}")


# ---------------------------------------------------------------------------
# HTTP route
# ---------------------------------------------------------------------------


@app.before_serving
async def startup():
    await kafka_bus.start()
    await _recover_in_flight_transactions()


@app.after_serving
async def shutdown():
    await kafka_bus.stop()
    await asyncio.to_thread(db.close)


# async def handle_checkout():
#     cmd = await http_request.get_json(force=True, silent=True)
#     if not cmd:
#         abort(400, "Invalid JSON")
#     required = {"order_id", "user_id", "total_cost", "items"}
#     missing = required - cmd.keys()
#     if missing:
#         abort(400, f"Missing fields: {missing}")
#     result = await run_checkout_saga(cmd)
#     return jsonify(result), result["status_code"]


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
