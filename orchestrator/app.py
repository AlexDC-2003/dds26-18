import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from types import SimpleNamespace

import redis
from aiokafka import AIOKafkaConsumer
from msgspec import Struct, msgpack
from quart import Quart, abort

from kafka_bus import KafkaBus

app = Quart("orchestrator")

DB_ERROR_STR = "DB error"

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

KAFKA_TIMEOUT_SEC = float(os.environ.get("KAFKA_REQUEST_TIMEOUT_SEC", "15"))
COMMIT_RETRY_SLEEP_SEC = float(os.environ.get("KAFKA_COMMIT_RETRY_SLEEP_SEC", "0.05"))

ORCHESTRATOR_COMMANDS_TOPIC = os.environ.get("KAFKA_ORCHESTRATOR_COMMANDS_TOPIC", "orchestrator.commands")
ORCHESTRATOR_REPLIES_TOPIC = os.environ.get("KAFKA_ORCHESTRATOR_REPLIES_TOPIC", "orchestrator.replies")

kafka_bus = KafkaBus()
_commands_consumer: AIOKafkaConsumer | None = None


@app.before_serving
async def startup():
    global _commands_consumer
    await kafka_bus.start()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
    _commands_consumer = AIOKafkaConsumer(
        ORCHESTRATOR_COMMANDS_TOPIC,
        bootstrap_servers=bootstrap,
        group_id="orchestrator-group",
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    await _commands_consumer.start()
    await _recover_pending_transactions()
    asyncio.create_task(_consume_commands())


@app.after_serving
async def shutdown():
    if _commands_consumer:
        await _commands_consumer.stop()
    await kafka_bus.stop()
    await asyncio.to_thread(db.close)


# --- Async Redis helpers ---

async def rget(key: str):
    return await asyncio.to_thread(db.get, key)


async def rset(key: str, value: bytes, **kwargs):
    return await asyncio.to_thread(db.set, key, value, **kwargs)


# --- 2PC State ---

TX_KEY_PREFIX = "tx:"
TX_STARTED    = "STARTED"
TX_PREPARING  = "PREPARING"
TX_PREPARED   = "PREPARED"
TX_COMMITTING = "COMMITTING"
TX_COMPLETED  = "COMPLETED"
TX_ABORTED    = "ABORTED"


class TxRecord(Struct):
    tx_id: str
    order_id: str
    user_id: str
    total_cost: float
    items: list[tuple[str, int]]
    state: str
    prepared_items: list[tuple[str, int]]
    stock_prepared: bool
    payment_prepared: bool
    stock_committed: bool
    payment_committed: bool
    created_at: float
    updated_at: float
    error: str | None


def _tx_key(tx_id: str) -> str:
    return f"{TX_KEY_PREFIX}{tx_id}"


async def _get_tx(tx_id: str) -> TxRecord | None:
    try:
        raw = await rget(_tx_key(tx_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return msgpack.decode(raw, type=TxRecord) if raw else None


async def _save_tx(tx: TxRecord) -> None:
    tx.updated_at = time.time()
    try:
        await rset(_tx_key(tx.tx_id), msgpack.encode(tx))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


async def _get_or_create_tx_record(
    tx_id: str,
    order_id: str,
    user_id: str,
    total_cost: float,
    items: list[tuple[str, int]],
    tx_ts: float,
) -> TxRecord:
    existing = await _get_tx(tx_id)
    if existing:
        return existing

    tx = TxRecord(
        tx_id=tx_id,
        order_id=order_id,
        user_id=user_id,
        total_cost=total_cost,
        items=items,
        state=TX_STARTED,
        prepared_items=[],
        stock_prepared=False,
        payment_prepared=False,
        stock_committed=False,
        payment_committed=False,
        created_at=tx_ts,
        updated_at=tx_ts,
        error=None,
    )
    try:
        ok = await rset(_tx_key(tx_id), msgpack.encode(tx), nx=True)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    if ok:
        return tx
    return (await _get_tx(tx_id)) or tx


# --- Kafka messaging ---

def _reply_status_code(reply: dict, *, service: str) -> int:
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
        json=lambda: reply.get("payload") or reply,
    )


async def _kafka_cmd(topic: str, cmd: dict, *, service: str):
    reply = await kafka_bus.request(topic, cmd, timeout_sec=KAFKA_TIMEOUT_SEC)
    return _as_response_like(reply, service=service)


async def prepare_stock(tx_id: str, item_id: str, quantity: int, *, tx_ts: float):
    return await _kafka_cmd(
        os.environ["KAFKA_STOCK_COMMANDS_TOPIC"],
        {
            "msg_id": f"reserve:{tx_id}:{item_id}",
            "tx_id": tx_id, "tx_ts": tx_ts,
            "type": "prepare_stock",
            "payload": {"item_id": item_id, "quantity": quantity},
        },
        service="stock",
    )


async def commit_stock(tx_id: str, *, tx_ts: float):
    return await _kafka_cmd(
        os.environ["KAFKA_STOCK_COMMANDS_TOPIC"],
        {
            "msg_id": f"commit_stock:{tx_id}",
            "tx_id": tx_id, "tx_ts": tx_ts,
            "type": "commit_stock", "payload": {},
        },
        service="stock",
    )


async def abort_stock(tx_id: str, *, tx_ts: float):
    return await _kafka_cmd(
        os.environ["KAFKA_STOCK_COMMANDS_TOPIC"],
        {
            "msg_id": f"abort_stock:{tx_id}",
            "tx_id": tx_id, "tx_ts": tx_ts,
            "type": "abort_stock", "payload": {},
        },
        service="stock",
    )


async def prepare_payment(tx_id: str, user_id: str, amount: float, *, tx_ts: float):
    return await _kafka_cmd(
        os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"],
        {
            "msg_id": f"prepare_payment:{tx_id}:{user_id}",
            "tx_id": tx_id, "tx_ts": tx_ts,
            "type": "prepare_payment",
            "payload": {"user_id": user_id, "amount": amount},
        },
        service="payment",
    )


async def commit_payment(tx_id: str, *, tx_ts: float):
    return await _kafka_cmd(
        os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"],
        {
            "msg_id": f"commit_payment:{tx_id}",
            "tx_id": tx_id, "tx_ts": tx_ts,
            "type": "commit_payment", "payload": {},
        },
        service="payment",
    )


async def abort_payment(tx_id: str, *, tx_ts: float):
    return await _kafka_cmd(
        os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"],
        {
            "msg_id": f"abort_payment:{tx_id}",
            "tx_id": tx_id, "tx_ts": tx_ts,
            "type": "abort_payment", "payload": {},
        },
        service="payment",
    )


# --- Helpers ---

def _is_lock_error(err: str | None) -> bool:
    if not err:
        return False
    low = err.lower()
    return "wait-die" in low or "lock timeout" in low


def _prepared_as_dict(prepared_items: list[tuple[str, int]]) -> dict[str, int]:
    d: dict[str, int] = defaultdict(int)
    for item_id, qty in prepared_items:
        d[item_id] += qty
    return d


def _set_prepared_qty(tx: TxRecord, item_id: str, qty: int) -> None:
    for i, (iid, _) in enumerate(tx.prepared_items):
        if iid == item_id:
            tx.prepared_items[i] = (iid, qty)
            return
    tx.prepared_items.append((item_id, qty))


async def _abort_participants(tx: TxRecord) -> None:
    if tx.payment_prepared and not tx.payment_committed:
        try:
            await abort_payment(tx.tx_id, tx_ts=tx.created_at)
        except Exception:
            pass
    # Always try to abort stock — some items may have been prepared even if
    # tx.stock_prepared was not yet set (e.g. parallel prepares).  The abort
    # handler is idempotent and handles missing tx records as noop.
    if not tx.stock_committed:
        try:
            await abort_stock(tx.tx_id, tx_ts=tx.created_at)
        except Exception:
            pass


# --- Core transaction logic ---

async def _run_transaction(body: dict) -> dict:
    tx_id: str = body["tx_id"]
    order_id: str = body["order_id"]
    user_id: str = body["user_id"]
    total_cost: float = float(body["total_cost"])
    items: list[tuple[str, int]] = [tuple(pair) for pair in body["items"]]
    tx_ts: float = float(body["tx_ts"])

    tx = await _get_or_create_tx_record(tx_id, order_id, user_id, total_cost, items, tx_ts)

    if tx.state == TX_COMPLETED:
        return {"status": "committed"}

    if tx.state == TX_ABORTED:
        if _is_lock_error(tx.error):
            return {"status": "lock_contention", "error": tx.error}
        if tx.error == "__crash_abort__":
            return {"status": "timeout", "error": "orchestrator restarted, please retry"}
        return {"status": "aborted", "error": tx.error or "previously aborted"}

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in tx.items:
        items_quantities[item_id] += quantity

    # --- Prepare phase ---
    if tx.state in (TX_STARTED, TX_PREPARING):
        tx.state = TX_PREPARING
        await _save_tx(tx)

        prepared_now = _prepared_as_dict(tx.prepared_items)
        for item_id, quantity in items_quantities.items():
            if prepared_now.get(item_id, 0) >= quantity:
                continue
            try:
                stock_reply = await prepare_stock(tx.tx_id, item_id, quantity, tx_ts=tx.created_at)
            except asyncio.TimeoutError:
                tx.state = TX_ABORTED
                tx.error = f"Timeout waiting for stock prepare on item {item_id}"
                await _save_tx(tx)
                await _abort_participants(tx)
                return {"status": "timeout", "error": tx.error}
            if stock_reply.status_code != 200:
                error_body = stock_reply.json()
                tx.error = (error_body.get("error") if isinstance(error_body, dict) else None) or f"Error on item {item_id}"
                tx.state = TX_ABORTED
                await _save_tx(tx)
                await _abort_participants(tx)
                if _is_lock_error(tx.error):
                    return {"status": "lock_contention", "error": tx.error}
                return {"status": "aborted", "error": tx.error}
            _set_prepared_qty(tx, item_id, quantity)
            tx.stock_prepared = True
            await _save_tx(tx)
            print(f"Prepared stock item={item_id} qty={quantity} tx={tx.tx_id}", flush=True)

        if not tx.payment_prepared:
            try:
                user_reply = await prepare_payment(tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at)
            except asyncio.TimeoutError:
                tx.state = TX_ABORTED
                tx.error = "Timeout waiting for payment prepare"
                await _save_tx(tx)
                await _abort_participants(tx)
                return {"status": "timeout", "error": tx.error}
            if user_reply.status_code != 200:
                payment_body = user_reply.json()
                payment_err = payment_body.get("error") if isinstance(payment_body, dict) else None
                if _is_lock_error(payment_err):
                    tx.state = TX_ABORTED
                    tx.error = payment_err
                    await _save_tx(tx)
                    await _abort_participants(tx)
                    return {"status": "lock_contention", "error": tx.error}
                tx.state = TX_ABORTED
                tx.error = f"User out of credit: txid={tx.tx_id}"
                await _save_tx(tx)
                await _abort_participants(tx)
                return {"status": "aborted", "error": tx.error}
            tx.payment_prepared = True
            await _save_tx(tx)
            print(f"Prepared payment user={tx.user_id} amount={tx.total_cost} tx={tx.tx_id}", flush=True)

        tx.state = TX_PREPARED
        await _save_tx(tx)
        print(f"Prepared tx={tx.tx_id} order={order_id}", flush=True)

    # --- Commit phase ---
    if tx.state in (TX_PREPARED, TX_COMMITTING):
        tx.state = TX_COMMITTING
        await _save_tx(tx)

        while not (tx.payment_committed and tx.stock_committed):
            if not tx.payment_committed:
                try:
                    r = await commit_payment(tx.tx_id, tx_ts=tx.created_at)
                    if r.status_code == 200:
                        tx.payment_committed = True
                        print(f"Committed payment user={tx.user_id} tx={tx.tx_id}", flush=True)
                    else:
                        tx.error = "Failed to commit payment, retrying"
                    await _save_tx(tx)
                except Exception:
                    pass

            if not tx.stock_committed:
                try:
                    r = await commit_stock(tx.tx_id, tx_ts=tx.created_at)
                    if r.status_code == 200:
                        tx.stock_committed = True
                        print(f"Committed stock order={order_id} tx={tx.tx_id}", flush=True)
                    else:
                        tx.error = "Failed to commit stock, retrying"
                    await _save_tx(tx)
                except Exception:
                    pass

            if not (tx.payment_committed and tx.stock_committed):
                await asyncio.sleep(COMMIT_RETRY_SLEEP_SEC)

        tx.error = None
        tx.state = TX_COMPLETED
        await _save_tx(tx)
        print(f"Completed tx={tx.tx_id} order={order_id}", flush=True)

    return {"status": "committed"}


# --- Kafka command consumer ---

async def _consume_commands() -> None:
    assert _commands_consumer is not None
    async for msg in _commands_consumer:
        try:
            data = json.loads(msg.value.decode("utf-8"))
        except Exception:
            continue
        asyncio.create_task(_handle_command(data))


async def _handle_command(data: dict) -> None:
    msg_id = data.get("msg_id")
    try:
        result = await _run_transaction(data)
    except Exception as e:
        result = {"status": "aborted", "error": f"Internal error: {e}"}
    result["msg_id"] = msg_id
    await kafka_bus.publish(ORCHESTRATOR_REPLIES_TOPIC, result)


async def _recover_pending_transactions():
    lock_key = "orchestrator:recovery_lock"
    acquired = await rset(lock_key, "1", nx=True, ex=60)
    if not acquired:
        return

    print("Starting Orchestrator recovery scan...", flush=True)
    try:
        cursor = 0
        while True:
            cursor, keys = await asyncio.to_thread(db.scan, cursor, match=f"{TX_KEY_PREFIX}*", count=100)

            for key in keys:
                key_str = key.decode("utf-8")
                raw = await rget(key_str)
                if not raw:
                    continue

                tx = msgpack.decode(raw, type=TxRecord)

                if tx.state in (TX_COMPLETED, TX_ABORTED):
                    continue

                if tx.state in (TX_STARTED, TX_PREPARING):
                    print(f"Recovery: Rolling back stuck tx {tx.tx_id}", flush=True)
                    tx.state = TX_ABORTED
                    tx.error = "__crash_abort__"
                    await _save_tx(tx)
                    await _abort_participants(tx)

                elif tx.state in (TX_PREPARED, TX_COMMITTING):
                    print(f"Recovery: Resuming commit for tx {tx.tx_id}", flush=True)
                    asyncio.create_task(_run_transaction({
                        "tx_id": tx.tx_id,
                        "order_id": tx.order_id,
                        "user_id": tx.user_id,
                        "total_cost": tx.total_cost,
                        "items": tx.items,
                        "tx_ts": tx.created_at,
                    }))

            if cursor == 0:
                break
    finally:
        # Delete the lock when finished so other workers can run if needed later
        await asyncio.to_thread(db.delete, lock_key)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
