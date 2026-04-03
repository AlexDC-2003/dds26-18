import logging
import os
import json
import asyncio
import time
import redis
from collections import defaultdict

from kafka_bus import KafkaBus
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from msgspec import msgpack, Struct
from quart import Quart, jsonify

app = Quart("orchestrator-service")

# ── Redis ────────────────────────────────────────────────────────────────────

db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']),
)

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_TIMEOUT_SEC = float(os.environ.get("KAFKA_REQUEST_TIMEOUT_SEC", "15"))
COMMIT_RETRY_SLEEP_SEC = float(os.environ.get("KAFKA_COMMIT_RETRY_SLEEP_SEC", "0.05"))
RECOVERY_STALENESS_SEC = 30

_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
_commands_topic = os.environ.get("KAFKA_ORCHESTRATOR_COMMANDS_TOPIC", "orchestrator.commands")
_replies_topic = os.environ.get("KAFKA_ORCHESTRATOR_REPLIES_TOPIC", "orchestrator.replies")
_group_id = os.environ.get("KAFKA_ORCHESTRATOR_GROUP_ID", "orchestrator-service")

# KafkaBus handles request/reply to stock and payment participants.
# It consumes stock.replies + payment.replies (no group_id — every instance
# sees every reply, matched by msg_id).
kafka_bus = KafkaBus()

# Command consumer + reply producer for the orchestrator's own protocol.
_consumer: AIOKafkaConsumer | None = None
_producer: AIOKafkaProducer | None = None
_consume_task: asyncio.Task | None = None


# ── Async Redis helpers ──────────────────────────────────────────────────────

async def rget(key: str):
    return await asyncio.to_thread(db.get, key)

async def rset(key: str, value: bytes, **kwargs):
    return await asyncio.to_thread(db.set, key, value, **kwargs)


# ── 2PC Transaction State ───────────────────────────────────────────────────

TX_KEY_PREFIX = "tx:"

TX_STARTED    = "STARTED"
TX_PREPARING  = "PREPARING"
TX_PREPARED   = "PREPARED"
TX_COMMITTING = "COMMITTING"
TX_COMPLETED  = "COMPLETED"
TX_ABORTED    = "ABORTED"


class TxValue(Struct):
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


class _DatabaseTransientError(Exception):
    pass


def _tx_key(tx_id: str) -> str:
    return f"{TX_KEY_PREFIX}{tx_id}"


async def _get_tx(tx_id: str) -> TxValue | None:
    try:
        raw = await rget(_tx_key(tx_id))
    except (redis.exceptions.RedisError, RuntimeError):
        raise _DatabaseTransientError("Redis is starting up...")
    return msgpack.decode(raw, type=TxValue) if raw else None


async def _save_tx(tx: TxValue) -> None:
    tx.updated_at = time.time()
    try:
        await rset(_tx_key(tx.tx_id), msgpack.encode(tx))
    except (redis.exceptions.RedisError, RuntimeError):
        raise _DatabaseTransientError("Redis is starting up...")


async def _get_or_create_tx_record(
    tx_id: str, order_id: str, user_id: str,
    total_cost: float, items: list[tuple[str, int]],
) -> TxValue:
    existing = await _get_tx(tx_id)
    if existing:
        return existing

    now = time.time()
    tx = TxValue(
        tx_id=tx_id, order_id=order_id, user_id=user_id,
        total_cost=total_cost, items=list(items),
        state=TX_STARTED, prepared_items=[],
        stock_prepared=False, payment_prepared=False,
        stock_committed=False, payment_committed=False,
        created_at=now, updated_at=now, error=None,
    )

    try:
        ok = await rset(_tx_key(tx_id), msgpack.encode(tx), nx=True)
    except (redis.exceptions.RedisError, RuntimeError):
        raise _DatabaseTransientError("Redis connecting...")

    if ok:
        return tx
    return (await _get_tx(tx_id)) or tx


# ── Helpers ──────────────────────────────────────────────────────────────────

def _reply_status_code(reply: dict) -> int:
    if "status_code" in reply:
        return int(reply["status_code"])
    if "ok" in reply:
        return 200 if reply["ok"] else 400
    return 400


def _is_lock_error(err: str | None) -> bool:
    if not err:
        return False
    low = err.lower()
    return "wait-die" in low or "lock timeout" in low


def _is_already_aborted_error(err: str | None) -> bool:
    if not err:
        return False
    return "already aborted" in err.lower()


def _prepared_as_dict(prepared_items: list[tuple[str, int]]) -> dict[str, int]:
    d: dict[str, int] = defaultdict(int)
    for item_id, qty in prepared_items:
        d[item_id] += qty
    return d


def _set_prepared_qty(tx: TxValue, item_id: str, qty: int) -> None:
    for i, (iid, _) in enumerate(tx.prepared_items):
        if iid == item_id:
            tx.prepared_items[i] = (iid, qty)
            return
    tx.prepared_items.append((item_id, qty))


# ── Participant Communication ────────────────────────────────────────────────

async def _participant_request(topic: str, cmd: dict, tx_id: str) -> tuple[int, str | None]:
    """Send a command to a participant and return (status_code, error_string)."""
    reply = await kafka_bus.request(topic, cmd, timeout_sec=KAFKA_TIMEOUT_SEC, key=tx_id.encode())
    sc = _reply_status_code(reply)
    err = reply.get("error")
    return sc, err


async def prepare_stock(tx_id: str, item_id: str, quantity: int, *, tx_ts: float):
    cmd = {
        "msg_id": f"reserve:{tx_id}:{item_id}",
        "tx_id": tx_id, "tx_ts": tx_ts,
        "type": "prepare_stock",
        "payload": {"item_id": item_id, "quantity": quantity},
    }
    return await _participant_request(os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, tx_id)


async def commit_stock(tx_id: str, *, tx_ts: float):
    cmd = {
        "msg_id": f"commit_stock:{tx_id}",
        "tx_id": tx_id, "tx_ts": tx_ts,
        "type": "commit_stock", "payload": {},
    }
    return await _participant_request(os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, tx_id)


async def abort_stock(tx_id: str, *, tx_ts: float):
    cmd = {
        "msg_id": f"abort_stock:{tx_id}",
        "tx_id": tx_id, "tx_ts": tx_ts,
        "type": "abort_stock", "payload": {},
    }
    return await _participant_request(os.environ["KAFKA_STOCK_COMMANDS_TOPIC"], cmd, tx_id)


async def prepare_payment(tx_id: str, user_id: str, amount: float, *, tx_ts: float):
    cmd = {
        "msg_id": f"prepare_payment:{tx_id}:{user_id}",
        "tx_id": tx_id, "tx_ts": tx_ts,
        "type": "prepare_payment",
        "payload": {"user_id": user_id, "amount": amount},
    }
    return await _participant_request(os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"], cmd, tx_id)


async def commit_payment(tx_id: str, *, tx_ts: float):
    cmd = {
        "msg_id": f"commit_payment:{tx_id}",
        "tx_id": tx_id, "tx_ts": tx_ts,
        "type": "commit_payment", "payload": {},
    }
    return await _participant_request(os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"], cmd, tx_id)


async def abort_payment(tx_id: str, *, tx_ts: float):
    cmd = {
        "msg_id": f"abort_payment:{tx_id}",
        "tx_id": tx_id, "tx_ts": tx_ts,
        "type": "abort_payment", "payload": {},
    }
    return await _participant_request(os.environ["KAFKA_PAYMENT_COMMANDS_TOPIC"], cmd, tx_id)


async def _abort_participants(tx: TxValue) -> None:
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


# ── 2PC Protocol ─────────────────────────────────────────────────────────────

async def _run_2pc(
    tx_id: str, order_id: str, user_id: str,
    items: list[tuple[str, int]], total_cost: float,
) -> dict:
    try:
        tx = await _get_or_create_tx_record(tx_id, order_id, user_id, total_cost, items)
    except _DatabaseTransientError as e:
        return {"status": "timeout", "error": str(e)}

    if tx.state == TX_COMPLETED:
        return {"status": "committed"}

    if tx.state == TX_ABORTED:
        if tx.error == "__crash_abort__":
            return {"status": "timeout", "error": "orchestrator restarted"}
        if _is_lock_error(tx.error):
            return {"status": "lock_contention", "error": tx.error}
        return {"status": "aborted", "error": tx.error or "previously aborted"}

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in tx.items:
        items_quantities[item_id] += quantity

    # ── Prepare phase ────────────────────────────────────────────────────
    if tx.state in (TX_STARTED, TX_PREPARING):
        tx.state = TX_PREPARING
        await _save_tx(tx)
        prepared_now = _prepared_as_dict(tx.prepared_items)

        for item_id, quantity in items_quantities.items():
            if prepared_now.get(item_id, 0) >= quantity:
                continue
            try:
                sc, err = await prepare_stock(tx.tx_id, item_id, quantity, tx_ts=tx.created_at)
            except asyncio.TimeoutError:
                return {"status": "timeout"}

            if sc != 200:
                err_msg = err or "Stock error"
                if _is_lock_error(err_msg):
                    return {"status": "lock_contention", "error": err_msg}
                if _is_already_aborted_error(err_msg):
                    tx.state = TX_ABORTED
                    tx.error = "__crash_abort__"
                    await _save_tx(tx)
                    await _abort_participants(tx)
                    return {"status": "timeout", "error": "orchestrator restarted"}

                tx.state = TX_ABORTED
                tx.error = err_msg
                await _save_tx(tx)
                await _abort_participants(tx)
                return {"status": "aborted", "error": tx.error}

            _set_prepared_qty(tx, item_id, quantity)
            tx.stock_prepared = True
            await _save_tx(tx)

        if not tx.payment_prepared:
            try:
                sc, err = await prepare_payment(tx.tx_id, tx.user_id, tx.total_cost, tx_ts=tx.created_at)
            except asyncio.TimeoutError:
                return {"status": "timeout"}

            if sc != 200:
                pay_err = err or "Payment error"
                if _is_lock_error(pay_err):
                    return {"status": "lock_contention", "error": pay_err}
                if _is_already_aborted_error(pay_err):
                    tx.state = TX_ABORTED
                    tx.error = "__crash_abort__"
                    await _save_tx(tx)
                    await _abort_participants(tx)
                    return {"status": "timeout", "error": "orchestrator restarted"}

                tx.state = TX_ABORTED
                tx.error = pay_err
                await _save_tx(tx)
                await _abort_participants(tx)
                return {"status": "aborted", "error": tx.error}

            tx.payment_prepared = True
            await _save_tx(tx)

        tx.state = TX_PREPARED
        await _save_tx(tx)

    # ── Commit phase ─────────────────────────────────────────────────────
    if tx.state in (TX_PREPARED, TX_COMMITTING):
        tx.state = TX_COMMITTING
        await _save_tx(tx)

        while not (tx.payment_committed and tx.stock_committed):
            if not tx.payment_committed:
                try:
                    sc, _ = await commit_payment(tx.tx_id, tx_ts=tx.created_at)
                    if sc == 200:
                        tx.payment_committed = True
                    await _save_tx(tx)
                except Exception:
                    pass
            if not tx.stock_committed:
                try:
                    sc, _ = await commit_stock(tx.tx_id, tx_ts=tx.created_at)
                    if sc == 200:
                        tx.stock_committed = True
                    await _save_tx(tx)
                except Exception:
                    pass
            if not (tx.payment_committed and tx.stock_committed):
                await asyncio.sleep(COMMIT_RETRY_SLEEP_SEC)

        tx.state = TX_COMPLETED
        await _save_tx(tx)

    return {"status": "committed"}


# ── Command Consumer ─────────────────────────────────────────────────────────

async def _consume_commands() -> None:
    assert _consumer is not None
    async for msg in _consumer:
        try:
            cmd = json.loads(msg.value.decode("utf-8"))
        except Exception:
            continue
        asyncio.create_task(_process_command(cmd))


async def _process_command(cmd: dict) -> None:
    msg_id = cmd.get("msg_id")
    tx_id = cmd.get("tx_id")
    typ = cmd.get("type")
    payload = cmd.get("payload") or {}

    if typ != "checkout" or not msg_id or not tx_id:
        await _send_reply(msg_id, tx_id, "aborted", "invalid command")
        return

    order_id = payload.get("order_id", "")
    user_id = str(payload.get("user_id", ""))
    raw_items = payload.get("items", [])
    total_cost = float(payload.get("total_cost", 0))

    # JSON arrays → tuples expected by TxValue
    items = [(str(i[0]), int(i[1])) for i in raw_items]

    result = await _run_2pc(tx_id, order_id, user_id, items, total_cost)
    await _send_reply(msg_id, tx_id, result.get("status", "aborted"), result.get("error"))


async def _send_reply(msg_id: str | None, tx_id: str | None, status: str, error: str | None = None) -> None:
    if _producer is None:
        return
    reply = {"msg_id": msg_id, "tx_id": tx_id, "status": status, "error": error}
    try:
        payload = json.dumps(reply).encode("utf-8")
        await _producer.send_and_wait(_replies_topic, payload)
    except Exception as e:
        print(f"Failed to send orchestrator reply: {e}", flush=True)


# ── Recovery ─────────────────────────────────────────────────────────────────

async def _recover_pending_transactions() -> None:
    lock_key = "orchestrator:recovery_lock"
    acquired = await rset(lock_key, "1", nx=True, ex=60)
    if not acquired:
        return

    print("Starting Orchestrator recovery scan...", flush=True)
    try:
        now = time.time()
        cursor = 0
        while True:
            cursor, keys = await asyncio.to_thread(
                db.scan, cursor, match=f"{TX_KEY_PREFIX}*", count=100,
            )
            for key in keys:
                raw = await rget(key.decode())
                if not raw:
                    continue

                tx = msgpack.decode(raw, type=TxValue)
                if tx.state in (TX_COMPLETED, TX_ABORTED):
                    continue

                age = now - tx.updated_at
                if age < RECOVERY_STALENESS_SEC:
                    print(
                        f"Recovery: Skipping tx {tx.tx_id} "
                        f"(updated {age:.1f}s ago, may be active)",
                        flush=True,
                    )
                    continue

                if tx.state in (TX_STARTED, TX_PREPARING):
                    print(f"Recovery: Aborting stuck tx {tx.tx_id}", flush=True)
                    tx.state = TX_ABORTED
                    tx.error = "__crash_abort__"
                    await _save_tx(tx)
                    await _abort_participants(tx)
                elif tx.state in (TX_PREPARED, TX_COMMITTING):
                    print(f"Recovery: Resuming commit for tx {tx.tx_id}", flush=True)
                    asyncio.create_task(
                        _run_2pc(tx.tx_id, tx.order_id, tx.user_id, tx.items, tx.total_cost)
                    )

            if cursor == 0:
                break
    finally:
        await asyncio.to_thread(db.delete, lock_key)


# ── Lifecycle ────────────────────────────────────────────────────────────────

@app.before_serving
async def startup():
    global _consumer, _producer, _consume_task

    # KafkaBus for stock/payment request-reply (consumes stock.replies + payment.replies)
    await kafka_bus.start()

    # Producer for orchestrator.replies
    _producer = AIOKafkaProducer(bootstrap_servers=_bootstrap)
    await _producer.start()

    # Consumer for orchestrator.commands (group_id distributes partitions across replicas)
    _consumer = AIOKafkaConsumer(
        _commands_topic,
        bootstrap_servers=_bootstrap,
        group_id=_group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await _consumer.start()
    _consume_task = asyncio.create_task(_consume_commands())

    await _recover_pending_transactions()


@app.after_serving
async def shutdown():
    global _consumer, _producer, _consume_task

    if _consume_task:
        _consume_task.cancel()
        _consume_task = None
    if _consumer:
        await _consumer.stop()
        _consumer = None
    if _producer:
        await _producer.stop()
        _producer = None

    await kafka_bus.stop()
    await asyncio.to_thread(db.close)


@app.get('/health')
async def health():
    return jsonify({"ok": True})


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
