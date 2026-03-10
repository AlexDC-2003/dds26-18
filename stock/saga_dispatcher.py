import json
import time
import redis

from lock_manager import acquire_lock, release_lock, random_backoff, LockDeadlockAbort
redis_client = None


def set_redis_client(client):
    global redis_client
    redis_client = client


# ---------------------------------------------------------
# Reply Builders (Standardized)
# ---------------------------------------------------------

def build_success(command, payload):
    return {
        "msg_id": command["msg_id"],
        "tx_id": command["tx_id"],
        "ok": True,
        "payload": payload
    }


def build_error(command, error_message):
    return {
        "msg_id": command.get("msg_id"),
        "tx_id": command.get("tx_id"),
        "ok": False,
        "error": error_message
    }


# ---------------------------------------------------------
# Dispatcher with Idempotency
# ---------------------------------------------------------

def stock_dispatcher(command):

    required_fields = ["msg_id", "tx_id", "type", "payload"]
    for field in required_fields:
        if field not in command:
            return build_error(command, f"Missing field: {field}")

    msg_type = command["type"]

    try:
        if msg_type == "prepare_stock":
            reply = handle_prepare_stock(command)
        elif msg_type == "commit_stock":
            reply = handle_commit_stock(command)
        elif msg_type == "abort_stock":
            reply = handle_abort_stock(command)

        else:
            reply = build_error(command, f"Unknown command type: {msg_type}")
    except LockDeadlockAbort as e:
        # Assumed deadlock — sleep a random back-off then re-raise so kafka_bus can retry the command
        print(f"[2PL] Deadlock abort for tx {command.get('tx_id')}: {e}")
        time.sleep(random_backoff())
        raise

    return reply


# ---------------------------------------------------------
# 2PC HANDLERS
# ---------------------------------------------------------

def _tx_key(tx_id: str) -> str:
    return f"stock:2pc:tx:{tx_id}"


def _read_tx(tx_id: str):
    raw = redis_client.get(_tx_key(tx_id))
    if not raw:
        return None
    return json.loads(raw)


def _write_tx(tx_id: str, tx_doc: dict) -> None:
    redis_client.set(_tx_key(tx_id), json.dumps(tx_doc), ex=86400)


def _new_tx(command: dict) -> dict:
    return {
        "tx_id": command["tx_id"],
        "state": "PREPARED",
        "items": {},
        "updated_at": time.time(),
    }


def handle_prepare_stock(command):
    item_id = command["payload"].get("item_id")
    quantity = int(command["payload"].get("quantity", 0))
    print(f"Handling prepare_stock for item_id: {item_id}, quantity: {quantity}, tx_id: {command['tx_id']}")
    if not item_id or quantity <= 0:
        return build_error(command, "Invalid payload")

    key = f"item:{item_id}"
    tx_id = command["tx_id"]
    tx = _read_tx(tx_id) or _new_tx(command)
    if tx.get("state") == "ABORTED":
        return build_error(command, "Transaction already aborted")
    if tx.get("state") == "COMMITTED":
        return build_success(command, {"item_id": item_id, "prepared": quantity, "state": "COMMITTED"})

    prepared_map = tx.setdefault("items", {})
    already_prepared = int(prepared_map.get(item_id, 0))
    if already_prepared >= quantity:
        print(f"Idempotent prepare_stock for item_id: {item_id}, already prepared: {already_prepared} in tx_id: {tx_id}")
        return build_success(command, {"item_id": item_id, "prepared": already_prepared, "state": tx.get("state", "PREPARED")})

    acquire_lock(item_id, tx_id)
    print(f"Acquired lock for item_id: {item_id} in tx_id: {tx_id}")
    if not redis_client.exists(key):
        print(f"Item not found for item_id: {item_id} in tx_id: {tx_id}")
        release_lock(item_id, tx_id)
        return build_error(command, "Item not found")

    current_stock = int(redis_client.hget(key, "stock"))
    if current_stock < quantity:
        print(f"Insufficient stock for item_id: {item_id}, requested: {quantity}, available: {current_stock} in tx_id: {tx_id}")
        release_lock(item_id, tx_id)
        return build_error(command, "Insufficient stock")

    prepared_map[item_id] = quantity
    tx["state"] = "PREPARED"
    tx["updated_at"] = time.time()
    _write_tx(tx_id, tx)
    print(f"Prepared stock for item_id: {item_id}, quantity: {quantity} in tx_id: {tx_id}")
    return build_success(command, {"item_id": item_id, "prepared": quantity, "state": "PREPARED"})

def handle_commit_stock(command):
    print(f"Handling commit_stock for tx_id: {command['tx_id']}")
    tx_id = command["tx_id"]
    tx = _read_tx(tx_id)

    if tx is None:
        return build_success(command, {"state": "COMMITTED", "noop": True})
    if tx.get("state") == "ABORTED":
        return build_error(command, "Transaction already aborted")
    if tx.get("state") == "COMMITTED":
        return build_success(command, {"state": "COMMITTED", "noop": True})

    items = tx.get("items", {})
    for item_id in sorted(items.keys()):
        quantity = int(items[item_id])
        key = f"item:{item_id}"
        try:
            if not redis_client.exists(key):
                release_lock(item_id, tx_id)
                return build_error(command, "Item not found")

            with redis_client.pipeline() as pipe:
                while True:
                    try:
                        pipe.watch(key)
                        current_stock = int(pipe.hget(key, "stock"))
                        if current_stock < quantity:
                            pipe.unwatch()
                            release_lock(item_id, tx_id)
                            return build_error(command, "Insufficient stock at commit")
                        pipe.multi()
                        pipe.hincrby(key, "stock", -quantity)
                        pipe.execute()
                        break
                    except redis.WatchError:
                        continue

        except Exception as e:
            release_lock(item_id, tx_id)
            print(f"DB error during commit for item_id: {item_id} in tx_id: {tx_id}: {e}")
            return build_error(command, f"DB error during commit: {e}")

        release_lock(item_id, tx_id)

    tx["state"] = "COMMITTED"
    tx["updated_at"] = time.time()
    _write_tx(tx_id, tx)
    print(f"Committed stock for tx_id: {tx_id}")
    return build_success(command, {"state": "COMMITTED"})


def handle_abort_stock(command):
    tx_id = command["tx_id"]
    tx = _read_tx(tx_id)
    if tx is None:
        return build_success(command, {"state": "ABORTED", "noop": True})
    if tx.get("state") == "ABORTED":
        return build_success(command, {"state": "ABORTED", "noop": True})
    if tx.get("state") == "COMMITTED":
        return build_error(command, "Transaction already committed")

    items = tx.get("items", {})
    for item_id in sorted(items.keys()):
        release_lock(item_id, tx_id)

    tx["state"] = "ABORTED"
    tx["updated_at"] = time.time()
    _write_tx(tx_id, tx)
    print(f"Aborted stock transaction for tx_id: {tx_id}")
    return build_success(command, {"state": "ABORTED"})
