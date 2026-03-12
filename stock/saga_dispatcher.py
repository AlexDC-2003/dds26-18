import json
import logging
import time
import redis

from lock_manager import acquire_lock, release_lock, random_backoff, LockDeadlockAbort

logger = logging.getLogger(__name__)
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

    msg_id = command["msg_id"]
    msg_type = command["type"]
    if msg_type == "release_stock":
        item_id = command["payload"].get("item_id", "")
        log_key = f"saga:release:{command['tx_id']}:{item_id}"
    elif msg_type == "reserve_stock":
        item_id = command["payload"].get("item_id", "")
        log_key = f"saga:msg:reserve:{command['tx_id']}:{item_id}"
    else:
        log_key = f"saga:msg:{msg_id}"

    # -------------------------
    # IDEMPOTENCY CHECK
    # -------------------------
    existing = redis_client.get(log_key)
    if existing:
        logger.info("[IDEMPOTENT] type=%s tx=%s msg=%s item=%s — returning cached reply",
                    msg_type, command.get("tx_id"), msg_id, command["payload"].get("item_id", ""))
        return json.loads(existing)

    try:
        if msg_type == "reserve_stock":
            reply = handle_reserve_stock(command)

        elif msg_type == "release_stock":
            reply = handle_release_stock(command)

        else:
            reply = build_error(command, f"Unknown command type: {msg_type}")
    except LockDeadlockAbort as e:
        # Assumed deadlock — sleep a random back-off then re-raise so kafka_bus can retry the command
        print(f"[2PL] Deadlock abort for tx {command.get('tx_id')}: {e}")
        time.sleep(random_backoff())
        raise

    # -------------------------
    # STORE RESULT (durable log)
    # -------------------------
    # redis_client.set(log_key, json.dumps(reply),  ex=86400)

    return reply


# ---------------------------------------------------------
# SAGA HANDLERS
# ---------------------------------------------------------

def handle_reserve_stock(command):
    item_id = command["payload"].get("item_id")
    quantity = int(command["payload"].get("quantity", 0))
    log_key = f"saga:msg:reserve:{command['tx_id']}:{item_id}"

    if not item_id or quantity <= 0:
        return build_error(command, "Invalid payload")

    key = f"item:{item_id}"
    acquire_lock(item_id, command["tx_id"])
    try:
        if not redis_client.exists(key):
            logger.warning("[RESERVE:ERROR] tx=%s item=%s — item not found", command["tx_id"], item_id)
            return build_error(command, "Item not found")

        with redis_client.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(key)
                    current_stock = int(pipe.hget(key, "stock"))

                    if current_stock < quantity:
                        pipe.unwatch()
                        logger.info("[RESERVE:INSUFFICIENT] tx=%s item=%s qty=%s stock=%s",
                                    command["tx_id"], item_id, quantity, current_stock)
                        return build_error(command, "Insufficient stock")

                    reply = build_success(command, {"item_id": item_id, "reserved": quantity})

                    pipe.multi()
                    pipe.hincrby(key, "stock", -quantity)
                    pipe.set(log_key, json.dumps(reply),  ex=86400)
                    pipe.execute()
                    logger.info("[RESERVE:COMMITTED] tx=%s item=%s qty=%s stock_before=%s stock_after=%s",
                                command["tx_id"], item_id, quantity, current_stock, current_stock - quantity)
                    break

                except redis.WatchError:
                    continue

    finally:
        # 2PL shrinking phase: release lock after operation completes
        release_lock(item_id, command["tx_id"])

    return reply

def handle_release_stock(command):
    item_id = command["payload"].get("item_id")
    quantity = int(command["payload"].get("quantity", 0))
    log_key = f"saga:release:{command['tx_id']}:{item_id}"

    if not item_id or quantity <= 0:
        return build_error(command, "Invalid payload")

    # Guard: only release if a reserve was committed for this tx+item.
    # Prevents erroneously adding stock for items that were never reserved
    # (e.g. retry releases sent by the order service on TX_ABORTED reentry).
    reserve_log_key = f"saga:msg:reserve:{command['tx_id']}:{item_id}"
    if not redis_client.exists(reserve_log_key):
        # No committed reserve found — safe no-op
        logger.info("[RELEASE:NOOP] tx=%s item=%s — no reserve key found, skipping release",
                    command["tx_id"], item_id)
        return build_success(command, {"item_id": item_id, "released": 0})

    key = f"item:{item_id}"

    # 2PL growing phase
    acquire_lock(item_id, command["tx_id"])
    try:
        if not redis_client.exists(key):
            logger.warning("[RELEASE:ERROR] tx=%s item=%s — item not found", command["tx_id"], item_id)
            return build_error(command, "Item not found")

        current_stock = int(redis_client.hget(key, "stock") or 0)
        reply = build_success(command, {"item_id": item_id, "released": quantity})

        with redis_client.pipeline(transaction=True) as pipe:
            pipe.hincrby(key, "stock", quantity)
            pipe.set(log_key, json.dumps(reply), ex=86400)
            # Delete the reserve idempotency key atomically with the release.
            # If the order-db loses the TX_ABORTED record (AOF fsync window) and a
            # retry comes in, the missing reserve key forces a real re-reservation
            # instead of silently returning "reserved" for stock that is no longer held.
            pipe.delete(reserve_log_key)
            pipe.execute()
        logger.info("[RELEASE:COMMITTED] tx=%s item=%s qty=%s stock_before=%s stock_after=%s",
                    command["tx_id"], item_id, quantity, current_stock, current_stock + quantity)

    finally:
        # 2PL shrinking phase
        release_lock(item_id, command["tx_id"])

    return reply
