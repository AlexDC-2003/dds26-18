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
        log_key = f"saga:reserve:{command['tx_id']}:{item_id}"
    else:
        log_key = f"saga:msg:{msg_id}"
    quantity = int(command["payload"].get("quantity", 0))
    tx_id = command["tx_id"]
    # -------------------------
    # IDEMPOTENCY CHECK
    # -------------------------
    
    state = redis_client.get(log_key)
    if state and msg_type == "release_stock":
        logger.info("[RELEASE:IDEMPOTENT] tx=%s item=%s already released", tx_id, item_id)
        return build_success(command, {"item_id": item_id, "released": 0})

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
    log_key = f"saga:reserve:{command['tx_id']}:{item_id}"

    if not item_id or quantity <= 0:
        return build_error(command, "Invalid payload")

    key = f"item:{item_id}"
    acquire_lock(item_id, command["tx_id"])
    try:
        # IDP check inside lock
        state = redis_client.get(log_key)
        if state:
            if state == "RESERVED":
                logger.info("[RESERVE:IDEMPOTENT] tx=%s item=%s already reserved", command["tx_id"], item_id)
                return build_success(command, {"item_id": item_id, "reserved": quantity})
            if state == "RELEASED":
                logger.info("[RESERVE:BLOCKED] tx=%s item=%s already released", command["tx_id"], item_id)
                return build_error(command, "Reservation already released")

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
                    pipe.set(log_key, "RESERVED", ex=86400)
                    pipe.execute()
                    logger.info("[RESERVE:COMMITTED] tx=%s item=%s qty=%s stock_before=%s stock_after=%s",
                                command["tx_id"], item_id, quantity, current_stock, current_stock - quantity)
                    break

                except redis.WatchError:
                    continue

    finally:
        release_lock(item_id, command["tx_id"])

    return reply

def handle_release_stock(command):
    item_id = command["payload"].get("item_id")
    quantity = int(command["payload"].get("quantity", 0))
    log_key = f"saga:release:{command['tx_id']}:{item_id}"
    reserve_log_key = f"saga:reserve:{command['tx_id']}:{item_id}"

    if not item_id or quantity <= 0:
        return build_error(command, "Invalid payload")

    key = f"item:{item_id}"

    # 2PL growing phase: acquire lock first, THEN check idempotency/no-op.
    acquire_lock(item_id, command["tx_id"])
    try:
        # 1) If we've already processed this release, return cached reply (idempotent).
        existing_release = redis_client.get(log_key)
        if existing_release:
            try:
                return json.loads(existing_release)
            except Exception:
                # defensive fallback if stored value isn't JSON
                return build_success(command, {"item_id": item_id, "released": 0})

        # 2) If there was no reservation (or it's missing), it's a safe no-op.
        if not redis_client.exists(reserve_log_key):
            logger.info("[RELEASE:NOOP] tx=%s item=%s — no reserve key found, skipping release",
                        command["tx_id"], item_id)
            return build_success(command, {"item_id": item_id, "released": 0})

        if not redis_client.exists(key):
            logger.warning("[RELEASE:ERROR] tx=%s item=%s — item not found", command["tx_id"], item_id)
            return build_error(command, "Item not found")

        current_stock = int(redis_client.hget(key, "stock") or 0)
        reply = build_success(command, {"item_id": item_id, "released": quantity})

        # perform increment + set release log + set reserve state atomically
        with redis_client.pipeline(transaction=True) as pipe:
            pipe.hincrby(key, "stock", quantity)
            pipe.set(log_key, json.dumps(reply), ex=86400)
            pipe.set(reserve_log_key, "RELEASED", ex=86400)
            pipe.execute()

        logger.info("[RELEASE:COMMITTED] tx=%s item=%s qty=%s stock_before=%s stock_after=%s",
                    command["tx_id"], item_id, quantity, current_stock, current_stock + quantity)

    finally:
        release_lock(item_id, command["tx_id"])

    return reply