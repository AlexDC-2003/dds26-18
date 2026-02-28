import json
import redis

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
    log_key = f"saga:msg:{msg_id}"

    # -------------------------
    # IDEMPOTENCY CHECK
    # -------------------------
    existing = redis_client.get(log_key)
    if existing:
        return json.loads(existing)

    msg_type = command["type"]

    if msg_type == "reserve_stock":
        reply = handle_reserve_stock(command)

    elif msg_type == "release_stock":
        reply = handle_release_stock(command)

    else:
        reply = build_error(command, f"Unknown command type: {msg_type}")

    # -------------------------
    # STORE RESULT (durable log)
    # -------------------------
    redis_client.set(log_key, json.dumps(reply))

    return reply


# ---------------------------------------------------------
# SAGA HANDLERS
# ---------------------------------------------------------

def handle_reserve_stock(command):
    item_id = command["payload"].get("item_id")
    quantity = int(command["payload"].get("quantity", 0))

    if not item_id or quantity <= 0:
        return build_error(command, "Invalid payload")

    key = f"item:{item_id}"

    if not redis_client.exists(key):
        return build_error(command, "Item not found")

    with redis_client.pipeline() as pipe:
        while True:
            try:
                pipe.watch(key)
                current_stock = int(pipe.hget(key, "stock"))

                if current_stock < quantity:
                    pipe.unwatch()
                    return build_error(command, "Insufficient stock")

                pipe.multi()
                pipe.hincrby(key, "stock", -quantity)
                pipe.execute()
                break

            except redis.WatchError:
                continue

    return build_success(command, {
        "item_id": item_id,
        "reserved": quantity
    })


def handle_release_stock(command):
    item_id = command["payload"].get("item_id")
    quantity = int(command["payload"].get("quantity", 0))

    if not item_id or quantity <= 0:
        return build_error(command, "Invalid payload")

    key = f"item:{item_id}"

    if not redis_client.exists(key):
        return build_error(command, "Item not found")

    redis_client.hincrby(key, "stock", quantity)

    return build_success(command, {
        "item_id": item_id,
        "released": quantity
    })