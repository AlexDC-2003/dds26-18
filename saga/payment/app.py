import atexit
import logging
import os
import time
import uuid
import redis

from flask import Flask, Response, abort, jsonify
from msgspec import Struct, msgpack
from kafka_worker import PaymentKafkaWorker
from lock_manager import LockManager, WaitDieAbort, transaction_context

DB_ERROR_STR = "DB error"
app = Flask("payment-service")
db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)
lock_manager = LockManager(db=db)

def close_db_connection() -> None:
    db.close()
atexit.register(close_db_connection)

class UserValue(Struct):
    credit: int

def get_user_from_db(user_id: str) -> UserValue:
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    user: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if user is None:
        abort(400, f"User: {user_id} not found!")
    return user


def _update_credit_2pl(user_id: str, delta: int) -> int:
    """Apply delta to user credit under a 2PL exclusive lock.
    Each REST call is its own short-lived transaction that will wait up to
    WAIT_TIMEOUT_SECONDS for the lock, then die (retry at the HTTP level).
    Returns the new credit balance.
    Aborts on lock conflict, DB error, or insufficient funds.
    """
    resources = [f"user:{user_id}"]
    try:
        with transaction_context(lock_manager, resources) as txn:
            #Growing phase complete
            try:
                raw = db.get(user_id)
            except redis.exceptions.RedisError:
                abort(400, DB_ERROR_STR)
            if not raw:
                abort(400, f"User: {user_id} not found!")
            user = msgpack.decode(raw, type=UserValue)
            new_credit = user.credit + delta
            if new_credit < 0:
                abort(400, f"User: {user_id} credit cannot get reduced below zero!")
            user.credit = new_credit

            try:
                db.set(user_id, msgpack.encode(user))
            except redis.exceptions.RedisError:
                abort(400, DB_ERROR_STR)
            return new_credit

    except WaitDieAbort as e:
        abort(409, f"Transaction aborted (wait-then-die): {e}")


# ---- External REST API ----

@app.post("/create_user")
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return jsonify({"user_id": key})

@app.post("/batch_init/<n>/<starting_money>")
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(UserValue(credit=starting_money)) for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})

@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify({"user_id": user_id, "credit": user_entry.credit})

@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    new_credit = _update_credit_2pl(user_id, int(amount))
    return Response(f"User: {user_id} credit updated to: {new_credit}", status=200)

@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    new_credit = _update_credit_2pl(user_id, -int(amount))
    return Response(f"User: {user_id} credit updated to: {new_credit}", status=200)


# ---- Internal Kafka (event-driven) ----

INTERNAL_TRANSPORT = os.environ.get("INTERNAL_TRANSPORT", "kafka")
kafka_worker: PaymentKafkaWorker | None = None
print("Payment service INTERNAL_TRANSPORT =", INTERNAL_TRANSPORT, flush=True)
if INTERNAL_TRANSPORT == "kafka":
    kafka_worker = PaymentKafkaWorker(db=db)
    kafka_worker.start()
    atexit.register(kafka_worker.stop)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    logging.getLogger("kafka_worker").handlers = gunicorn_logger.handlers
    logging.getLogger("kafka_worker").setLevel(logging.INFO)