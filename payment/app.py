import atexit
import logging
import os
import uuid

import redis
from flask import Flask, Response, abort, jsonify
from msgspec import Struct, msgpack

from kafka_worker import PaymentKafkaWorker

DB_ERROR_STR = "DB error"

app = Flask("payment-service")


db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


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


def _update_credit_atomic(user_id: str, delta: int) -> int:
    """Atomically apply delta to user credit using WATCH/MULTI/EXEC.

    - delta > 0: add funds
    - delta < 0: subtract funds (fails if it would go below 0)

    Returns new credit.
    """
    for _ in range(10):
        pipe = db.pipeline()
        try:
            pipe.watch(user_id)
            raw = pipe.get(user_id)
            if not raw:
                pipe.unwatch()
                abort(400, f"User: {user_id} not found!")

            user = msgpack.decode(raw, type=UserValue)
            new_credit = user.credit + delta
            if new_credit < 0:
                pipe.unwatch()
                abort(400, f"User: {user_id} credit cannot get reduced below zero!")

            user.credit = new_credit

            pipe.multi()
            pipe.set(user_id, msgpack.encode(user))
            pipe.execute()
            return new_credit

        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
        finally:
            try:
                pipe.reset()
            except Exception:
                pass

    abort(400, "Concurrent update; retry")


# ---- External REST API (must remain unchanged) ----


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
    new_credit = _update_credit_atomic(user_id, int(amount))
    return Response(f"User: {user_id} credit updated to: {new_credit}", status=200)


@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    new_credit = _update_credit_atomic(user_id, -int(amount))
    return Response(f"User: {user_id} credit updated to: {new_credit}", status=200)


# ---- Internal Kafka (event-driven) ----

INTERNAL_TRANSPORT = os.environ.get("INTERNAL_TRANSPORT", "kafka")

kafka_worker: PaymentKafkaWorker | None = None

print("Payment service INTERNAL_TRANSPORT =", INTERNAL_TRANSPORT, flush=True)
if INTERNAL_TRANSPORT == "kafka":
    print("Matei" , flush=True)
    kafka_worker = PaymentKafkaWorker(db=db)
    kafka_worker.start()
    atexit.register(kafka_worker.stop)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)