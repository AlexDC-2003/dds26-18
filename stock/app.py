from flask import Flask, abort, jsonify
from msgspec import Struct, msgpack
import logging
import redis
import os
import uuid

from kafka_infra import StockKafkaInfrastructure
from saga_dispatcher import stock_dispatcher, set_redis_client
import lock_manager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

DB_ERROR_STR = "DB error"

app = Flask("stock-service")

redis_client = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
    decode_responses=True
)

class StockValue(Struct):
    stock: int
    price: int

set_redis_client(redis_client)
lock_manager.set_redis_client(redis_client)  # shared redis into lock manager

kafka_infra = StockKafkaInfrastructure(dispatcher=stock_dispatcher)

kafka_infra.start()
@app.route("/item/create/<price>", methods=["POST"])
def create_item(price):
    item_id = str(uuid.uuid4())
    redis_client.hset(f"item:{item_id}", mapping={
        "stock": 0,
        "price": price
    })
    return jsonify({"item_id": item_id}), 200


@app.route("/find/<item_id>", methods=["GET"])
def find_item(item_id):
    item = redis_client.hgetall(f"item:{item_id}")
    if not item:
        return jsonify({"error": "Item not found"}), 400

    return jsonify({
        "stock": int(item["stock"]),
        "price": float(item["price"])
    }), 200


@app.route("/add/<item_id>/<amount>", methods=["POST"])
def add_stock(item_id, amount):
    key = f"item:{item_id}"
    if not redis_client.exists(key):
        return jsonify({"error": "Item not found"}), 400

    redis_client.hincrby(key, "stock", int(amount))
    return jsonify({"done": True}), 200


@app.route("/subtract/<item_id>/<amount>", methods=["POST"])
def subtract_stock(item_id, amount):
    key = f"item:{item_id}"

    if not redis_client.exists(key):
        return jsonify({"error": "Item not found"}), 400

    with redis_client.pipeline() as pipe:
        while True:
            try:
                pipe.watch(key)
                current_stock = int(pipe.hget(key, "stock"))
                if current_stock < int(amount):
                    pipe.unwatch()
                    return jsonify({"error": "Insufficient stock"}), 400
                pipe.multi()
                pipe.hincrby(key, "stock", -int(amount))
                pipe.execute()
                break
            except redis.WatchError:
                continue
    return jsonify({"done": True}), 200


@app.route("/batch_init/<n>/<starting_stock>/<item_price>", methods=["POST"])
def batch_init_items(n, starting_stock, item_price):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)

    pipeline = redis_client.pipeline()
    for i in range(n):
        item_id = i
        pipeline.hset(f"item:{item_id}", mapping={
            "stock": starting_stock,
            "price": item_price
        })
    pipeline.execute()

    return jsonify({"msg": "Batch init for stock successful"}), 200

#Remove before final release
@app.route("/find", methods=["GET"])
def find_all_items():
    keys = redis_client.keys("item:*")
    if not keys:
        return jsonify([]), 200

    items = {}
    for key in keys:
        item = redis_client.hgetall(key)
        item_id = key.split(":", 1)[1]
        items[item_id] = {
            "stock": int(item["stock"]),
            "price": float(item["price"])
        }

    return jsonify(items), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)