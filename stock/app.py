from flask import Flask, jsonify
import redis
import os
import uuid

from kafka_infra import StockKafkaInfrastructure
from saga_dispatcher import stock_dispatcher, set_redis_client

app = Flask(__name__)

redis_client = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
    decode_responses=True
)

set_redis_client(redis_client)

kafka_infra = StockKafkaInfrastructure(dispatcher=stock_dispatcher)

kafka_infra.start()
@app.route("/stock/item/create/<price>", methods=["POST"])
def create_item(price):
    item_id = str(uuid.uuid4())
    redis_client.hset(f"item:{item_id}", mapping={
        "stock": 0,
        "price": price
    })
    return jsonify({"item_id": item_id}), 200


@app.route("/stock/find/<item_id>", methods=["GET"])
def find_item(item_id):
    item = redis_client.hgetall(f"item:{item_id}")
    if not item:
        return jsonify({"error": "Item not found"}), 400

    return jsonify({
        "stock": int(item["stock"]),
        "price": float(item["price"])
    }), 200


@app.route("/stock/add/<item_id>/<amount>", methods=["POST"])
def add_stock(item_id, amount):
    key = f"item:{item_id}"
    if not redis_client.exists(key):
        return jsonify({"error": "Item not found"}), 400

    redis_client.hincrby(key, "stock", int(amount))
    return jsonify({"done": True}), 200


@app.route("/stock/subtract/<item_id>/<amount>", methods=["POST"])
def subtract_stock(item_id, amount):
    key = f"item:{item_id}"

    if not redis_client.exists(key):
        return jsonify({"error": "Item not found"}), 400

    current_stock = int(redis_client.hget(key, "stock"))

    if current_stock < int(amount):
        return jsonify({"error": "Insufficient stock"}), 400

    redis_client.hincrby(key, "stock", -int(amount))
    return jsonify({"done": True}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)