import os
import json
import threading
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
STOCK_COMMANDS_TOPIC = os.environ["KAFKA_STOCK_COMMANDS_TOPIC"]
STOCK_REPLIES_TOPIC = os.environ["KAFKA_STOCK_REPLIES_TOPIC"]


class StockKafkaInfrastructure:

    def __init__(self, dispatcher):
        self.dispatcher = dispatcher

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self.consumer = KafkaConsumer(
            STOCK_COMMANDS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="stock-service-group",
        )

    def start(self):
        thread = threading.Thread(target=self._consume_loop, daemon=True)
        thread.start()

    def _consume_loop(self):
        print("Stock consumer started...")
        for message in self.consumer:
            command = message.value
            try:
                reply = self.dispatcher(command)
                self.producer.send(STOCK_REPLIES_TOPIC, reply)
                self.producer.flush()
            except Exception as e:
                print(f"Error processing command: {e}")