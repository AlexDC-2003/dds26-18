#!/bin/bash

topics=(
  "order"
  "stock"
  "payment"
)

for topic in "${topics[@]}"; do
  echo "🚀 Creating Kafka topic: $topic"
  docker compose exec kafka /bin/kafka-topics --create \
    --topic "$topic" \
    --bootstrap-server kafka:9092 \
    --partitions 6 \
    --replication-factor 1
done