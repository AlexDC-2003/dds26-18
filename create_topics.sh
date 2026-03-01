#!/bin/bash

topics=(
  "order"
  "payment.commands"
  "payment.replies"
  "stock.replies"
  "stock.commands"
)

for topic in "${topics[@]}"; do
  echo "🚀 Creating Kafka topic: $topic"
  kafka-topics --create \
    --if-not-exists \
    --topic "$topic" \
    --bootstrap-server kafka:9092 \
    --partitions 6 \
    --replication-factor 1
done