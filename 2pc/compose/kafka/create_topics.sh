#!/bin/bash

topics=(
  "order"
  "stock.commands"
  "stock.replies"
  "payment.commands"
  "payment.replies"
  "order.events"
)

for topic in "${topics[@]}"; do
  echo "🚀 Creating Kafka topic: $topic"
  kafka-topics --create \
    --if-not-exists  \
    --topic "$topic" \
    --bootstrap-server kafka:9092 \
    --partitions 3 \
    --replication-factor 1
done