#!/bin/bash

topics=(
  "order"
  "stock.commands"
  "stock.replies"
  "payment.commands"
  "payment.replies"
  "order.events"
  "checkout.commands"
  "checkout.replies"
)

for topic in "${topics[@]}"; do
  echo "Creating Kafka topic: $topic (20 partitions)"
  kafka-topics --create \
    --if-not-exists  \
    --topic "$topic" \
    --bootstrap-server kafka:9092 \
    --partitions 20 \
    --replication-factor 1
done