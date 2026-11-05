#!/bin/bash

topics=(
  "order"
  "stock.commands"
  "stock.replies"
  "payment.commands"
  "payment.replies"
  "order.events"
  "orchestrator.commands"
  "orchestrator.replies"
)

for topic in "${topics[@]}"; do
  echo "Creating Kafka topic: $topic (15 partitions)"
  kafka-topics --create \
    --if-not-exists  \
    --topic "$topic" \
    --bootstrap-server kafka:9092 \
    --partitions 15 \
    --replication-factor 1
done
