#!/bin/bash
# ═══════════════════════════════════════════════════════════
# Mako — Create Kafka topics for examples
# ═══════════════════════════════════════════════════════════
#
# Usage: ./create-topics.sh
# Run this after `docker compose up -d` and Kafka is healthy.

set -euo pipefail

KAFKA_CONTAINER="mako-kafka"
BROKER="localhost:29092"

topics=(
    "events.orders:6:1"
    "events.orders.dlq:3:1"
    "events.payments:6:1"
    "events.payments.dlq:3:1"
    "events.orders.enriched:6:1"
    "events.my-events:6:1"
    "events.my-events.dlq:3:1"
)

echo "Creating Kafka topics..."
echo ""

for entry in "${topics[@]}"; do
    IFS=":" read -r topic partitions replication <<< "$entry"
    echo -n "  $topic (${partitions}p, rf=${replication})... "
    docker exec "$KAFKA_CONTAINER" kafka-topics \
        --bootstrap-server "$BROKER" \
        --create \
        --if-not-exists \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor "$replication" \
        2>/dev/null && echo "OK" || echo "ALREADY EXISTS"
done

echo ""
echo "Listing topics:"
docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$BROKER" \
    --list
