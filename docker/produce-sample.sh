#!/bin/bash
# ═══════════════════════════════════════════════════════════
# Mako — Produce sample events to Kafka
# ═══════════════════════════════════════════════════════════
#
# Usage: ./produce-sample.sh [topic] [count]
# Defaults: topic=events.orders, count=10

set -euo pipefail

KAFKA_CONTAINER="mako-kafka"
BROKER="localhost:29092"
TOPIC="${1:-events.orders}"
COUNT="${2:-10}"

echo "Producing $COUNT sample events to $TOPIC..."
echo ""

for i in $(seq 1 "$COUNT"); do
    TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    AMOUNT=$(awk "BEGIN{printf \"%.2f\", 10 + rand() * 490}")
    STATUS="completed"
    if [ $((i % 5)) -eq 0 ]; then STATUS="test"; fi
    ENV="production"
    if [ $((i % 4)) -eq 0 ]; then ENV="staging"; fi
    RISK=$(awk "BEGIN{printf \"%.2f\", rand()}")

    EVENT=$(cat <<EOF
{"event_id":"evt-$(printf '%04d' $i)","event_type":"order.created","email":"user${i}@example.com","phone":"+1555$(printf '%07d' $i)","credit_card_number":"4111$(printf '%012d' $i)","amount":${AMOUNT},"risk_score":${RISK},"status":"${STATUS}","customer_id":"cust-$(printf '%03d' $i)","environment":"${ENV}","timestamp":"${TS}"}
EOF
    )

    echo "$EVENT" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
        --bootstrap-server "$BROKER" \
        --topic "$TOPIC" \
        2>/dev/null

    echo "  [$i/$COUNT] evt-$(printf '%04d' $i) | \$$AMOUNT | $STATUS | $ENV"
done

echo ""
echo "Done. Consume with:"
echo "  docker exec $KAFKA_CONTAINER kafka-console-consumer \\"
echo "    --bootstrap-server $BROKER --topic $TOPIC --from-beginning --max-messages $COUNT"
