#!/usr/bin/env bash
# Crée le topic 'orders' et applique les ACLs producer-app / consumer-app.
# Modèle :
#   producer-app  : WRITE + DESCRIBE sur orders, IDEMPOTENT_WRITE cluster
#   consumer-app  : READ + DESCRIBE sur orders, READ sur les groupes 'analytics-*'
set -euo pipefail

BROKER_INTERNAL="kafka-sec-1:29092"
SEC_COMPOSE="${SEC_COMPOSE:-docker compose -f docker-compose.security.yml}"
TOPIC="orders"

run_in_broker() {
  ${SEC_COMPOSE} exec -T -e KAFKA_OPTS= kafka-sec-1 "$@"
}

echo "[acls] création du topic ${TOPIC} (3p, RF=3)..."
run_in_broker kafka-topics \
  --bootstrap-server "$BROKER_INTERNAL" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2

echo "[acls] producer-app : WRITE + DESCRIBE sur $TOPIC"
run_in_broker kafka-acls \
  --bootstrap-server "$BROKER_INTERNAL" \
  --add \
  --allow-principal User:producer-app \
  --operation Write --operation Describe \
  --topic "$TOPIC"

echo "[acls] producer-app : IDEMPOTENT_WRITE cluster"
run_in_broker kafka-acls \
  --bootstrap-server "$BROKER_INTERNAL" \
  --add \
  --allow-principal User:producer-app \
  --operation IdempotentWrite \
  --cluster

echo "[acls] consumer-app : READ + DESCRIBE sur $TOPIC"
run_in_broker kafka-acls \
  --bootstrap-server "$BROKER_INTERNAL" \
  --add \
  --allow-principal User:consumer-app \
  --operation Read --operation Describe \
  --topic "$TOPIC"

echo "[acls] consumer-app : READ sur les groupes prefixed 'analytics-'"
run_in_broker kafka-acls \
  --bootstrap-server "$BROKER_INTERNAL" \
  --add \
  --allow-principal User:consumer-app \
  --operation Read \
  --group "analytics-" \
  --resource-pattern-type prefixed

echo ""
echo "[acls] ACLs en place :"
run_in_broker kafka-acls \
  --bootstrap-server "$BROKER_INTERNAL" \
  --list
