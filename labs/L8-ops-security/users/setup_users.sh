#!/usr/bin/env bash
# Crée les 2 utilisateurs SCRAM 'producer-app' et 'consumer-app' via kafka-configs.
# 'admin' est déjà créé au bootstrap (--add-scram dans kafka-storage format).
# Idempotent : --alter --add-config remplace si le user existe déjà.
set -euo pipefail

BROKER_INTERNAL="kafka-sec-1:29092"
SEC_COMPOSE="${SEC_COMPOSE:-docker compose -f docker-compose.security.yml}"

run_in_broker() {
  ${SEC_COMPOSE} exec -T -e KAFKA_OPTS= kafka-sec-1 "$@"
}

echo "[users] création de producer-app..."
run_in_broker kafka-configs \
  --bootstrap-server "$BROKER_INTERNAL" \
  --alter \
  --add-config 'SCRAM-SHA-256=[iterations=8192,password=producer-secret]' \
  --entity-type users \
  --entity-name producer-app

echo "[users] création de consumer-app..."
run_in_broker kafka-configs \
  --bootstrap-server "$BROKER_INTERNAL" \
  --alter \
  --add-config 'SCRAM-SHA-256=[iterations=8192,password=consumer-secret]' \
  --entity-type users \
  --entity-name consumer-app

echo "[users] users existants :"
run_in_broker kafka-configs \
  --bootstrap-server "$BROKER_INTERNAL" \
  --describe \
  --entity-type users
