#!/usr/bin/env bash
# Provoque un incident sur le cluster L1 plaintext :
#   - démarre un trafic de production en arrière-plan (kafka-producer-perf-test)
#   - stoppe kafka2 pour observer l'effet sur les métriques (URP, ISR shrinks).
#
# Cible : cluster L1 (kafka1/2/3 sur 9092/3/4). N'affecte PAS la stack sécurisée.
set -euo pipefail

ACTION="${1:-break}"           # break | restore | traffic-on | traffic-off
TOPIC="events.demo"
THROUGHPUT=1000                # messages/s
RECORD_SIZE=512
TRAFFIC_PIDFILE="/tmp/lab-l8-traffic.pid"

ensure_topic() {
  if ! docker exec -e KAFKA_OPTS= kafka1 kafka-topics --bootstrap-server kafka1:29092 --list | grep -q "^${TOPIC}$"; then
    echo "[break_broker] création de ${TOPIC} (3p, RF=3)..."
    docker exec -e KAFKA_OPTS= kafka1 kafka-topics \
      --bootstrap-server kafka1:29092 \
      --create --if-not-exists \
      --topic "$TOPIC" --partitions 3 --replication-factor 3 \
      --config min.insync.replicas=2
  fi
}

start_traffic() {
  ensure_topic
  if [[ -f "$TRAFFIC_PIDFILE" ]] && kill -0 "$(cat "$TRAFFIC_PIDFILE")" 2>/dev/null; then
    echo "[break_broker] trafic déjà actif (PID $(cat "$TRAFFIC_PIDFILE"))."
    return
  fi
  echo "[break_broker] démarrage du trafic (${THROUGHPUT} msg/s sur ${TOPIC})..."
  ( docker exec -e KAFKA_OPTS= kafka1 kafka-producer-perf-test \
      --topic "$TOPIC" \
      --num-records 100000000 \
      --record-size "$RECORD_SIZE" \
      --throughput "$THROUGHPUT" \
      --producer-props bootstrap.servers=kafka1:29092 acks=all >/dev/null 2>&1 ) &
  echo $! > "$TRAFFIC_PIDFILE"
  echo "[break_broker] trafic PID $(cat "$TRAFFIC_PIDFILE")."
}

stop_traffic() {
  if [[ -f "$TRAFFIC_PIDFILE" ]]; then
    PID=$(cat "$TRAFFIC_PIDFILE")
    kill "$PID" 2>/dev/null || true
    rm -f "$TRAFFIC_PIDFILE"
  fi
  # Kill côté conteneur aussi
  docker exec kafka1 pkill -f kafka-producer-perf-test 2>/dev/null || true
  echo "[break_broker] trafic arrêté."
}

break_broker() {
  echo "[break_broker] arrêt de kafka2..."
  docker stop kafka2
  echo "[break_broker] OK : observe Grafana 'Kafka Learning Dashboard' puis 'Kafka Cluster'."
}

restore_broker() {
  echo "[break_broker] redémarrage de kafka2..."
  docker start kafka2
  echo "[break_broker] OK : ISR doit se reconstruire en ~30s."
}

case "$ACTION" in
  traffic-on)   start_traffic ;;
  traffic-off)  stop_traffic ;;
  break)        break_broker ;;
  restore)      restore_broker ;;
  *)
    echo "Usage: $0 {traffic-on|traffic-off|break|restore}"
    exit 1
    ;;
esac
