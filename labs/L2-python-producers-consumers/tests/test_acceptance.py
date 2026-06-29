"""Test d'acceptation du lab L2 — Producers & Consumers Python avec Avro.

Ce test NOTE le travail de l'étudiant en vérifiant l'ÉTAT FINAL du cluster
(boîte noire) : il NE contient PAS la solution et NE produit rien lui-même.
L'étudiant exécute d'abord son lab (producers + création de topics + schéma),
PUIS on lance ce test qui se connecte à Kafka et au Schema Registry pour
valider des faits observables.

Prérequis avant de lancer ce test :
  1. La stack Docker est démarrée (`docker compose ps` → brokers + schema-registry UP).
  2. Le lab a été exécuté :
       - topic `orders.json` alimenté (`python producer_simple.py`)
       - topic `orders.avro` créé + alimenté (`python producer_avro.py`)
       - schéma `orders.avro-value` enregistré au Schema Registry.

Lancer :
    pip install confluent-kafka requests pytest
    pytest tests/test_acceptance.py -v

Configuration via variables d'environnement (mêmes défauts que le lab) :
    BOOTSTRAP_SERVERS    (défaut : localhost:9092,localhost:9093,localhost:9094)
    SCHEMA_REGISTRY_URL  (défaut : http://localhost:8081)

Si le cluster est injoignable, les tests sont SKIPPÉS (pas en échec) avec un
message clair indiquant de démarrer la stack.
"""

from __future__ import annotations

import json
import os
import struct
from typing import Any

import pytest
import requests
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

# --------------------------------------------------------------------------- #
# Configuration (env + défauts du lab)
# --------------------------------------------------------------------------- #
BOOTSTRAP_SERVERS = os.environ.get(
    "BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
)
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://localhost:8081"
).rstrip("/")

# Outcomes attendus du lab L2
TOPIC_JSON = "orders.json"
TOPIC_AVRO = "orders.avro"
SUBJECT_AVRO = "orders.avro-value"
EXPECTED_RECORD_NAME = "Order"
EXPECTED_NAMESPACE = "fr.formation.kafka.orders"
EXPECTED_FIELDS = {"id", "customer_id", "total", "currency", "created_at"}

ADMIN_TIMEOUT = 10.0
CONSUME_TIMEOUT = 20.0
CONFLUENT_MAGIC_BYTE = 0x00

pytestmark = pytest.mark.acceptance


# --------------------------------------------------------------------------- #
# Helpers de connexion + skip propre si cluster injoignable
# --------------------------------------------------------------------------- #
def _admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def _cluster_or_skip() -> dict[str, Any]:
    """Retourne la metadata du cluster, ou skip si Kafka injoignable."""
    try:
        admin = _admin_client()
        md = admin.list_topics(timeout=ADMIN_TIMEOUT)
    except (KafkaException, Exception) as exc:  # noqa: BLE001
        pytest.skip(
            "Cluster Kafka injoignable sur "
            f"BOOTSTRAP_SERVERS={BOOTSTRAP_SERVERS} ({exc}). "
            "Démarre la stack (`docker compose up -d`) puis relance le lab "
            "avant de noter."
        )
    if not md.brokers:
        pytest.skip(
            f"Aucun broker visible sur BOOTSTRAP_SERVERS={BOOTSTRAP_SERVERS}. "
            "Vérifie `docker compose ps`."
        )
    return md.topics


def _registry_get(path: str) -> requests.Response:
    """GET sur le Schema Registry, ou skip si injoignable."""
    url = f"{SCHEMA_REGISTRY_URL}{path}"
    try:
        return requests.get(url, timeout=ADMIN_TIMEOUT)
    except requests.RequestException as exc:
        pytest.skip(
            f"Schema Registry injoignable sur {SCHEMA_REGISTRY_URL} ({exc}). "
            "Vérifie le conteneur schema-registry (`docker compose ps`)."
        )


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def topics() -> dict[str, Any]:
    """Metadata des topics du cluster (skip si Kafka down)."""
    return _cluster_or_skip()


@pytest.fixture(scope="module")
def avro_schema() -> dict[str, Any]:
    """Schéma Avro enregistré pour orders.avro-value (skip / fail clair)."""
    resp = _registry_get(f"/subjects/{SUBJECT_AVRO}/versions/latest")
    assert resp.status_code == 200, (
        f"Le sujet '{SUBJECT_AVRO}' n'est pas enregistré au Schema Registry "
        f"(HTTP {resp.status_code}). Lance `python producer_avro.py` : au "
        "premier message, l'AvroSerializer doit enregistrer le schéma "
        "automatiquement. Vérifie aussi que le producer pointe bien sur "
        f"SCHEMA_REGISTRY_URL={SCHEMA_REGISTRY_URL}."
    )
    payload = resp.json()
    schema_str = payload.get("schema")
    assert schema_str, (
        f"La réponse du Schema Registry pour '{SUBJECT_AVRO}' ne contient pas "
        f"de champ 'schema'. Réponse reçue : {payload!r}."
    )
    return json.loads(schema_str)


# --------------------------------------------------------------------------- #
# Tests d'acceptation
# --------------------------------------------------------------------------- #
def test_topic_orders_json_exists(topics: dict[str, Any]) -> None:
    assert TOPIC_JSON in topics, (
        f"Le topic '{TOPIC_JSON}' n'existe pas. Étape 2 du lab : lance "
        "`python producer_simple.py` pour produire la baseline JSON "
        "(le topic est créé à la volée)."
    )


def test_topic_orders_json_has_messages(topics: dict[str, Any]) -> None:
    assert TOPIC_JSON in topics, (
        f"Le topic '{TOPIC_JSON}' n'existe pas — lance d'abord "
        "`python producer_simple.py`."
    )
    total = _count_messages(TOPIC_JSON)
    assert total > 0, (
        f"Le topic '{TOPIC_JSON}' est vide (0 message). Étape 2 : "
        "`python producer_simple.py` doit avoir publié des ordres JSON."
    )


def test_topic_orders_avro_exists(topics: dict[str, Any]) -> None:
    assert TOPIC_AVRO in topics, (
        f"Le topic '{TOPIC_AVRO}' n'existe pas. Étape 1 : crée-le "
        "(`kafka-topics --create --topic orders.avro --partitions 3 "
        "--replication-factor 3`), puis Étape 3 : `python producer_avro.py`."
    )


def test_topic_orders_avro_has_three_partitions(topics: dict[str, Any]) -> None:
    assert TOPIC_AVRO in topics, (
        f"Le topic '{TOPIC_AVRO}' n'existe pas — voir Étape 1 du lab."
    )
    nb_partitions = len(topics[TOPIC_AVRO].partitions)
    assert nb_partitions == 3, (
        f"Le topic '{TOPIC_AVRO}' a {nb_partitions} partition(s), attendu 3. "
        "Recrée-le avec `--partitions 3` (Étape 1 du lab)."
    )


def test_topic_orders_avro_has_messages(topics: dict[str, Any]) -> None:
    assert TOPIC_AVRO in topics, (
        f"Le topic '{TOPIC_AVRO}' n'existe pas — voir Étape 1/3 du lab."
    )
    total = _count_messages(TOPIC_AVRO)
    assert total > 0, (
        f"Le topic '{TOPIC_AVRO}' est vide (0 message). Étape 3 : "
        "`python producer_avro.py` doit avoir publié des ordres Avro."
    )


def test_subject_avro_value_registered(avro_schema: dict[str, Any]) -> None:
    # La fixture a déjà skippé/échoué si le sujet n'existe pas.
    assert isinstance(avro_schema, dict), (
        f"Le schéma de '{SUBJECT_AVRO}' n'est pas un objet JSON Avro valide. "
        "Vérifie que producer_avro.py utilise bien l'AvroSerializer avec "
        "schemas/order_v1.avsc."
    )


def test_avro_schema_is_valid_order_record(avro_schema: dict[str, Any]) -> None:
    assert avro_schema.get("type") == "record", (
        f"Le schéma '{SUBJECT_AVRO}' devrait être de type 'record', trouvé "
        f"{avro_schema.get('type')!r}. Utilise schemas/order_v1.avsc."
    )
    assert avro_schema.get("name") == EXPECTED_RECORD_NAME, (
        f"Le record Avro devrait s'appeler '{EXPECTED_RECORD_NAME}', trouvé "
        f"{avro_schema.get('name')!r} (voir schemas/order_v1.avsc)."
    )
    assert avro_schema.get("namespace") == EXPECTED_NAMESPACE, (
        f"Le namespace devrait être '{EXPECTED_NAMESPACE}', trouvé "
        f"{avro_schema.get('namespace')!r} (voir schemas/order_v1.avsc)."
    )
    field_names = {f.get("name") for f in avro_schema.get("fields", [])}
    missing = EXPECTED_FIELDS - field_names
    assert not missing, (
        f"Le schéma Avro '{SUBJECT_AVRO}' ne contient pas les champs attendus. "
        f"Manquant(s) : {sorted(missing)}. Champs trouvés : {sorted(field_names)}. "
        "Le schéma enregistré doit correspondre à schemas/order_v1.avsc."
    )


def test_can_consume_one_deserializable_avro_message(
    topics: dict[str, Any], avro_schema: dict[str, Any]
) -> None:
    """Vérifie qu'au moins 1 message Avro est consommable et désérialisable.

    On valide le format wire Confluent (magic byte 0x00 + schema id sur 4
    octets) et on désérialise réellement le payload binaire avec fastavro,
    en récupérant le schéma par id depuis le Schema Registry. Aucune logique
    de production n'est dupliquée ici : on lit l'état du topic.
    """
    assert TOPIC_AVRO in topics, (
        f"Le topic '{TOPIC_AVRO}' n'existe pas — voir Étape 1/3 du lab."
    )

    raw = _consume_first_message(TOPIC_AVRO)
    assert raw is not None, (
        f"Aucun message lisible sur '{TOPIC_AVRO}' en {CONSUME_TIMEOUT:.0f}s. "
        "Étape 3 : `python producer_avro.py` doit avoir publié des messages."
    )

    assert len(raw) >= 5, (
        f"Le message sur '{TOPIC_AVRO}' fait {len(raw)} octet(s) : trop court "
        "pour le format wire Confluent (magic byte + schema id sur 4 octets + "
        "payload). Le producer utilise-t-il bien l'AvroSerializer ?"
    )
    assert raw[0] == CONFLUENT_MAGIC_BYTE, (
        f"Premier octet du message = {raw[0]:#04x}, attendu "
        f"{CONFLUENT_MAGIC_BYTE:#04x} (magic byte Confluent). Le message n'a "
        "pas été produit avec l'AvroSerializer du Schema Registry."
    )

    schema_id = struct.unpack(">I", raw[1:5])[0]
    order = _deserialize_confluent_avro(raw, schema_id)

    assert isinstance(order, dict), (
        "Le message Avro n'a pas pu être désérialisé en dict. Le payload "
        "binaire ne correspond pas au schéma enregistré."
    )
    missing = EXPECTED_FIELDS - set(order.keys())
    assert not missing, (
        f"L'ordre désérialisé ne contient pas tous les champs attendus. "
        f"Manquant(s) : {sorted(missing)}. Ordre lu : {order!r}."
    )


# --------------------------------------------------------------------------- #
# Lecture d'état bas niveau (offsets + consume)
# --------------------------------------------------------------------------- #
def _count_messages(topic: str) -> int:
    """Compte les messages d'un topic via (high - low) watermarks sur toutes
    les partitions. Lecture d'état pure, ne consomme rien."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "l2-acceptance-count",
            "enable.auto.commit": False,
        }
    )
    try:
        md = consumer.list_topics(topic, timeout=ADMIN_TIMEOUT)
        tmd = md.topics.get(topic)
        if tmd is None or tmd.error is not None:
            return 0
        total = 0
        for pid in tmd.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, pid), timeout=ADMIN_TIMEOUT
            )
            total += max(0, high - low)
        return total
    finally:
        consumer.close()


def _consume_first_message(topic: str) -> bytes | None:
    """Consomme depuis le début et retourne la value brute du 1er message."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "l2-acceptance-consume",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.subscribe([topic])
        import time

        deadline = time.monotonic() + CONSUME_TIMEOUT
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            value = msg.value()
            if value is not None:
                return bytes(value)
        return None
    finally:
        consumer.close()


def _deserialize_confluent_avro(raw: bytes, schema_id: int) -> dict[str, Any]:
    """Désérialise un payload au format wire Confluent en récupérant le
    schéma writer par id auprès du Schema Registry, puis en décodant avec
    fastavro. Indépendant du code de l'étudiant."""
    resp = _registry_get(f"/schemas/ids/{schema_id}")
    assert resp.status_code == 200, (
        f"Impossible de récupérer le schéma id={schema_id} au Schema Registry "
        f"(HTTP {resp.status_code}). Le message référence un schéma inconnu."
    )
    writer_schema = json.loads(resp.json()["schema"])

    import io

    import fastavro

    payload = io.BytesIO(raw[5:])
    return fastavro.schemaless_reader(payload, writer_schema)
