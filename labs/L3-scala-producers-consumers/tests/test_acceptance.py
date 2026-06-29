"""
Test d'acceptation — Lab L3 (Scala producers/consumers).

Test BOITE NOIRE : il ne compile NI n'exécute le code Scala. Il vérifie
uniquement l'ETAT FINAL observable côté Kafka / Schema Registry une fois que
l'étudiant a lancé ses applications Scala.

PREREQUIS (à faire AVANT de lancer ce test) :
  1. La stack Docker est UP (3 brokers + Schema Registry).
       docker compose ps        # tout doit être "Up"/"healthy"
  2. Les topics sont créés (cf. lab.md étape 1) :
       orders.scala         (3 partitions, RF=3)
       orders.scala.avro    (3 partitions, RF=3)
  3. L'étudiant a lancé ses applications Scala depuis le répertoire du lab :
       sbt "runMain lab.ProducerSimple"   # 20 messages String -> orders.scala
       sbt "runMain lab.ProducerAvro"     # 20 messages Avro   -> orders.scala.avro
                                          #   + enregistre le schéma au Registry

COMMENT LANCER :
  pip install pytest confluent-kafka requests
  pytest -m acceptance labs/L3-scala-producers-consumers/tests/test_acceptance.py -v

CONFIGURATION (variables d'environnement, valeurs par défaut entre []) :
  BOOTSTRAP_SERVERS    [localhost:9092,localhost:9093,localhost:9094]
  SCHEMA_REGISTRY_URL  [http://localhost:8081]

Si le cluster Kafka est injoignable, les tests sont SKIPPÉS proprement
(et non échoués) — relancer la stack puis ré-exécuter.
"""

import io
import os
import struct

import pytest
import requests

try:
    from confluent_kafka import Consumer, KafkaException, TopicPartition
    from confluent_kafka.admin import AdminClient
except ImportError:  # pragma: no cover
    pytest.skip(
        "confluent-kafka non installé : `pip install confluent-kafka`",
        allow_module_level=True,
    )

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
BOOTSTRAP_SERVERS = os.environ.get(
    "BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
)
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://localhost:8081"
).rstrip("/")

# Contrat attendu (déduit de lab.md + solution) ----------------------------- #
TOPIC_STRING = "orders.scala"        # ProducerSimple / ProducerFs2 (String,String)
TOPIC_AVRO = "orders.scala.avro"     # ProducerAvro (Avro + Schema Registry)
AVRO_SUBJECT = f"{TOPIC_AVRO}-value"  # convention TopicNameStrategy
EXPECTED_MIN_MESSAGES = 20           # les producers envoient 20 messages

# Schéma Avro partagé avec le L2 Python (namespace + champs)
EXPECTED_NAMESPACE = "fr.formation.kafka.orders"
EXPECTED_RECORD_NAME = "Order"
EXPECTED_FIELDS = {"id", "customer_id", "total", "currency", "created_at"}

REQUEST_TIMEOUT = 10
ADMIN_TIMEOUT = 10


# --------------------------------------------------------------------------- #
# Connectivité — skip propre si Kafka / Schema Registry injoignable
# --------------------------------------------------------------------------- #
def _kafka_reachable() -> bool:
    try:
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
        md = admin.list_topics(timeout=ADMIN_TIMEOUT)
        return len(md.brokers) > 0
    except KafkaException:
        return False
    except Exception:
        return False


def _registry_reachable() -> bool:
    try:
        r = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=REQUEST_TIMEOUT)
        return r.status_code == 200
    except requests.RequestException:
        return False


pytestmark = pytest.mark.acceptance


@pytest.fixture(scope="module")
def admin():
    if not _kafka_reachable():
        pytest.skip(
            f"Cluster Kafka injoignable sur '{BOOTSTRAP_SERVERS}'. "
            "Démarrer la stack (`docker compose up -d`) puis relancer."
        )
    return AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def _topic_metadata(admin, topic):
    md = admin.list_topics(timeout=ADMIN_TIMEOUT)
    return md.topics.get(topic)


def _count_messages(topic, max_expected=200, timeout_s=15.0):
    """Compte (borné) les messages disponibles sur un topic en sommant
    (high_watermark - low_watermark) sur toutes les partitions."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "l3-acceptance-counter",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
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
                TopicPartition(topic, pid), timeout=timeout_s
            )
            total += max(0, high - low)
        return total
    finally:
        consumer.close()


def _fetch_one_avro_value(topic, timeout_s=20.0):
    """Récupère la valeur brute (bytes) du premier message du topic."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "l3-acceptance-avro-reader",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        consumer.subscribe([topic])
        import time

        deadline = time.time() + timeout_s
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            return msg.value()
        return None
    finally:
        consumer.close()


# --------------------------------------------------------------------------- #
# 1. Topics attendus existent
# --------------------------------------------------------------------------- #
def test_topic_string_existe(admin):
    tmd = _topic_metadata(admin, TOPIC_STRING)
    assert tmd is not None and tmd.error is None, (
        f"Le topic '{TOPIC_STRING}' n'existe pas. "
        "Crée-le (lab.md étape 1) puis lance `sbt \"runMain lab.ProducerSimple\"`."
    )
    assert len(tmd.partitions) == 3, (
        f"'{TOPIC_STRING}' doit avoir 3 partitions, trouvé {len(tmd.partitions)}. "
        "Recrée le topic avec --partitions 3."
    )


def test_topic_avro_existe(admin):
    tmd = _topic_metadata(admin, TOPIC_AVRO)
    assert tmd is not None and tmd.error is None, (
        f"Le topic '{TOPIC_AVRO}' n'existe pas. "
        "Crée-le (lab.md étape 1) puis lance `sbt \"runMain lab.ProducerAvro\"`."
    )
    assert len(tmd.partitions) == 3, (
        f"'{TOPIC_AVRO}' doit avoir 3 partitions, trouvé {len(tmd.partitions)}. "
        "Recrée le topic avec --partitions 3."
    )


# --------------------------------------------------------------------------- #
# 2. Les topics contiennent les messages produits
# --------------------------------------------------------------------------- #
def test_topic_string_contient_messages(admin):
    n = _count_messages(TOPIC_STRING)
    assert n >= EXPECTED_MIN_MESSAGES, (
        f"'{TOPIC_STRING}' contient {n} message(s), attendu >= {EXPECTED_MIN_MESSAGES}. "
        "As-tu lancé `sbt \"runMain lab.ProducerSimple\"` (qui envoie 20 messages) ?"
    )


def test_topic_avro_contient_messages(admin):
    n = _count_messages(TOPIC_AVRO)
    assert n >= EXPECTED_MIN_MESSAGES, (
        f"'{TOPIC_AVRO}' contient {n} message(s), attendu >= {EXPECTED_MIN_MESSAGES}. "
        "As-tu lancé `sbt \"runMain lab.ProducerAvro\"` (qui envoie 20 messages) ?"
    )


# --------------------------------------------------------------------------- #
# 3. Schéma Avro enregistré au Schema Registry
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def registry():
    if not _registry_reachable():
        pytest.skip(
            f"Schema Registry injoignable sur '{SCHEMA_REGISTRY_URL}'. "
            "Démarrer la stack puis relancer."
        )
    return SCHEMA_REGISTRY_URL


def test_subject_avro_enregistre(registry):
    r = requests.get(f"{registry}/subjects", timeout=REQUEST_TIMEOUT)
    assert r.status_code == 200, f"GET /subjects a renvoyé {r.status_code}."
    subjects = r.json()
    assert AVRO_SUBJECT in subjects, (
        f"Le sujet '{AVRO_SUBJECT}' est absent du Schema Registry "
        f"(sujets présents : {subjects}). "
        "Lance `sbt \"runMain lab.ProducerAvro\"` : le KafkaAvroSerializer "
        "enregistre automatiquement le schéma au premier envoi."
    )


def test_schema_avro_namespace_et_champs(registry):
    r = requests.get(
        f"{registry}/subjects/{AVRO_SUBJECT}/versions/latest",
        timeout=REQUEST_TIMEOUT,
    )
    assert r.status_code == 200, (
        f"Impossible de lire le schéma de '{AVRO_SUBJECT}' "
        f"(HTTP {r.status_code}). Le sujet est-il bien enregistré ?"
    )
    import json

    schema = json.loads(r.json()["schema"])

    assert schema.get("type") == "record", (
        f"Le schéma doit être un 'record', trouvé '{schema.get('type')}'."
    )
    assert schema.get("name") == EXPECTED_RECORD_NAME, (
        f"Nom du record attendu '{EXPECTED_RECORD_NAME}', "
        f"trouvé '{schema.get('name')}'."
    )
    assert schema.get("namespace") == EXPECTED_NAMESPACE, (
        f"Namespace attendu '{EXPECTED_NAMESPACE}' (partagé avec le L2 Python), "
        f"trouvé '{schema.get('namespace')}'. "
        "L'interop inter-langages dépend d'un namespace identique."
    )
    field_names = {f["name"] for f in schema.get("fields", [])}
    assert field_names == EXPECTED_FIELDS, (
        f"Champs attendus {sorted(EXPECTED_FIELDS)}, "
        f"trouvés {sorted(field_names)}. "
        "Le case class Order doit matcher le contrat partagé avec le L2."
    )


# --------------------------------------------------------------------------- #
# 4. Un message Avro est désérialisable (wire format Confluent)
# --------------------------------------------------------------------------- #
def test_message_avro_deserialisable(admin, registry):
    raw = _fetch_one_avro_value(TOPIC_AVRO)
    assert raw is not None, (
        f"Aucun message lu sur '{TOPIC_AVRO}'. "
        "Lance `sbt \"runMain lab.ProducerAvro\"` avant le test."
    )

    # Wire format Confluent : [magic byte 0x00][schema id int32 big-endian][payload Avro]
    assert len(raw) >= 5, (
        f"Message trop court ({len(raw)} octets) pour un wire format Confluent. "
        "La valeur doit être sérialisée en Avro (KafkaAvroSerializer), pas en String."
    )
    magic = raw[0]
    assert magic == 0, (
        f"Premier octet attendu 0x00 (magic byte Confluent), trouvé {magic:#x}. "
        "Le value.serializer doit être KafkaAvroSerializer, pas StringSerializer."
    )
    schema_id = struct.unpack(">I", raw[1:5])[0]
    assert schema_id > 0, (
        f"Schema id invalide ({schema_id}) dans le wire format Avro."
    )

    # Le schema id doit être résolvable côté Registry, puis le payload décodable.
    r = requests.get(f"{registry}/schemas/ids/{schema_id}", timeout=REQUEST_TIMEOUT)
    assert r.status_code == 200, (
        f"Le schema id {schema_id} référencé dans le message est introuvable "
        f"au Schema Registry (HTTP {r.status_code})."
    )

    try:
        import fastavro
    except ImportError:
        pytest.skip(
            "fastavro non installé : désérialisation du payload non vérifiée "
            "(magic byte + schema id déjà validés). `pip install fastavro` pour aller plus loin."
        )

    import json

    parsed_schema = fastavro.parse_schema(json.loads(r.json()["schema"]))
    record = fastavro.schemaless_reader(io.BytesIO(raw[5:]), parsed_schema)
    assert set(record.keys()) == EXPECTED_FIELDS, (
        f"Le record Avro désérialisé a les champs {sorted(record.keys())}, "
        f"attendu {sorted(EXPECTED_FIELDS)}."
    )
    assert isinstance(record["total"], float) and isinstance(record["created_at"], int), (
        "Types attendus : total=double, created_at=long. "
        "Vérifie les types du case class Order."
    )
