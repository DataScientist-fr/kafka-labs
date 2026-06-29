"""Test d'acceptation — Lab L4 (Kafka Connect / CDC Debezium / S3 sink).

Test "boite noire" qui NOTE l'etudiant : il verifie l'ETAT FINAL apres que
l'etudiant a deploye les connecteurs (source Debezium + sink S3/MinIO).

Ce qui est verifie via l'API REST de Kafka Connect (:8083), Kafka et le
Schema Registry :
  - le connecteur source Debezium existe et est RUNNING (connecteur ET tasks) ;
  - les 3 topics CDC ecommerce.public.{customers,orders,order_items} existent ;
  - les sujets (subjects) Avro -key et -value sont enregistres dans le SR ;
  - le connecteur sink S3 (bronze) existe et est RUNNING (connecteur ET tasks).

PREREQUIS :
  - L'infra Docker du lab tourne (`docker compose up -d` : connect, kafka1-3,
    schema-registry, postgres, minio, mc).
  - L'etudiant a deploye les connecteurs (etapes 2, 3 et 5 du lab) :
      ./scripts/register-debezium.sh
      (pre-creation des topics CDC)
      ./scripts/register-s3-sink.sh

LANCEMENT :
  pip install pytest requests confluent-kafka
  pytest -m acceptance labs/L4-kafka-connect-cdc/tests/test_acceptance.py -v

CONFIGURATION (variables d'environnement, valeurs par defaut sinon) :
  CONNECT_URL          http://localhost:8083
  BOOTSTRAP_SERVERS    localhost:9092,localhost:9093,localhost:9094
  SCHEMA_REGISTRY_URL  http://localhost:8081
  CONNECT_TIMEOUT      90   (secondes max d'attente du passage en RUNNING)
"""

import os
import time

import pytest
import requests

pytestmark = pytest.mark.acceptance

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083").rstrip("/")
BOOTSTRAP_SERVERS = os.environ.get(
    "BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
)
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://localhost:8081"
).rstrip("/")

# Delai max (s) pour laisser le snapshot Debezium et le passage RUNNING aboutir.
CONNECT_TIMEOUT = int(os.environ.get("CONNECT_TIMEOUT", "90"))

# Attendu deduit de lab.md + des configs de la solution.
SOURCE_CONNECTOR = "debezium-postgres-source"
SINK_CONNECTOR = "s3-sink-bronze"
CDC_TOPICS = [
    "ecommerce.public.customers",
    "ecommerce.public.orders",
    "ecommerce.public.order_items",
]

HTTP_TIMEOUT = 10  # s par requete HTTP


# --------------------------------------------------------------------------- #
# Helpers reseau (skip propre si l'infra est injoignable)
# --------------------------------------------------------------------------- #
def _connect_reachable():
    try:
        r = requests.get(f"{CONNECT_URL}/", timeout=HTTP_TIMEOUT)
        return r.status_code == 200
    except requests.RequestException:
        return False


def _registry_reachable():
    try:
        r = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=HTTP_TIMEOUT)
        return r.status_code == 200
    except requests.RequestException:
        return False


def _list_connectors():
    r = requests.get(f"{CONNECT_URL}/connectors", timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _connector_status(name):
    r = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _wait_running(name, timeout):
    """Attend que le connecteur ET au moins une task soient RUNNING.

    Retourne le dernier status observe (que ce soit RUNNING ou pas), pour
    permettre des assertions detaillees cote appelant.
    """
    deadline = time.time() + timeout
    last = None
    while True:
        try:
            last = _connector_status(name)
            conn_state = last.get("connector", {}).get("state")
            tasks = last.get("tasks", [])
            task_states = [t.get("state") for t in tasks]
            if (
                conn_state == "RUNNING"
                and tasks
                and all(s == "RUNNING" for s in task_states)
            ):
                return last
            # Inutile d'attendre si quelque chose a definitivement echoue.
            if conn_state == "FAILED" or any(s == "FAILED" for s in task_states):
                return last
        except requests.RequestException:
            last = None
        if time.time() >= deadline:
            return last
        time.sleep(2)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def connect():
    if not _connect_reachable():
        pytest.skip(
            f"Kafka Connect injoignable sur {CONNECT_URL} — "
            "demarrer l'infra (docker compose up -d) avant de noter."
        )
    return CONNECT_URL


@pytest.fixture(scope="session")
def kafka_topics():
    """Liste des topics du cluster via un AdminClient confluent-kafka."""
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError:  # pragma: no cover
        pytest.skip("confluent-kafka non installe (pip install confluent-kafka).")
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        md = admin.list_topics(timeout=15)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Cluster Kafka injoignable sur {BOOTSTRAP_SERVERS} : {exc}")
    return set(md.topics.keys())


@pytest.fixture(scope="session")
def registry_subjects():
    if not _registry_reachable():
        pytest.skip(f"Schema Registry injoignable sur {SCHEMA_REGISTRY_URL}.")
    r = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return set(r.json())


# --------------------------------------------------------------------------- #
# 1. Connecteur source Debezium
# --------------------------------------------------------------------------- #
def test_source_connector_deployed(connect):
    connectors = _list_connectors()
    assert SOURCE_CONNECTOR in connectors, (
        f"Le connecteur source '{SOURCE_CONNECTOR}' n'est pas deploye. "
        f"Connecteurs presents : {sorted(connectors)}. "
        "Attendu : deployer Debezium (etape 2 — register-debezium.sh)."
    )


def test_source_connector_running(connect):
    status = _wait_running(SOURCE_CONNECTOR, CONNECT_TIMEOUT)
    assert status is not None, (
        f"Statut introuvable pour '{SOURCE_CONNECTOR}' (connecteur non deploye ?)."
    )

    conn_state = status.get("connector", {}).get("state")
    assert conn_state == "RUNNING", (
        f"Le connecteur '{SOURCE_CONNECTOR}' est dans l'etat '{conn_state}', "
        f"attendu RUNNING. Statut complet : {status}"
    )

    tasks = status.get("tasks", [])
    assert tasks, (
        f"Le connecteur '{SOURCE_CONNECTOR}' n'a aucune task — "
        "verifier les logs Connect (slot/publication ?)."
    )
    bad = [t for t in tasks if t.get("state") != "RUNNING"]
    assert not bad, (
        f"Tasks non RUNNING pour '{SOURCE_CONNECTOR}' : "
        f"{[(t.get('id'), t.get('state'), t.get('trace', '')[:200]) for t in bad]}"
    )


# --------------------------------------------------------------------------- #
# 2. Topics CDC
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("topic", CDC_TOPICS)
def test_cdc_topic_exists(kafka_topics, topic):
    assert topic in kafka_topics, (
        f"Topic CDC '{topic}' absent du cluster. "
        f"Topics ecommerce.* presents : "
        f"{sorted(t for t in kafka_topics if t.startswith('ecommerce'))}. "
        "Les 3 topics doivent etre crees (etape 3) et alimentes par Debezium."
    )


# --------------------------------------------------------------------------- #
# 3. Sujets Avro dans le Schema Registry
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("topic", CDC_TOPICS)
def test_avro_value_subject_registered(registry_subjects, topic):
    subject = f"{topic}-value"
    assert subject in registry_subjects, (
        f"Sujet Avro '{subject}' absent du Schema Registry. "
        "Le converter Avro de Debezium doit enregistrer un schema de value "
        "par topic CDC (etape 3). "
        f"Sujets ecommerce.* presents : "
        f"{sorted(s for s in registry_subjects if s.startswith('ecommerce'))}."
    )


@pytest.mark.parametrize("topic", CDC_TOPICS)
def test_avro_key_subject_registered(registry_subjects, topic):
    subject = f"{topic}-key"
    assert subject in registry_subjects, (
        f"Sujet Avro '{subject}' absent du Schema Registry. "
        "Le converter Avro de Debezium doit enregistrer un schema de cle "
        "(base sur la PK) par topic CDC. "
        f"Sujets ecommerce.* presents : "
        f"{sorted(s for s in registry_subjects if s.startswith('ecommerce'))}."
    )


# --------------------------------------------------------------------------- #
# 4. Connecteur sink S3 (bronze)
# --------------------------------------------------------------------------- #
def test_sink_connector_deployed(connect):
    connectors = _list_connectors()
    assert SINK_CONNECTOR in connectors, (
        f"Le connecteur sink '{SINK_CONNECTOR}' n'est pas deploye. "
        f"Connecteurs presents : {sorted(connectors)}. "
        "Attendu : deployer le sink S3/MinIO (etape 5 — register-s3-sink.sh)."
    )


def test_sink_connector_running(connect):
    status = _wait_running(SINK_CONNECTOR, CONNECT_TIMEOUT)
    assert status is not None, (
        f"Statut introuvable pour '{SINK_CONNECTOR}' (connecteur non deploye ?)."
    )

    conn_state = status.get("connector", {}).get("state")
    assert conn_state == "RUNNING", (
        f"Le connecteur sink '{SINK_CONNECTOR}' est dans l'etat '{conn_state}', "
        f"attendu RUNNING (verifier credentials MinIO / bucket bronze). "
        f"Statut complet : {status}"
    )

    tasks = status.get("tasks", [])
    assert tasks, (
        f"Le connecteur sink '{SINK_CONNECTOR}' n'a aucune task active."
    )
    bad = [t for t in tasks if t.get("state") != "RUNNING"]
    assert not bad, (
        f"Tasks non RUNNING pour '{SINK_CONNECTOR}' : "
        f"{[(t.get('id'), t.get('state'), t.get('trace', '')[:200]) for t in bad]}"
    )
