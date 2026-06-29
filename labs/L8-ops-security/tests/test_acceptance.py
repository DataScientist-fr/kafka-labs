"""Test d'acceptation — Lab L8 (Ops & Sécurité : observabilité + SASL/SCRAM + ACL).

Test "boîte noire" qui NOTE l'état final du lab : il ne lit pas le code de
l'étudiant, il vérifie le comportement observable du cluster sécurisé `kafka-sec`
et des endpoints d'observabilité, après que l'étudiant a exécuté la mise en place.

PRÉREQUIS (à exécuter AVANT ce test, depuis labs/L8-ops-security) :
    make sec-up      # démarre la stack sécurisée (kafka-sec-1/2/3, ports 19092/3/4)
    make users       # crée les users SCRAM producer-app et consumer-app
    make acls        # crée le topic 'orders' et applique les ACLs

Le cluster `kafka-sec` expose le listener client EXTERNAL en SASL_PLAINTEXT /
SCRAM-SHA-256 sur localhost:19092,19093,19094 (l'inter-broker/controller reste
en PLAINTEXT). Les JMX exporters exposent /metrics sur 17071/17072/17073.

LANCEMENT :
    pip install pytest confluent-kafka==2.3.0 requests
    pytest tests/test_acceptance.py -v -m acceptance

Configuration via variables d'environnement (valeurs par défaut entre parenthèses) :
    L8_SASL_BOOTSTRAP   bootstrap SASL          (localhost:19092,localhost:19093,localhost:19094)
    L8_JMX_PORTS        ports JMX exporter      (17071,17072,17073)
    L8_JMX_HOST         hôte JMX exporter       (localhost)

Le test se SKIP proprement (pas d'échec) si le cluster sécurisé ou les endpoints
JMX sont injoignables — l'étudiant n'a alors pas exécuté les prérequis.
"""
from __future__ import annotations

import json
import os
import socket
import time
import uuid
from typing import Final

import pytest

requests = pytest.importorskip("requests", reason="requests requis pour l'observabilité")
confluent_kafka = pytest.importorskip(
    "confluent_kafka", reason="confluent-kafka requis (pip install confluent-kafka==2.3.0)"
)
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer  # noqa: E402
from confluent_kafka.admin import AdminClient  # noqa: E402

pytestmark = pytest.mark.acceptance


# --------------------------------------------------------------------------- #
# Configuration (env-overridable)
# --------------------------------------------------------------------------- #

SASL_BOOTSTRAP: Final = os.environ.get(
    "L8_SASL_BOOTSTRAP",
    "localhost:19092,localhost:19093,localhost:19094",
)
JMX_HOST: Final = os.environ.get("L8_JMX_HOST", "localhost")
JMX_PORTS: Final = [
    int(p) for p in os.environ.get("L8_JMX_PORTS", "17071,17072,17073").split(",") if p.strip()
]

TOPIC: Final = "orders"
GROUP_PREFIX: Final = "analytics-"  # ACL READ prefixed sur les groupes analytics-*

# Users SCRAM (creds de lab, non secrets) — alignés sur l'énoncé.
PRODUCER_USER: Final = "producer-app"
PRODUCER_PASS: Final = "producer-secret"  # nosec
CONSUMER_USER: Final = "consumer-app"
CONSUMER_PASS: Final = "consumer-secret"  # nosec

OP_TIMEOUT: Final = 30.0  # secondes pour produce/consume/metadata


# --------------------------------------------------------------------------- #
# Helpers de configuration client
# --------------------------------------------------------------------------- #

def _sasl_base(username: str, password: str) -> dict:
    """Config SASL_PLAINTEXT / SCRAM-SHA-256 commune (cf. clients du lab)."""
    return {
        "bootstrap.servers": SASL_BOOTSTRAP,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": username,
        "sasl.password": password,
    }


def _first_host_port() -> tuple[str, int]:
    first = SASL_BOOTSTRAP.split(",")[0].strip()
    host, _, port = first.rpartition(":")
    return host or "localhost", int(port)


def _tcp_reachable(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# --------------------------------------------------------------------------- #
# Fixtures de disponibilité (skip propre si prérequis non remplis)
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="session")
def secured_cluster() -> AdminClient:
    """Vérifie que le cluster SASL répond et expose le topic 'orders'.

    Skip si TCP injoignable ou si l'authentification admin échoue (prérequis
    `make sec-up && make users && make acls` non exécutés).
    """
    host, port = _first_host_port()
    if not _tcp_reachable(host, port):
        pytest.skip(
            f"cluster sécurisé injoignable sur {host}:{port} — "
            "exécuter `make sec-up && make users && make acls`"
        )

    # AdminClient avec un user applicatif (producer-app a DESCRIBE sur 'orders').
    admin = AdminClient(_sasl_base(PRODUCER_USER, PRODUCER_PASS))
    deadline = time.monotonic() + OP_TIMEOUT
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            md = admin.list_topics(timeout=5.0)
            if TOPIC in md.topics and md.topics[TOPIC].error is None:
                return admin
            last_err = RuntimeError(f"topic '{TOPIC}' absent des métadonnées")
        except KafkaException as exc:  # auth/connexion pas encore prête
            last_err = exc
        time.sleep(1.0)

    pytest.skip(
        f"topic '{TOPIC}' ou auth admin indisponible ({last_err}) — "
        "exécuter `make sec-up && make users && make acls`"
    )


# --------------------------------------------------------------------------- #
# 1. Observabilité — les 3 endpoints JMX exposent des métriques kafka_
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("port", JMX_PORTS, ids=lambda p: f"jmx:{p}")
def test_observabilite_endpoint_jmx_expose_metriques_kafka(port: int) -> None:
    """Chaque exporter JMX répond sur /metrics et publie des métriques `kafka_`."""
    url = f"http://{JMX_HOST}:{port}/metrics"
    if not _tcp_reachable(JMX_HOST, port):
        pytest.skip(f"exporter JMX injoignable sur {JMX_HOST}:{port} — exécuter `make sec-up`")

    resp = requests.get(url, timeout=5.0)
    assert resp.status_code == 200, (
        f"{url} a répondu HTTP {resp.status_code} (attendu 200)"
    )
    body = resp.text
    assert "kafka_" in body, (
        f"{url} ne contient aucune métrique préfixée 'kafka_' "
        "(l'agent jmx_prometheus_javaagent expose-t-il bien les MBeans Kafka ?)"
    )
    # Au moins une ligne de métrique exploitable (HELP/TYPE/échantillon kafka_*).
    kafka_lines = [
        ln for ln in body.splitlines()
        if ln.startswith("kafka_") or ln.startswith("# TYPE kafka_")
    ]
    assert kafka_lines, f"{url} : aucune ligne de métrique 'kafka_' exploitable"


# --------------------------------------------------------------------------- #
# 2. Auth OK — producer-app délivre un message sur 'orders' (WRITE autorisé)
# --------------------------------------------------------------------------- #

def test_auth_ok_producer_app_ecrit_sur_orders(secured_cluster: AdminClient) -> None:
    """producer-app (ACL WRITE sur 'orders') s'authentifie et délivre un message.

    Réussite = handshake SCRAM OK + autorisation WRITE OK + ack broker (offset).
    """
    delivery: dict[str, object] = {"err": None, "offset": None, "done": False}

    def _cb(err, msg) -> None:
        delivery["done"] = True
        if err is not None:
            delivery["err"] = err
        else:
            delivery["offset"] = msg.offset()

    producer = Producer({
        **_sasl_base(PRODUCER_USER, PRODUCER_PASS),
        "client.id": "l8-acceptance-producer",
        "acks": "all",
        "enable.idempotence": True,  # nécessite IDEMPOTENT_WRITE (ACL cluster)
        "message.timeout.ms": int(OP_TIMEOUT * 1000),
    })

    marker = uuid.uuid4().hex
    payload = json.dumps({"acceptance": True, "marker": marker}).encode()
    producer.produce(TOPIC, key=f"acc-{marker}".encode(), value=payload, on_delivery=_cb)
    remaining = producer.flush(OP_TIMEOUT)

    assert remaining == 0, "le message n'a pas été délivré dans le délai imparti"
    assert delivery["done"], "callback de livraison jamais appelé"
    assert delivery["err"] is None, (
        f"producer-app aurait dû être AUTORISÉ à écrire sur '{TOPIC}', "
        f"erreur reçue : {delivery['err']}"
    )
    assert isinstance(delivery["offset"], int) and delivery["offset"] >= 0, (
        "aucun offset retourné par le broker (message non persisté)"
    )


# --------------------------------------------------------------------------- #
# 3. Autz refus — des creds NON autorisés sont REFUSÉS par l'authorizer
# --------------------------------------------------------------------------- #

def test_autz_refus_consumer_app_ne_peut_pas_ecrire(secured_cluster: AdminClient) -> None:
    """consumer-app (READ seul) doit être REFUSÉ en écriture sur 'orders'.

    Reproduit la logique de `make test-denied` / producer_unauthorized.py :
    on attend une TopicAuthorizationException (refus d'autorisation), surtout
    PAS une livraison réussie.
    """
    refused: dict[str, object] = {"flag": False, "code": None, "err": None, "delivered": False}

    def _cb(err, msg) -> None:
        if err is not None:
            refused["flag"] = True
            refused["err"] = err
            code = getattr(err, "code", lambda: None)()
            refused["code"] = code
        else:
            refused["delivered"] = True

    producer = Producer({
        **_sasl_base(CONSUMER_USER, CONSUMER_PASS),
        "client.id": "l8-acceptance-rogue-producer",
        "acks": "all",
        "message.timeout.ms": int(OP_TIMEOUT * 1000),
    })
    try:
        producer.produce(TOPIC, key=b"rogue", value=json.dumps({"hack": True}).encode(),
                         on_delivery=_cb)
        producer.flush(OP_TIMEOUT)
    except KafkaException as exc:  # refus parfois remonté en exception synchrone
        refused["flag"] = True
        refused["err"] = exc
        refused["code"] = exc.args[0].code() if exc.args else None

    assert not refused["delivered"], (
        f"INATTENDU : consumer-app a réussi à écrire sur '{TOPIC}' — ACL WRITE "
        "indûment accordée (l'autorisation ne filtre pas)."
    )
    assert refused["flag"], (
        f"consumer-app aurait dû être REFUSÉ en écriture sur '{TOPIC}' "
        "(aucune erreur reçue dans le délai — ACLs appliquées ?)"
    )
    assert refused["code"] == KafkaError.TOPIC_AUTHORIZATION_FAILED, (
        "le refus attendu est TOPIC_AUTHORIZATION_FAILED (Topic authorization failed), "
        f"reçu : {refused['err']}"
    )


def test_autz_refus_producer_app_ne_peut_pas_lire(secured_cluster: AdminClient) -> None:
    """producer-app (WRITE seul) doit être REFUSÉ en lecture sur 'orders'.

    Reproduit la logique de producer_unauthorized.py : un consumer avec les
    creds producer-app doit récupérer TOPIC_AUTHORIZATION_FAILED (ou un refus
    de groupe GROUP_AUTHORIZATION_FAILED) — jamais un message.
    """
    consumer = Consumer({
        **_sasl_base(PRODUCER_USER, PRODUCER_PASS),
        # Groupe hors préfixe ACL 'analytics-' pour ne pas dépendre d'un droit de groupe.
        "group.id": f"rogue-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    denied_codes = {
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.GROUP_AUTHORIZATION_FAILED,
    }
    refusal_code: int | None = None
    got_message = False

    deadline = time.monotonic() + OP_TIMEOUT
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                code = msg.error().code()
                if code in denied_codes:
                    refusal_code = code
                    break
                # autres erreurs (ex : _PARTITION_EOF) : on continue
                continue
            got_message = True
            break
    finally:
        consumer.close()

    assert not got_message, (
        f"INATTENDU : producer-app a pu LIRE '{TOPIC}' — ACL READ indûment accordée."
    )
    assert refusal_code in denied_codes, (
        f"producer-app aurait dû être REFUSÉ en lecture sur '{TOPIC}' "
        "(TopicAuthorizationException attendue, ou Group authorization failed) — "
        "aucun refus explicite reçu dans le délai (ACLs appliquées ?)"
    )
