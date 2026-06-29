"""
Test d'acceptation — Lab L6 (Scala Spark Structured Streaming -> silver).

Test BOITE NOIRE : il ne compile NI n'exécute le code Scala. Il inspecte
uniquement l'ETAT FINAL matérialisé dans l'object store (MinIO / S3A) une fois
que l'étudiant a buildé puis soumis son application Spark Scala. La lecture se
fait directement sur les objets S3 (boto3) + parsing du journal de transactions
Delta (`_delta_log/*.json`) : AUCUNE SparkSession ni dépendance lourde
(pyarrow / deltalake) n'est requise.

Le lab matérialise la couche SILVER de l'architecture médaillon (bronze ->
silver). Le bucket et le préfixe sont déduits de lab.md + application.conf :
les tables sont écrites sous `s3a://silver/<table>` en format Delta.

PREREQUIS (à faire AVANT de lancer ce test) :
  1. La stack Docker est UP (brokers + Schema Registry + MinIO + Spark + connector
     CDC du L4 + bronze/customers matérialisé par le L5).
       docker compose ps        # tout doit être "Up"/"healthy"
  2. Producer payments synthétique lancé (lab.md étape 8) pour alimenter
     `ecommerce.public.payments` (sinon `orders_paid` peut rester vide).
  3. L'étudiant a buildé puis soumis son application Scala depuis le lab :
       sbt assembly
       docker cp target/scala-2.12/*-assembly-*.jar spark-master:/opt/jars/
       docker exec -it spark-master spark-submit \
         --master spark://spark-master:7077 \
         --packages io.delta:delta-spark_2.12:3.1.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.spark:spark-avro_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
         --class lab.SparkApp /opt/jars/<jar>
     Laisser tourner quelques micro-batches (>= 1-2 min) pour finaliser au moins
     une fenêtre tumbling (watermark 10 min : voir note dans test ci-dessous).

COMMENT LANCER :
  pip install pytest boto3
  pytest -m acceptance labs/L6-scala-spark-streaming/tests/test_acceptance.py -v

CONFIGURATION (variables d'environnement, valeurs par défaut entre []) :
  MINIO_ENDPOINT   [http://localhost:9000]
  MINIO_ACCESS_KEY [minioadmin]
  MINIO_SECRET_KEY [minioadmin]
  SILVER_BUCKET    [silver]
  SILVER_PREFIX    []            # préfixe optionnel devant <table> dans le bucket
  L6_MIN_ROWS      [1]           # nb minimal de lignes dans la table principale

Si MinIO est injoignable ou si AUCUNE table silver n'a encore été produite, les
tests sont SKIPPÉS proprement (et non échoués) — relancer le job Spark puis
ré-exécuter.
"""

import json
import os

import pytest

try:
    import boto3
    from botocore.client import Config as BotoConfig
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover
    pytest.skip(
        "boto3 non installé : `pip install boto3`",
        allow_module_level=True,
    )

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000").rstrip("/")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "silver")
# Préfixe optionnel à l'intérieur du bucket (vide par défaut : tables à la racine).
SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "").strip("/")
MIN_ROWS = int(os.environ.get("L6_MIN_ROWS", "1"))

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 15

# Contrat attendu (déduit de lab.md + solution) ----------------------------- #
# Table principale notée : tumbling window 1 min (étape 4, checklist Validation).
MAIN_TABLE = "orders_revenue_1m"

# Schéma minimal attendu par table (sous-ensemble : on tolère des colonnes en
# plus, mais ces colonnes-clés DOIVENT être présentes). On ne vérifie pas les
# types exacts du journal Delta pour rester robuste aux versions.
EXPECTED_SCHEMAS = {
    "orders_revenue_1m": {
        "window_start", "window_end", "status", "orders_count", "revenue",
    },
    "orders_avg_basket_5m": {
        "window_start", "window_end", "status", "avg_basket", "orders_count",
    },
    "orders_enriched": {
        "order_id", "customer_id", "status", "total_amount", "kafka_ts",
    },
    "orders_paid": {
        "order_id", "customer_id", "order_amount", "payment_amount",
        "order_ts", "payment_ts", "delay_seconds",
    },
}

pytestmark = pytest.mark.acceptance


# --------------------------------------------------------------------------- #
# Client S3 / MinIO — skip propre si injoignable
# --------------------------------------------------------------------------- #
def _make_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},  # MinIO : path-style obligatoire
            connect_timeout=CONNECT_TIMEOUT,
            read_timeout=READ_TIMEOUT,
            retries={"max_attempts": 1},
        ),
        region_name="us-east-1",
    )


def _minio_reachable(client) -> bool:
    try:
        client.list_buckets()
        return True
    except (BotoCoreError, ClientError, Exception):
        return False


def _table_prefix(table: str) -> str:
    """Préfixe S3 (sans bucket) de la table, ex. 'orders_revenue_1m/'."""
    parts = [p for p in (SILVER_PREFIX, table) if p]
    return "/".join(parts) + "/"


def _list_keys(client, prefix: str, max_keys: int = 1000):
    """Liste (bornée) des clés sous un préfixe du bucket silver."""
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": SILVER_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
            if len(keys) >= max_keys:
                return keys
        if not resp.get("IsTruncated"):
            return keys
        token = resp.get("NextContinuationToken")


def _delta_log_entries(client, table: str):
    """Retourne (commit_keys, parquet_keys) sous <table>/ ; commit_keys = JSON
    du _delta_log triés (chronologiques)."""
    prefix = _table_prefix(table)
    keys = _list_keys(client, prefix)
    commits = sorted(
        k for k in keys
        if "/_delta_log/" in k and k.endswith(".json")
    )
    parquets = [
        k for k in keys
        if k.endswith(".parquet") and "/_delta_log/" not in k
    ]
    return commits, parquets


def _read_text(client, key: str) -> str:
    obj = client.get_object(Bucket=SILVER_BUCKET, Key=key)
    return obj["Body"].read().decode("utf-8")


def _delta_schema_fields(client, commit_keys):
    """Reconstitue le set de colonnes en lisant la dernière action 'metaData'
    présente dans le journal Delta (parse des lignes JSON des commits)."""
    fields = set()
    for key in commit_keys:  # ordre chronologique : la dernière metaData gagne
        text = _read_text(client, key)
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            action = json.loads(line)
            meta = action.get("metaData")
            if meta and "schemaString" in meta:
                schema = json.loads(meta["schemaString"])
                fields = {f["name"] for f in schema.get("fields", [])}
    return fields


def _delta_row_count(client, commit_keys):
    """Compte les lignes via les stats `add`/`remove` du journal Delta.
    Si `numRecords` absent des stats, retourne None (indéterminé)."""
    total = 0
    determinate = False
    for key in commit_keys:
        text = _read_text(client, key)
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            action = json.loads(line)
            add = action.get("add")
            remove = action.get("remove")
            if add and add.get("stats"):
                try:
                    n = json.loads(add["stats"]).get("numRecords")
                    if n is not None:
                        total += int(n)
                        determinate = True
                except (ValueError, TypeError):
                    pass
            if remove and remove.get("stats"):
                try:
                    n = json.loads(remove["stats"]).get("numRecords")
                    if n is not None:
                        total -= int(n)
                except (ValueError, TypeError):
                    pass
    return total if determinate else None


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def s3():
    client = _make_client()
    if not _minio_reachable(client):
        pytest.skip(
            f"MinIO injoignable sur '{MINIO_ENDPOINT}'. "
            "Démarrer la stack (`docker compose up -d`) puis relancer."
        )
    # Bucket silver présent ?
    try:
        client.head_bucket(Bucket=SILVER_BUCKET)
    except (ClientError, BotoCoreError):
        pytest.skip(
            f"Bucket '{SILVER_BUCKET}' absent sur MinIO. "
            "Il est créé en L1 et alimenté par le job Spark du L6. "
            "Vérifier la stack puis soumettre l'application Spark."
        )
    return client


@pytest.fixture(scope="module")
def main_table_commits(s3):
    """Commits Delta de la table principale ; skip si la table n'existe pas
    encore (job non lancé)."""
    commits, _ = _delta_log_entries(s3, MAIN_TABLE)
    if not commits:
        pytest.skip(
            f"Aucune table Delta '{MAIN_TABLE}' sous "
            f"s3a://{SILVER_BUCKET}/{_table_prefix(MAIN_TABLE)} "
            "(pas de _delta_log/*.json). As-tu lancé `sbt assembly` puis "
            "`spark-submit --class lab.SparkApp ...` et laissé tourner le job ?"
        )
    return commits


# --------------------------------------------------------------------------- #
# 1. La table principale existe et est au format Delta
# --------------------------------------------------------------------------- #
def test_table_principale_existe_format_delta(s3, main_table_commits):
    # main_table_commits non vide => _delta_log/*.json présent => format Delta.
    assert len(main_table_commits) >= 1, (
        f"'{MAIN_TABLE}' n'a aucun commit Delta : le sink doit être "
        "`format(\"delta\")` (un _delta_log/ est créé sous la table)."
    )
    _, parquets = _delta_log_entries(s3, MAIN_TABLE)
    assert len(parquets) >= 1, (
        f"'{MAIN_TABLE}' a un _delta_log mais aucun fichier Parquet de données. "
        "Aucune fenêtre n'a encore été finalisée : avec un watermark de 10 min, "
        "il faut soit attendre, soit produire des events couvrant > window.end + watermark."
    )


# --------------------------------------------------------------------------- #
# 2. Schéma Bronze/silver attendu (colonnes window + agrégats)
# --------------------------------------------------------------------------- #
def test_table_principale_schema(s3, main_table_commits):
    fields = _delta_schema_fields(s3, main_table_commits)
    assert fields, (
        f"Impossible de lire le schéma Delta de '{MAIN_TABLE}' "
        "(aucune action metaData dans _delta_log)."
    )
    expected = EXPECTED_SCHEMAS[MAIN_TABLE]
    missing = expected - fields
    assert not missing, (
        f"Colonnes manquantes dans '{MAIN_TABLE}' : {sorted(missing)}. "
        f"Trouvées : {sorted(fields)}. Attendu (au moins) : {sorted(expected)}. "
        "La tumbling window doit exposer window_start/window_end + status + "
        "orders_count + revenue."
    )


# --------------------------------------------------------------------------- #
# 3. La table principale contient des lignes (>= N)
# --------------------------------------------------------------------------- #
def test_table_principale_contient_lignes(s3, main_table_commits):
    n = _delta_row_count(s3, main_table_commits)
    if n is None:
        # Pas de stats numRecords : on retombe sur la présence de Parquet.
        _, parquets = _delta_log_entries(s3, MAIN_TABLE)
        assert len(parquets) >= 1, (
            f"'{MAIN_TABLE}' : aucune statistique de lignes et aucun Parquet. "
            "La table semble vide."
        )
        pytest.skip(
            "numRecords absent des stats Delta : présence de données confirmée "
            "via les fichiers Parquet, mais le comptage exact est indisponible."
        )
    assert n >= MIN_ROWS, (
        f"'{MAIN_TABLE}' contient {n} ligne(s), attendu >= {MIN_ROWS}. "
        "Le pipeline a-t-il consommé des orders et finalisé au moins une fenêtre ? "
        "(produire des events + laisser tourner > watermark)."
    )


# --------------------------------------------------------------------------- #
# 4. Les autres tables silver du lab existent avec le bon schéma
#    (skip individuel propre si une table n'a pas encore été produite)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "table",
    [t for t in EXPECTED_SCHEMAS if t != MAIN_TABLE],
)
def test_tables_secondaires_schema(s3, table):
    commits, _ = _delta_log_entries(s3, table)
    if not commits:
        pytest.skip(
            f"Table '{table}' non encore produite sous "
            f"s3a://{SILVER_BUCKET}/{_table_prefix(table)}. "
            "Étapes 5/7/8 du lab (sliding window, stream-static join, "
            "stream-stream join). Pour 'orders_paid', vérifier que le producer "
            "payments tourne."
        )
    fields = _delta_schema_fields(s3, commits)
    expected = EXPECTED_SCHEMAS[table]
    missing = expected - fields
    assert not missing, (
        f"Colonnes manquantes dans '{table}' : {sorted(missing)}. "
        f"Trouvées : {sorted(fields)}. Attendu (au moins) : {sorted(expected)}."
    )
