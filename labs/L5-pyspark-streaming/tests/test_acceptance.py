"""Test d'acceptation — Lab L5 (PySpark Structured Streaming -> Bronze Delta).

But pedagogique
---------------
Ce test NOTE l'etudiant. Il valide l'ETAT FINAL produit par le job, en boite
noire : on ne lit pas le code de l'etudiant, on inspecte la table Bronze Delta
ecrite sur MinIO (S3). Il faut donc avoir execute le pipeline AVANT de lancer
le test :

    cd labs/L5-pyspark-streaming
    python -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    python setup_spark.py        # smoke test
    python write_bronze_delta.py # laisser tourner 2-3 min puis Ctrl-C

Prerequis (sinon les tests se SKIPpent proprement, ils n'echouent pas) :
- Cluster L1 up (Kafka + Schema Registry + MinIO) et L4 alimente les topics CDC.
- MinIO joignable sur http://localhost:9000 (creds minioadmin/minioadmin).
- Bucket `bronze` avec une table Delta sous le prefixe `orders/`.
- `pip install boto3` (requis pour ce test).

Lancement
---------
    pytest labs/L5-pyspark-streaming/tests/test_acceptance.py -v
    # ou, pour ne lancer que les tests d'acceptation du projet :
    pytest -m acceptance -v

Configuration via variables d'environnement (valeurs par defaut entre []) :
- MINIO_ENDPOINT     [http://localhost:9000]
- MINIO_ACCESS_KEY   [minioadmin]
- MINIO_SECRET_KEY   [minioadmin]
- BRONZE_BUCKET      [bronze]
- BRONZE_PREFIX      [orders]      (table Delta dans le bucket)
- L5_MIN_ROWS        [1]           (nombre minimal de lignes attendues)

NOTE : ce test n'expose volontairement aucune solution. Il verifie uniquement
des PROPRIETES OBSERVABLES de la sortie (table lisible, schema, metadonnees,
volume), pas l'implementation.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.acceptance

# --------------------------------------------------------------------------- #
# Configuration (env-driven)
# --------------------------------------------------------------------------- #
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "bronze")
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "orders").strip("/")
MIN_ROWS = int(os.environ.get("L5_MIN_ROWS", "1"))

# Colonnes attendues dans le Bronze, par categorie. On reste tolerant sur les
# noms exacts (le lab autorise plusieurs conventions de nommage CDC), d'ou les
# groupes "au moins une de" plutot qu'une egalite stricte du schema.
EXPECTED_BUSINESS_COLS = {"order_id", "customer_id", "status", "total_amount", "currency"}
# Metadonnees CDC : op (c/u/d/r) + timestamp source.
EXPECTED_OP_ALIASES = {"op", "__op"}
EXPECTED_CDC_TS_ALIASES = {"cdc_ts_ms", "__source_ts_ms", "ts_ms"}
# Metadonnees Kafka (lineage technique du message).
EXPECTED_KAFKA_COLS = {"topic", "partition", "offset"}
# Metadonnees Bronze (lineage d'ingestion ajoute par l'etudiant).
EXPECTED_INGEST_TS_ALIASES = {
    "ingested_at", "_ingestion_timestamp", "ingestion_timestamp", "ingest_ts",
}


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def s3():
    """Client boto3 pointant sur MinIO. Skip si boto3 absent ou MinIO injoignable."""
    boto3 = pytest.importorskip("boto3", reason="pip install boto3 requis pour ce test")
    from botocore.client import Config
    from botocore.exceptions import BotoCoreError, ClientError

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4", connect_timeout=5, retries={"max_attempts": 1}),
        region_name="us-east-1",
    )
    try:
        client.list_buckets()
    except (BotoCoreError, ClientError, OSError) as exc:
        pytest.skip(
            f"MinIO injoignable sur {MINIO_ENDPOINT} ({exc}). "
            "Demarrer la stack L1 (docker compose up) avant le test d'acceptation."
        )
    return client


@pytest.fixture(scope="session")
def bronze_objects(s3):
    """Liste paginée des objets sous bronze/orders/. Skip si le Bronze est absent."""
    from botocore.exceptions import ClientError

    # Le bucket doit exister.
    try:
        buckets = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    except ClientError as exc:  # pragma: no cover - couvert par la fixture s3
        pytest.skip(f"Impossible de lister les buckets MinIO : {exc}")

    if BRONZE_BUCKET not in buckets:
        pytest.skip(
            f"Bucket '{BRONZE_BUCKET}' absent ({sorted(buckets)}). "
            "Lancer `python write_bronze_delta.py` pour ecrire le Bronze."
        )

    prefix = f"{BRONZE_PREFIX}/"
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    if not keys:
        pytest.skip(
            f"Aucun objet sous s3://{BRONZE_BUCKET}/{prefix}. "
            "Le job d'ecriture Bronze n'a pas (encore) produit de donnees : "
            "lancer `python write_bronze_delta.py` 2-3 minutes."
        )
    return keys


# --------------------------------------------------------------------------- #
# Tests : structure physique de la table Delta (boto3 seul)
# --------------------------------------------------------------------------- #
def test_delta_log_present(bronze_objects):
    """La table Bronze doit etre une vraie table Delta : un _delta_log/ existe."""
    log_files = [
        k for k in bronze_objects
        if f"/{ '_delta_log' }/" in f"/{k}" or k.startswith(f"{BRONZE_PREFIX}/_delta_log/")
    ]
    assert log_files, (
        f"Aucun dossier _delta_log/ sous s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}/. "
        "Le sink doit etre `.format('delta')` (pas du parquet brut) : "
        "le journal transactionnel _delta_log/ est la signature d'une table Delta."
    )


def test_delta_commit_json_present(bronze_objects):
    """Au moins un commit Delta (.json versionné) doit avoir ete ecrit."""
    commits = [
        k for k in bronze_objects
        if "/_delta_log/" in f"/{k}".replace(f"{BRONZE_PREFIX}", "") or "_delta_log/" in k
    ]
    commits = [k for k in commits if k.endswith(".json")]
    assert commits, (
        "Aucun fichier de commit `*.json` dans _delta_log/. "
        "Un micro-batch ecrit = un commit Delta : laisser tourner le job assez "
        "longtemps pour produire au moins un commit (version 0)."
    )


def test_parquet_data_files_present(bronze_objects):
    """Des fichiers de donnees parquet doivent exister (la table n'est pas vide)."""
    parquet = [k for k in bronze_objects if k.endswith(".parquet")]
    assert parquet, (
        f"Aucun fichier part-*.parquet sous s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}/. "
        "La table Delta existe mais ne contient aucune donnee : verifier que "
        "Kafka/L4 alimente bien le topic `ecommerce.public.orders`."
    )


# --------------------------------------------------------------------------- #
# Tests : contenu logique (schema + metadonnees + volume)
# Lecture Delta-aware si `deltalake` est dispo ; sinon skip propre.
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def bronze_table():
    """Charge la table Delta via le lib `deltalake` (lecture coherente du _delta_log).

    Skip proprement si `deltalake` n'est pas installe : les tests de structure
    (boto3) suffisent alors a valider l'essentiel.
    """
    deltalake = pytest.importorskip(
        "deltalake",
        reason="`pip install deltalake` pour les assertions schema/volume (optionnel)",
    )
    storage_options = {
        "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
        "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_REGION": "us-east-1",
    }
    table_uri = f"s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}"
    try:
        dt = deltalake.DeltaTable(table_uri, storage_options=storage_options)
    except Exception as exc:  # noqa: BLE001 - on veut un skip lisible, pas un crash
        pytest.skip(f"Impossible d'ouvrir la table Delta {table_uri} via deltalake : {exc}")
    return dt


@pytest.fixture(scope="session")
def bronze_schema_cols(bronze_table):
    return {field.name for field in bronze_table.schema().fields}


def test_table_is_readable_as_delta(bronze_table):
    """La table doit etre ouvrable comme Delta et exposer une version >= 0."""
    version = bronze_table.version()
    assert version is not None and version >= 0, (
        "La table Delta n'expose pas de version valide : "
        "le _delta_log/ est peut-etre corrompu ou incomplet."
    )


def test_schema_has_business_columns(bronze_schema_cols):
    """Les colonnes metier de la commande doivent etre presentes (schema preserve)."""
    missing = EXPECTED_BUSINESS_COLS - bronze_schema_cols
    assert not missing, (
        f"Colonnes metier manquantes dans le Bronze : {sorted(missing)}. "
        f"Schema observe : {sorted(bronze_schema_cols)}. "
        "Le Bronze doit preserver le schema de la source (T4 §3.2) : "
        "deserialiser correctement l'Avro et exposer les champs de `orders`."
    )


def test_schema_has_cdc_metadata(bronze_schema_cols):
    """Les metadonnees CDC (operation + timestamp source) doivent etre presentes."""
    assert bronze_schema_cols & EXPECTED_OP_ALIASES, (
        f"Aucune colonne d'operation CDC parmi {sorted(EXPECTED_OP_ALIASES)}. "
        "Conserver l'operation Debezium (c/u/d/r) pour tracer le changement."
    )
    assert bronze_schema_cols & EXPECTED_CDC_TS_ALIASES, (
        f"Aucune colonne de timestamp CDC parmi {sorted(EXPECTED_CDC_TS_ALIASES)}. "
        "Conserver le timestamp source (ts_ms) pour ordonner les changements."
    )


def test_schema_has_kafka_lineage(bronze_schema_cols):
    """Les metadonnees Kafka (topic/partition/offset) doivent etre conservees."""
    missing = EXPECTED_KAFKA_COLS - bronze_schema_cols
    assert not missing, (
        f"Metadonnees Kafka manquantes : {sorted(missing)}. "
        "Conserver topic/partition/offset garantit la tracabilite de chaque ligne "
        "jusqu'a son message Kafka d'origine."
    )


def test_schema_has_bronze_ingestion_metadata(bronze_schema_cols):
    """Le Bronze doit porter une metadonnee d'ingestion (lineage temporel)."""
    assert bronze_schema_cols & EXPECTED_INGEST_TS_ALIASES, (
        f"Aucune colonne d'horodatage d'ingestion parmi "
        f"{sorted(EXPECTED_INGEST_TS_ALIASES)}. Schema observe : "
        f"{sorted(bronze_schema_cols)}. Ajouter un `current_timestamp()` "
        "(ex. `ingested_at`) pour dater l'entree de chaque ligne dans le Bronze."
    )


def test_bronze_has_minimum_rows(bronze_table):
    """La table doit contenir au moins L5_MIN_ROWS lignes."""
    # to_pyarrow_dataset evite de tout charger en memoire ; count_rows est lazy.
    try:
        dataset = bronze_table.to_pyarrow_dataset()
        n_rows = dataset.count_rows()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Comptage des lignes indisponible (pyarrow manquant ?) : {exc}")
    assert n_rows >= MIN_ROWS, (
        f"Le Bronze contient {n_rows} ligne(s) < minimum attendu {MIN_ROWS}. "
        "Laisser le job tourner plus longtemps, ou verifier que L4/Debezium "
        "alimente le topic `ecommerce.public.orders`."
    )
