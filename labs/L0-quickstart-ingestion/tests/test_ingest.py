"""Tests pytest pour le pipeline d'ingestion L0.

Avant-goût des tests pytest (chapitre « Ingestion API & fichiers », lab 5). Lance avec :
    pip install pytest
    pytest tests/ -v

Les tests sont volontairement simples : on teste la fonction pure validate()
+ un end-to-end avec un fichier temporaire.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Permet d'importer ingest.py depuis le dossier parent
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from ingest import validate, main as ingest_main


# ---------- Tests unitaires sur validate() ----------

def test_validate_record_complet_ok():
    record = {
        "order_id": "O-001",
        "customer_id": "C-100",
        "amount": 42.5,
        "currency": "EUR",
        "created_at": "2026-04-01T10:12:00Z",
    }
    assert validate(record) == []


def test_validate_champ_manquant():
    record = {
        "order_id": "O-001",
        # customer_id absent
        "amount": 42.5,
        "currency": "EUR",
        "created_at": "2026-04-01T10:12:00Z",
    }
    errors = validate(record)
    assert "missing_field:customer_id" in errors


def test_validate_champ_null_compte_comme_manquant():
    record = {
        "order_id": "O-001",
        "customer_id": None,
        "amount": 42.5,
        "currency": "EUR",
        "created_at": "2026-04-01T10:12:00Z",
    }
    errors = validate(record)
    assert "null_field:customer_id" in errors


def test_validate_amount_non_numerique():
    record = {
        "order_id": "O-001",
        "customer_id": "C-100",
        "amount": "unknown",
        "currency": "EUR",
        "created_at": "2026-04-01T10:12:00Z",
    }
    errors = validate(record)
    assert "amount_not_numeric" in errors


def test_validate_amount_bool_est_rejete():
    """bool est sous-classe de int en Python — on l'exclut explicitement."""
    record = {
        "order_id": "O-001",
        "customer_id": "C-100",
        "amount": True,
        "currency": "EUR",
        "created_at": "2026-04-01T10:12:00Z",
    }
    errors = validate(record)
    assert "amount_not_numeric" in errors


@pytest.mark.parametrize("amount,attendu_erreur", [
    (42, False),         # int OK
    (42.5, False),       # float OK
    (0, False),          # zéro OK (négatif pas encore filtré)
    (-10, False),        # négatif OK (exercice L0.A : à ajouter)
    ("42", True),        # string KO
    (None, True),        # null KO (compte comme null_field)
    ([], True),          # liste KO
])
def test_validate_amount_parametrise(amount, attendu_erreur):
    record = {
        "order_id": "O-001",
        "customer_id": "C-100",
        "amount": amount,
        "currency": "EUR",
        "created_at": "2026-04-01T10:12:00Z",
    }
    errors = validate(record)
    has_error = any("amount" in e or "null_field" in e for e in errors)
    assert has_error == attendu_erreur


# ---------- Test end-to-end sur main() ----------

def test_main_pipeline_complet(tmp_path: Path):
    """Vérifie qu'un fichier réaliste produit le bon split valid/rejected."""
    source = tmp_path / "orders.jsonl"
    source.write_text(
        '{"order_id":"O-001","customer_id":"C-100","amount":42.5,"currency":"EUR","created_at":"2026-04-01T10:12:00Z"}\n'
        '{"order_id":"O-002","amount":99.99,"currency":"EUR","created_at":"2026-04-01T10:15:00Z"}\n'  # manque customer_id
        'not valid json\n'
        '\n'  # ligne vide
        '{"order_id":"O-005","customer_id":"C-103","amount":150.0,"currency":"EUR","created_at":"2026-04-01T10:17:00Z"}\n',
        encoding="utf-8",
    )
    bronze = tmp_path / "bronze"
    log = tmp_path / "logs/ingest.log"

    exit_code = ingest_main(source, bronze, log)

    assert exit_code == 0
    # 2 valides, 2 rejetés (manque champ + json invalide), 1 vide
    valid_files = list((bronze / "valid").glob("*.jsonl"))
    rejected_files = list((bronze / "rejected").glob("*.jsonl"))
    assert len(valid_files) == 1
    assert len(rejected_files) == 1

    valid_lines = valid_files[0].read_text().strip().splitlines()
    rejected_lines = rejected_files[0].read_text().strip().splitlines()
    assert len(valid_lines) == 2
    assert len(rejected_lines) == 2

    # Vérif des métadonnées d'ingestion
    valid_rec = json.loads(valid_lines[0])
    for k in ("_ingestion_timestamp", "_batch_id", "_source_file"):
        assert k in valid_rec

    rejected_rec = json.loads(rejected_lines[0])
    assert "_errors" in rejected_rec
    assert len(rejected_rec["_errors"]) > 0


def test_main_fichier_inexistant(tmp_path: Path, capsys):
    """Source absente → exit 1, pas d'écriture en bronze."""
    bronze = tmp_path / "bronze"
    log = tmp_path / "logs/ingest.log"

    exit_code = ingest_main(tmp_path / "inexistant.jsonl", bronze, log)

    assert exit_code == 1
    assert not bronze.exists() or not any(bronze.iterdir())
    captured = capsys.readouterr()
    assert "ERROR" in captured.err


def test_main_fichier_vide(tmp_path: Path):
    """Source 0 octet → exit 2 avec status EMPTY."""
    source = tmp_path / "vide.jsonl"
    source.touch()
    bronze = tmp_path / "bronze"
    log = tmp_path / "logs/ingest.log"

    exit_code = ingest_main(source, bronze, log)

    assert exit_code == 2
    log_content = log.read_text().strip()
    assert '"status": "EMPTY"' in log_content
