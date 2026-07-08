"""Mini pipeline d'ingestion JSONL → bronze/valid + bronze/rejected.

Pattern fondateur de la formation « Ingestion de données » (chapitre « Fondamentaux d'une ingestion fiable »).
Stdlib uniquement (Python 3.10+).
On code défensivement : aucune exception ne doit faire planter le run.
En cas de doute, on QUARANTINE — on ne LÈVE pas.

Usage:
    python ingest.py [data/orders.jsonl]

Exit codes:
    0 — OK ou WARN (run effectif)
    1 — fichier source inaccessible
    2 — run vide (rien à ingérer)
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

REQUIRED_FIELDS = ["order_id", "customer_id", "amount", "currency", "created_at"]


def now_iso() -> str:
    """Timestamp UTC ISO 8601. Toujours UTC en Bronze, jamais le local."""
    return datetime.now(timezone.utc).isoformat()


def validate(record: dict) -> list[str]:
    """Retourne la liste des motifs d'erreur, vide si record valide."""
    errors: list[str] = []
    for f in REQUIRED_FIELDS:
        if f not in record:
            errors.append(f"missing_field:{f}")
        # IMPORTANT : un champ présent mais à None compte aussi comme manquant
        elif record[f] is None:
            errors.append(f"null_field:{f}")
    if "amount" in record and record.get("amount") is not None:
        if not isinstance(record["amount"], (int, float)):
            errors.append("amount_not_numeric")
        elif isinstance(record["amount"], bool):
            # bool est sous-classe d'int en Python. On exclut explicitement.
            errors.append("amount_not_numeric")
    return errors


def main(input_path: Path, bronze_dir: Path, log_path: Path) -> int:
    """Renvoie le code retour : 0=OK, 1=fichier source manquant, 2=run vide."""

    # IMPORTANT : on vérifie l'existence du fichier source AVANT toute action.
    # Pas de batch_id générée si on ne va rien faire.
    if not input_path.exists():
        print(f"ERROR: source file not found: {input_path}", file=sys.stderr)
        return 1

    batch_id = str(uuid.uuid4())
    valid_path = bronze_dir / "valid" / f"{batch_id}.jsonl"
    rejected_path = bronze_dir / "rejected" / f"{batch_id}.jsonl"
    valid_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    n_read = n_valid = n_rejected = n_empty = 0
    started = now_iso()

    # IMPORTANT : encoding="utf-8", errors="replace".
    # En cas d'octet invalide, on ne plante pas — on remplace par U+FFFD,
    # et la ligne ira en rejected (json.loads échouera).
    # En prod, on monitorerait le compteur n_replaced.
    with input_path.open(encoding="utf-8", errors="replace") as f, \
         valid_path.open("w", encoding="utf-8") as fv, \
         rejected_path.open("w", encoding="utf-8") as fr:

        for lineno, raw in enumerate(f, 1):
            # Ligne vide : on incrémente un compteur dédié, on ne quarantine pas.
            # (Une ligne vide n'est pas une donnée corrompue, c'est du bruit
            # de fin de fichier ou de séparation.)
            if not raw.strip():
                n_empty += 1
                continue

            n_read += 1
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError as e:
                # IMPORTANT : on ne lève pas, on quarantine.
                # La ligne brute est conservée dans _raw, motif dans _errors.
                fr.write(json.dumps({
                    "_lineno": lineno,
                    "_raw": raw.rstrip("\n"),
                    "_errors": [f"json_decode:{e.msg}"],
                    "_ingestion_timestamp": now_iso(),
                    "_batch_id": batch_id,
                    "_source_file": str(input_path),
                }) + "\n")
                n_rejected += 1
                continue

            # JSON valide mais pas un dict (ex: "string", 42, [1,2,3])
            if not isinstance(rec, dict):
                fr.write(json.dumps({
                    "_lineno": lineno,
                    "_raw": raw.rstrip("\n"),
                    "_errors": ["not_an_object"],
                    "_ingestion_timestamp": now_iso(),
                    "_batch_id": batch_id,
                    "_source_file": str(input_path),
                }) + "\n")
                n_rejected += 1
                continue

            errors = validate(rec)
            enriched = {
                **rec,
                "_ingestion_timestamp": now_iso(),
                "_batch_id": batch_id,
                "_source_file": str(input_path),
            }
            if errors:
                enriched["_errors"] = errors
                fr.write(json.dumps(enriched) + "\n")
                n_rejected += 1
            else:
                fv.write(json.dumps(enriched) + "\n")
                n_valid += 1

    finished = now_iso()

    # Statut :
    #   EMPTY  — aucune ligne lue
    #   WARN   — tout en rejected, ou > 10% rejected
    #   OK     — sinon
    if n_read == 0:
        status = "EMPTY"
    elif n_valid == 0:
        status = "WARN"
    elif n_rejected / n_read > 0.10:
        status = "WARN"
    else:
        status = "OK"

    log = {
        "batch_id": batch_id,
        "source": str(input_path),
        "started": started,
        "finished": finished,
        "n_read": n_read,
        "n_valid": n_valid,
        "n_rejected": n_rejected,
        "n_empty_lines": n_empty,
        "status": status,
    }
    with log_path.open("a", encoding="utf-8") as fl:
        fl.write(json.dumps(log) + "\n")
    print(json.dumps(log, indent=2))

    return 0 if n_read > 0 else 2


if __name__ == "__main__":
    exit_code = main(
        Path(sys.argv[1] if len(sys.argv) > 1 else "data/orders.jsonl"),
        Path("bronze"),
        Path("logs/ingest.log"),
    )
    sys.exit(exit_code)
