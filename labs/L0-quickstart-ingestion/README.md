# Lab L0 — Quick-start ingestion

Voir [lab.md](./lab.md) pour le sujet complet du lab.

## Structure

```
L0-quickstart-ingestion/
├── lab.md              ← sujet pédagogique
├── README.md           ← ce fichier
├── data/
│   └── orders.jsonl    ← 7 lignes utiles + 1 vide + 4 patterns d'erreur
├── ingest.py           ← version pédagogique défensive
├── ingest_typed.py     ← variante typée (dataclasses + typing)
└── tests/
    └── test_ingest.py  ← squelette pytest (preview M4)
```

## Démarrage rapide

```bash
python ingest.py data/orders.jsonl
```

Sortie attendue :

```json
{
  "n_read": 7,
  "n_valid": 4,
  "n_rejected": 3,
  "n_empty_lines": 1,
  "status": "WARN"
}
```

## Cours associé

Formation « Ingestion de données » — chapitre « Fondamentaux d'une ingestion fiable ».
