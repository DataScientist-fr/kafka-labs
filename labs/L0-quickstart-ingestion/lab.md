# Lab L0 — Quick-start ingestion : premier pipeline valid/rejected
**Durée** : 25 min
**Stack** : Python 3.10+ (stdlib uniquement, aucun pip install)

> **Cours associé** : formation « Ingestion de données », chapitre « Fondamentaux d'une ingestion fiable ». C'est le **pattern fondateur** du parcours : on le retrouvera dans tous les chapitres suivants (API & fichiers, SQL incrémental, puis Kafka, CDC et Spark dans la formation suivante) avec des transports différents — la géométrie *valid / rejected / log* reste la même.
> **Pas de paire bilingue** : Python pur, stdlib uniquement, 25 min. À refaire en Scala/Java si parcours JVM.

## Objectifs

Construire en 25 minutes un mini-pipeline de fichier qui :

- ingère un JSONL ligne à ligne ;
- valide les champs requis (`order_id`, `customer_id`, `amount`, `currency`, `created_at`) ;
- sépare les enregistrements valides et rejetés en deux fichiers `bronze/valid/` et `bronze/rejected/` ;
- enrichit chaque enregistrement avec **les 4 métadonnées d'ingestion** du module : `_ingestion_timestamp`, `_batch_id`, `_source_file`, `_errors` (uniquement côté rejected) ;
- log son exécution en JSON structuré (1 ligne par run) avec compteurs et statut ;
- gère défensivement les cas d'erreur (fichier inexistant, ligne vide, encodage cassé, JSON invalide) **sans planter**.

C'est le **pattern fondateur** du parcours — **VALID / REJECTED / LOG**.

## Prérequis

- Python 3.10+ installé en local
- Aucun container, aucun pip install
- 25 minutes devant vous

## Architecture du lab

```
data/orders.jsonl  →  ingest.py  →  bronze/valid/<batch_id>.jsonl
                                  →  bronze/rejected/<batch_id>.jsonl
                                  →  logs/ingest.log  (1 ligne JSON par run)
```

Le pipeline est **append-only** : chaque run écrit dans un fichier identifié par son `batch_id` (UUID4). On ne supprime, on n'écrase rien. C'est la base de l'idempotence et du replay (vus en M4).

## Setup

```
lab-L0/
├── data/
│   └── orders.jsonl       # fourni, 7 lignes utiles + 1 vide + 4 patterns d'erreur
├── ingest.py              # version pédagogique défensive (à écrire)
├── ingest_typed.py        # variante typée avec dataclasses (à écrire)
└── tests/
    └── test_ingest.py     # preview pytest (annexe)
```

Cloner ou télécharger ce dossier. Lire `data/orders.jsonl` :

```bash
cat data/orders.jsonl
```

7 lignes utiles + 1 ligne vide. La ligne 3 manque `customer_id`. La ligne 4 a un `amount` non numérique. La ligne 6 n'est pas du JSON. La ligne 7 est vide (cas réel : éditeur qui ajoute une ligne blanche en fin de fichier). On valide donc **4 patterns d'erreur** en un seul jeu de données.

## Étapes

### 1. Lire le code `ingest.py`

Ouvrir [`ingest.py`](./ingest.py) et lire les commentaires. Trois patterns clés :

- **`errors="replace"`** sur l'ouverture du fichier : en cas d'octet invalide UTF-8, on remplace par `U+FFFD` au lieu de planter. La ligne ira en rejected via le `json.loads` qui échouera.
- **Quarantine au lieu d'exception** : `json.JSONDecodeError` est attrapé, on écrit en rejected avec le motif `json_decode:...` et on continue. Pareil pour les records qui ne sont pas des objets (`not_an_object`).
- **`_batch_id` généré APRÈS la vérif du fichier source** : pas de log fantôme si le fichier n'existe pas.

### 2. Exécuter sur le jeu fourni

```bash
python ingest.py data/orders.jsonl
```

Sortie attendue (résumé JSON imprimé sur stdout) :

```json
{
  "batch_id": "<uuid4>",
  "source": "data/orders.jsonl",
  "started": "...",
  "finished": "...",
  "n_read": 7,
  "n_valid": 4,
  "n_rejected": 3,
  "n_empty_lines": 1,
  "status": "WARN"
}
```

### 3. Inspecter les sorties

```bash
ls -la bronze/valid bronze/rejected logs/
cat bronze/valid/*.jsonl       # 4 lignes
cat bronze/rejected/*.jsonl    # 3 lignes avec _errors
cat logs/ingest.log            # 1 ligne JSON
```

### 4. Tester les cas d'erreur défensifs

```bash
# Fichier inexistant : exit code 1, message d'erreur clair, rien écrit en bronze/
python ingest.py data/inexistant.jsonl
echo "exit code: $?"   # → 1

# Fichier vide : status EMPTY, exit code 2
touch /tmp/empty.jsonl
python ingest.py /tmp/empty.jsonl
echo "exit code: $?"   # → 2

# Fichier 100% corrompu : status WARN (n_valid == 0)
printf 'not json\n{also not json\n' > /tmp/junk.jsonl
python ingest.py /tmp/junk.jsonl
```

### 5. Relancer sur le fichier d'origine

Observer qu'un **nouveau `batch_id`** est généré et que les anciens fichiers ne sont pas écrasés. Le pipeline est strictement **append-only**.

### 6. (Bonus) Variante typée

Lire [`ingest_typed.py`](./ingest_typed.py) — même comportement mais avec `dataclasses`, `Literal` et `Iterator`. Exécuter :

```bash
python ingest_typed.py data/orders.jsonl
```

Produit les mêmes sorties que la version de base. Cette variante est prête pour `mypy` ou `pyright`.

### 7. (Bonus) Tests automatisés

Lire [`tests/test_ingest.py`](./tests/test_ingest.py) — squelette pytest. Lancer (si pytest installé) :

```bash
pip install pytest
pytest tests/ -v
```

## Critères de validation

- [ ] 4 lignes dans `bronze/valid/<batch_id>.jsonl`
- [ ] 3 lignes dans `bronze/rejected/<batch_id>.jsonl` avec un champ `_errors` détaillant la raison
- [ ] Chaque ligne enrichie de `_ingestion_timestamp`, `_batch_id`, `_source_file`
- [ ] 1 ligne JSON dans `logs/ingest.log`
- [ ] Une seconde exécution n'écrase pas la première (nouveau `batch_id`)
- [ ] Fichier inexistant → exit code `1`, aucune trace en bronze
- [ ] Fichier vide → exit code `2` avec `status="EMPTY"`

## Ce qui casse en prod, vraiment

Cinq incidents observés sur ce type de pipeline en production — chacun corrigé par une ligne défensive du `ingest.py` :

1. **Fichier source 0 octet** → notre version gère `EMPTY` → exit 2. Sans ça, log `OK n_read=0` trompeur, le métier découvre l'absence de données 4 jours plus tard.
2. **Encodage non UTF-8** → `errors="replace"` accepte sans crasher, données altérées vont en rejected. Monitorer `n_replaced` en V2.
3. **Volume × 10 du jour au lendemain** → notre boucle streaming (`for lineno, raw in enumerate(f, 1)`) tient. `f.read()` aurait OOM-crash le serveur.
4. **Deux exécutions concurrentes** → deux `batch_id` distincts, pas de doublon, mais concurrence pour les ressources. Verrou (`flock`, fichier `.lock`) ou orchestrateur (Airflow, Argo) en V2.
5. **Espace disque saturé** → bronze grossit, personne ne purge. Politique de rétention dès J0 + monitoring d'espace disque.

## Mini-exercices

> **Exercice L0.A** — Modifier `validate()` pour rejeter les enregistrements où `amount` est négatif. Quel motif d'erreur ajoutez-vous ? Convention recommandée : `amount_negative` (snake_case, préfixe sémantique `amount_*`).

> **Exercice L0.B** — Ajouter à `validate()` la vérification que `created_at` est une chaîne ISO 8601 parsable. Indice : `datetime.fromisoformat()` lève `ValueError` si invalide. Motif suggéré : `invalid_iso_timestamp`.

> **Exercice L0.C** — Modifier le pipeline pour rejeter les ordres avec `amount > 100000` (suspicion de fraude). Faut-il les mettre en `rejected` ou créer un dossier `bronze/suspicious/` ? *Réponse* : préférer une 3ᵉ branche `suspicious/` ou un champ `_flagged` — ce n'est pas une donnée invalide, c'est une donnée à vérifier.

## Pour aller plus loin

Ce lab est le **pattern fondateur** du parcours. Il sera décliné dans :

- **Ingestion API & fichiers** (labs intégrés au chapitre) — même pattern, source = API REST avec pagination + retry
- **Ingestion SQL incrémentale** (labs intégrés au chapitre) — même pattern, source = PostgreSQL avec watermark
- **Fondamentaux Kafka** (labs [L1](../L1-setup/lab.md) + [L2](../L2-python-producers-consumers/lab.md)) — même pattern, transport = Kafka
- **Écosystème Kafka / CDC** (lab [L4](../L4-kafka-connect-cdc/lab.md)) — même pattern, validation = Avro + Schema Registry, rejected = topic DLQ Kafka
- **Spark Structured Streaming** (lab [L5](../L5-pyspark-streaming/lab.md)) — même pattern, exécuté en distribué par Spark Structured Streaming

La géométrie *valid / rejected / log* est non négociable. Seuls le transport et le moteur changent.

## Dépannage

| Symptôme | Cause probable | Action |
|---|---|---|
| Mes logs JSON sont vides | Fichier source absent → pipeline stoppé avant écriture | Vérifier exit code (1 = source absente) |
| Mes logs JSON sont vides | Droits écriture sur `logs/` | `chmod +w logs/` |
| `bronze/valid/` contient 5 lignes au lieu de 4 | Ligne 6 (JSON invalide) acceptée à tort | Vérifier que `json.JSONDecodeError` est bien attrapé |
| `bronze/rejected/` contient 4 lignes au lieu de 3 | Ligne vide écrite en rejected | Vérifier la condition `if not raw.strip(): continue` |
| Exception sur fichier 0 octet | Cas EMPTY non géré | Tester `if n_read == 0: status = "EMPTY"; return 2` |
