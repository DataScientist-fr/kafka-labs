# Rendu & évaluation des labs

Ce document explique **comment récupérer les labs, comment rendre ton travail, et comment tu es évalué**. À lire avant de commencer.

---

## 1. Comment tu es noté

| Volet | Quoi | Évaluation |
|---|---|---|
| **Connaissances** | Les quiz de chaque chapitre | **Automatique** — score enregistré, seuil 70 %, 3 tentatives |
| **Pratique** | Le **Projet** de chaque module (labs L0 → L8) | **Dépôt sur l'activité Projet**, évalué par le formateur (tests d'acceptation + revue) |

Pour chaque lab, un **test d'acceptation** vérifie le *résultat* attendu (pas ton code ligne par ligne). Tu peux le lancer toi-même pour savoir où tu en es **avant** de déposer.

---

## 2. Workflow de rendu (dépôt sur la plateforme)

1. **Télécharge le ZIP des labs** attaché à l'activité **Projet** du module (section « fichiers associés »). Il contient tous les labs (`labs/L0` … `labs/L8`), la stack Docker (`docker/`) et ce guide.
2. **Décompresse** et complète les `TODO` du lab concerné, en suivant son `lab.md`.
3. **Auto-vérifie** : démarre la stack, exécute le lab, puis lance le test d'acceptation (cf. §3).
4. **Dépose ton rendu** (code + captures ou sortie qui prouvent le résultat) via le bouton **« Soumettre »** de l'activité Projet. C'est ce dépôt que le formateur évalue.

> Astuce : dépose une archive claire (un dossier par lab) + un court `NOTES.md` expliquant tes choix et difficultés — ça valorise ta démarche.

> Le code de référence est aussi consultable sur GitHub : **github.com/DataScientist-fr/kafka-labs** (les corrigés des `TODO` n'y sont pas — volontaire).

---

## 3. Lancer les tests d'acceptation

Chaque lab a un test dans `labs/L<n>-.../tests/test_acceptance.py`. Pré-requis : la **stack Docker du lab démarrée** et le **lab exécuté** (les tests vérifient l'état laissé par ton run).

```bash
# une fois par machine
pip install pytest confluent-kafka requests fastavro minio

# depuis le dossier d'un lab, après avoir exécuté le lab :
cd labs/L2-python-producers-consumers
pytest tests/ -v -m acceptance
```

- Un test **vert** = l'objectif observable du lab est atteint.
- Un test **skipped** = la stack ou la sortie attendue est introuvable (relis les prérequis du `lab.md`).
- Un test **rouge** = le message d'erreur t'indique ce qui manque.

> ⚠️ Rappel : sur la stack centrale, toute commande `docker exec kafka1 kafka-…` doit neutraliser l'agent JMX : `docker exec -e KAFKA_OPTS= kafka1 kafka-topics …`

---

## 4. Definition of Done (par lab)

Ce qui doit être vrai pour considérer un lab **réussi**. (Les tests d'acceptation contrôlent ces points.)

### L0 — Quickstart ingestion
- [ ] Le pipeline d'ingestion produit `bronze/valid/` et `bronze/rejected/` avec métadonnées (`_ingestion_timestamp`, `_batch_id`, `_source_file`).
- [ ] Les tests unitaires fournis passent (`pytest`).

### L1 — Setup cluster
- [ ] Cluster KRaft 3 brokers en bonne santé, quorum formé (`kafka-metadata-quorum … describe --status`).
- [ ] Topic répliqué créé (`RF=3`, `min.insync.replicas=2`) ; produce → consume fonctionnel.

### L2 — Producers/Consumers Python
- [ ] Topic `orders.json` alimenté (`producer_simple.py`).
- [ ] Topic `orders.avro` créé (3 partitions) et alimenté (`producer_avro.py`).
- [ ] Sujet `orders.avro-value` enregistré au Schema Registry (record `Order`, namespace `fr.formation.kafka.orders`, 5 champs).
- [ ] Messages Avro au format wire Confluent, désérialisables par un consumer.

### L3 — Producers/Consumers Scala
- [ ] Le projet **compile** (`sbt compile`).
- [ ] Topics `orders.scala` et `orders.scala.avro` (3 partitions) alimentés (≥ 20 messages chacun).
- [ ] Sujet `orders.scala.avro-value` enregistré (même contrat `Order` que L2) ; messages Avro désérialisables.

### L4 — Kafka Connect / CDC
- [ ] Connecteur source `debezium-postgres-source` `RUNNING` (connecteur **et** tasks).
- [ ] Topics CDC `ecommerce.public.{customers,orders,order_items}` créés, sujets `-key`/`-value` enregistrés.
- [ ] Sink `s3-sink-bronze` `RUNNING` (connecteur **et** tasks).

### L5 — PySpark Streaming → Bronze
- [ ] Table **Bronze Delta** sous `s3://bronze/orders/` (`_delta_log/` + parquet), lisible.
- [ ] Colonnes métier préservées + métadonnées **CDC** (op, ts source) + **traçabilité Kafka** (topic/partition/offset) + **horodatage d'ingestion** (`ingested_at`/`_ingestion_timestamp`).
- [ ] Reprise sans perte ni doublon (checkpoint) ; table non vide.

### L6 — Scala Spark Streaming → Silver
> L6 **consomme le Bronze de L5** et produit la couche **Silver** (agrégations fenêtrées + jointures).
- [ ] Le projet **compile/assemble** (`sbt assembly`) et le job tourne assez longtemps pour finaliser ≥ 1 fenêtre.
- [ ] `s3a://silver/orders_revenue_1m/` au format Delta, schéma tumbling (`window_start, window_end, status, orders_count, revenue`), ≥ 1 ligne.
- [ ] Sliding window `orders_avg_basket_5m`, stream-static join `orders_enriched`, stream-stream join `orders_paid` matérialisés.

### L7 — Event Sourcing & Saga
- [ ] Les 4 services (order, payment, stock, shipping) complétés et lancés.
- [ ] Les tests `tests/test_saga_happy_path.py` **et** `tests/test_saga_compensation.py` passent.

### L8 — Ops & Sécurité
- [ ] Observabilité : les 3 endpoints JMX exposent des métriques, Prometheus scrape les brokers.
- [ ] Cluster sécurisé up ; users SCRAM + ACLs créés.
- [ ] Producteur **autorisé** délivre sur `orders` ; client **non autorisé** est **refusé**.

---

## 5. Intégrité

- Le travail est **individuel** sauf consigne contraire.
- Les corrigés ne sont **pas** fournis (volontaire). Cherche, teste, demande de l'aide — mais le code déposé doit être le tien.
- Les tests d'acceptation valident un *résultat* : reproduire la sortie sans comprendre la démarche ne passe pas la revue.
