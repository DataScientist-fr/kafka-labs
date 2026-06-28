# Lab L7 — Event sourcing + Saga sur use case e-commerce

**Durée** : 2h
**Stack** : Python 3.10+, confluent-kafka, fastavro, pydantic, Postgres optionnel (outbox)

> **Cours associé** : **lab d'approfondissement** — pas rattaché à un chapitre obligatoire de B1-M06. Mobilise les concepts vus en M7 (CDC) (outbox pattern) et en T2 — Event-Driven Architecture (event sourcing). À recommander aux apprenants qui veulent aller plus loin que le bronze layer et toucher au design patterns event-driven complets.

## Objectifs

À l'issue de ce lab, l'apprenant sera capable de :

- Implémenter un système **event-sourced** : les événements sont la source de vérité, l'état est dérivé par fold.
- Comprendre la **compaction** Kafka et son rôle pour les snapshots.
- Implémenter une **saga choreography** : pas d'orchestrateur, chaque service réagit aux événements.
- Implémenter une **saga orchestration** (challenge) : un orchestrateur central pilote les étapes.
- Garantir l'**idempotence** des consumers et la **compensation** en cas d'échec.
- Faire le lien avec la théorie : **T2** (EDA, saga, outbox) et **T4** (les events alimentent le bronze layer pour analytics).

## Prérequis

- L1 (cluster Kafka up via `docker compose up`)
- L2 (Python + Avro + Schema Registry)
- T2 lu et compris (event sourcing, saga, outbox, idempotence)

Vérifier l'infra :

```bash
docker compose ps
curl -s http://localhost:8081/subjects   # Schema Registry doit répondre
psql -h localhost -U postgres -d ecommerce -c "select 1"   # optionnel pour outbox
```

Variables d'environnement utilisées :

| Variable             | Défaut                                                       |
|----------------------|--------------------------------------------------------------|
| `BOOTSTRAP_SERVERS`  | `localhost:9092,localhost:9093,localhost:9094`               |
| `SCHEMA_REGISTRY_URL`| `http://localhost:8081`                                      |
| `PG_DSN`             | `postgresql://postgres:postgres@localhost:5432/ecommerce`    |

Installer les dépendances :

```bash
cd labs/L7-event-sourcing-saga
pip install -r requirements.txt
```

## Architecture

Le use case **e-commerce** simulé : une commande passe par les états

```
Created -> Paid -> Reserved -> Shipped -> Delivered
              \         \        \
               -> Cancelled (compensation à chaque étape)
```

Quatre services indépendants collaborent :

- **OrderService** : reçoit les commandes, émet `OrderCreated`, écoute la suite pour mettre à jour l'agrégat.
- **PaymentService** : écoute `OrderCreated`, émet `PaymentCompleted` ou `PaymentFailed`.
- **StockService** : écoute `PaymentCompleted`, émet `StockReserved` ou `StockOutOfStock` (et `StockReleased` pour compensation).
- **ShippingService** : écoute `StockReserved`, émet `OrderShipped` puis `OrderDelivered`.

Trois topics structurent le flux :

| Topic                | Rétention      | Compaction  | Partition key  | Rôle                                                |
|----------------------|----------------|-------------|----------------|-----------------------------------------------------|
| `orders.commands`    | 7 jours        | non         | `order_id`     | Commandes adressées au système (PlaceOrder, ...)    |
| `orders.events`      | 30 jours       | non         | `order_id`     | Événements émis (OrderCreated, PaymentCompleted...) |
| `orders.snapshots`   | infini         | **oui**     | `order_id`     | Dernière vue de chaque order, bootstrap rapide      |

Le partitionnement par `order_id` garantit que **tous les événements d'un même order arrivent dans l'ordre** sur la même partition (propriété indispensable pour le replay).

### Saga choreography vs saga orchestration

Saga choreography : pas de chef d'orchestre. Chaque service écoute les events qui le concernent et émet à son tour.

<!-- mermaid-source
%%{init: {'theme':'base', 'themeVariables': {'primaryColor':'#1F2937','primaryTextColor':'#F9FAFB','primaryBorderColor':'#374151','lineColor':'#6366F1','fontFamily':'Inter, system-ui, sans-serif','fontSize':'14px'}}}%%
sequenceDiagram
    participant Order as OrderService
    participant Payment as PaymentService
    participant Stock as StockService
    participant Ship as ShippingService
    Order->>Payment: OrderCreated
    Payment->>Stock: PaymentCompleted
    Stock->>Ship: StockReserved
    Ship--&gt;>Order: OrderShipped
    Ship--&gt;>Order: OrderDelivered
    rect rgb(255,228,225)
    Note over Order,Ship: Compensation : OutOfStock<br/>chaque service défait sa propre étape
    Stock->>Payment: StockOutOfStock
    Payment->>Order: PaymentRefunded
    Order--&gt;>Order: OrderCancelled
    end
-->
![Saga choreography vs saga orchestration](../../figures/L7/01.svg)
[Source Excalidraw](../../figures/L7/01.excalidraw)

Saga orchestration : un orchestrateur centralise la logique du workflow.

<!-- mermaid-source
%%{init: {'theme':'base', 'themeVariables': {'primaryColor':'#1F2937','primaryTextColor':'#F9FAFB','primaryBorderColor':'#374151','lineColor':'#6366F1','fontFamily':'Inter, system-ui, sans-serif','fontSize':'14px'}}}%%
sequenceDiagram
    participant Client
    participant Orch as OrderOrchestrator
    participant Payment as PaymentService
    participant Stock as StockService
    participant Ship as ShippingService
    Client->>Orch: PlaceOrder (command)
    Orch->>Payment: ChargePayment (command)
    Payment--&gt;>Orch: PaymentCompleted
    Orch->>Stock: ReserveStock (command)
    Stock--&gt;>Orch: StockReserved
    Orch->>Ship: ShipOrder (command)
    Ship--&gt;>Orch: OrderShipped
    rect rgb(255,228,225)
    Note over Orch,Ship: Sur StockOutOfStock, l'orchestrateur<br/>déclenche RefundPayment puis CancelOrder
    Stock--&gt;>Orch: StockOutOfStock
    Orch->>Payment: RefundPayment (command)
    end
-->
![Saga choreography vs saga orchestration](../../figures/L7/02.svg)
[Source Excalidraw](../../figures/L7/02.excalidraw)

Tableau de comparaison à retenir :

| Aspect                  | Chorégraphie                          | Orchestration                                |
|-------------------------|---------------------------------------|-----------------------------------------------|
| Couplage                | Faible entre services                 | Services couplés à l'orchestrateur            |
| Visibilité workflow     | Diffuse, à reconstituer via les events| Centralisée, lisible dans un seul fichier     |
| Évolution               | Ajouter un service = nouvel abonnement| Modifier l'orchestrateur                      |
| Risque                  | Cascades imprévues, debugging difficile| SPOF (orchestrateur), complexité interne     |
| Outils typiques         | Kafka pur                             | Temporal, Camunda, Step Functions             |
| Recommandation          | < 4-5 services, workflow court        | Workflow long, beaucoup de branches           |

> Règle pratique (vue en T2) : **chorégraphie pour < 4-5 services**, **orchestration au-delà** ou si le workflow a beaucoup de branches conditionnelles et compensations multiples.

## Étape 1 — Modéliser les events (Avro)

Les schémas sont dans `schemas/`. Lecture obligatoire avant de coder :

- `order_created.avsc` : émis par OrderService, contient items et amount.
- `payment_completed.avsc`, `payment_failed.avsc` : décisions du PaymentService.
- `stock_reserved.avsc`, `stock_out_of_stock.avsc`, `stock_released.avsc` : décisions du StockService et compensation.
- `order_shipped.avsc`, `order_delivered.avsc` : émis par ShippingService.
- `order_cancelled.avsc` : état terminal d'échec.
- `order_snapshot.avsc` : état complet courant d'un Order (utilisé pour le topic compacté).

Tous les events partagent un préambule commun :

| Champ      | Type   | Rôle                                                       |
|------------|--------|-------------------------------------------------------------|
| `event_id` | string | UUID unique, **clé d'idempotence** côté consumer.           |
| `order_id` | string | Clé de partitionnement (toutes les events du même order vont sur la même partition). |
| `occurred_at`| long (timestamp-millis) | Quand l'événement s'est produit côté producer.    |
| `version`  | int    | Version de l'agrégat après application — utile pour replay et concurrence optimiste. |

> Bonne pratique : ne **jamais** muter un event Avro déjà publié. Si la sémantique évolue, créer un nouveau schéma et faire migrer les consumers (cf. T2 §6).

## Étape 2 — OrderService (squelette)

Ouvrir `services/order_service.py`. Compléter les zones `TODO` :

1. Consumer sur `orders.commands` filtré sur le type `PlaceOrder`.
2. Émettre `OrderCreated` sur `orders.events` (clé = `order_id`).
3. Consumer parallèle sur `orders.events` :
   - sur `OrderShipped` -> mettre à jour l'état interne en mémoire.
   - sur `PaymentFailed` ou `StockOutOfStock` -> émettre `OrderCancelled`.
4. Maintenir un `set[str]` des `event_id` déjà traités pour l'idempotence.

Lancer :

```bash
python -m services.order_service
```

## Étape 3 — PaymentService (squelette)

`services/payment_service.py` :

1. Consumer sur `orders.events`, filtre sur `OrderCreated`.
2. Logique simulée : `total > 1000` -> `PaymentFailed` (montant trop élevé), sinon `PaymentCompleted` avec un `transaction_id`.
3. Idempotence : si l'`order_id` a déjà déclenché un paiement, ne rien refaire.

Lancer :

```bash
python -m services.payment_service
```

## Étape 4 — StockService et compensation

`services/stock_service.py` :

1. Consumer sur `orders.events`, filtre sur `PaymentCompleted`.
2. Stock simulé en mémoire : un dict `{sku -> qty}`. Décrément si possible -> `StockReserved`. Sinon -> `StockOutOfStock`.
3. **Compensation** : sur `OrderCancelled` après `StockReserved`, émettre `StockReleased` et restituer la quantité.

> Note pédagogique : la compensation est une **opération sémantique**, pas un rollback technique. `StockReleased` est un nouvel événement, pas un effacement.

## Étape 5 — ShippingService

`services/shipping_service.py` :

1. Consumer sur `orders.events`, filtre sur `StockReserved`.
2. Émet `OrderShipped` (avec un `tracking_number` aléatoire), puis après quelques secondes simule la livraison avec `OrderDelivered`.

## Étape 6 — Saga test : happy path

Lancer le test pytest qui passe une commande dont le total est faible et le stock disponible :

```bash
pytest tests/test_saga_happy_path.py -v
```

Le test :

1. Démarre les 4 services en threads séparés (ou attend qu'ils tournent en background).
2. Publie une command `PlaceOrder` sur `orders.commands`.
3. Consomme `orders.events` jusqu'à voir `OrderShipped` (timeout 30s).
4. Vérifie la séquence : `OrderCreated` -> `PaymentCompleted` -> `StockReserved` -> `OrderShipped`.

## Étape 7 — Compensation : forcer un OutOfStock

```bash
pytest tests/test_saga_compensation.py -v
```

Ce test :

1. Pré-vide le stock du SKU testé.
2. Publie une commande dessus.
3. Vérifie la chaîne de compensation : `OrderCreated` -> `PaymentCompleted` -> `StockOutOfStock` -> `OrderCancelled` (et idéalement un `PaymentRefunded` côté solution complète).

## Étape 8 — Reconstruire l'état d'un Order par replay

Ouvrir `replay/replay_order.py`. Compléter la fonction `replay_order(order_id)` :

1. Créer un consumer **éphémère** (group.id unique aléatoire) avec `auto.offset.reset=earliest`.
2. Souscrire à `orders.events`.
3. Lire **toutes** les partitions jusqu'à atteindre les end-offsets connus au démarrage.
4. Filtrer par `order_id`.
5. Appliquer les events un par un sur un agrégat initialement vide (fold).

Lancer :

```bash
python -m replay.replay_order <order_id>
```

> Pédagogie : c'est exactement ce que fait un système event-sourced quand un service redémarre. Comparé à une lecture SQL, on paie un coût de scan, **mais** :
>
> - on a un audit complet (chaque transition).
> - on peut reconstruire **toute** projection (finance, analytics, ML feature) à partir du même log.
> - on peut faire du **time-travel** : `replay until version=N`.

## Étape 9 — Snapshot via topic compacté

Pour éviter de relire 1M d'events à chaque redémarrage, on émet périodiquement un **snapshot** de l'état courant dans un **topic compacté**.

Compaction Kafka :

- La rétention par compaction conserve **la dernière valeur par clé** (ici `order_id`).
- Combiné avec une rétention temporelle, on peut nettoyer les snapshots historiques tout en gardant l'état courant.

Compléter `replay/snapshot_writer.py` :

1. Consume `orders.events`.
2. Maintient en mémoire l'agrégat de chaque order (fold incrémental).
3. Toutes les N events ou tous les K secondes, émet un `OrderSnapshot` sur `orders.snapshots` avec `key=order_id`.
4. Au démarrage d'un service consommateur, on **bootstrap** depuis `orders.snapshots` (lecture rapide, dernière version par order), puis on rejoue les events depuis la version du snapshot.

Configurer le topic compacté :

```bash
docker exec -e KAFKA_OPTS= kafka1 kafka-configs --bootstrap-server kafka1:29092 \
  --entity-type topics --entity-name orders.snapshots --alter \
  --add-config cleanup.policy=compact,min.cleanable.dirty.ratio=0.1,segment.ms=60000
```

> Lien avec T4 : ces snapshots peuvent aussi alimenter le **silver layer** d'un lakehouse (vue à jour de chaque order) tandis que `orders.events` alimente le **bronze layer** (historique immuable).

## Validation

- [ ] Topics `orders.commands`, `orders.events`, `orders.snapshots` créés (`./setup_topics.sh`)
- [ ] `orders.snapshots` configuré avec `cleanup.policy=compact`
- [ ] Schémas Avro enregistrés au Schema Registry (`curl /subjects`)
- [ ] Les 4 services tournent sans erreur, on voit les events dans Kafka UI
- [ ] `pytest tests/test_saga_happy_path.py` passe (commande -> shipped)
- [ ] `pytest tests/test_saga_compensation.py` passe (OutOfStock -> cancelled)
- [ ] `replay_order.py <id>` reconstruit l'état d'un order à partir des events
- [ ] `snapshot_writer.py` produit des snapshots dans le topic compacté

## Pour aller plus loin (challenge)

### 1. Saga orchestration

Implémenter `solutions/L7-event-sourcing-saga/services/orchestrator.py` :

- Un seul consumer écoute `orders.events`.
- Une state machine en mémoire (ou en Postgres) suit chaque saga.
- L'orchestrateur émet des commands explicites : `ChargePayment`, `ReserveStock`, `ShipOrder`, et leurs compensations `RefundPayment`, `ReleaseStock`.

Comparer la lisibilité du workflow avec la version chorégraphiée.

### 2. Outbox pattern (Postgres + Debezium)

Cf. T2 §4.5. Au lieu de produire directement dans Kafka depuis le service, on écrit dans une table `outbox` **dans la même transaction** que la donnée métier :

```sql
BEGIN;
INSERT INTO orders (id, status) VALUES (...);
INSERT INTO outbox (event_type, payload) VALUES ('OrderCreated', '{...}');
COMMIT;
```

Un connecteur Debezium lit ensuite la table outbox et publie dans Kafka.

Voir `solutions/L7-event-sourcing-saga/outbox/postgres_schema.sql` et `outbox/outbox_writer.py` pour l'implémentation simulée (sans Debezium, on poll la table).

### 3. Idempotence dure (test)

`solutions/L7-event-sourcing-saga/tests/test_idempotence.py` republie deux fois le même événement et vérifie que l'état final est inchangé.

### 4. Lien analytics (T4)

Ajouter un connecteur Kafka Connect S3 / Iceberg sur `orders.events` pour alimenter un **bronze layer**, et faire une projection Spark Structured Streaming (cf. L5) pour calculer un dashboard `commandes par jour x état`.

## Dépannage

| Symptôme                                                 | Cause probable                                                | Solution                                                              |
|----------------------------------------------------------|---------------------------------------------------------------|-----------------------------------------------------------------------|
| `KafkaError{code=_TRANSPORT}`                            | Brokers non joignables                                        | `docker compose ps`, vérifier ports 9092-9094                          |
| `SchemaRegistryError: Subject not found`                 | Le service n'a pas encore publié son schéma                   | Démarrer les services dans l'ordre : Order, Payment, Stock, Shipping  |
| Événements consommés deux fois                            | Pas d'idempotence côté consumer                               | Maintenir un set d'`event_id` traités, vérifier avant d'agir          |
| Saga bloquée à `OrderCreated`                            | PaymentService ne consomme pas `orders.events`                | Vérifier `group.id` unique par service, `auto.offset.reset=earliest`  |
| Replay très lent                                          | Pas de snapshot, on relit 1M events                           | Activer `snapshot_writer.py` et bootstrap depuis `orders.snapshots`   |
| `orders.snapshots` ne compacte pas                        | `cleanup.policy=delete` (défaut) ou pas assez de dirty ratio  | Voir étape 9, vérifier avec `kafka-configs --describe`                |
| Compensation jamais déclenchée                            | Le consumer du service de compensation ignore l'event source  | Vérifier les filtres par type d'event dans chaque service             |
| Plusieurs services réagissent au même event différemment  | Mauvais `group.id` partagé                                    | Chaque service a **son propre** `group.id` (sinon load-balancing)     |
