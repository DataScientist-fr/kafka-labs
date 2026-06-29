# Runbook Production — Plateforme Kafka

> **Objet** : guide opérationnel consolidé pour exploiter le cluster Kafka et son écosystème (Connect, Schema Registry, Spark Structured Streaming) en production. Synthèse des modules **M6.6** (observabilité), **M8** (Kafka en production) et du lab **L8** (ops & sécurité).
>
> **Public** : data engineer d'astreinte, DataOps.
> **À garder ouvert pendant une astreinte.** Chaque incident a : *symptôme → diagnostic → action → vérification*.

---

## 0. Référence rapide (à connaître par cœur)

**Garanties de durabilité standard (topics critiques)**

| Paramètre | Valeur prod | Pourquoi |
|---|---|---|
| `replication.factor` | **3** | tolère la perte d'1 broker (et 1 en upgrade) |
| `min.insync.replicas` | **2** | règle d'or `RF-1` : écriture refusée plutôt que perdue |
| `acks` (producer) | **all** | l'écriture attend les ISR |
| `enable.idempotence` | **true** | pas de doublon sur retry |
| `cleanup.policy` | `delete` (logs) / `compact` (état) | selon l'usage du topic |

**Endpoints**

| Service | URL/port | Usage |
|---|---|---|
| Kafka (interne) | `kafka{1,2,3}:29092` | clients dans le réseau Docker |
| Kafka (hôte) | `localhost:9092-9094` | clients sur l'hôte |
| Kafka UI | `http://localhost:18080` | inspection topics/lag/partitions |
| Connect REST | `http://localhost:8083` | connecteurs (CDC, sinks) |
| Schema Registry | `http://localhost:8081` | schémas Avro + compat |
| Prometheus | `http://localhost:9090` | métriques + alertes |
| Grafana | `http://localhost:13000` | dashboards (lag, URP, throughput) |
| JMX exporter (par broker) | `:7071 / :7072 / :7073` | scrape Prometheus |

> ⚠️ **Piège CLI** : les brokers exposent un agent JMX via `KAFKA_OPTS`. Toute commande `docker exec <broker> kafka-…` plante (JVM FATAL, port JMX déjà pris). **Toujours neutraliser** : `docker exec -e KAFKA_OPTS= kafka1 kafka-topics …`

---

## 1. Golden signals & seuils d'alerte

Les 4 signaux à surveiller en continu (dashboard Grafana + règles Prometheus) :

| Signal | Métrique | Seuil alerte | Gravité |
|---|---|---|---|
| **Controller actif** | `kafka_controller_kafkacontroller_activecontrollercount` | `!= 1` pendant > 30 s | **SEV-1** |
| **Partitions sous-répliquées (URP)** | `kafka_server_replicamanager_underreplicatedpartitions` | `> 0` pendant > 5 min | **SEV-2** |
| **Consumer lag** | `kafka_consumergroup_lag` (kafka-exporter) | `> N min de prod` ou croissance continue | **SEV-2/3** |
| **ISR shrink** | `kafka_server_replicamanager_isrshrinkspersec` | `> 0` répété | **SEV-2** |

Autres à grapher : débit in/out par topic, taille des logs (`kafka_log_log_size`), espace disque broker, latence producer (`request-latency`), erreurs Connect.

> **Lag = `high_watermark − committed_offset`** par partition. Un lag *stable* est normal ; un lag qui *croît* = consumer trop lent / bloqué / rebalance storm.

---

## 2. Opérations courantes

### 2.1 État de santé du cluster (à faire en début d'astreinte)
```bash
# Quorum controllers (doit montrer un LeaderId stable + 3 voters)
docker exec -e KAFKA_OPTS= kafka1 kafka-metadata-quorum \
  --bootstrap-server localhost:29092 describe --status

# Partitions sous-répliquées (doit être vide)
docker exec -e KAFKA_OPTS= kafka1 kafka-topics \
  --bootstrap-server localhost:29092 --describe --under-replicated-partitions

# Lag de tous les groupes
docker exec -e KAFKA_OPTS= kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:29092 --all-groups --describe
```

### 2.2 Redémarrage glissant (rolling restart, sans coupure)
Prérequis : `URP = 0` avant de commencer.
1. Pour chaque broker, **un à la fois** : `docker restart kafka<N>`.
2. Attendre que `URP` revienne à **0** et que le broker rejoigne l'ISR **avant** de passer au suivant.
3. Vérifier `ActiveControllerCount == 1` après chaque étape.
> Ne jamais redémarrer 2 brokers en parallèle avec `RF=3` : on tombe sous `min.insync.replicas=2` → écritures bloquées.

### 2.3 Créer / inspecter un topic
```bash
docker exec -e KAFKA_OPTS= kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic <nom> --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2
docker exec -e KAFKA_OPTS= kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --describe --topic <nom>
```

### 2.4 Augmenter les partitions (⚠️ irréversible, casse l'ordre par clé)
```bash
docker exec -e KAFKA_OPTS= kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --alter --topic <nom> --partitions <N>
```
> On **augmente** seulement, jamais on ne diminue. Augmenter rebat le hash → l'ordre par clé n'est plus garanti pour les **nouveaux** messages. À planifier.

---

## 3. Playbooks d'incident

### 🔴 SEV-1 — `ActiveControllerCount = 0` (ou > 1)
**Symptôme** : alerte controller ; impossible de créer un topic / élire un leader ; metadata figée.
**Diagnostic**
```bash
docker exec -e KAFKA_OPTS= kafka1 kafka-metadata-quorum \
  --bootstrap-server localhost:29092 describe --status   # LeaderId ? CurrentVoters ?
docker logs kafka1 2>&1 | grep -iE "controller|quorum|election|disconnected" | tail
```
- `0` = pas de controller élu (perte de quorum). `> 1` = split-brain.
**Action**
1. Vérifier la connectivité réseau entre les 3 nœuds (le quorum KRaft a besoin de la majorité, 2/3).
2. Si un nœud est down → le redémarrer ; le quorum se reforme dès que 2/3 sont up.
3. Si réseau partitionné → restaurer la connectivité ; **ne pas** forcer/supprimer de metadata.
**Vérif** : `ActiveControllerCount == 1` stable, `kafka-metadata-quorum` montre un LeaderId.

### 🟠 SEV-2 — Broker down / `URP > 0`
**Symptôme** : `UnderReplicatedPartitions > 0`, un broker absent de l'ISR.
**Diagnostic**
```bash
docker ps -a | grep kafka          # quel broker est down ?
docker logs kafka<N> 2>&1 | tail -40
df -h                              # disque plein ? (cause #1)
docker exec -e KAFKA_OPTS= kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --describe --under-replicated-partitions
```
**Action**
- Broker crashé → `docker start kafka<N>` ; il rattrape l'ISR automatiquement.
- Disque plein → voir §4 (rétention) ; libérer avant de redémarrer.
- Tant que **2/3** brokers sont up avec `min.insync.replicas=2`, **les écritures `acks=all` continuent** : pas de perte, pas de blocage. C'est exactement ce que la combinaison protège.
**Vérif** : `URP` revient à 0, le broker est dans tous les ISR.

### 🟠 SEV-2/3 — Lag consumer qui explose
**Symptôme** : alerte lag ; `throughput in` normal mais `out` à 0 / faible (cf. exercice M6.6).
**Diagnostic (dans l'ordre)**
1. Le consumer group est-il **actif** ? `kafka-consumer-groups --describe --group <g>` → colonne `CONSUMER-ID`/`HOST` vide = personne ne consomme.
2. **Rebalance storm** ? logs consumer : `Revoking`/`Rejoining` en boucle → revoir `max.poll.interval.ms`, `session.timeout.ms`, passer à `cooperative-sticky`.
3. **Traitement lent / bloqué** ? le consumer poll mais ne commit pas → profiler le traitement, vérifier un appel externe bloquant.
4. **Pas assez de consumers** ? `nb consumers > nb partitions` → des consumers oisifs ; sinon ajouter des consumers (≤ nb partitions) ou des partitions.
**Action** : redémarrer/scaler le consumer group, corriger le traitement, ajuster les timeouts.
**Vérif** : lag décroît, `throughput out` repart.

### 🟠 Producer : `NotEnoughReplicasException` / écritures refusées
**Symptôme** : producers en erreur, écritures bloquées sur un topic.
**Cause** : moins de `min.insync.replicas` (2) brokers dans l'ISR (souvent 2 brokers down).
**Action** : restaurer un broker (§ broker down). **Ne pas** baisser `min.insync.replicas` à 1 « pour débloquer » → on réintroduit le risque de perte. C'est un refus *volontaire et sain*.

### 🟠 Kafka Connect — connecteur `FAILED`
**Diagnostic**
```bash
curl -s localhost:8083/connectors/<nom>/status | jq .
# regarder connector.state + tasks[].state + trace
curl -s localhost:8083/connectors/<nom>/status | jq -r '.tasks[].trace'
```
**Action**
- Erreur transitoire (source indispo, réseau) → `curl -X POST localhost:8083/connectors/<nom>/restart?includeTasks=true`.
- Erreur de désérialisation / message poison → voir §5 (DLQ).
- CDC Debezium : vérifier le slot WAL côté PostgreSQL (`SELECT * FROM pg_replication_slots;`), l'espace disque du WAL.
**Vérif** : `state == RUNNING` pour le connecteur **et** ses tasks.

### 🟡 Schema Registry — `incompatible schema`
**Symptôme** : un producer ne peut plus publier après un changement de schéma.
**Diagnostic**
```bash
curl -s localhost:8081/subjects/<topic>-value/versions/latest | jq .
curl -s localhost:8081/config/<topic>-value | jq .   # niveau de compat
```
**Action** : la compat par défaut est **BACKWARD** → on peut **ajouter un champ optionnel avec `default`**, jamais en retirer un obligatoire ni changer un type. Corriger le schéma producteur pour respecter BACKWARD ; ne *jamais* relâcher la compat en prod pour « faire passer ».

---

## 4. Rétention & disque

- Rétention par défaut **7 jours** ; ajuster `retention.ms` / `retention.bytes` par topic selon le besoin de replay et la capacité disque.
- **Disque qui se remplit** : identifier les gros topics
  ```bash
  docker exec -e KAFKA_OPTS= kafka1 du -sh /var/lib/kafka/data/* | sort -rh | head
  ```
  → baisser temporairement `retention.ms` sur le topic fautif (`kafka-configs --alter --add-config retention.ms=…`), **jamais** supprimer des segments à la main.
- Headroom cible : viser **× 3** sur le débit et garder > 20 % de disque libre.

---

## 5. Replay & Dead Letter Queue (DLQ)

- **Rejouer un topic depuis le début** (sans toucher au pipeline existant) : lancer un consumer avec un **nouveau `group.id`** (il n'a pas d'offset → `auto.offset.reset=earliest` rejoue tout). Méthode non destructive.
- **DLQ** : les messages non traitables (désérialisation, validation) atterrissent sur `<topic>.DLQ`. Procédure :
  1. Inspecter : `kafka-console-consumer` sur la DLQ (avec `-e KAFKA_OPTS=`), lire le header d'erreur.
  2. Corriger la cause (schéma, mapping, bug consumer).
  3. **Replay contrôlé** : republier les messages DLQ corrigés vers le topic source via le script de replay documenté du pipeline.
- Toujours **logguer/alerter** quand la DLQ se remplit — une DLQ silencieuse = perte de données déguisée.

---

## 6. Sécurité (ops courantes)

> Modèle : SASL/SCRAM + ACL sur le **listener client** ; inter-broker/controller sur réseau de confiance (PLAINTEXT en lab ; **mTLS** recommandé en prod).

**Créer un utilisateur SCRAM**
```bash
kafka-configs --bootstrap-server <broker> --alter \
  --add-config 'SCRAM-SHA-256=[iterations=8192,password=<secret>]' \
  --entity-type users --entity-name <app>
```
**Accorder un droit (principe du moindre privilège)**
```bash
kafka-acls --bootstrap-server <broker> --add \
  --allow-principal User:<app> --producer --topic <topic>
# consumer : --consumer --group <group>
```
- **Rotation de secret** : créer le nouveau credential, déployer côté client, puis retirer l'ancien.
- **Révocation** : `kafka-acls --remove …` ; vérifier avec `kafka-acls --list`.
- `admin` est super-user (bypass ACL) — usage strictement opérationnel.

---

## 7. Matrice de gravité & escalade

| Gravité | Définition | Délai | Exemples |
|---|---|---|---|
| **SEV-1** | Service interrompu / perte de données possible | immédiat, 24/7 | `ActiveControllerCount != 1`, 2 brokers down, perte de quorum |
| **SEV-2** | Dégradé, risque si non traité | < 1 h en heures ouvrées | `URP > 0`, lag croissant, connecteur FAILED |
| **SEV-3** | Mineur, pas d'impact immédiat | jour ouvré suivant | lag stable élevé, métrique borderline, schéma à faire évoluer |

**Escalade** : DataOps d'astreinte → lead data → éditeur/fournisseur (si managé : Confluent/MSK/Event Hubs support).

---

## 8. Checklist « topic production-ready » (avant mise en prod)

- [ ] `RF=3`, `min.insync.replicas=2`, producer `acks=all` + idempotence
- [ ] Nombre de partitions dimensionné (débit × headroom ×3) et clé de partition sans skew
- [ ] Schéma Avro versionné dans Schema Registry, compat **BACKWARD**
- [ ] Rétention définie explicitement (`retention.ms`/`bytes`)
- [ ] DLQ + plan de replay documenté
- [ ] Dashboard Grafana : lag, URP, débit, ISR
- [ ] Alertes Prometheus : ActiveControllerCount, URP, lag, disque
- [ ] ACLs au moindre privilège pour chaque application
- [ ] Runbook à jour + astreinte identifiée

---

*Stack de référence : Kafka KRaft (3 brokers), Schema Registry, Kafka Connect, Spark Structured Streaming, Prometheus/Grafana. Voir labs L1 (cluster), L4 (Connect/CDC), L5/L6 (streaming), L8 (ops & sécurité) et modules M6/M8.*
