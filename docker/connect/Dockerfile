# Image Kafka Connect avec les plugins nécessaires aux labs :
# - Debezium Postgres (CDC) — Lab L4
# - Confluent S3 Sink (export vers MinIO) — Lab L5
# - JDBC source/sink (option pour Lab L4)
#
# Build : docker compose build connect
# (déclenché automatiquement par `docker compose up`)

FROM confluentinc/cp-kafka-connect:7.6.0

USER root

# Plugins Confluent Hub
# Debezium 2.5 (pgoutput) prend en charge Postgres 17 en logical decoding.
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4-1 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.13 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6 \
 && confluent-hub install --no-prompt confluentinc/connect-transforms:1.4.4

# Dossier pour plugins custom (montés via volume ./connect/connectors)
RUN mkdir -p /connectors && chown appuser:appuser /connectors

USER appuser
