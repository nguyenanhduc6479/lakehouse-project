# Kafka Connect with the Debezium Postgres connector. The base image already ships the Confluent
# AvroConverter (/usr/share/java/kafka-serde-tools) and runs Java 11 — which is why Debezium stays
# on 2.7.x (3.x needs Java 17). Phase 4 adds the Iceberg sink to this image.
FROM confluentinc/cp-kafka-connect:7.6.0

ARG DEBEZIUM_VERSION=2.7.4.Final
ARG DEBEZIUM_SHA1=466c8d256f237709fc7786296417d1f424055902

# Plugin tarball from Maven Central, checksum-verified, unpacked into a directory already on
# CONNECT_PLUGIN_PATH (owned by appuser, so no root needed)
RUN curl -fsSL -o /tmp/debezium.tar.gz \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
 && echo "${DEBEZIUM_SHA1}  /tmp/debezium.tar.gz" | sha1sum -c - \
 && tar -xzf /tmp/debezium.tar.gz -C /usr/share/confluent-hub-components \
 && rm /tmp/debezium.tar.gz
