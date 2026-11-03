# Kafka Connect with the Debezium Postgres connector (source) and the Iceberg sink. The base image
# already ships the Confluent AvroConverter (/usr/share/java/kafka-serde-tools) and runs Java 11 —
# which pins Debezium to 2.7.x (3.x needs Java 17) and Iceberg to 1.10.x (1.11+ is Java 17
# bytecode). Version choice: docs/roadmap.md, phase 4 step 4.1.

# --- Stage 1: build the Iceberg Kafka Connect runtime zip --------------------------------------
# Not published as an artifact (only the bare connector jar is on Maven Central), so build it from
# the source release. Iceberg's build accepts JDK 11/17/21 and always compiles with `release 11`.
FROM eclipse-temurin:17.0.20_8-jdk-noble AS iceberg-build

ARG ICEBERG_VERSION=1.10.2
ARG ICEBERG_SHA512=7eba5c85037bc150efcea3e06dae6653be961a275f1bc5d5333c9a22d18c67d68c40f6396ee9501bfad15906d9df13858940326ea2258728c8e0d5eedd65b9bd

WORKDIR /src
RUN curl -fsSL -o iceberg.tar.gz \
      "https://archive.apache.org/dist/iceberg/apache-iceberg-${ICEBERG_VERSION}/apache-iceberg-${ICEBERG_VERSION}.tar.gz" \
 && echo "${ICEBERG_SHA512}  iceberg.tar.gz" | sha512sum -c - \
 && tar -xzf iceberg.tar.gz --strip-components=1 \
 && rm iceberg.tar.gz

# Only the runtime distribution (without the Hive metastore client: the catalog is REST). The
# release has no gradle-wrapper.jar; gradlew downloads it. The cache mount keeps Gradle's
# dependency downloads between rebuilds (BuildKit), not in the image.
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon -x test -x integrationTest \
      :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip \
 && mkdir /plugin \
 && cd /plugin \
 && jar xf /src/kafka-connect/kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-${ICEBERG_VERSION}.zip

# --- Stage 2: the Connect worker ----------------------------------------------------------------
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

# Iceberg sink: one plugin directory (manifest.json + lib/*.jar), isolated by Connect's plugin
# class loader from Debezium's jars
COPY --from=iceberg-build --chown=appuser:appuser /plugin/ /usr/share/confluent-hub-components/
