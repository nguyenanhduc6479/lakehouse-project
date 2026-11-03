# File: spark/Dockerfile

FROM apache/spark:3.5.1

USER root

# RUN rm -rf /var/lib/apt/lists/* && apt-get update && \
#     apt-get install -y --no-install-recommends curl zip && \
#     rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y --no-install-recommends curl zip \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pandas pyarrow fastparquet
 
# --- hadoop aws 3.3.4 
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
# --- iceberg (Polaris uses the REST catalog built into iceberg-spark-runtime, no nessie needed) ---
# Same version as the Kafka Connect Iceberg sink (connect/Dockerfile): Spark compacts / expires
# snapshots on the bronze tables the sink keeps committing to, so writer and maintainer must agree.
# 1.10.x is the newest line with Java 11 bytecode (this image runs Java 11).
 && curl -fL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.2.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.2/iceberg-spark-runtime-3.5_2.12-1.10.2.jar \
 && curl -fL -o /opt/spark/jars/iceberg-aws-bundle-1.10.2.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.2/iceberg-aws-bundle-1.10.2.jar \
# --- postgres ---
 && curl -L -o /opt/spark/jars/postgresql-42.6.0.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar \
# --- kafka client (matches Confluent CP 7.6) ---
 && curl -L -o /opt/spark/jars/kafka-clients-3.6.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar \
 && chmod 644 /opt/spark/jars/*.jar

# Worker work dir (driver/executor stdout+stderr in cluster mode). Created here, owned by spark, so a
# fresh named volume mounted on it inherits that ownership — no chown job needed.
RUN mkdir -p /opt/spark/work && chown spark:spark /opt/spark/work

USER spark
 
ENV PYTHONPATH=/opt/spark/jobs