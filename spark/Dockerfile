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
 && curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
 && curl -L -o /opt/spark/jars/iceberg-aws-bundle-1.5.2.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar \
# --- postgres ---
 && curl -L -o /opt/spark/jars/postgresql-42.6.0.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar \
# --- datahub spark lineage (fat jar already bundles datahub-client, no separate download) ---
 && curl -L -o /opt/spark/jars/acryl-spark-lineage_2.12-0.2.18.jar https://repo1.maven.org/maven2/io/acryl/acryl-spark-lineage_2.12/0.2.18/acryl-spark-lineage_2.12-0.2.18.jar \
 && zip -q -d /opt/spark/jars/acryl-spark-lineage_2.12-0.2.18.jar "org/antlr/*" \
# --- kafka client (matches Confluent CP 7.6) ---
 && curl -L -o /opt/spark/jars/kafka-clients-3.6.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar \
 && chmod 644 /opt/spark/jars/*.jar
 
USER spark
 
ENV PYTHONPATH=/opt/spark/jobs