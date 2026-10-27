# File: airflow/Dockerfile
FROM apache/airflow:2.9.2-python3.11

USER root

# thrift-sasl cho PyHive kết nối Spark Thrift Server
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-dev \
 && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.11.txt"