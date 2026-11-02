# File: source/Dockerfile — image custom-generator (service `generator`, compose/source.yml)
FROM python:3.12.16-slim-bookworm

# logs straight to stdout (Docker json-file -> Alloy -> Loki), no .pyc files
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator.py .

# no root needed: it only talks to Postgres over the network
USER nobody

ENTRYPOINT ["python", "generator.py"]
