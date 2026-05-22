# Imagem base para os Flyte tasks de consumo_preco.
# Build context: directório consumo_preco/ (pipeline root)
#   docker build -t localhost:30000/consumo-preco:latest -f workflows/Dockerfile .
FROM python:3.11-slim

WORKDIR /app

# Copia todo o sub-pipeline (inclui workflows/ e 04_quality/sql)
COPY . /app/

RUN pip install --no-cache-dir -r /app/workflows/requirements.txt

# Variáveis de ambiente para pods Flyte (serviços via host.docker.internal)
ENV TRINO_HOST=host.docker.internal \
    TRINO_PORT=8080 \
    MINIO_ENDPOINT=http://host.docker.internal:9000 \
    MINIO_ACCESS_KEY=minioadmin \
    MINIO_SECRET_KEY=minioadmin \
    RAW_BUCKET=warehouse
