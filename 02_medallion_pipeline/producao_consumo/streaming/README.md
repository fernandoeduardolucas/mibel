# Streaming fictício 30s — producao_consumo

Este fluxo cria dados fictícios de produção/consumo a cada **30 segundos**, publica no Kafka/Redpanda e materializa os dados em **Bronze e Gold**. O Grafana deve ler da camada **Gold**.

## 1) Publicar eventos fictícios

```bash
cd 02_medallion_pipeline/producao_consumo/streaming/scripts/python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements_streaming.txt
python fake_producao_consumo_producer.py
```

## 2) Validar leitura direta no Kafka (Trino)

```sql
SELECT *
FROM kafka.default.producao_consumo_events
LIMIT 20;
```

## 3) Carregar Bronze e Gold

Executar em Trino:

```sql
-- ficheiro: 02_medallion_pipeline/producao_consumo/streaming/sql/01_streaming_to_iceberg.sql
```

Tabelas destino:
- Bronze: `iceberg.bronze.producao_consumo_streaming`
- Gold: `iceberg.gold.dp_producao_consumo_streaming`

## 4) Grafana

Dashboard incluído:
- `Producao Consumo Streaming (30s)`

As queries do dashboard foram definidas para ler da tabela Gold `iceberg.gold.dp_producao_consumo_streaming`.
