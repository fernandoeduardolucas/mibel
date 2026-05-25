# Streaming fictício 30s — producao_consumo

Este fluxo cria dados fictícios de produção/consumo a cada **30 segundos**, publica no Kafka/Redpanda e materializa os dados em **Bronze e Gold**. O Grafana deve ler da camada **Gold**.

> Este módulo é **autónomo**: tudo o que é necessário para o tópico Kafka deste fluxo está nesta pasta (`streaming/kafka_config/producao_consumo_events.json`).



## 0) Pré-check obrigatório (erro: Table kafka.default.producao_consumo_events does not exist)

Antes de executar as queries, confirma no Trino se a tabela Kafka está registada:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM kafka;
SHOW TABLES FROM kafka.default;
```

Deves ver `producao_consumo_events` na lista.

Se **não** aparecer:
1. Confirmar que o ficheiro local existe: `02_medallion_pipeline/DP01_Producao_Consumo/streaming/kafka_config/producao_consumo_events.json`.
2. Garantir que este ficheiro está montado no Trino em `/etc/trino/kafka/producao_consumo_events.json` (copiar/sincronizar para `01_docker_stack/kafka_config/`).
3. Reiniciar o Trino para recarregar `/etc/trino/kafka`:

```bash
cd 01_docker_stack
docker compose restart trino
```

4. Voltar a validar:

```sql
SHOW TABLES FROM kafka.default;
DESCRIBE kafka.default.producao_consumo_events;
```


### Sincronizar configuração Kafka deste módulo

Para manter o streaming de `producao_consumo` isolado de outras partes, a definição do tópico fica versionada aqui:

- `02_medallion_pipeline/DP01_Producao_Consumo/streaming/kafka_config/producao_consumo_events.json`

Quando necessário, sincroniza para a stack Docker:

```bash
cp 02_medallion_pipeline/DP01_Producao_Consumo/streaming/kafka_config/producao_consumo_events.json \
   01_docker_stack/kafka_config/producao_consumo_events.json
```

## 1) Publicar eventos fictícios

```bash
cd 02_medallion_pipeline/DP01_Producao_Consumo/streaming/scripts/python
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements_streaming.txt
python fake_producao_consumo_producer.py
```

## 2) Validar leitura direta no Kafka (Trino)

```sql
SELECT *
FROM kafka.default.producao_consumo_events
LIMIT 20;
```

## 3) Publicar e materializar automaticamente (num único script)

Executar apenas o `fake_producao_consumo_producer.py` (ele publica no Kafka **e** materializa para Bronze/Gold automaticamente):

```bash
cd 02_medallion_pipeline/DP01_Producao_Consumo/streaming/scripts/python
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements_streaming.txt
python fake_producao_consumo_producer.py --trino-host localhost --trino-port 8080 --interval-seconds 30
```

O script executa o SQL incremental (`sql/01_streaming_to_iceberg.sql`) após cada publicação, sem necessidade de correr `INSERT` manualmente.

Tabelas destino:
- Bronze: `iceberg.bronze.producao_consumo_streaming`
- Gold: `iceberg.gold.dp_producao_consumo_streaming`


Notas importantes:
- Os dois `INSERT` estão preparados para execução **incremental** por `event_id` (podem ser reexecutados sem duplicar dados).
- Para atualização contínua, mantenha o `fake_producao_consumo_producer.py` ativo (ex.: `--interval-seconds 30`).

## 4) Grafana

Dashboard incluído:
- `Producao Consumo Streaming (30s)`

As queries do dashboard foram definidas para ler da tabela Gold `iceberg.gold.dp_producao_consumo_streaming`.


## 5) Troubleshooting (Gold com 0 rows)

Se `SELECT * FROM iceberg.gold.dp_producao_consumo_streaming` devolver 0 linhas, normalmente é porque o materializador automático não está ativo ou ainda não completou um ciclo após novos eventos no Kafka.

Importante: o ficheiro `sql/01_streaming_to_iceberg.sql` continua em modo **batch/snapshot**, mas agora é executado automaticamente pelo `fake_producao_consumo_producer.py` em cada ciclo.

Ordem recomendada:

1. Iniciar produtor e aguardar alguns eventos.
2. Verificar se o tópico tem dados:

```sql
SELECT count(*) AS kafka_rows
FROM kafka.default.producao_consumo_events;
```

3. Confirmar que `fake_producao_consumo_producer.py` está em execução (sem erros no terminal e com mensagens `[OK]` de materialização).

4. Confirmar contagens:

```sql
SELECT count(*) AS bronze_rows FROM iceberg.bronze.producao_consumo_streaming;
SELECT count(*) AS gold_rows   FROM iceberg.gold.dp_producao_consumo_streaming;
```

Se `kafka_rows > 0` e `bronze_rows = 0`, o `INSERT Kafka -> Bronze` falhou ou não foi executado no mesmo ambiente do broker `localhost:19092`.


## 6) Verificação rápida ponta-a-ponta (Kafka -> Gold)

Se o produtor está a imprimir `Publicado: ...`, valida em 3 passos:

```sql
-- A) Kafka está a receber
SELECT count(*) AS kafka_rows
FROM kafka.default.producao_consumo_events;

-- B) Garantir que o producer com materialização automática está a correr
-- script: 02_medallion_pipeline/DP01_Producao_Consumo/streaming/scripts/python/fake_producao_consumo_producer.py

-- C) Gold está a receber
SELECT count(*) AS gold_rows
FROM iceberg.gold.dp_producao_consumo_streaming;

-- D) Últimos eventos no Gold
SELECT event_id, event_ts, consumo_total_kwh, producao_total_kwh, saldo_kwh
FROM iceberg.gold.dp_producao_consumo_streaming
ORDER BY event_ts DESC
LIMIT 10;
```

Critério objetivo:
- `kafka_rows` deve aumentar enquanto o produtor está ligado.
- com o materializador ativo, `gold_rows` também deve aumentar ao longo dos ciclos.
- os `event_id`/`event_ts` mais recentes no Gold devem corresponder aos que o produtor acabou de publicar.

