# 1. Objetivo

Este documento define o schema técnico real (alinhado com o DDL implementado) das tabelas Bronze e Silver do projeto, incluindo colunas, tipos de dados, papel de cada campo e principais regras de transformação entre camadas.

---

# 2. Schema Bronze

## 2.1 `bronze.consumo_raw`

**Tabela Iceberg:** `iceberg.bronze.consumo_raw`  
**Localização MinIO:** `s3a://warehouse/bronze/consumo_raw/`  
**Origem:** `consumo-total-nacional.csv` (fonte REN)  
**Granularidade de origem:** 15 minutos  
**Formato:** Parquet (Iceberg format_version=2)  
**Particionamento:** `process_date`  
**Função:** Preservar a estrutura da fonte de consumo com metadados de ingestão mínimos. Sem transformações semânticas.

### Colunas

| Coluna       | Tipo                        | Obrigatória | Origem          | Descrição |
|--------------|-----------------------------|-------------|-----------------|-----------|
| datahora     | TIMESTAMP(6) WITH TIME ZONE | Sim         | Fonte           | Timestamp original da fonte (UTC) para o registo de 15 min |
| dia          | INTEGER                     | Não         | Fonte           | Dia do mês (campo redundante da fonte, preservado para rastreabilidade) |
| mes          | INTEGER                     | Não         | Fonte           | Mês (campo redundante da fonte) |
| ano          | INTEGER                     | Não         | Fonte           | Ano (campo redundante da fonte) |
| date_raw     | VARCHAR                     | Não         | Fonte           | Campo `date` original em string |
| time_raw     | VARCHAR                     | Não         | Fonte           | Campo `time` original em string |
| bt           | DOUBLE                      | Não         | Fonte           | Consumo Baixa Tensão (kW) |
| mt           | DOUBLE                      | Não         | Fonte           | Consumo Média Tensão (kW) |
| at           | DOUBLE                      | Não         | Fonte           | Consumo Alta Tensão (kW) |
| mat          | DOUBLE                      | Não         | Fonte           | Consumo Muito Alta Tensão (kW) |
| total        | DOUBLE                      | Sim         | Fonte           | Consumo total nacional no intervalo de 15 min (kW) |
| process_date | DATE                        | Sim         | Derivada        | Data lógica de ingestão — chave de partição e idempotência |

### Propriedades Iceberg (catálogo)

| Propriedade     | Valor          |
|-----------------|----------------|
| layer           | bronze         |
| domain          | consumo_preco  |
| schema_version  | 1              |
| retention_policy| indefinite     |
| source_system   | ren_csv        |

### Regras
- Preservar o valor original de todas as colunas da fonte sem transformação semântica
- `datahora` é ingerido como TIMESTAMP WITH TIME ZONE (já interpretado como UTC pelo script de limpeza)
- Não agregar nem converter granularidade nesta camada
- `process_date` é a chave de partição — permite DELETE + INSERT idempotente por dia

---

## 2.2 `bronze.preco_raw`

**Tabela Iceberg:** `iceberg.bronze.preco_raw`  
**Localização MinIO:** `s3a://warehouse/bronze/preco_raw/`  
**Origem:** `Day-ahead Market Prices_*.csv` (fonte OMIE)  
**Granularidade de origem:** horária (horas 1–25)  
**Formato:** Parquet (Iceberg format_version=2)  
**Particionamento:** `process_date`  
**Função:** Preservar os dados tabulares MIBEL sem interpretar semanticamente a numeração de horas.

### Colunas

| Coluna              | Tipo    | Obrigatória | Origem   | Descrição |
|---------------------|---------|-------------|----------|-----------|
| date_raw            | VARCHAR | Sim         | Fonte    | Data original da linha em string (fonte OMIE) |
| hour                | INTEGER | Sim         | Fonte    | Hora original OMIE (1–24 normal; 25 em mudança DST de outono) |
| price_portugal_raw  | DOUBLE  | Sim         | Fonte    | Preço day-ahead de Portugal (€/MWh) |
| price_spain_raw     | DOUBLE  | Não         | Fonte    | Preço day-ahead de Espanha (€/MWh) — preservado para referência futura |
| process_date        | DATE    | Sim         | Derivada | Data lógica de ingestão — chave de partição e idempotência |

### Propriedades Iceberg (catálogo)

| Propriedade     | Valor          |
|-----------------|----------------|
| layer           | bronze         |
| domain          | consumo_preco  |
| schema_version  | 1              |
| retention_policy| indefinite     |
| source_system   | omie_csv       |

### Regras
- Não interpretar semanticamente `hour` nesta camada (a conversão para UTC só acontece em Silver)
- A coluna `price_spain_raw` é preservada mesmo não sendo usada no data product final
- Linhas de metadata do ficheiro CSV são descartadas no processo de ingestão antes de chegar a esta tabela
- `process_date` é a chave de partição — permite DELETE + INSERT idempotente por dia

---

# 3. Schema Silver

## 3.1 `silver.consumo_hourly`

**Tabela Iceberg:** `iceberg.silver.consumo_hourly`  
**Localização MinIO:** `s3a://warehouse/silver/consumo_hourly/`  
**Origem upstream:** `bronze.consumo_raw`  
**Granularidade de saída:** horária  
**Formato:** Parquet (Iceberg format_version=2)  
**Particionamento:** `year`, `month`  
**Função:** Agregar consumo de 15 min para granularidade horária e normalizar para UTC canónico.

### Colunas

| Coluna     | Tipo                        | Obrigatória | Origem / Derivação          | Descrição |
|------------|-----------------------------|-------------|-----------------------------|-----------|
| ts_utc     | TIMESTAMP(6) WITH TIME ZONE | Sim         | Derivada de `datahora`      | Timestamp UTC canónico que representa o início da hora |
| total_mwh  | DOUBLE                      | Sim         | `SUM(total) / 1000`         | Consumo nacional horário agregado em MWh |
| year       | INTEGER                     | Sim         | `YEAR(ts_utc)`              | Ano — coluna de partição |
| month      | INTEGER                     | Sim         | `MONTH(ts_utc)`             | Mês — coluna de partição |

### Propriedades Iceberg (catálogo)

| Propriedade    | Valor                |
|----------------|----------------------|
| layer          | silver               |
| domain         | consumo_preco        |
| schema_version | 1                    |
| grain          | hourly               |
| upstream_table | bronze.consumo_raw   |

### Regras de transformação
- Truncar `datahora` à hora para formar `ts_utc`
- Agregar `total` (kW) por hora e dividir por 1000 → `total_mwh` (MWh)
- Particionamento por `year`/`month` — DELETE + INSERT por dia garante idempotência

### Regras de qualidade
- `ts_utc` NOT NULL e único por tabela
- `total_mwh` >= 0
- Expectativa de ~4 registos Bronze por hora (intervalo 15 min)
- Sem nulos em `ts_utc` ou `total_mwh`

---

## 3.2 `silver.preco_hourly`

**Tabela Iceberg:** `iceberg.silver.preco_hourly`  
**Localização MinIO:** `s3a://warehouse/silver/preco_hourly/`  
**Origem upstream:** `bronze.preco_raw`  
**Granularidade de saída:** horária  
**Formato:** Parquet (Iceberg format_version=2)  
**Particionamento:** `year`, `month`  
**Função:** Converter a numeração OMIE de horas (1–24) para timestamp UTC canónico e normalizar preços.

### Colunas

| Coluna                | Tipo                        | Obrigatória | Origem / Derivação                  | Descrição |
|-----------------------|-----------------------------|-------------|-------------------------------------|-----------|
| ts_utc                | TIMESTAMP(6) WITH TIME ZONE | Sim         | `date_raw + (hour - 1) horas`       | Timestamp UTC canónico que representa o início da hora |
| price_portugal_eur_mwh| DOUBLE                      | Sim         | `price_portugal_raw`                | Preço day-ahead de Portugal em €/MWh |
| price_spain_eur_mwh   | DOUBLE                      | Não         | `price_spain_raw`                   | Preço day-ahead de Espanha em €/MWh — preservado para análise comparativa PT vs ES |
| year                  | INTEGER                     | Sim         | `YEAR(ts_utc)`                      | Ano — coluna de partição |
| month                 | INTEGER                     | Sim         | `MONTH(ts_utc)`                     | Mês — coluna de partição |

### Propriedades Iceberg (catálogo)

| Propriedade    | Valor              |
|----------------|--------------------|
| layer          | silver             |
| domain         | consumo_preco      |
| schema_version | 1                  |
| grain          | hourly             |
| upstream_table | bronze.preco_raw   |

### Regras de transformação
- `ts_utc = CAST(date_raw AS DATE) + INTERVAL (hour - 1) HOURS` (hora OMIE começa em 1)
- Hora 25 (dia com mudança DST de outono) é descartada — não tem correspondência UTC direta válida
- `price_spain_eur_mwh` mantido para análise comparativa PT/ES, apesar de não fazer parte do data product principal

### Regras de qualidade
- `ts_utc` NOT NULL e único por tabela
- `price_portugal_eur_mwh` NOT NULL
- `ts_utc` deve estar alinhado com limite da hora (minuto=0, segundo=0)
- `price_portugal_eur_mwh` dentro de intervalo plausível (–500 a 3000 €/MWh)
- Coerência entre `date_raw`, `hour` Bronze e `ts_utc` Silver

---

# 4. Campos preservados vs descartados

## Bronze → Silver (consumo)

### Preservados semanticamente
- `datahora` → normalizado para `ts_utc`
- `total` → agregado e convertido para `total_mwh`

### Descartados em Silver
- `dia`, `mes`, `ano` — redundantes (deriváveis de `ts_utc`)
- `date_raw`, `time_raw` — redundantes após parse de `datahora`
- `bt`, `mt`, `at`, `mat` — decomposição por tensão não necessária para o data product atual
- `process_date` — descartado (partição é por `year`/`month` em Silver)

**Justificação:** Silver expõe apenas as colunas necessárias para integração temporal com preços e construção da Gold.

---

## Bronze → Silver (preço)

### Preservados semanticamente
- `date_raw` + `hour` → convertidos para `ts_utc`
- `price_portugal_raw` → renomeado para `price_portugal_eur_mwh`
- `price_spain_raw` → renomeado para `price_spain_eur_mwh` (preservado para análise futura)

### Descartados em Silver
- `process_date` — descartado (partição passa a ser por `year`/`month`)
- Linhas com `hour = 25` (DST) — descartadas

**Justificação:** Silver prepara o dataset para join temporal 1:1 com consumo. Coluna Espanha mantida para valor analítico adicional.

---

# 5. Resultado esperado após Silver

Após a camada Silver, o projeto dispõe de duas tabelas horárias consistentes em UTC:

- `silver.consumo_hourly` — consumo horário nacional em MWh
- `silver.preco_hourly` — preço horário PT e ES em €/MWh

Estas tabelas permitem:
- Join temporal 1:1 por `ts_utc`
- Construção segura das features em Gold
- Rastreabilidade até à camada Bronze via `upstream_table` property
