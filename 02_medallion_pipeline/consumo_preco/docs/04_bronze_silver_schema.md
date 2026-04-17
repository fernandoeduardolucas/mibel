# 1. Objetivo

Este documento define o schema técnico das tabelas Bronze e Silver do projeto, incluindo colunas, tipos de dados, papel de cada campo e principais regras de transformação entre camadas.

O objetivo é garantir consistência de implementação no lakehouse e preparar a construção posterior das tabelas Gold.

---

# 2. Schema Bronze

## 2.1 `bronze.consumo_raw`

**Origem:** `consumo-total-nacional.csv`  
**Granularidade de origem:** 15 minutos  
**Função:** preservar a estrutura da fonte de consumo com metadados de ingestão.

### Colunas

| Coluna         | Tipo       | Obrigatória | Origem / Derivação | Descrição |
|----------------|------------|-------------|--------------------|-----------|
| ingestion_ts   | TIMESTAMP  | Sim         | Derivada           | Timestamp técnico da ingestão |
| process_date   | DATE       | Sim         | Derivada           | Data lógica da execução / ingestão |
| source_file    | VARCHAR    | Sim         | Derivada           | Nome do ficheiro de origem |
| row_num        | BIGINT     | Sim         | Derivada           | Número sequencial da linha no ficheiro |
| datahora_raw   | VARCHAR    | Sim         | Fonte (`datahora`) | Valor temporal original da fonte |
| dia            | INT        | Não         | Fonte              | Dia presente no ficheiro |
| mes            | INT        | Não         | Fonte              | Mês presente no ficheiro |
| ano            | INT        | Não         | Fonte              | Ano presente no ficheiro |
| date_raw       | VARCHAR    | Não         | Fonte (`date`)     | Campo de data original |
| time_raw       | VARCHAR    | Não         | Fonte (`time`)     | Campo de hora original |
| bt             | DOUBLE     | Não         | Fonte              | Consumo em BT |
| mt             | DOUBLE     | Não         | Fonte              | Consumo em MT |
| at             | DOUBLE     | Não         | Fonte              | Consumo em AT |
| mat            | DOUBLE     | Não         | Fonte              | Consumo em MAT |
| total          | DOUBLE     | Sim         | Fonte              | Valor total de consumo da linha |

### Regras
- preservar o valor original das colunas da fonte
- não eliminar redundâncias nesta camada
- não converter a granularidade temporal
- não aplicar regras de negócio nesta camada

---

## 2.2 `bronze.preco_raw`

**Origem:** `Day-ahead Market Prices_20230101_20260311.csv`  
**Granularidade de origem:** horária  
**Função:** preservar os dados tabulares do ficheiro MIBEL e respetivos metadados técnicos.

### Colunas

| Coluna           | Tipo       | Obrigatória | Origem / Derivação   | Descrição |
|------------------|------------|-------------|----------------------|-----------|
| ingestion_ts     | TIMESTAMP  | Sim         | Derivada             | Timestamp técnico da ingestão |
| process_date     | DATE       | Sim         | Derivada             | Data lógica da execução / ingestão |
| source_file      | VARCHAR    | Sim         | Derivada             | Nome do ficheiro de origem |
| row_num          | BIGINT     | Sim         | Derivada             | Número sequencial da linha útil |
| unit_raw         | VARCHAR    | Não         | Metadata ficheiro    | Unidade indicada na fonte |
| accessed_on_raw  | VARCHAR    | Não         | Metadata ficheiro    | Momento textual de acesso à informação |
| date_raw         | VARCHAR    | Sim         | Fonte (`Date`)       | Data original da linha |
| hour_raw         | INT        | Sim         | Fonte (`Hour`)       | Hora original da linha |
| portugal_price   | DOUBLE     | Sim         | Fonte (`Portugal`)   | Preço de Portugal |
| spain_price      | DOUBLE     | Não         | Fonte (`Spain`)      | Preço de Espanha |

### Regras
- ignorar as linhas de metadata no corpo tabular da ingestão
- preservar a metadata relevante do ficheiro em colunas próprias, quando aplicável
- não interpretar semanticamente `hour_raw` nesta camada
- não remover a coluna `spain_price` nesta camada

---

# 3. Schema Silver

## 3.1 `silver.consumo_hourly`

**Origem upstream:** `bronze.consumo_raw`  
**Granularidade de saída:** horária  
**Função:** normalizar e agregar o consumo para representação horária canónica.

### Colunas

| Coluna         | Tipo       | Obrigatória | Origem / Derivação | Descrição |
|----------------|------------|-------------|--------------------|-----------|
| timestamp_utc  | TIMESTAMP  | Sim         | Derivada           | Timestamp horário normalizado em UTC |
| year           | INT        | Sim         | Derivada           | Ano derivado de `timestamp_utc` |
| month          | INT        | Sim         | Derivada           | Mês derivado de `timestamp_utc` |
| day            | INT        | Sim         | Derivada           | Dia derivado de `timestamp_utc` |
| hour           | INT        | Sim         | Derivada           | Hora derivada de `timestamp_utc` |
| consumo_total  | DOUBLE     | Sim         | Agregada (`total`) | Consumo total agregado à hora |
| source_min_ts  | TIMESTAMP  | Não         | Derivada           | Menor timestamp de origem agregado |
| source_max_ts  | TIMESTAMP  | Não         | Derivada           | Maior timestamp de origem agregado |
| source_rows    | INT        | Não         | Derivada           | Número de registos de origem agregados |
| process_date   | DATE       | Sim         | Derivada           | Data lógica do processamento |

### Regras de transformação
- parse de `datahora_raw`
- validação do timestamp de origem
- normalização temporal para `timestamp_utc`
- agregação por hora sobre a métrica `total`
- criação de colunas de calendário (`year`, `month`, `day`, `hour`)
- cálculo de colunas técnicas de controlo (`source_min_ts`, `source_max_ts`, `source_rows`)

### Regras de qualidade
- `timestamp_utc` único
- `consumo_total` >= 0
- `source_rows` esperado próximo de 4 por hora, salvo falhas na origem
- ausência de nulos em `timestamp_utc` e `consumo_total`

---

## 3.2 `silver.preco_hourly`

**Origem upstream:** `bronze.preco_raw`  
**Granularidade de saída:** horária  
**Função:** normalizar o preço horário de Portugal para representação consistente em UTC.

### Colunas

| Coluna           | Tipo       | Obrigatória | Origem / Derivação    | Descrição |
|------------------|------------|-------------|------------------------|-----------|
| timestamp_utc    | TIMESTAMP  | Sim         | Derivada               | Timestamp horário normalizado em UTC |
| year             | INT        | Sim         | Derivada               | Ano derivado de `timestamp_utc` |
| month            | INT        | Sim         | Derivada               | Mês derivado de `timestamp_utc` |
| day              | INT        | Sim         | Derivada               | Dia derivado de `timestamp_utc` |
| hour             | INT        | Sim         | Derivada               | Hora derivada de `timestamp_utc` |
| market_price_pt  | DOUBLE     | Sim         | Derivada (`portugal_price`) | Preço horário PT |
| source_date_raw  | VARCHAR    | Não         | Fonte                  | Data original da linha para auditoria |
| source_hour_raw  | INT        | Não         | Fonte                  | Hora original da linha para auditoria |
| process_date     | DATE       | Sim         | Derivada               | Data lógica do processamento |

### Regras de transformação
- parse de `date_raw`
- validação de `hour_raw`
- interpretação da hora original segundo a lógica da fonte
- tratamento de casos especiais (ex.: hora 25)
- construção de `timestamp_utc`
- criação de colunas de calendário (`year`, `month`, `day`, `hour`)
- seleção da métrica PT (`portugal_price`)

### Regras de qualidade
- `timestamp_utc` único
- ausência de nulos em `timestamp_utc` e `market_price_pt`
- `market_price_pt` dentro de intervalo plausível
- coerência entre `source_date_raw`, `source_hour_raw` e `timestamp_utc`

---

# 4. Campos preservados vs descartados

## Bronze → Silver (consumo)
### Preservados semanticamente
- timestamp de origem
- métrica `total`

### Descartados na representação Silver
- `dia`
- `mes`
- `ano`
- `date_raw`
- `time_raw`
- `bt`
- `mt`
- `at`
- `mat`

**Justificação:** não são necessários para o data product definido neste elemento do projeto.

---

## Bronze → Silver (preço)
### Preservados semanticamente
- data original
- hora original
- preço PT

### Descartados na representação Silver
- `spain_price`
- `unit_raw`
- `accessed_on_raw`

**Justificação:** a tabela Silver prepara apenas o dataset necessário à integração temporal com consumo e ao data product final.

---

# 5. Resultado esperado após Silver

Após a camada Silver, o projeto deverá dispor de duas tabelas horárias consistentes em UTC:

- `silver.consumo_hourly`
- `silver.preco_hourly`

Estas tabelas devem permitir:
- join temporal 1:1 por `timestamp_utc`
- construção segura das features em Gold
- rastreabilidade suficiente até à camada Bronze