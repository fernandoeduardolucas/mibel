# 1. Objetivo do Lakehouse

O lakehouse do projeto organiza os dados em três camadas — Bronze, Silver e Gold — com o objetivo de garantir rastreabilidade da origem, normalização progressiva dos dados e disponibilização de produtos de dados prontos a consumir.

A arquitetura segue uma abordagem medallion:
- **Bronze**: preservação da origem com transformação mínima
- **Silver**: limpeza, normalização e harmonização temporal
- **Gold**: data products prontos para análise, serving e machine learning

O projeto adota **UTC como tempo canónico** nas camadas Silver e Gold.

---

# 2. Schemas / Camadas

## Bronze
Schema destinado à ingestão dos dados de origem com preservação máxima da estrutura original.

### Tabelas
- `bronze.consumo_raw`
- `bronze.preco_raw`

---

## Silver
Schema destinado à limpeza, tipagem, harmonização temporal e preparação intermédia dos datasets.

### Tabelas
- `silver.consumo_hourly`
- `silver.preco_hourly`

---

## Gold
Schema destinado aos produtos finais prontos para consumo analítico e machine learning.

### Tabelas
- `gold.dp_energy_market_hourly`
- `gold.feat_load_forecasting_hourly`

---

# 3. Desenho físico por tabela

## 3.1 Bronze

### `bronze.consumo_raw`
**Origem:** `consumo-total-nacional.csv`

**Função:**
Preservar os dados originais de consumo elétrico nacional com metadados técnicos de ingestão, sem aplicar regras de negócio ou limpeza semântica.

**Conteúdo esperado:**
- colunas originais da fonte
- metadados de ingestão
- campos temporais ainda em representação de origem

**Observações:**
- mantém colunas redundantes da fonte (`dia`, `mes`, `ano`, `date`, `time`) para rastreabilidade
- não elimina nem reinterpreta colunas nesta camada
- não agrega 15 minutos para 1 hora nesta camada

---

### `bronze.preco_raw`
**Origem:** `Day-ahead Market Prices_20230101_20260311.csv`

**Função:**
Preservar os dados originais de preços MIBEL com metadados técnicos e metadata relevante do ficheiro de origem.

**Conteúdo esperado:**
- colunas tabulares da fonte (`Date`, `Hour`, `Portugal`, `Spain`)
- metadados de ingestão
- metadata do ficheiro, quando aplicável (`unit_raw`, `accessed_on_raw`)

**Observações:**
- a coluna `Spain` é preservada nesta camada mesmo não sendo usada no data product final
- a interpretação da hora (`Hour`) só será tratada em Silver
- a existência de linhas de metadata no ficheiro deve ser resolvida no processo de ingestão, não no desenho lógico da tabela final Bronze

---

## 3.2 Silver

### `silver.consumo_hourly`
**Origem upstream:** `bronze.consumo_raw`

**Função:**
Normalizar o dataset de consumo para granularidade horária e tempo canónico UTC.

**Transformações principais:**
- parsing de `datahora_raw`
- validação temporal
- agregação de 15 minutos para 1 hora
- seleção da métrica de consumo relevante (`total`)
- criação de `timestamp_utc`

**Conteúdo esperado:**
- 1 linha por hora
- timestamp normalizado
- consumo horário agregado
- colunas técnicas mínimas necessárias para controlo

**Observações:**
- esta tabela já representa consumo harmonizado e pronto para integração
- é a base temporal do join com o dataset de preços

---

### `silver.preco_hourly`
**Origem upstream:** `bronze.preco_raw`

**Função:**
Limpar e normalizar o dataset de preços MIBEL PT para representação horária consistente em UTC.

**Transformações principais:**
- parsing de `date_raw`
- tratamento da coluna `hour_raw`
- tratamento de casos especiais de mudança de hora (ex.: hora 25)
- seleção da métrica de Portugal
- criação de `timestamp_utc`

**Conteúdo esperado:**
- 1 linha por hora
- timestamp normalizado
- preço PT horário
- colunas técnicas mínimas de controlo

**Observações:**
- a coluna `Spain` deixa de ser necessária nesta camada
- esta tabela fica preparada para join 1:1 com `silver.consumo_hourly`

---

## 3.3 Gold

### `gold.dp_energy_market_hourly`
**Origem upstream:**
- `silver.consumo_hourly`
- `silver.preco_hourly`

**Função:**
Disponibilizar o principal produto analítico do projeto, integrando consumo e preço em grão horário.

**Transformações principais:**
- join por `timestamp_utc`
- criação de colunas de calendário
- criação de lags
- criação de rolling averages

**Conteúdo esperado:**
- 1 linha por `timestamp_utc`
- métricas analíticas
- features temporais
- colunas derivadas para exploração analítica

**Consumidores:**
- dashboard
- API
- analistas
- base para feature table ML

---

### `gold.feat_load_forecasting_hourly`
**Origem upstream:**
- `gold.dp_energy_market_hourly`

**Função:**
Disponibilizar uma feature table pronta para treino e avaliação de modelos de previsão de consumo.

**Transformações principais:**
- derivação de target (`consumo_next_hour`)
- seleção de features relevantes
- remoção de registos sem target

**Conteúdo esperado:**
- 1 linha por `timestamp_utc`
- conjunto de features temporais e históricas
- target para supervised learning

**Consumidores:**
- workflow de ML
- MLflow

---

# 4. Estratégia de particionamento

A estratégia de particionamento deve ser simples, consistente e adequada ao volume real do projeto, evitando complexidade artificial.

## Bronze
Particionamento recomendado:
- por `process_date`

**Justificação:**
- facilita rastreabilidade da ingestão
- suporta reprocessamento e auditoria
- é consistente com workflows incrementais

---

## Silver
Particionamento recomendado:
- por `year`
- opcionalmente por `month` se necessário

**Justificação:**
- melhora filtragem temporal
- é adequado para datasets horários
- mantém granularidade de partição razoável

---

## Gold
Particionamento recomendado:
- por `year`
- opcionalmente por `month`

**Justificação:**
- os consumidores analíticos e ML filtram naturalmente por intervalos temporais
- evita excesso de ficheiros pequenos por partições horárias ou diárias

---

# 5. Convenções de naming

## Schemas
- `bronze`
- `silver`
- `gold`

## Tabelas
- usar nomes curtos, descritivos e consistentes
- evitar espaços, hífens e nomes excessivamente longos
- usar `snake_case`

## Convenções aplicadas
- `bronze.consumo_raw`
- `bronze.preco_raw`
- `silver.consumo_hourly`
- `silver.preco_hourly`
- `gold.dp_energy_market_hourly`
- `gold.feat_load_forecasting_hourly`

---

# 6. Princípios de desenho adotados

- preservar a origem em Bronze
- limpar e harmonizar em Silver
- expor consumo analítico em Gold
- separar claramente dados operacionais, intermédios e finais
- usar UTC como tempo canónico
- evitar regras de negócio prematuras em Bronze
- manter o desenho simples e proporcional ao âmbito do projeto

---

# 7. Resultado esperado do desenho

No final da implementação, o lakehouse deverá permitir:
- reconstituir a origem dos dados
- integrar consumo e preço de forma temporalmente consistente
- servir um produto analítico horário
- alimentar um workflow de machine learning reprodutível