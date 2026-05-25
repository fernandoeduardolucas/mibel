# 04 — Application (Serving via Grafana)

Camada de serving do projecto MIBEL. Todos os Data Products são servidos exclusivamente via **Grafana 10.4**, auto-provisionado a partir de ficheiros JSON. O datasource executa SQL direto sobre Trino/Iceberg — sem ETL adicional, a tabela Gold é a única fonte de verdade.

- **Grafana:** <http://localhost:3300> · credenciais `admin / admin`
- **Datasource:** `Trino-Iceberg` (UID: `trino-iceberg`) → `http://trino:8080`

---

## Dashboards

Todos provisionados automaticamente pelo Docker Compose a partir de `grafana/dashboards/`. Não requerem configuração manual.

| Ficheiro | DP | Título | Tabela Gold |
| --- | --- | --- | --- |
| `producao_consumo_overview.json` | DP-01 | Balanço da Rede Nacional — Produção · Consumo · Saldo | `iceberg.gold.dp_energia_balance_hourly` |
| `consumo_preco_overview.json` | DP-02 Static | Mercado Ibérico MIBEL — Consumo & Preços Day-Ahead | `iceberg.gold.dp_energy_market_hourly` |
| `consumo_preco_streaming_overview.json` | DP-02 Streaming | Decisão de Mercado — Consumo × Preço MIBEL | `iceberg.gold.dp_energy_market_api_hourly` |
| `meteo_producao_overview.json` | DP-03 | Clima & Produção Renovável — Observatório Meteorológico | `iceberg.gold.dp_meteo_producao_daily_features` |

---

## Conteúdo dos Dashboards

### DP-01 — Produção vs Consumo (`producao_consumo_overview.json`)

Fonte: `iceberg.gold.dp_energia_balance_hourly` · Grain: horário

- **KPIs:** saldo líquido (MWh), ratio produção/consumo, horas em défice e excedente
- **Série temporal:** produção DGM + PRE e consumo nacional por hora
- **Sazonalidade:** distribuição mensal de saldo e flags de défice/excedente

---

### DP-02 Static — Consumo vs Preço OMIE (`consumo_preco_overview.json`)

Fonte: `iceberg.gold.dp_energy_market_hourly` · Grain: horário

- **KPIs:** preço médio day-ahead (€/MWh), consumo médio horário, horas com preço negativo
- **Perfil horário:** preço e consumo médio por hora (0–23 UTC)
- **Sazonalidade:** preço médio por dia da semana e por mês
- **Correlação:** scatter consumo vs preço, heatmap hora × dia da semana
- **Qualidade:** null rates, unicidade `ts_utc`, taxa de join, freshness

---

### DP-02 Streaming — Decisão de Mercado (`consumo_preco_streaming_overview.json`)

Fonte: `iceberg.gold.dp_energy_market_api_hourly` · Grain: horário · Origem: Energy-Charts API

**Pergunta de negócio respondida:** *Em que intervalo horário diário é mais vantajoso vender energia renovável à rede?*

Dashboard numa única página com quatro secções:

#### 1. KPIs de Decisão (topo — 8 cards)

| KPI | Semântica |
| --- | --- |
| Melhor Hora para Vender (UTC) | Hora com preço médio mais alto no período selecionado |
| Preço Médio (€/MWh) | Média de `market_price_pt` no período |
| Consumo Médio (MWh) | Média de `consumo_total` no período |
| Pico de Preço (€/MWh) | Máximo de `market_price_pt` |
| Horas Preço Negativo | WARN — preços negativos são válidos no MIBEL |
| Freshness (h) | Verde < 4h · Amarelo < 8h · Vermelho ≥ 8h (SLO: 4h) |
| Taxa de Join (%) | Registos com consumo AND preço não-nulos (SLO: ≥ 98%) |
| Estado Global do Contrato | OK / WARN / FAIL — resultado composto de todas as regras |

#### 2. Análise do Produto (centro)

- **Série temporal:** `consumo_total` e `market_price_pt` com eixo duplo
- **Barras por hora (0–23 UTC):** preço médio com gradiente de cor — janela de decisão principal
- **Scatter:** correlação consumo vs preço
- **Heatmap:** preço médio por hora × dia da semana (tabela com color-background)
- **Médias móveis 24h:** tendência suavizada de consumo e preço

#### 3. Qualidade & Contrato (base)

Seis stat cards de verificação rápida + tabela com 9 checks ordenados por severidade (FAIL → WARN → PASS):

| Check | Threshold | Tipo |
| --- | --- | --- |
| `null_rate.consumo_total` | 0% | FAIL |
| `null_rate.market_price_pt` | 0% | FAIL |
| `uniqueness.ts_utc` | 0 duplicados | FAIL |
| `range.hora_utc [0-23]` | 0 violações | FAIL |
| `range.dia_semana [0-6]` | 0 violações | FAIL |
| `lag_consistency.consumo_lag_1h` | ≤ 1% nulos | WARN |
| `preco_negativo.count` | > 100h = WARN (válido no MIBEL) | WARN |
| `freshness.slo_4h` | ≤ 4h = PASS · ≤ 8h = WARN | FAIL |
| `join_integrity.consumo_x_preco` | ≥ 98% = PASS | WARN |

#### 4. ML — Consumidor do Produto (base)

Fonte: `iceberg.gold.feat_load_forecasting_api_hourly`

- **Stats:** exemplos de treino, null rate do target, cobertura feat/gold
- **Metadados:** Algoritmo Gradient Boosting · target `consumo_next_hour` · rastreamento MLflow `localhost:15000`
- **Série temporal:** `consumo_total` (atual) vs `consumo_next_hour` (target do modelo — LEAD 1h)

---

### DP-03 — Meteorologia + Produção (`meteo_producao_overview.json`)

Fonte: `iceberg.gold.dp_meteo_producao_daily_features` · Grain: diário

- **KPIs meteorológicos:** temperatura média, precipitação, velocidade do vento
- **Série temporal:** temperatura vs produção renovável total
- **Scatter:** temperatura × produção fotovoltaica

---

## Estrutura de Ficheiros

```text
04_application/
└── grafana/
    ├── README_ML_METRICS.md                         Guia: integração de métricas ML no Grafana via Iceberg
    ├── dashboards/
    │   ├── producao_consumo_overview.json           DP-01
    │   ├── consumo_preco_overview.json              DP-02 Static
    │   ├── consumo_preco_streaming_overview.json    DP-02 Streaming — Decisão de Mercado
    │   └── meteo_producao_overview.json             DP-03
    └── provisioning/
        ├── dashboards/
        │   └── dashboards.yml                       Auto-load dos ficheiros JSON
        └── datasources/
            └── trino.yml                            Datasource Trino (UID: trino-iceberg)
```

---

## Datasource

O datasource `Trino-Iceberg` (UID: `trino-iceberg`) está configurado em `grafana/provisioning/datasources/trino.yml` e aponta para `http://trino:8080` com catalog `iceberg` e schema `gold` como defaults.

Todas as queries dos dashboards usam SQL direto sobre o Trino — não existe camada intermédia entre o Gold e o Grafana.

---

## Operar os Dashboards

### Recarregar após edição (sem reiniciar o stack)

```powershell
curl -X POST http://localhost:3300/api/admin/provisioning/dashboards/reload -u admin:admin
```

### Iniciar o stack

```powershell
cd 01_docker_stack
docker compose up -d
```

Grafana disponível em <http://localhost:3300> após ~30 segundos.

### Convenções de escrita de queries

| Situação | Usar `$__timeFilter(ts_utc)`? |
| --- | --- |
| Análise contextual (KPIs, gráficos, scatter) | Sim — respeita o time range do Grafana |
| Checks de qualidade, freshness, null rates | Não — verifica a tabela completa |
| Contagem de exemplos ML | Não — conta toda a feature table |
