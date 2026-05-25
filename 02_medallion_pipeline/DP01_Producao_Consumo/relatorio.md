# Relatório Grafana — `producao_consumo` (Gold)

## 1) Objetivo

Este relatório descreve a visão de negócio do produto `producao_consumo` no Grafana, com foco na camada **Gold** do lakehouse:

- tabela principal: `iceberg.gold.dp_energia_balance_hourly`;
- consumo no dashboard: `producao_consumo_overview.json`.

Para fins de análise e decisão, os painéis devem consumir a camada curada (Gold), onde já existem indicadores calculados e consistência semântica.

---

## 2) Fonte de dados do dashboard

- Datasource Grafana: `trino-iceberg`.
- Tabela consultada: `iceberg.gold.dp_energia_balance_hourly`.
- Granularidade principal: horária (UTC).

Isto garante que as métricas exibidas no dashboard correspondem ao produto final do pipeline Medallion.

---

## 3) KPIs de negócio usados no Grafana

A camada Gold já expõe os campos que suportam os painéis principais:

- `consumo_total_kwh`
- `producao_total_kwh`
- `saldo_kwh`
- `ratio_producao_consumo`
- `flag_defice`
- `flag_excedente`

Com esses indicadores, o dashboard permite:

- monitorizar cobertura de produção face ao consumo;
- identificar horas com défice/excedente;
- acompanhar tendência temporal do equilíbrio energético.

---

## 4) Justificação técnica: por que Gold no Grafana

A camada Gold é a recomendada para consumo analítico porque:

1. já aplica regras de qualidade e deduplicação herdadas do pipeline;
2. centraliza lógica de negócio (KPIs e flags) em SQL versionado;
3. reduz risco de interpretações inconsistentes entre consumidores.

A Bronze continua essencial para rastreabilidade e auditoria, mas não deve ser a camada principal de visualização executiva.

---

## 5) Resultado esperado

Com esta abordagem, o relatório `producao_consumo` no Grafana passa a representar de forma direta o produto de dados final, mantendo:

- governança (regras explícitas);
- consistência de KPI;
- reutilização por analytics e ML.
