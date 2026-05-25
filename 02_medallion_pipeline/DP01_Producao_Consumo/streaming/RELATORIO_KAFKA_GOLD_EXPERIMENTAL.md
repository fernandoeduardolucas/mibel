# Relatório Kafka Streaming (Experimental) — foco em Gold

## 1) Contexto

Foi criado um fluxo streaming com eventos fictícios de produção/consumo a cada 30 segundos, publicados em Kafka-compatible (Redpanda) e lidos via Trino.

Fluxo implementado:

`producer -> kafka.default.producao_consumo_events -> iceberg.gold.dp_producao_consumo_streaming -> Grafana`

---

## 2) Decisão experimental adotada

**Para fins experimentais neste projeto, focamos apenas o consumo da camada Gold no stream**.

Ou seja:

- o dashboard de streaming consulta **Gold**;
- a validação funcional do fluxo foi orientada ao produto curado;
- a implementação completa e operacional de governança da Bronze no stream fica explicitamente fora do escopo desta entrega experimental.

> Nota: a arquitetura Medallion completa continua recomendada em produção (Bronze/Silver/Gold), mas aqui priorizamos velocidade de prova de conceito no consumo analítico.

---

## 3) Produto Gold do stream

Tabela alvo:

- `iceberg.gold.dp_producao_consumo_streaming`

Campos de valor analítico:

- `event_ts`
- `consumo_total_kwh`
- `producao_total_kwh`
- `saldo_kwh`
- `ratio_producao_consumo`
- `flag_defice`
- `flag_excedente`

Esta modelação permite observar o comportamento quase em tempo real com semântica de negócio pronta para dashboard.

---

## 4) Dashboard Grafana de streaming

Dashboard:

- `04_application/grafana/dashboards/producao_consumo_streaming_overview.json`

Painéis principais:

1. Consumo vs Produção (kWh)
2. Saldo (kWh)
3. Rácio Produção/Consumo

Todos os painéis leem da tabela Gold de streaming.

---

## 5) Limitações assumidas nesta fase

Por ser uma versão experimental:

- não foi formalizado um ciclo de qualidade completo da Bronze para stream;
- não foi implementada camada Silver dedicada para streaming;
- não há SLA de produção definido para o processo contínuo de materialização.

Estas decisões foram intencionais para acelerar a validação do consumo em Grafana sobre dados streaming curados em Gold.

---

## 6) Próximos passos recomendados

1. Formalizar pipeline streaming Medallion completo (Bronze -> Silver -> Gold).
2. Definir políticas de deduplicação incremental/idempotência por janela temporal.
3. Adicionar quality checks automatizados para stream.
4. Definir SLA/observabilidade operacional (freshness, lag, erro de ingestão).
