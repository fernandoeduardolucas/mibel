# A) Especificação de Data Products — domínio `consumo_producao`

> Secção alinhada com o enunciado para o cenário **consumo_producao** (dados de consumo e produção elétrica nacional).

## Contexto e objetivo

A organização fictícia (operador energético) pretende monitorizar, em quase-tempo-real, o equilíbrio entre consumo e produção e antecipar situações de défice. O lakehouse disponibiliza dados limpos em Bronze/Silver e produtos analíticos em Gold para operação, reporting e ML.

---

## Data Product 1 — `dp_consumo_producao_hourly_balance`

### Perguntas analíticas, métricas e consumidores

**Perguntas**
- Em que horas existe défice (`produção < consumo`) ou excedente (`produção > consumo`)?
- Qual a cobertura energética por hora (`produção/consumo`)?
- Como evolui o saldo energético ao longo do dia/mês?

**Métricas principais**
- `consumo_total_kwh`
- `producao_total_kwh`
- `saldo_kwh`
- `ratio_producao_consumo`
- `flag_defice`
- `flag_excedente`
- `flag_missing_source`

**Consumidores**
- Dashboard operacional (equipa de operação de rede).
- API para integração com aplicações internas.

### Grão e chaves

- **Grão**: 1 linha por hora UTC.
- **Chave de negócio**: `timestamp_utc`.

### Contrato de dados (schema + SLAs/SLOs)

**Schema v1**
- `timestamp_utc TIMESTAMP NOT NULL`
- `consumo_total_kwh DOUBLE`
- `producao_total_kwh DOUBLE`
- `saldo_kwh DOUBLE`
- `ratio_producao_consumo DOUBLE`
- `flag_defice BOOLEAN`
- `flag_excedente BOOLEAN`
- `flag_missing_source BOOLEAN`

**Regras de qualidade**
- Unicidade de `timestamp_utc`.
- `consumo_total_kwh >= 0` e `producao_total_kwh >= 0`.
- `flag_defice` e `flag_excedente` não podem ser simultaneamente verdadeiros.

**SLAs/SLOs**
- Publicação do produto: até T+30min após fecho da hora.
- Freshness máxima: 2h.
- Completude horária: >= 99%.

### Estratégia de schema evolution/versionamento

- Versionamento semântico (`v1`, `v2`, ...).
- Adição de colunas opcionais = mudança compatível.
- Mudança de definição de métrica/unidade = nova major.

---

## Data Product 2 — `dp_consumo_producao_hourly_mix`

### Perguntas analíticas, métricas e consumidores

**Perguntas**
- Qual o contributo de `DGM` e `PRE` para a produção total?
- Que tipo de produção sustenta melhor períodos de pico de consumo?

**Métricas principais**
- `producao_total_kwh`
- `producao_dgm_kwh`
- `producao_pre_kwh`
- `share_dgm = producao_dgm_kwh / producao_total_kwh`
- `share_pre = producao_pre_kwh / producao_total_kwh`

**Consumidores**
- Dashboard de planeamento energético.
- Analytics SQL (ad-hoc) para equipa de decisão.

### Grão e chaves

- **Grão**: 1 linha por hora UTC.
- **Chave de negócio**: `timestamp_utc`.

### Contrato de dados (schema + SLAs/SLOs)

**Schema v1**
- `timestamp_utc TIMESTAMP NOT NULL`
- `producao_total_kwh DOUBLE NOT NULL`
- `producao_dgm_kwh DOUBLE`
- `producao_pre_kwh DOUBLE`
- `share_dgm DOUBLE`
- `share_pre DOUBLE`

**Regras de qualidade**
- `producao_total_kwh >= 0`.
- `share_dgm` e `share_pre` no intervalo `[0,1]` quando `producao_total_kwh > 0`.
- `share_dgm + share_pre` próximo de 1 (tolerância técnica, quando aplicável).

**SLAs/SLOs**
- Publicação: até T+30min.
- Freshness máxima: 2h.
- Null-rate nas métricas core (`producao_total_kwh`): 0%.

### Estratégia de schema evolution/versionamento

- Novas fontes de produção entram como novas colunas em minor (`share_hidro`, por exemplo).
- Alteração de fórmula de `share_*` implica major.

---

## Data Product 3 — `dp_consumo_producao_daily_ml_features`

### Perguntas analíticas, métricas e consumidores

**Perguntas**
- É possível prever défice diário a partir de histórico de consumo/produção?
- Quais variáveis mais explicam variações de saldo?

**Métricas/features principais**
- `date_utc`
- `consumo_total_mwh_d`
- `producao_total_mwh_d`
- `saldo_mwh_d`
- `ratio_producao_consumo_d`
- `horas_defice_d`
- `horas_excedente_d`
- `target_defice_next_day` (label)

**Consumidores**
- Workflow Flyte de treino e scoring.
- MLflow para tracking de experiências/modelos.

### Grão e chaves

- **Grão**: 1 linha por dia UTC.
- **Chave de negócio**: `date_utc`.
- **Chave técnica**: (`date_utc`, `feature_set_version`).

### Contrato de dados (schema + SLAs/SLOs)

**Schema v1**
- `date_utc DATE NOT NULL`
- `consumo_total_mwh_d DOUBLE NOT NULL`
- `producao_total_mwh_d DOUBLE NOT NULL`
- `saldo_mwh_d DOUBLE NOT NULL`
- `ratio_producao_consumo_d DOUBLE`
- `horas_defice_d INTEGER`
- `horas_excedente_d INTEGER`
- `target_defice_next_day BOOLEAN`
- `feature_set_version VARCHAR NOT NULL`

**Regras de qualidade**
- Unicidade de `date_utc`.
- Zero nulos nas features obrigatórias.
- Coerência: `horas_defice_d + horas_excedente_d <= 24`.

**SLAs/SLOs**
- Publicação diária até 07:00 UTC.
- Freshness máxima: 1 dia.
- Completude diária: >= 99.5%.

### Estratégia de schema evolution/versionamento

- `feature_set_version` obrigatório em todas as linhas.
- Adição de features não destrutivas = minor.
- Alteração de target ou transformação base = major.

---

## Critérios de aceitação transversais (consumo_producao)

- Rastreabilidade completa Bronze -> Silver -> Gold para todos os produtos.
- Contrato explícito por produto (schema, regras de qualidade e SLOs).
- Quebra de contrato bloqueia promoção para produção.
- Produtos servidos por SQL/API e preparados para consumo por dashboards e ML.
