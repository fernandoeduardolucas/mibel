# Relatório Técnico Detalhado — Dashboard `producao_consumo_overview`

## 1) Resumo Executivo
O dashboard `producao_consumo_overview` foi desenhado para suportar **monitorização operacional do balanço energético nacional**, com três objetivos principais:

1. **Operação em tempo quase real:** acompanhar produção, consumo, saldo e risco de défice por hora.
2. **Qualidade de dados:** validar cobertura e integridade da camada Gold para garantir confiança analítica.
3. **Inteligência preditiva (ML):** usar um classificador de défice para antecipar risco na hora seguinte e apoiar decisão.

No período visível nos screenshots (aprox. 2022-05-31 a 2026-05-31), o comportamento geral sugere sistema frequentemente perto do equilíbrio (`ratio_producao_consumo` próximo de 1), com episódios pontuais de défice e picos isolados que merecem investigação operacional.

---

## 2) Arquitetura de dados que alimenta o dashboard

## 2.1 Tabelas fonte
- **`iceberg.gold.dp_energia_balance_hourly`**
  - Série horária consolidada de produção/consumo.
  - Base para KPIs, séries temporais, rácio, correlação e qualidade de dados.
- **`iceberg.gold.ml_training_metrics`**
  - Histórico de métricas de treino/validação do modelo registado.
- **`iceberg.gold.ml_defice_predictions`**
  - Saída de inferência online/batch para previsão de défice da próxima hora.

## 2.2 Granularidade e unidade
- A maioria das variáveis base está em **kWh**.
- Painéis convertem para **MWh** (`/1000`) e **GWh** (`/1e6`) consoante o contexto.
- Granularidade primária é **horária** (`timestamp_utc`), com agregações diárias em painéis de tendência.

## 2.3 Janela temporal (Grafana)
Todos os painéis usam o mesmo padrão de filtro temporal:

```sql
timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                  AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
```

Isto assegura consistência analítica entre KPIs, séries e tabelas quando o utilizador altera o time range global.

---

## 3) Dicionário funcional das métricas

- **`producao_total_kwh`**: energia produzida no intervalo horário.
- **`consumo_total_kwh`**: energia consumida no intervalo horário.
- **`saldo_kwh`**: `producao_total_kwh - consumo_total_kwh`.
  - `saldo > 0`: excedente.
  - `saldo < 0`: défice.
- **`ratio_producao_consumo`**: rácio de cobertura.
  - `> 1`: produção cobre consumo com folga.
  - `= 1`: equilíbrio.
  - `< 1`: produção insuficiente.
- **`flag_defice`**: indicador booleano para hora com défice.
- **`producao_dgm_kwh` / `producao_pre_kwh`**: decomposição da produção por mix (DGM vs PRE).

---

## 4) Inventário completo de painéis com queries e análise

## 4.1 KPIs — Produção, Consumo, Saldo, Cobertura

### [2] Produção Total (GWh)
```sql
SELECT ROUND(SUM(producao_total_kwh)/1e6,1) AS value
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
```
**Análise:** mede volume energético acumulado produzido no período selecionado. É um KPI de escala (não de eficiência).

### [3] Consumo Total (GWh)
```sql
SELECT ROUND(SUM(consumo_total_kwh)/1e6,1) AS value
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
```
**Análise:** complementar ao KPI anterior; permite comparar pressão de procura vs oferta agregada.

### [4] Rácio Médio Prod/Cons
```sql
SELECT ROUND(AVG(ratio_producao_consumo),3) AS value
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
  AND ratio_producao_consumo IS NOT NULL
```
**Análise:** indicador de cobertura média. Como é média simples horária, pode mascarar extremos; deve ser lido com séries e quantis.

### [5] Saldo Total (GWh)
```sql
SELECT ROUND(SUM(saldo_kwh)/1e6,1) AS value
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
```
**Análise:** visão acumulada da “folga estrutural” do período. Pode ser positivo mesmo com muitas horas em défice.

### [6] Pico de Consumo (MWh)
```sql
SELECT ROUND(MAX(consumo_total_kwh)/1000,1) AS value
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
```
**Análise:** captura stress máximo da procura. Útil para planeamento de capacidade e robustez da rede.

### [7] Horas em Défice
```sql
SELECT COUNT(*) AS value
FROM iceberg.gold.dp_energia_balance_hourly
WHERE flag_defice = true
  AND timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                        AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
```
**Análise:** frequencímetro de risco operacional. Deve ser combinado com severidade do défice (`saldo_kwh` negativo absoluto).

---

## 4.2 Produção vs Consumo — Série Horária

### [11] Produção vs Consumo Nacional (MWh/h)
```sql
SELECT date_trunc('hour', timestamp_utc) AT TIME ZONE 'UTC' AS time,
       ROUND(producao_total_kwh/1000,2) AS "Producao (MWh)",
       ROUND(consumo_total_kwh/1000,2) AS "Consumo (MWh)",
       ROUND(saldo_kwh/1000,2) AS "Saldo (MWh)"
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
ORDER BY 1
```
**Análise:** painel-chave para identificar sazonalidade diária/semanal e eventos atípicos (quebras abruptas, spikes e outliers).

### [12] Mix Energético — DGM vs PRE
```sql
SELECT 'DGM (Mercado)' AS categoria, SUM(producao_dgm_kwh) AS valor
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
UNION ALL
SELECT 'PRE (Renovaveis)', SUM(producao_pre_kwh)
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
ORDER BY valor DESC
```
**Análise:** mostra peso relativo das fontes. Bom para leitura macro, mas sem detalhe temporal intradiário.

---

## 4.3 Rácio Produção/Consumo — Série e Distribuição

### [21] Rácio Produção/Consumo — Série Horária
```sql
SELECT date_trunc('hour', timestamp_utc) AT TIME ZONE 'UTC' AS time,
       ROUND(AVG(ratio_producao_consumo),4) AS "Racio Prod/Cons"
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
  AND ratio_producao_consumo IS NOT NULL
GROUP BY date_trunc('hour', timestamp_utc)
ORDER BY 1
```
**Análise:** evidencia estabilidade estrutural em torno de 1 e oscilações anómalas (picos >1.2 ou quedas <0.9, por exemplo).

### [22] Perfil Horário Médio — Rácio por Hora do Dia
```sql
SELECT CAST(TIMESTAMP '2000-01-01 00:00:00' + (hour(timestamp_utc) * INTERVAL '1' HOUR) AS TIMESTAMP WITH TIME ZONE) AS time,
       ROUND(AVG(ratio_producao_consumo),4) AS "Racio Medio"
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
  AND ratio_producao_consumo IS NOT NULL
GROUP BY hour(timestamp_utc)
ORDER BY 1
```
**Análise:** normaliza o comportamento por hora do dia (0–23), útil para identificar padrões de carga/cobertura por ciclo diário.

---

## 4.4 Tendências — Agregação Diária

### [31] Produção vs Consumo — Agregação Diária (MWh)
```sql
SELECT date_trunc('day', timestamp_utc) AT TIME ZONE 'UTC' AS time,
       ROUND(SUM(producao_total_kwh)/1000,1) AS "Producao (MWh)",
       ROUND(SUM(consumo_total_kwh)/1000,1) AS "Consumo (MWh)"
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
GROUP BY date_trunc('day', timestamp_utc)
ORDER BY 1
```
**Análise:** reduz ruído horário e melhora leitura de tendência macro e sazonalidade de médio prazo.

### [32] Variação Diária do Rácio Produção/Consumo
```sql
SELECT date_trunc('day', timestamp_utc) AT TIME ZONE 'UTC' AS time,
       ROUND(AVG(ratio_producao_consumo),4) AS "Racio Medio",
       ROUND(MAX(ratio_producao_consumo),4) AS "Racio Max",
       ROUND(MIN(ratio_producao_consumo),4) AS "Racio Min"
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
  AND ratio_producao_consumo IS NOT NULL
GROUP BY date_trunc('day', timestamp_utc)
ORDER BY 1
```
**Análise:** mede amplitude intradiária da cobertura; dias com `max-min` elevado tendem a ser operacionalmente mais voláteis.

---

## 4.5 Correlação e Dados Recentes

### [41] Scatter: Rácio vs Consumo (MWh)
```sql
SELECT ROUND(consumo_total_kwh/1000,2) AS "Consumo (MWh)",
       ROUND(ratio_producao_consumo,4) AS "Racio"
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
  AND ratio_producao_consumo IS NOT NULL
ORDER BY timestamp_utc
LIMIT 2000
```
**Análise:** explora dependência entre nível de consumo e cobertura. Se os pontos colapsam perto de 1, o sistema é estável porém pouco discriminativo para causalidade visual.

### [42] Últimas 24 Horas no Intervalo
```sql
SELECT CAST(date_trunc('hour', timestamp_utc) AS VARCHAR) AS hora,
       ROUND(producao_total_kwh/1000,2) AS "Prod (MWh)",
       ROUND(consumo_total_kwh/1000,2) AS "Cons (MWh)",
       ROUND(saldo_kwh/1000,2) AS "Saldo (MWh)",
       ROUND(ratio_producao_consumo,4) AS racio,
       CAST(flag_defice AS VARCHAR) AS defice
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
ORDER BY timestamp_utc DESC
LIMIT 24
```
**Análise:** painel de operação tática; permite validar “estado agora” sem abrir query tool externa.

---

## 4.6 Qualidade de Dados — Cobertura & Integridade

### [51] Completude dos Dados Gold (%)
```sql
SELECT ROUND(CAST(COUNT(*) AS DOUBLE)/(date_diff('hour',MIN(timestamp_utc),MAX(timestamp_utc))+1)*100,2) AS value
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
```
**Análise:** mede preenchimento relativo à janela observada. Valor 100% implica sem buracos na série entre min/max.

### [52] Horas em Falta (Gold)
```sql
SELECT date_diff('hour',MIN(timestamp_utc),MAX(timestamp_utc))+1-COUNT(*) AS value
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
```
**Análise:** converte completude para número absoluto de lacunas, mais acionável para equipas de engenharia de dados.

### [53] Nulos Prod/Cons
```sql
SELECT COUNT(*) AS value
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
  AND (producao_total_kwh IS NULL OR consumo_total_kwh IS NULL)
```
**Análise:** protege contra “linhas presentes mas inválidas” (lacuna semântica).

### [54] Registos por Dia — Cobertura Gold
```sql
SELECT date_trunc('day', timestamp_utc) AT TIME ZONE 'UTC' AS time,
       COUNT(*) AS registos
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
GROUP BY date_trunc('day', timestamp_utc)
ORDER BY 1
```
**Análise:** esperado próximo de 24 registos/dia. Desvios indicam falhas de ingestão, atrasos ou duplicação/truncagem.

---

## 5) Camada de ML — análise detalhada

## 5.1 Modelo monitorizado
- **Registered model:** `producao_consumo_defice_classifier`
- **Tipo de problema:** classificação binária.
- **Target operacional:** prever se na próxima hora haverá défice (`pred_class=1`) ou não (`pred_class=0`).

## 5.2 Métricas de treino exibidas

### [56] Accuracy
```sql
SELECT accuracy AS value
FROM iceberg.gold.ml_training_metrics
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts DESC
LIMIT 1
```

### [57] Precision
```sql
SELECT precision AS value
FROM iceberg.gold.ml_training_metrics
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts DESC
LIMIT 1
```

### [58] Recall
```sql
SELECT recall AS value
FROM iceberg.gold.ml_training_metrics
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts DESC
LIMIT 1
```

### [59] F1
```sql
SELECT f1 AS value
FROM iceberg.gold.ml_training_metrics
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts DESC
LIMIT 1
```

### [60] Histórico F1 vs ROC-AUC
```sql
SELECT event_ts AS time, f1 AS value, 'f1' AS metric
FROM iceberg.gold.ml_training_metrics
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts
```
```sql
SELECT event_ts AS time, roc_auc AS value, 'roc_auc' AS metric
FROM iceberg.gold.ml_training_metrics
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts
```

### Interpretação técnica das métricas (com base no screenshot)
Com os valores visíveis (aprox. Accuracy 0.751, Precision 0.652, Recall 0.596, F1 0.623):
- **Accuracy** aceitável para baseline operacional, mas potencialmente inflacionada se classes forem desbalanceadas.
- **Recall** < Precision sugere que o modelo falha mais em captar alguns défices (falsos negativos), ponto crítico em operação elétrica.
- **F1** intermédio indica compromisso razoável, mas não ótimo, entre detetar défice e evitar alarmes falsos.
- **ROC-AUC** no histórico deve ser acompanhado para detetar degradação de separabilidade ao longo do tempo.

---

## 5.3 Serving / inferência em produção

### [62] Previsão Próxima Hora (Classe)
```sql
SELECT pred_class AS value
FROM iceberg.gold.ml_defice_predictions
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts DESC
LIMIT 1
```

### [63] Probabilidade de Défice (t+1h)
```sql
SELECT pred_prob_defice AS value
FROM iceberg.gold.ml_defice_predictions
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts DESC
LIMIT 1
```

### [64] Última Inferência Gravada
```sql
SELECT CAST(ref_timestamp_utc AS VARCHAR) AS "Ref UTC",
       CAST(predicted_for_utc AS VARCHAR) AS "Prevista UTC",
       pred_class AS "Classe",
       ROUND(pred_prob_defice,3) AS "Prob"
FROM iceberg.gold.ml_defice_predictions
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts DESC
LIMIT 1
```

### Leitura operacional
- `pred_class` dá decisão discreta para cockpit.
- `pred_prob_defice` permite política de thresholds (ex.: alerta amarelo >0.5; vermelho >0.75).
- `Ref UTC` vs `Prevista UTC` valida coerência temporal da inferência.

---

## 6) Análise dos screenshots fornecidos

## 6.1 Sinais observados
1. **Rácio global próximo de 1** na maior parte da série: indica sistema tendencialmente equilibrado.
2. **Spikes e quedas abruptas** no rácio e no saldo: possíveis eventos reais extremos ou anomalias de dados.
3. **Qualidade Gold alta** (100% completude, 0 horas em falta, mas com contagem de nulos não-zero): cobertura temporal pode estar ótima mesmo com qualidade semântica a melhorar.
4. **Scatter comprimido** perto de rácio 1: baixa dispersão estrutural.
5. **Painel “Data outside time range”** no perfil horário: problema de configuração visual/escala temporal, não necessariamente falta de dados.

## 6.2 Hipóteses explicativas para anomalias
- Interrupções/paragens de fontes de produção específicas.
- Eventos de procura anormal (ondas de calor/frio, eventos massivos).
- Atraso de ingestão e posterior backfill criando degraus aparentes.
- Erro de timezone na construção do eixo temporal em alguns painéis.

---

## 7) Limitações atuais do dashboard

1. **Sem quantificação de severidade do défice** (apenas contagem de horas).
2. **Sem segmentação regional** (apenas visão nacional agregada).
3. **Sem análise explícita de drift de dados/modelo**.
4. **Sem painéis de custo de erro ML** (impacto de falsos negativos vs falsos positivos).
5. **Sem benchmark entre versões de modelo** no mesmo gráfico.

---

## 8) Recomendações de evolução (priorizadas)

## Prioridade Alta
1. **Adicionar KPI de severidade do défice**
   - Ex.: soma de `ABS(saldo_kwh)` apenas quando `saldo_kwh < 0`.
2. **Alerting com thresholds de probabilidade ML**
   - Alertar NOC/Operações quando `pred_prob_defice >= 0.7`.
3. **Corrigir painel de perfil horário**
   - Garantir range coerente com timestamp sintético e timezone único.

## Prioridade Média
4. **Introduzir métricas robustas de distribuição**
   - p50/p90/p95 do `ratio_producao_consumo` e do `saldo_kwh`.
5. **Painel de erros de classificação por janela temporal**
   - TP/FP/FN/TN por semana para avaliar confiabilidade operacional.
6. **Monitor de drift**
   - PSI/KS por features críticas e drift da variável alvo.

## Prioridade Baixa
7. **Comparador de modelos** (champion/challenger).
8. **Enriquecimento contextual** com meteo/mercado para análise causal.

---

## 9) Queries adicionais recomendadas (não presentes no dashboard atual)

### 9.1 Severidade acumulada de défice (MWh)
```sql
SELECT ROUND(SUM(CASE WHEN saldo_kwh < 0 THEN ABS(saldo_kwh) ELSE 0 END)/1000,2) AS defice_severidade_mwh
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
```

### 9.2 Percentis do rácio de cobertura
```sql
SELECT approx_percentile(ratio_producao_consumo, 0.50) AS p50,
       approx_percentile(ratio_producao_consumo, 0.90) AS p90,
       approx_percentile(ratio_producao_consumo, 0.95) AS p95
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
  AND ratio_producao_consumo IS NOT NULL
```

### 9.3 Taxa de horas em défice (%)
```sql
SELECT ROUND(100.0 * SUM(CASE WHEN flag_defice THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_defice_pct
FROM iceberg.gold.dp_energia_balance_hourly
WHERE timestamp_utc BETWEEN CAST(from_unixtime($__from / 1000) AS TIMESTAMP(6))
                       AND CAST(from_unixtime($__to / 1000) AS TIMESTAMP(6))
```

---

## 10) Conclusão
O dashboard já fornece uma base sólida para observabilidade energética e monitorização de ML. A combinação de KPIs acumulados, séries horárias, verificações de qualidade e inferência preditiva é adequada para uso operacional. No entanto, para elevar maturidade analítica e capacidade de resposta, recomenda-se evoluir para:

- medição explícita de **severidade** (não só frequência),
- **alertas preditivos** orientados por probabilidade,
- correção de consistência temporal em painéis específicos,
- e monitorização sistemática de **drift** e custo de erro do modelo.

Estas melhorias aumentam a confiança, reduzem risco de decisão e aproximam o dashboard de um verdadeiro cockpit de operação inteligente da rede.
