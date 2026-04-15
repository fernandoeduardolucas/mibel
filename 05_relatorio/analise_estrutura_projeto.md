# Análise da estrutura do projeto (trabalho de grupo)

## Objetivo desta análise
Avaliar o estado atual do repositório face aos 3 requisitos académicos:
1. **Lakehouse**
2. **Modelos preditivos**
3. **Aplicação para visualização/recomendação**

---

## 1) Leitura rápida da estrutura atual

O projeto está organizado por fases:

- `01_bootstrap/tead_2.0_v1.2/` → infraestrutura local (Docker Compose)
- `02_medallion_pipeline/` → dados de base (meteo, consumo/preço)
- `03_ml_pipeline/` → área reservada a ML (ainda sem conteúdo)
- `04_application/` → área reservada à app (ainda sem conteúdo)
- `05_relatorio/` → relatório e documentação final

### Evidências relevantes

- Stack lakehouse já prevista com **MinIO + Hive Metastore + Trino + MLflow + PostgreSQL**, com buckets `warehouse` e `mlflow`.
- Existem datasets base em CSV para:
  - meteorologia (`open-meteo-...csv`)
  - consumo nacional (`consumo-total-nacional.csv`)
- O `03_ml_pipeline/README.MD` e `04_application/README.MD` estão vazios.

---

## 2) Avaliação por requisito académico

### A) Lakehouse

**Estado atual: parcialmente implementado (infraestrutura pronta, pipeline incompleto).**

**Pontos fortes já presentes**
- Armazenamento objeto (MinIO) para dados e artefactos.
- Catálogo/metastore (Hive Metastore).
- Motor SQL analítico (Trino).
- Tracking de experiências ML (MLflow).

**Lacunas para fechar requisito**
- Não há evidência de tabelas Bronze/Silver/Gold materializadas.
- Não há scripts/workflows no repositório para ingestão, limpeza e modelação de camadas.
- Faltam contratos de dados (schema esperado, validações e versionamento de tabelas).

**Conclusão lakehouse**
- A base técnica está correta para um lakehouse académico.
- Falta operacionalizar o pipeline medallion end-to-end com outputs verificáveis.

---

### B) Modelos preditivos

**Estado atual: por implementar.**

**O que já existe para suportar ML**
- MLflow está integrado na stack e pronto para registar experiências.
- Dados de consumo e meteorologia permitem criar features temporais e exógenas.

**Lacunas**
- Sem notebooks/scripts de treino.
- Sem definição clara da variável-alvo (ex.: consumo total t+1h, pico diário, etc.).
- Sem baseline e métricas (MAE/RMSE/MAPE).
- Sem separação treino/validação/teste temporal.

**Conclusão modelos preditivos**
- O projeto tem bom potencial, mas ainda não cumpre o requisito de “modelos preditivos” até existir treino + avaliação + registo no MLflow.

---

### C) Aplicação de visualização/recomendação

**Estado atual: por implementar.**

**Lacunas**
- Pasta `04_application/` sem conteúdo funcional.
- Sem dashboard, sem API, sem camada de recomendação.

**Recomendação mínima para cumprir requisito académico**
- Dashboard com:
  - séries históricas (consumo, meteo)
  - previsão vs real
  - KPI de erro do modelo
- Módulo de recomendação simples baseado em regras + previsão, por exemplo:
  - “evitar consumo intensivo nas próximas X horas” quando previsão de pico > limiar.

**Conclusão aplicação**
- Ainda não cumpre o requisito; precisa de MVP funcional com visualização e lógica de recomendação.

---

## 3) Proposta de divisão de trabalho (grupo)

### Frente 1 — Data/Lakehouse
- Ingestão para Bronze (raw CSV -> objetos versionados)
- Transformações Silver (normalização de timestamps, tratamento de nulos/outliers)
- Data mart Gold para ML e dashboard
- Queries Trino de validação

### Frente 2 — ML
- Definir problema de previsão (horizonte e target)
- Criar baseline (média móvel / regressão simples)
- Treinar modelo principal (ex.: XGBoost/LightGBM ou similar)
- Registar runs e métricas em MLflow

### Frente 3 — Aplicação
- Construir dashboard (Streamlit/Dash)
- Integrar previsão e indicadores
- Implementar recomendação baseada em regras de negócio
- Preparar demo final orientada a decisão

---

## 4) Roadmap curto (2 sprints)

### Sprint 1 (infra + dados + baseline)
- [ ] Definir schema Bronze/Silver/Gold
- [ ] Automatizar ingestão e transformação mínima
- [ ] Criar baseline preditiva e logging em MLflow
- [ ] Publicar primeira versão do dashboard com dados históricos

### Sprint 2 (modelo + recomendação + robustez)
- [ ] Melhorar features e modelo
- [ ] Comparar modelos com métricas padronizadas
- [ ] Ativar recomendações no front-end
- [ ] Ensaiar narrativa final (problema -> dados -> previsão -> decisão)

---

## 5) Critérios de aceitação sugeridos (para a entrega)

- **Lakehouse:** evidência de 3 camadas (Bronze/Silver/Gold) + queries de validação.
- **ML:** pelo menos 1 baseline + 1 modelo melhorado, métricas em conjunto de teste temporal, runs no MLflow.
- **App:** interface executável que mostre dados, previsões e recomendações acionáveis.

---

## Conclusão geral

A estrutura atual está bem orientada e já contém uma fundação lakehouse credível. Contudo, para cumprir plenamente o enunciado académico, o projeto precisa de transformar a base de infraestrutura em **pipelines de dados reais**, **modelos preditivos avaliados** e uma **aplicação funcional de visualização/recomendação**.


## 6) Recomendação específica para `02_medallion_pipeline`

Para melhorar a colaboração em grupo, recomenda-se normalizar a estrutura por **domínio + camada medallion** e consolidar documentação e regras partilhadas. Uma proposta concreta foi adicionada em `02_medallion_pipeline/README.MD`.

Sugestão prática imediata:
- renomear `1-bronze_project`, `2-silver_project`, `3-gold_project` para `bronze`, `silver`, `gold`;
- criar convenção SQL por ordem (`01_create_tables.sql`, `02_transform.sql`, `99_checks.sql`);
- centralizar documentação em `docs/`;
- remover artefactos de IDE (`.idea`) desta área;
- criar script único de execução (ou workflow Flyte) para corrida end-to-end.

**Atualização:** parte destas recomendações já foi aplicada em `02_medallion_pipeline/producao_consumo` (renomeação de pastas para `bronze/silver/gold`, criação de `pipeline/run_all.sh` e `99_checks.sql` por camada).
