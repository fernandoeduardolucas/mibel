# ML Pipeline

Flow de MLflow para o domínio **producao_consumo**.

## O que este flow faz

- Lê dados de `iceberg.gold.producao_vs_consumo_hourly` via Trino.
- Cria features temporais e lags de consumo/produção/saldo/ratio.
- Treina um `RandomForestClassifier` para prever `flag_defice` na hora seguinte (`t+1`).
- Regista métricas, parâmetros e artefactos no MLflow.
- Regista o modelo em `producao_consumo_defice_classifier`.

## Ficheiro principal

- `03_ml_pipeline/producao_consumo_mlflow_flow.py`

## Execução local

Antes de correr localmente, instala as dependências (incluindo `mlflow`):

```bash
python -m pip install pandas==2.2.3 scikit-learn==1.6.1 trino==0.336.0 mlflow==3.10.1 boto3
```

```bash
python 03_ml_pipeline/producao_consumo_mlflow_flow.py
```

Se estiveres fora de Docker (a correr Python diretamente no host), podes precisar de configurar endpoints explicitamente:

```bash
export TRINO_HOST=localhost
export TRINO_PORT=8080
export MLFLOW_TRACKING_URI=http://localhost:15000
export MLFLOW_S3_ENDPOINT_URL=http://localhost:9000
```

## Execução com Flyte (remoto)

```bash
pyflyte run --remote -p flytesnacks -d development \
  03_ml_pipeline/producao_consumo_mlflow_flow.py producao_consumo_training_wf
```

## Configuração padrão usada no script

- Trino: `http://host.docker.internal:8080` (Flyte task pods)
- Tabela: `iceberg.gold.producao_vs_consumo_hourly`
- MLflow: `http://host.docker.internal:15000` (Flyte task pods)
- Experiência: `producao-consumo-defice`

### Endpoints a usar dentro de Flyte Sandbox tasks

Como as tasks Flyte correm num container/K3s isolado, usa `host.docker.internal` para aceder aos serviços publicados no host:

- MinIO S3 API: `http://host.docker.internal:9000`
- MLflow: `http://host.docker.internal:15000`
- Trino: `http://host.docker.internal:8080`
- Hive Metastore: `thrift://host.docker.internal:9083`

Se precisares, posso também criar um segundo flow para **re-treino agendado** + **promoção automática de versão no Model Registry**.

## Checklist rápida: Flyte + projeto a funcionar

No Docker Desktop, para haver orquestração real entre Flyte e este projeto, o `flyte-sandbox` e os serviços da stack (`trino`, `mlflow`, `minio`, `hive-metastore`) devem estar **a correr**.

### Sinais de que está OK

- `flyte-sandbox` em estado **running**.
- `tead_20_v12` (ou serviços individuais) em estado **running**.
- UI Flyte acessível em `http://localhost:30080`.
- MLflow acessível em `http://localhost:15000`.
- Trino acessível em `http://localhost:8080`.

### Comandos de validação

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
curl -sf http://localhost:30080 >/dev/null && echo "Flyte UI OK"
curl -sf http://localhost:15000 >/dev/null && echo "MLflow OK"
curl -sf http://localhost:8080 >/dev/null && echo "Trino OK"
```

Se os containers aparecerem parados no Docker Desktop, a orquestração ainda **não** está ativa; primeiro inicia os containers e depois executa o `pyflyte run --remote ...`.

## Agora que a UI do Flyte está no ar (`localhost:30080`), o que fazer?

1. **Confirma que a stack de dados está running** (`trino`, `mlflow`, `minio`, `hive-metastore`).
2. **Executa o workflow remoto** (no host, com `pyflyte` instalado):

   ```bash
   pyflyte run --remote -p flytesnacks -d development \
     03_ml_pipeline/producao_consumo_mlflow_flow.py producao_consumo_training_wf
   ```

3. **Na UI do Flyte**:
   - entra em `flytesnacks` → `development`;
   - abre a execution criada;
   - valida que as tasks ficam `SUCCEEDED`.

4. **Valida integração externa**:
   - MLflow (`http://localhost:15000`): confirmar experimento novo e métricas;
   - Trino (`http://localhost:8080`): confirmar que a leitura da tabela Gold ocorreu sem erro.

### Erros comuns rápidos

- `Connection refused` para `localhost` dentro da task Flyte  
  → usar `host.docker.internal` nos endpoints de runtime da task (já definido neste projeto).
- `project/domain not found` no `pyflyte run`  
  → usar `-p flytesnacks -d development`.

## Quando a execução está `RUNNING` no Flyte: próximos passos

Se estiveres no ecrã de execução (como no teu screenshot), faz isto:

1. Clica no node `train_producao_consumo_...` para abrir logs da task.
2. Espera o estado mudar de `RUNNING` para `SUCCEEDED`.
3. Depois de concluir, valida:
   - **Flyte**: workflow com estado final `SUCCEEDED`;
   - **MLflow** (`http://localhost:15000`): nova run no experimento `producao-consumo-defice`;
   - **Artefactos**: métricas e modelo registados.

### Se ficar preso muito tempo em `RUNNING`

- Ver logs da task na UI do Flyte e procurar erros de ligação (Trino/MLflow/MinIO).
- Confirmar no host:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
curl -sf http://localhost:15000 >/dev/null && echo "MLflow OK"
curl -sf http://localhost:8080 >/dev/null && echo "Trino OK"
```

- Se necessário, terminar a execução atual e relançar o `pyflyte run --remote ...`.

## O que foi feito (resumo) e o que testar

### O que foi feito

- Foi executado um workflow remoto no Flyte (`producao_consumo_training_wf`) no projeto `flytesnacks` e domínio `development`.
- A execução chegou a estado final `SUCCEEDED`.
- O treino registou resultados no MLflow, incluindo o experimento `producao-consumo-defice`.

### O que testar agora (checklist prático)

1. **Reprodutibilidade mínima**
   - Relançar a execução pelo botão **Relaunch** na UI Flyte.
   - Confirmar que uma nova execution também termina em `SUCCEEDED`.

2. **Rastreabilidade no MLflow**
   - Abrir `http://localhost:15000`.
   - Entrar no experimento `producao-consumo-defice`.
   - Confirmar criação de uma nova run com métricas, parâmetros e artefactos.

3. **Sanidade da infraestrutura**
   - Confirmar containers essenciais em running (`flyte-sandbox`, `trino`, `mlflow`, `minio`, `hive-metastore`).
   - Confirmar endpoints do host acessíveis (`30080`, `15000`, `8080`).

4. **Teste de falha controlada (opcional)**
   - Parar temporariamente o `trino`.
   - Relançar workflow e validar que falha com erro de conexão (esperado).
   - Voltar a subir `trino` e confirmar recuperação no próximo run.

## FAQ rápida

### Este experimento altera a base de dados?

Não, este flow de ML **não escreve** na tabela Gold nem em Trino: ele apenas faz `SELECT` para ler dados e treinar o modelo.
O output principal é guardado no MLflow (métricas, parâmetros, artefactos e modelo registado).

### Onde está o resultado do experimento?

- **Flyte UI**: estado da execução (`SUCCEEDED`/`FAILED`) e logs da task.
- **MLflow UI** (`http://localhost:15000`): experimento, runs, métricas, parâmetros e artefactos.
- **Model Registry (MLflow)**: modelo registado como `producao_consumo_defice_classifier`.

### Qual foi o experimento usado?

O nome do experimento é `producao-consumo-defice` (valor default de `MLFLOW_EXPERIMENT` neste projeto).
