# Backend produção/consumo (Python REST API em camadas + MVC)

Backend REST em Python organizado em camadas, usando como fonte de dados o Gold SQL:

- `02_medallion_pipeline/producao_consumo/03_gold/sql/01_gold_trino.sql`

## Arquitetura

A API foi organizada no padrão **MVC + camadas**:

- **Model**: `app/models/energy_point.py`
- **Controller (API)**: `app/controllers/producao_consumo_controller.py`
- **Service/Business**: `app/services/producao_consumo_service.py`
- **Repository (acesso a dados)**: `app/repositories/producao_consumo_repository.py`
- **Query DB/Cliente Trino**: `app/db/trino_client.py`

## Ligação à base de dados (Trino)

A ligação é feita **sem docker exec**, diretamente ao Trino em:

- JDBC URL: `jdbc:trino://localhost:8080/`
- user: `trino`
- password: *(sem password)*

No backend Python, isto corresponde a:

- host: `localhost`
- port: `8080`
- user: `trino`
- catalog: `iceberg`
- schema: `gold`

## Estrutura

```text
04_application/producao_consumo/backend/
├── app/
│   ├── config.py
│   ├── main.py
│   ├── models/
│   │   └── energy_point.py
│   ├── db/
│   │   └── trino_client.py
│   ├── repositories/
│   │   └── producao_consumo_repository.py
│   ├── services/
│   │   └── producao_consumo_service.py
│   └── controllers/
│       └── producao_consumo_controller.py
└── server.py
```

## Endpoints REST

- `GET /health`
- `GET /api/v1/producao-consumo/hourly?start=2024-01-01T00:00:00+00:00&end=2024-01-02T00:00:00+00:00`
- `GET /api/v1/producao-consumo/daily`
- `GET /api/v1/producao-consumo/monthly`
- `GET /api/v1/producao-consumo/analytics`
- `GET /api/v1/producao-consumo/db-connection`
- `GET /api/v1/producao-consumo/predictions/next-hour`

## Como correr

Na raiz do repositório:

```bash
cd 04_application/producao_consumo/backend
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python server.py
```

> **macOS/Homebrew (PEP 668):** se aparecer `externally-managed-environment`, use sempre o ambiente virtual (`.venv`) acima e evite instalar dependências no Python global.

### Erros comuns

- `ModuleNotFoundError: No module named 'pandas'`
  - O pacote não foi instalado no ambiente ativo. Ative o `.venv` e volte a correr `python -m pip install -r requirements.txt`.
- `cd: no such file or directory: 04_application/producao_consumo/backend`
  - Esse comando só funciona quando executado a partir da raiz do repositório. Se já estiver dentro de `.../backend`, não faça `cd` novamente.
- `zsh: command not found: py`
  - Em macOS, use `python`/`python3` (o launcher `py` é típico do Windows).

Por omissão, a API sobe em `http://localhost:8081`.
