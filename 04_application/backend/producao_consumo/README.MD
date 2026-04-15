# Backend - Dashboard produção/consumo

API HTTP em Python (sem dependências externas) para alimentar o dashboard.

## Executar

```bash
cd 04_application/backend/producao_consumo
python3 server.py
```

A API arranca por omissão em `http://localhost:8000`.

## Endpoints

- `GET /health`
- `GET /api/overview`
- `GET /api/timeseries?group=day`
- `GET /api/timeseries?group=month`

## Fonte de dados

Os dados são lidos dos CSV do pipeline medallion:

- `02_medallion_pipeline/producao_consumo/01_bronze/data/raw/consumo-total-nacional.csv`
- `02_medallion_pipeline/producao_consumo/01_bronze/data/raw/energia-produzida-total-nacional.csv`
