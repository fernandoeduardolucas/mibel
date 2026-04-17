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

Os dados são lidos diretamente da tabela Gold no Trino:

- `iceberg.gold.producao_vs_consumo_hourly`
