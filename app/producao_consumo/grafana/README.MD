# Grafana Dashboard (Produção vs Consumo)

O provisioning do Grafana está em:

- `infrastructure/grafana/provisioning/datasources/trino.yml`
- `infrastructure/grafana/provisioning/dashboards/dashboards.yml`
- `infrastructure/grafana/dashboards/producao_consumo_overview.json`

## Como iniciar

```bash
cd infrastructure
GRAFANA_PORT=3300 docker compose up -d trino grafana
```

Depois abrir: http://localhost:3300 (ou a porta definida em `GRAFANA_PORT`).

- User: `admin`
- Password: `admin`

O dashboard é carregado automaticamente na pasta **MIBEL**.
