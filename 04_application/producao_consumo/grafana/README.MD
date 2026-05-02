# Grafana Dashboard (Produção vs Consumo)

O provisioning do Grafana está em:

- `01_bootstrap/tead_2.0_v1.2/grafana/provisioning/datasources/trino.yml`
- `01_bootstrap/tead_2.0_v1.2/grafana/provisioning/dashboards/dashboards.yml`
- `01_bootstrap/tead_2.0_v1.2/grafana/dashboards/producao_consumo_overview.json`

## Como iniciar

```bash
cd 01_bootstrap/tead_2.0_v1.2
GRAFANA_PORT=3300 docker compose up -d trino grafana
```

Depois abrir: http://localhost:3300 (ou a porta definida em `GRAFANA_PORT`).

- User: `admin`
- Password: `admin`

O dashboard é carregado automaticamente na pasta **MIBEL**.
