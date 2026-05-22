# 04 — Application (Visualização via Grafana)

Camada de serving do projecto MIBEL. Conforme requisito do cliente, a visualização de todos os Data Products é exclusivamente via **Grafana**.

---

## Dashboards

Os três dashboards são provisionados automaticamente pelo Docker Compose a partir de `grafana/dashboards/`. Não requerem configuração manual.

| DP | Dashboard | URL |
| --- | --- | --- |
| DP-01 | Balanco da Rede Nacional — Producao . Consumo . Saldo | <http://localhost:3300/d/mibel-energy-intelligence-v2> |
| DP-02 | Mercado Iberico MIBEL — Consumo & Precos Day-Ahead | <http://localhost:3300/d/mibel-consumo-preco-v1> |
| DP-03 | Clima & Producao Renovavel — Observatorio Meteorologico | <http://localhost:3300/d/dp03-meteo-producao> |

Credenciais: `admin / admin` — Grafana disponível em <http://localhost:3300>

---

## Estrutura de Ficheiros

```text
04_application/
└── grafana/
    ├── dashboards/
    │   ├── producao_consumo_overview.json   # DP-01
    │   ├── consumo_preco_overview.json      # DP-02
    │   └── meteo_producao_overview.json     # DP-03
    └── provisioning/
        ├── dashboards/
        │   └── dashboards.yml              # Auto-load dos ficheiros JSON
        └── datasources/
            └── trino.yml                   # Datasource Trino (UID: trino-iceberg)
```

---

## Alterar Dashboards

Editar o ficheiro JSON correspondente em `grafana/dashboards/` e aplicar sem reiniciar o stack:

```powershell
curl -X POST http://localhost:3300/api/admin/provisioning/dashboards/reload `
     -u admin:admin
```

---

## Iniciar o Stack

```powershell
cd 01_docker_stack
docker compose up -d
```

Grafana disponível em <http://localhost:3300> (~30 segundos após startup).
