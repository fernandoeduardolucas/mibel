# 04 — Application (APIs HTTP + Frontends React)

Camada de serving do projeto MIBEL: três backends HTTP independentes e três frontends React que expõem os dados das tabelas Gold do lakehouse a utilizadores finais e sistemas externos.

---

## Diagrama de Arquitetura

```
Browser / Cliente HTTP
        │
        ├── GET /api/...   ──►  Backend HTTP (Python BaseHTTPRequestHandler)
        │                              │
        │                    ┌─────────┴──────────┐
        │                    │  MVC Pattern        │
        │                    │  Route → Controller │
        │                    │  → Service          │
        │                    │  → Repository       │
        │                    │  → Trino Client     │
        │                    └─────────┬──────────┘
        │                              │ SQL
        │                              ▼
        │                   iceberg.gold.*  (Trino)
        │
        └── index.html  ──►  Frontend (React 18, ESM CDN)
                                    │
                                    │ fetch(/api/...)
                                    ▼
                             Backend HTTP local
```

**Padrão MVC uniforme nos três backends:**

```
HTTP Handler (routing)
  └─► Controller (orquestra a resposta)
        └─► Service (lógica de negócio)
              └─► Repository (queries SQL parametrizadas)
                    └─► Trino DBAPI (trino Python connector)
```

---

## Estrutura de Ficheiros

```
04_application/
│
├── producao_consumo/                     DP-01
│   ├── backend/
│   │   ├── server.py                    Entry point HTTP (:8081)
│   │   ├── db/trino_client.py           Conexão Trino parametrizável
│   │   ├── models/                      Dataclasses de domínio
│   │   ├── controllers/                 Handlers HTTP por rota
│   │   ├── services/                    Lógica de negócio
│   │   └── repositories/               Queries SQL → Gold DP-01
│   └── frontend/
│       ├── index.html                   Importmap React 18 ESM
│       ├── main.js                      Entry point → src/main.js
│       ├── styles.css                   Estilos globais
│       └── src/
│           ├── main.js                  createRoot + StrictMode
│           └── components/
│               ├── DashboardApp.js      Layout principal + fetch
│               ├── KpiGrid.js           Cards KPI (saldo, ratio, flags)
│               ├── DataTables.js        Tabela horária paginada
│               ├── QuestionCards.js     Respostas às perguntas analíticas
│               ├── models.js            Tipos de dados
│               ├── services.js          fetch wrapper
│               └── formatters.js        Formatação de valores
│
├── backend/
│   └── consumo_preco/                   DP-02 (path histórico)
│       ├── server.py                    Entry point HTTP (:8000)
│       ├── db/trino_client.py
│       ├── models/
│       ├── controllers/
│       ├── services/
│       └── repositories/               Queries SQL → Gold DP-02
│
├── frontend/
│   └── consumo_preco/                   DP-02 frontend
│       ├── index.html
│       ├── main.js
│       ├── styles.css
│       └── src/
│           ├── main.js
│           └── components/
│               ├── DashboardApp.js
│               ├── KpiGrid.js
│               ├── TimeSeriesTable.js
│               └── AnalyticsChart.js
│
└── meteo_producao/                      DP-03
    ├── backend/
    │   ├── app/
    │   │   ├── main.py                  Entry point HTTP (:8083)
    │   │   ├── db/trino_client.py
    │   │   ├── models/
    │   │   ├── controllers/
    │   │   ├── services/
    │   │   └── repositories/           Queries SQL → Gold DP-03
    │   └── server.py
    └── frontend/
        ├── index.html
        ├── main.js
        ├── styles.css
        └── src/
            ├── main.js
            └── components/
                ├── DashboardApp.js
                ├── KpiGrid.js
                ├── TimeSeriesChart.js
                ├── ScatterChart.js
                ├── CorrelationTable.js
                └── ImportanceBar.js
```

---

## O Que Foi Implementado

### Backends (Python `BaseHTTPRequestHandler`)

Três servidores HTTP sem framework — escolha deliberada para simplicidade de deploy local. Cada backend expõe JSON sobre HTTP simples.

| DP | Porta | Rotas principais |
|---|---|---|
| DP-01 producao_consumo | 8081 | `GET /api/balance` — saldo horário, flags, ratio |
| DP-02 consumo_preco | 8000 | `GET /api/market` — consumo, preço, custo estimado |
| DP-03 meteo_producao | 8083 | `GET /api/meteo` — features diárias, correlações |

Todos os backends:

- Resolvem `host.docker.internal` → `localhost` automaticamente
- Aceitam variáveis de ambiente `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`
- Retornam JSON com CORS headers para facilitar desenvolvimento local
- Executam queries parametrizadas (sem concatenação de strings) sobre `iceberg.gold.*`

### Frontends (React 18, ESM CDN)

Três Single Page Applications sem build step — React importado via importmap ESM.sh (CDN). Abre diretamente no browser sem servidor de ficheiros estáticos.

| DP | Componentes principais | Visualizações |
|---|---|---|
| DP-01 | DashboardApp, KpiGrid, DataTables, QuestionCards | KPI cards (saldo, ratio, défices), tabela horária, respostas às perguntas analíticas |
| DP-02 | DashboardApp, KpiGrid, TimeSeriesTable, AnalyticsChart | KPI cards (consumo, preço, custo), série temporal, análise de correlação preço-consumo |
| DP-03 | DashboardApp, KpiGrid, TimeSeriesChart, ScatterChart, CorrelationTable, ImportanceBar | KPI cards meteo, série temporal temperatura/produção, scatter meteo×produção, importâncias de features |

### Justificação das Decisões

| Decisão | Justificação |
|---|---|
| **Python `BaseHTTPRequestHandler` sem framework** | Zero dependências externas; sem overhead de inicialização; adequado para APIs de leitura com baixo volume de pedidos; facilita deploy embebido num contentor mínimo |
| **React 18 via ESM.sh (sem build)** | Elimina Node.js, Webpack, Vite e qualquer toolchain de frontend; ficheiro `index.html` abre diretamente no browser sem servidor; reduz drasticamente a complexidade de setup |
| **Importmap para React** | Permite usar `import React from "react"` no browser sem bundler; versão pinned (`18.3.1`) garante reprodutibilidade |
| **Queries diretas ao Trino Gold** | Sem camada de cache intermédia; a tabela Gold é a fonte de verdade; simplifica o stack e elimina sincronização entre caches e lakehouse |
| **Separação MVC** | Controller/Service/Repository desacoplados permitem testar a lógica de negócio independentemente do HTTP handler; substituição futura do Trino por outra fonte requer apenas novo Repository |

---

## Passo a Passo de Execução

### Pré-requisito

Stack Docker a correr com dados nas tabelas Gold (ver `02_medallion_pipeline/README.md`).

### Backend DP-01 (porta 8081)

```powershell
cd 04_application/producao_consumo/backend
python server.py
```

### Backend DP-02 (porta 8000)

```powershell
cd 04_application/backend/consumo_preco
python server.py
```

### Backend DP-03 (porta 8083)

```powershell
# Usar -m para resolver imports do pacote app
cd 04_application/meteo_producao/backend
python -m app.main
```

### Frontends (abrir directamente no browser)

```
04_application/producao_consumo/frontend/index.html  → DP-01
04_application/frontend/consumo_preco/index.html     → DP-02
04_application/meteo_producao/frontend/index.html    → DP-03
```

Abrir com `File > Open` no browser ou via extensão "Live Server" no VS Code.

### Variáveis de Ambiente (opcional)

```powershell
$env:TRINO_HOST = "localhost"    # default: localhost / host.docker.internal
$env:TRINO_PORT = "8080"
$env:TRINO_USER = "tead"
```

### Verificar que o Backend Responde

```powershell
# DP-01
curl http://localhost:8081/api/balance

# DP-02
curl http://localhost:8000/api/market

# DP-03
curl http://localhost:8083/api/meteo
```
