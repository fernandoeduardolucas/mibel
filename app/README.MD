# Aplicações

Este módulo contém os backends e frontends dos dashboards finais.

## Estrutura

- `backend/consumo_preco/server.py`: API HTTP em Python para consumo/preço.
- `frontend/consumo_preco/index.html`: dashboard web de consumo/preço.
- `producao_consumo/backend/server.py`: API HTTP em Python para produção/consumo.
- `producao_consumo/frontend/index.html`: dashboard web de produção/consumo.
- `producao_consumo/grafana/`: instruções de Grafana para produção/consumo.

## Quickstart produção/consumo

Terminal 1:

```bash
cd app/producao_consumo/backend
python3 server.py
```

Terminal 2:

```bash
cd app/producao_consumo/frontend
python3 -m http.server 4173
```

Depois, abrir `http://localhost:4173`.

## Quickstart consumo/preço

Terminal 1:

```bash
python3 app/backend/consumo_preco/server.py
```

Terminal 2:

```bash
cd app/frontend/consumo_preco
python3 -m http.server 4174
```

Depois, abrir `http://localhost:4174`.
