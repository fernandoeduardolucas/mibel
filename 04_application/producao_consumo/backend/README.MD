# Aplicação - produção/consumo

Este módulo contém o backend e frontend do dashboard final de produção/consumo.

## Estrutura

- `backend/producao_consumo/server.py`: API HTTP em Python.
- `frontend/producao_consumo/index.html`: dashboard web.

## Quickstart

Terminal 1:

```bash
cd 04_application/backend/producao_consumo
python3 server.py
```

Terminal 2:

```bash
cd 04_application/frontend/producao_consumo
python3 -m http.server 4173
```

Depois, abrir `http://localhost:4173`.
