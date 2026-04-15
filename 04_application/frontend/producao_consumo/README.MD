# Frontend - Dashboard produção/consumo

Dashboard web (HTML/CSS/JS) para visualizar KPIs e série temporal de produção vs consumo.

## Executar

No diretório do frontend, levantar um servidor estático:

```bash
cd 04_application/frontend/producao_consumo
python3 -m http.server 4173
```

Abrir `http://localhost:4173` no browser.

> A API base por defeito é `http://localhost:8000`, editável no próprio dashboard.
> Antes de abrir o dashboard, inicie também o backend:
> `python3 04_application/backend/producao_consumo/server.py`
