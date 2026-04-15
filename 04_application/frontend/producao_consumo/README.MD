# Frontend - Dashboard produção/consumo

Dashboard web (HTML/CSS/JS) para visualizar KPIs e série temporal de produção vs consumo.

## Executar

No diretório do frontend, levantar um servidor estático:

```bash
cd 04_application/frontend/producao_consumo
python3 -m http.server 4173
```

Abrir `http://localhost:4173` no browser.

> A API base é preenchida automaticamente com o mesmo host da página e porta `8000` (ex.: se abrir em `http://192.168.1.20:4173`, tenta `http://192.168.1.20:8000`).
> Se não responder, o frontend tenta também `127.0.0.1`, `localhost`, `::1` e `host.docker.internal`.
> Antes de abrir o dashboard, inicie também o backend:
> `python3 04_application/backend/producao_consumo/server.py`
