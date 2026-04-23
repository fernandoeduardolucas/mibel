# Frontend React — Dashboard Produção/Consumo

Frontend em **React (sem build step)** com arquitetura por camadas para consumo da API:

- **models**: definição de perguntas analíticas e opções de configuração.
- **services**: integração com endpoints REST.
- **components**: composição visual do dashboard.
- **utils**: formatação e helpers reutilizáveis.

## Funcionalidades

- Dashboard configurável (base da API + agregação diária/mensal).
- Cartões com perguntas analíticas.
- KPIs principais de consumo/produção e dependência PRE/DGM.
- Cartão de previsão ML da próxima hora (probabilidade de défice).
- Tabelas de défice horário e série temporal agregada.
- Design profissional responsivo.

## Executar

1. Subir backend:

```bash
cd 04_application/producao_consumo/backend
python3 server.py
```

2. Servir frontend:

```bash
cd 04_application/producao_consumo/frontend
python3 -m http.server 4173
```

3. Abrir no browser:

- `http://localhost:4173`
