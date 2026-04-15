# 02_medallion_pipeline

## Estado atual (aplicado no projeto)

Foi aplicada uma normalização inicial da estrutura do domínio `producao_consumo`:

- `1-bronze_project` -> `bronze`
- `2-silver_project` -> `silver`
- `3-gold_project` -> `gold`
- criação de `pipeline/run_all.sh` para execução sequencial
- adição de `99_checks.sql` em Bronze/Silver/Gold
- remoção de artefactos de IDE (`.idea`) da camada Bronze

## Estrutura atual

```text
02_medallion_pipeline/
├── consumo_preco/
├── meteo_producao/
├── producao_consumo/
│   ├── bronze/
│   │   ├── data/
│   │   ├── docs/
│   │   ├── scripts/python/
│   │   ├── sql/
│   │   │   ├── bronze_trino.sql
│   │   │   └── 99_checks.sql
│   │   └── README.txt
│   ├── silver/
│   │   ├── docs/
│   │   ├── sql/
│   │   │   ├── 01_silver_trino.sql
│   │   │   └── 99_checks.sql
│   │   └── README.txt
│   ├── gold/
│   │   ├── docs/
│   │   ├── sql/
│   │   │   ├── 01_gold_trino.sql
│   │   │   └── 99_checks.sql
│   │   └── README.txt
│   ├── pipeline/
│   │   └── run_all.sh
│   ├── docs/
│   │   ├── INDEX.md
│   │   └── produto_gold_a_sem_silver.docx
│   └── README.md
└── README.MD
```

## Próximo passo recomendado

Aplicar o mesmo padrão de camadas ao fluxo `meteo_producao + consumo_preco` para convergir no domínio analítico `meteo_consumo_preco`.
