# Project Execution Guide

## Objective

This document describes the recommended execution sequence for the
Brazil Legislative Analytics Medallion project.

It provides guidance for running the project in Databricks during development,
validation and final demonstration phases.

---

# Execution Strategy Overview

The project supports two execution strategies:

1. Notebook-by-notebook execution
2. Full pipeline orchestration

During development, notebook-by-notebook execution is recommended because it
reduces Databricks Free Edition overhead and makes debugging easier.

For final validation and demonstration, the full pipeline job can be executed.

---

# Recommended Execution Flow

```text
Setup
→ Bronze
→ Silver
→ Gold
→ Marts
→ Quality
→ Jobs
```

---

# 1. Initial Setup

Run the setup notebooks before executing any pipeline layer.

Recommended order:

```text
00_setup/00_create_catalog_schemas
00_setup/01_project_config
00_setup/02_audit_tables
00_setup/90_validate_project_setup
```

Expected result:

- catalog created
- schemas created
- audit tables created
- project configuration loaded
- setup validation passed

---

# 2. Bronze Layer Execution

The Bronze layer is responsible for raw ingestion and source fidelity.

Recommended order:

```text
01_bronze/01_bronze_deputados
01_bronze/02_bronze_frentes
01_bronze/03_bronze_eventos
01_bronze/04a_bronze_votacoes_csv_fallback
01_bronze/05a_bronze_votos_csv_fallback
01_bronze/06a_bronze_despesas_ceap_csv_fallback
01_bronze/07a_bronze_orgaos_csv_fallback
01_bronze/08a_bronze_orgaos_membros_csv_fallback
01_bronze/09a_bronze_proposicoes_csv_fallback
```

API notebooks may be preserved for validation and controlled replay scenarios:

```text
01_bronze/04_bronze_votacoes
01_bronze/05_bronze_votos
01_bronze/06_bronze_despesas_ceap
01_bronze/07_bronze_orgaos
01_bronze/08_bronze_orgaos_membros
01_bronze/09_bronze_proposicoes
```

---

# 3. Bronze Validation

After Bronze execution, validate table creation:

```sql
SHOW TABLES IN brazil_legislative_analytics.bronze;
```

Recommended count validation:

```sql
SELECT 'br_deputados' AS table_name, COUNT(*) AS total_records FROM brazil_legislative_analytics.bronze.br_deputados
UNION ALL
SELECT 'br_eventos', COUNT(*) FROM brazil_legislative_analytics.bronze.br_eventos
UNION ALL
SELECT 'br_votacoes', COUNT(*) FROM brazil_legislative_analytics.bronze.br_votacoes
UNION ALL
SELECT 'br_votos', COUNT(*) FROM brazil_legislative_analytics.bronze.br_votos
UNION ALL
SELECT 'br_orgaos', COUNT(*) FROM brazil_legislative_analytics.bronze.br_orgaos
UNION ALL
SELECT 'br_orgaos_membros', COUNT(*) FROM brazil_legislative_analytics.bronze.br_orgaos_membros
UNION ALL
SELECT 'br_proposicoes', COUNT(*) FROM brazil_legislative_analytics.bronze.br_proposicoes
UNION ALL
SELECT 'br_despesas_ceap', COUNT(*) FROM brazil_legislative_analytics.bronze.br_despesas_ceap;
```

---

# 4. Silver Layer Execution

The Silver layer is responsible for:

- data cleansing
- standardization
- deduplication
- data typing
- business validation
- invalid record segregation

Recommended order:

```text
02_silver/01_silver_deputados
02_silver/02_silver_orgaos
02_silver/03_silver_eventos
02_silver/04_silver_votacoes
02_silver/05_silver_votos
02_silver/06_silver_proposicoes
02_silver/07_silver_despesas_ceap
02_silver/08_silver_orgaos_membros
02_silver/09_silver_registros_rejeitados
```

Recommended strategy:

- start with smaller/reference entities
- validate each Silver table before moving forward
- process large tables after rules are stable
- keep invalid records traceable

---

# 5. Silver Validation

Recommended validations:

```sql
SHOW TABLES IN brazil_legislative_analytics.silver;
```

Basic validation examples:

```sql
SELECT COUNT(*) FROM brazil_legislative_analytics.silver.slv_deputados;
SELECT COUNT(*) FROM brazil_legislative_analytics.silver.slv_orgaos;
SELECT COUNT(*) FROM brazil_legislative_analytics.silver.slv_votacoes;
```

For rejected records:

```sql
SELECT motivo_rejeicao, COUNT(*) AS total_records
FROM brazil_legislative_analytics.silver.slv_registros_rejeitados
GROUP BY motivo_rejeicao
ORDER BY total_records DESC;
```

---

# 6. Gold Layer Execution

The Gold layer is responsible for dimensional modeling and analytical readiness.

Recommended order:

```text
03_gold/dim_deputado
03_gold/dim_orgao
03_gold/dim_partido
03_gold/dim_data
03_gold/fato_votacoes
03_gold/fato_votos
03_gold/fato_eventos
03_gold/fato_despesas_ceap
03_gold/fato_presenca_eventos
```

Gold layer principles:

- use star schema
- define table grain clearly
- avoid physical fact-to-fact relationships
- use dimensions as analytical conformed entities
- prepare data for marts and KPIs

---

# 7. Marts Layer Execution

The Marts layer provides final analytical datasets.

Recommended examples:

```text
05_marts/am_panorama_despesas_ceap
05_marts/am_presenca_parlamentar
05_marts/am_votacoes
05_marts/am_eventos_legislativos
05_marts/am_proposicoes
```

Marts should answer business questions and support final analytical delivery.

---

# 8. Quality Checks

Quality notebooks should be executed after each main layer is created.

Recommended order:

```text
06_quality/01_quality_bronze_checks
06_quality/02_quality_silver_checks
06_quality/03_quality_gold_checks
06_quality/04_traceability_checks
```

During development, quality checks may be executed selectively to reduce
Databricks Free Edition overhead.

For final validation, quality checks should be executed fully.

---

# 9. Full Pipeline Execution

Main orchestration notebook:

```text
07_jobs/01_run_full_pipeline
```

Recommended for:

- final validation
- demonstration
- integration execution
- pipeline replay

The full pipeline supports:

- setup validation
- Bronze execution
- fallback ingestion
- quality execution
- operational logging
- controlled failure behavior

---

# 10. Development Execution Mode

During development, the recommended approach is:

```text
Run notebooks individually by layer
```

Benefits:

- faster debugging
- less serverless overhead
- easier error isolation
- better control over large tables
- reduced risk of exhausting Databricks Free Edition limits

---

# 11. CSV Fallback Strategy

CSV fallback is the recommended operational strategy for high-volume or unstable endpoints.

Fallback notebooks include:

```text
04a_bronze_votacoes_csv_fallback
05a_bronze_votos_csv_fallback
06a_bronze_despesas_ceap_csv_fallback
07a_bronze_orgaos_csv_fallback
08a_bronze_orgaos_membros_csv_fallback
09a_bronze_proposicoes_csv_fallback
```

API notebooks are preserved for:

- validation
- controlled extraction
- replay
- future compatibility

---

# 12. Troubleshooting

## API timeout

Recommended action:

- use CSV fallback when available
- avoid broad API filters
- reduce pagination scope
- retry later if API instability persists

## Databricks Free Edition daily limit

Recommended action:

- stop heavy execution
- continue documentation work
- resume processing after limit resets
- avoid rerunning Bronze unnecessarily

## Missing audit table

Run:

```text
00_setup/02_audit_tables
```

Then validate:

```sql
SHOW TABLES IN brazil_legislative_analytics.audit;
```

## Missing Bronze table

Run the related Bronze ingestion notebook individually.

## Full pipeline slow execution

Recommended action:

- use notebook-by-notebook execution during development
- reserve full pipeline execution for final validation

---

# 13. Recommended Daily Workflow

## Development

```text
1. Run only the notebook being developed
2. Validate output table
3. Check audit logs
4. Commit changes to Git
5. Continue to next entity
```

## Final Validation

```text
1. Run setup validation
2. Run full pipeline
3. Run quality checks
4. Validate audit logs
5. Create Git release tag
```

---

# 14. Current Recommended Path

At the current project stage:

```text
Bronze Layer: completed
Silver Layer: next implementation step
Gold Layer: planned
Marts Layer: planned
Quality Final Review: planned
```

Recommended next execution:

```text
02_silver/01_silver_deputados
```

---

# References

- `/docs/operations/pipeline_orchestration.md`
- `/docs/decisions/api_limitations.md`
- `/docs/decisions/silver_layer_strategy.md`
- `/docs/governance/data_quality.md`
- `/docs/governance/traceability.md`
- `/docs/architecture/README.md`