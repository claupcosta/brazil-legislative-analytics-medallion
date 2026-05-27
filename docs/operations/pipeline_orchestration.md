# Pipeline Orchestration Strategy

## Objective

This document describes the operational execution strategy adopted for the
Brazil Legislative Analytics Medallion project during development,
validation and final demonstration phases.

The project supports both:

- full end-to-end orchestration execution
- notebook-by-notebook execution by Medallion layer

---

# Full Pipeline Orchestration

Main orchestration notebook:

```text
07_jobs/01_run_full_pipeline
```

Responsibilities:

- execute Setup notebooks
- execute Bronze ingestion notebooks
- execute Silver transformation notebooks
- execute Gold analytical notebooks
- execute Quality validation notebooks
- register operational execution logs
- support pipeline monitoring and replay execution

---

# Operational Execution Modes

The project supports multiple operational execution strategies.

## 1. Full Pipeline Execution

Recommended for:

- final demonstrations
- integration validation
- delivery execution
- end-to-end operational validation

Characteristics:

- sequential notebook orchestration
- audit logging enabled
- quality validations enabled
- lineage persistence enabled

Example:

```python
RUN_MODE = "full"
```

---

## 2. Development Execution Mode

Recommended for:

- notebook development
- transformation validation
- iterative testing
- Databricks Free Edition execution

Characteristics:

- notebook-by-notebook execution
- reduced orchestration overhead
- selective layer execution
- faster iteration cycle

Example:

```python
RUN_MODE = "development"
```

---

# Databricks Free Edition Considerations

During project development, the Databricks Free Edition environment
presented operational limitations related to:

- Serverless startup latency
- Unity Catalog synchronization overhead
- Delta transaction commit latency
- notebook orchestration overhead
- external API instability
- notebook context switching overhead
- audit logging latency

These limitations do not impact:

- architectural consistency
- Medallion implementation
- data lineage
- governance standards
- analytical outputs

However, they may increase total execution time during orchestration runs.

---

# Development Execution Strategy

To improve development productivity and reduce operational latency,
the project adopted notebook-by-notebook execution during iterative development.

This strategy was intentionally selected to:

- reduce orchestration overhead
- minimize Serverless startup delays
- simplify debugging
- isolate ingestion failures
- accelerate transformation validation
- improve execution stability

The orchestration pipeline remains fully available for:

- final execution
- validation scenarios
- replay execution
- demonstration purposes

---

# CSV Fallback Operational Strategy

Several Câmara dos Deputados API endpoints presented instability,
pagination inconsistency or timeout behavior during development.

Because of this, the project implemented operational CSV fallback ingestion
strategies for selected entities.

Fallback ingestion notebooks include:

```text
04a_bronze_votacoes_csv_fallback
05a_bronze_votos_csv_fallback
06a_bronze_despesas_ceap_csv_fallback
07a_bronze_orgaos_csv_fallback
08a_bronze_orgaos_membros_csv_fallback
09a_bronze_proposicoes_csv_fallback
```

Benefits:

- improved operational stability
- deterministic ingestion behavior
- historical ingestion consistency
- reduced API dependency
- replay execution support

The original API ingestion notebooks were preserved for:

- lineage completeness
- operational compatibility
- replay scenarios
- future migration support

---

# Pipeline Resilience Strategy

The pipeline was designed with resilience and operational continuity principles.

Implemented strategies include:

- retry policies
- timeout handling
- API error classification
- operational logging
- audit persistence
- CSV fallback ingestion
- selective notebook execution
- quality validation checkpoints

---

# Audit and Logging Strategy

Audit and monitoring capabilities are implemented through:

```text
audit.aud_log_execucao_pipeline
audit.aud_log_erros_pipeline
audit.aud_log_qualidade_dados
```

During development execution,
audit persistence may be selectively disabled
to reduce Serverless overhead in Databricks Free Edition.

This optimization does not impact:

- pipeline reproducibility
- lineage strategy
- final delivery execution
- governance implementation

---

# Recommended Operational Workflow

## Development Phase

Recommended execution flow:

```text
Setup
→ Bronze
→ Silver
→ Gold
→ Quality
```

Executed manually by notebook.

---

## Final Validation Phase

Recommended execution flow:

```text
01_run_full_pipeline
```

Executed as integrated orchestration pipeline.

---

# Architectural Consistency

The operational execution strategy adopted during development
does not modify the project architecture.

The project preserves:

- Medallion architecture principles
- dimensional modeling standards
- governance implementation
- lineage strategy
- auditability
- analytical reproducibility
- operational traceability

---

# References

- `/docs/architecture/README.md`
- `/docs/architecture/integrated_architecture.png`
- `/docs/governance/traceability.md`
- `/docs/governance/data_quality.md`
- `/docs/decisions/api_limitations.md`
- `/docs/standards/modeling_rules.md`