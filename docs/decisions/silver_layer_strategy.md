# Silver Layer Strategy

## Objective

This document defines the architectural, operational and governance strategy
for the Silver layer of the Brazil Legislative Analytics Medallion project.

The Silver layer is responsible for transforming raw Bronze datasets into
validated, standardized, analytics-ready and governance-compliant datasets.

This layer acts as the controlled transformation boundary between:

```text
Bronze → Raw ingestion
Silver → Curated and standardized data
Gold → Analytical dimensional modeling
```

---

# Silver Layer Responsibilities

The Silver layer is responsible for:

- data cleansing
- schema normalization
- data standardization
- data typing
- deduplication
- quality validation
- business validation
- invalid record segregation
- analytical enrichment
- governance standardization
- preparation for dimensional modeling

---

# Silver Layer Principles

The Silver layer follows the principles below.

## 1. Analytical Readiness

Silver datasets must be prepared for downstream analytical consumption.

This includes:

- standardized schemas
- typed fields
- normalized structures
- validated identifiers
- controlled nullability

---

## 2. Governance Enforcement

Silver is the first layer where governance and business validation rules are enforced.

Examples:

- invalid record segregation
- duplicate prevention
- mandatory field validation
- business integrity validation
- reference consistency validation

---

## 3. Controlled Transformation

Silver transformations must preserve lineage and traceability.

Each transformation must maintain:

- source reference
- execution identifier
- processing timestamp
- ingestion lineage
- operational metadata

---

## 4. Reproducibility

Silver transformations must be deterministic and reproducible.

This means:

- same input → same output
- stable transformation rules
- controlled enrichment
- auditable processing logic

---

# Silver Layer Scope

The Silver layer includes:

- schema normalization
- field renaming
- type casting
- duplicate removal
- invalid record identification
- null handling
- enrichment preparation
- controlled business validation

The Silver layer does NOT include:

- analytical KPIs
- marts
- rankings
- aggregated indicators
- dashboards
- star schema modeling
- executive reporting

Those responsibilities belong to the Gold and Marts layers.

---

# Silver Naming Convention

Recommended naming pattern:

```text
silver.slv_<entity_name>
```

Examples:

```text
silver.slv_deputados
silver.slv_eventos
silver.slv_votacoes
silver.slv_votos
silver.slv_orgaos
silver.slv_proposicoes
silver.slv_despesas_ceap
```

---

# Silver Layer Tables

## Main Curated Tables

| Table | Description |
|---|---|
| slv_deputados | Standardized deputies |
| slv_eventos | Standardized legislative events |
| slv_votacoes | Standardized voting sessions |
| slv_votos | Standardized voting records |
| slv_orgaos | Standardized legislative bodies |
| slv_orgaos_membros | Standardized organization members |
| slv_proposicoes | Standardized propositions |
| slv_despesas_ceap | Standardized parliamentary expenses |

---

# Invalid Records Strategy

The Silver layer implements controlled segregation of invalid records.

Rejected records must not be discarded silently.

Instead, they must be redirected into controlled rejection tables.

Recommended table:

```text
silver.slv_registros_rejeitados
```

---

# Rejected Records Responsibilities

Rejected record tables must preserve:

- original source payload
- rejection reason
- validation rule
- execution identifier
- processing timestamp
- source table
- source layer

This strategy ensures:

- auditability
- traceability
- governance transparency
- replay capability
- data quality explainability

---

# Suggested Rejected Records Schema

| Column | Description |
|---|---|
| execution_id | Pipeline execution identifier |
| processing_timestamp | Processing timestamp |
| source_layer | Source Medallion layer |
| source_table | Source table |
| entity_name | Entity name |
| rejected_record_id | Original identifier |
| rejection_reason | Rejection category |
| validation_rule | Validation rule triggered |
| original_payload | Original raw payload |

---

# Data Quality Strategy

Silver is the primary quality enforcement layer.

Implemented validations may include:

- null validation
- duplicate validation
- schema conformity
- business integrity validation
- identifier validation
- type validation
- reference validation
- mandatory field validation

---

# Blocking vs Non-Blocking Validation

## Blocking Validation

Blocking validations prevent records from entering Silver datasets.

Examples:

- invalid identifiers
- malformed dates
- invalid CNPJ
- missing primary keys

These records are redirected to rejected tables.

---

## Non-Blocking Validation

Non-blocking validations generate warnings but preserve records.

Examples:

- optional field nullability
- partial enrichment failure
- missing secondary metadata

Warnings must be logged for auditability.

---

# Deduplication Strategy

Silver datasets must implement deterministic deduplication.

Possible approaches:

- business keys
- ingestion timestamp priority
- hash comparison
- latest record selection
- unique operational identifiers

Deduplication logic must be documented and reproducible.

---

# Type Standardization

Silver datasets must apply explicit type casting.

Examples:

| Data Type | Standard |
|---|---|
| Dates | DATE |
| Timestamps | TIMESTAMP |
| Monetary values | DECIMAL |
| Quantities | INTEGER |
| Flags | BOOLEAN or INTEGER |
| Identifiers | STRING |

Implicit typing must be avoided whenever possible.

---

# CNPJ Validation Strategy

The parliamentary expenses entity (`slv_despesas_ceap`) implements
a multi-stage CNPJ validation strategy.

---

## Stage 1 — Local Validation

Executed directly inside Spark transformations.

Validation rules:

- remove non-numeric characters
- validate length = 14 digits
- validate repeated invalid sequences
- validate check digits (DV)
- validate non-null identifiers

Possible outputs:

```text
fl_cnpj_formato_valido
fl_cnpj_dv_valido
```

Invalid records may be redirected to rejected tables.

---

## Stage 2 — External API Enrichment

Executed only for previously validated CNPJs.

Recommended strategy:

```text
SELECT DISTINCT cnpj
```

The project intentionally avoids API requests per transaction row.

Benefits:

- reduced API load
- improved performance
- lower timeout risk
- deterministic enrichment

---

# CNPJ Enrichment Table

Recommended enrichment table:

```text
silver.slv_cnpj_enriquecido
```

Suggested fields:

| Column | Description |
|---|---|
| cnpj | CNPJ |
| razao_social | Legal company name |
| nome_fantasia | Trade name |
| situacao_cadastral | Registration status |
| natureza_juridica | Legal nature |
| porte_empresa | Company size |
| municipio | City |
| uf | State |
| data_abertura | Opening date |
| api_consulta_timestamp | API timestamp |
| fl_api_sucesso | API success flag |
| tx_erro_api | API error message |

---

# Incremental Processing Strategy

Some Silver entities support incremental processing.

Examples:

- despesas_ceap
- votacoes
- votos
- proposicoes

Possible strategies:

- execution timestamp filtering
- ingestion date filtering
- deterministic hash comparison
- deduplication windows
- replay control

---

# Silver Lineage Strategy

The Silver layer preserves lineage from Bronze ingestion.

Mandatory lineage fields may include:

| Column | Description |
|---|---|
| execution_id | Pipeline execution identifier |
| ingestion_timestamp | Original Bronze ingestion |
| processing_timestamp | Silver processing timestamp |
| source_table | Bronze source table |
| source_file | CSV fallback source |
| source_endpoint | API endpoint |
| record_hash | Deterministic hash |

---

# Silver Grain Definition

Each Silver table must define a stable grain.

Examples:

| Table | Grain |
|---|---|
| slv_votos | One deputy vote per voting session |
| slv_votacoes | One voting session |
| slv_eventos | One legislative event |
| slv_despesas_ceap | One parliamentary expense transaction |
| slv_proposicoes | One legislative proposition |

Stable grain definition is mandatory before Gold modeling.

---

# Silver to Gold Contract

The Silver layer acts as the trusted source for Gold dimensional modeling.

Gold layer assumptions:

- Silver datasets are validated
- identifiers are standardized
- types are normalized
- duplicates are controlled
- business validation is enforced

Gold should not reimplement Silver cleansing logic.

---

# Recommended Silver Execution Order

Recommended execution sequence:

```text
01_silver_deputados
02_silver_orgaos
03_silver_eventos
04_silver_votacoes
05_silver_votos
06_silver_proposicoes
07_silver_despesas_ceap
08_silver_orgaos_membros
09_silver_registros_rejeitados
```

Smaller reference entities should be processed first.

Large-volume entities should be processed after validation rules stabilize.

---

# Silver Operational Recommendations

During development:

- execute notebooks individually
- validate outputs incrementally
- avoid full pipeline reruns unnecessarily
- validate rejected records frequently
- preserve deterministic transformations

For Databricks Free Edition:

- avoid unnecessary repartition operations
- avoid excessive cache persistence
- process large tables incrementally
- validate transformations using samples first

---

# Silver Layer Governance

Silver governance principles include:

- traceability
- explainability
- auditability
- deterministic processing
- controlled rejection handling
- analytical consistency
- operational reproducibility

---

# Future Enhancements

Planned future enhancements may include:

- automated enrichment pipelines
- CDC support
- SCD Type 2 implementation
- advanced quality metrics
- automated anomaly detection
- enrichment cache optimization
- governance dashboards

---

# References

- `/docs/governance/data_quality.md`
- `/docs/governance/traceability.md`
- `/docs/operations/execution_guide.md`
- `/docs/operations/pipeline_orchestration.md`
- `/docs/decisions/api_limitations.md`
- `/docs/architecture/README.md`