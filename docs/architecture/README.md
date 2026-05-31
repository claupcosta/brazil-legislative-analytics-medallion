# Brazil Legislative Analytics Medallion

## Overview

Brazil Legislative Analytics Medallion is a Databricks-based analytical engineering project designed to ingest, process, govern and analyze legislative open data from the Câmara dos Deputados Open Data API.

The project applies Medallion Architecture principles using Bronze, Silver and Gold layers to provide scalable, traceable and analytics-ready datasets for legislative analysis scenarios.

The solution combines:

- API ingestion
- CSV fallback ingestion strategies
- dimensional modeling
- governance implementation
- auditability
- data quality validation
- operational lineage
- analytical marts

---

# Project Objectives

The project aims to:

- centralize legislative public datasets
- provide analytics-ready curated datasets
- implement governance and traceability standards
- support parliamentary analytical scenarios
- demonstrate Medallion Architecture implementation
- support incremental and historical ingestion strategies
- enable reproducible analytical workflows

---

# Main Technologies

## Platform

- Databricks Free Edition
- Unity Catalog
- Delta Lake
- Apache Spark

## Languages

- Python
- SQL

## Storage and Processing

- Delta Tables
- Unity Catalog Volumes
- CSV fallback ingestion
- API ingestion

---

# Medallion Architecture

The project follows the Medallion Architecture pattern.

## Bronze Layer

Responsibilities:

- raw ingestion
- source fidelity preservation
- ingestion metadata generation
- operational lineage
- raw payload persistence

Characteristics:

- immutable ingestion strategy
- API and CSV fallback ingestion
- deterministic ingestion hashes
- operational logging

Main tables:

```text
br_deputados
br_despesas_ceap
br_eventos
br_frentes
br_orgaos
br_orgaos_membros
br_proposicoes
br_votacoes
br_votos
```

---

## Silver Layer

Responsibilities:

- data normalization
- quality validation
- schema standardization
- deduplication
- analytical enrichment
- business rule application

Characteristics:

- typed analytical datasets
- governance-ready structures
- conformed entities
- reusable curated datasets

---

## Gold Layer

Responsibilities:

- dimensional modeling
- analytical marts
- KPIs
- curated business outputs
- analytical consumption

Characteristics:

- star schema modeling
- fact and dimension tables
- business-oriented datasets
- optimized analytical structures

---

# Data Sources

## Câmara dos Deputados Open Data API

Official source:

```text
https://dadosabertos.camara.leg.br/swagger/api.html
```

Main entities:

- deputados
- orgaos
- eventos
- votacoes
- votos
- despesas CEAP
- proposicoes
- frentes parlamentares

---

# CSV Fallback Strategy

Some Câmara API endpoints presented instability, timeout behavior or pagination inconsistencies during development.

To ensure operational continuity and deterministic ingestion behavior, the project implemented CSV fallback ingestion strategies.

Fallback notebooks:

```text
04a_bronze_votacoes_csv_fallback
05a_bronze_votos_csv_fallback
06a_bronze_despesas_ceap_csv_fallback
07a_bronze_orgaos_csv_fallback
08a_bronze_orgaos_membros_csv_fallback
09a_bronze_proposicoes_csv_fallback
```

Benefits:

- operational stability
- replay support
- historical consistency
- deterministic ingestion
- reduced API dependency

---

# Governance Strategy

The project applies governance principles across all Medallion layers.

Implemented capabilities:

- audit logging
- operational lineage
- deterministic record hashes
- quality validation
- execution monitoring
- data traceability
- lineage preservation
- schema standardization 

Audit tables:

```text
audit.aud_log_execucao_pipeline
audit.aud_log_erros_pipeline
audit.aud_log_qualidade_dados
```

---

# Dimensional Modeling

The Gold layer applies dimensional modeling principles using star schema structures.

Implemented concepts:

- fact tables
- dimension tables
- analytical marts
- shared dimensions
- analytical KPIs
- grain definition
- surrogate analytical structures

Main dimensions:

```text
dim_deputado
dim_orgao
dim_partido
dim_tempo
dim_uf
```

Main facts:

```text
fato_votacoes
fato_votos
fato_eventos
fato_despesas_ceap
fato_presenca_eventos
```

---

# Project Structure

```text
BRAZIL-LEGISLATIVE-ANALYTICS-MEDALLION
│
├── assets
├── config
├── docs
│   ├── architecture
│   ├── business_rules
│   ├── data_dictionary
│   ├── decisions
│   ├── diagrams
│   ├── governance
│   ├── operations
│   └── standards
│
├── notebooks
│   ├── 00_setup
│   ├── 01_bronze
│   ├── 02_silver
│   ├── 03_gold
│   ├── 04_quality
│   ├── 07_jobs
│   └── 99_utils
│
└── README.md
```

---

# Operational Execution Strategy

The project supports two execution modes.

## Full Pipeline Execution

Main orchestration notebook:

```text
07_jobs/01_run_full_pipeline
```

Recommended for:

- end-to-end execution
- validation
- demonstrations
- final delivery

---

## Development Execution

Recommended for:

- notebook development
- iterative testing
- Databricks Free Edition execution

Characteristics:

- notebook-by-notebook execution
- reduced orchestration overhead
- isolated debugging
- faster validation cycles

Additional details:

```text
/docs/operations/pipeline_orchestration.md
```

---

# Data Quality Strategy

Quality validations are implemented across all Medallion layers.

Validation examples:

- null validation
- duplicate validation
- schema conformity
- referential validation
- business rule validation
- operational completeness validation

Quality outputs are persisted into audit tables.

---

# Lineage and Traceability

The project preserves lineage and operational traceability across ingestion and transformation flows.

Implemented capabilities:

- ingestion timestamps
- source lineage
- deterministic hashes
- execution identifiers
- notebook traceability
- pipeline execution logging

---

# Databricks Free Edition Considerations

The project was developed using Databricks Free Edition.

During development, some operational limitations were identified:

- Serverless startup latency
- orchestration overhead
- API timeout instability
- Unity Catalog synchronization latency
- Delta commit overhead

These limitations do not impact:

- architecture quality
- governance implementation
- analytical outputs
- dimensional modeling consistency

---

# Documentation Structure

Additional documentation is available under:

```text
/docs/business_rules
/docs/data_dictionary
/docs/decisions
/docs/governance
/docs/operations
/docs/standards
```

---

# References

## Câmara dos Deputados Open Data

```text
https://dadosabertos.camara.leg.br/swagger/api.html
```

## Databricks Documentation

```text
https://docs.databricks.com/
```

## Delta Lake Documentation

```text
https://docs.delta.io/
```