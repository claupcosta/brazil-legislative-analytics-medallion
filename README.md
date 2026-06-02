# Brazil Legislative Analytics Medallion

## Legislative Analytics Platform Built on Databricks Medallion Architecture

[![GitHub Repository](https://img.shields.io/badge/GitHub-Repository-blue?logo=github)](https://github.com/claupcosta/brazil-legislative-analytics-medallion)

**Repository:**  
https://github.com/claupcosta/brazil-legislative-analytics-medallion

Project developed for ingestion, curation, dimensional modeling, and analytical consumption of public data from the Brazilian Chamber of Deputies using Databricks, Apache Spark, Delta Lake, and Medallion Architecture.


The solution was designed following modern Data Engineering, Analytics Engineering, Data Governance, Data Quality, and Observability practices, simulating patterns commonly found in enterprise-grade Data Platform environments.

---

# How to Evaluate This Project

## Repository

GitHub Repository:

https://github.com/claupcosta/brazil-legislative-analytics-medallion

The complete source code, documentation, notebooks, dimensional models, governance artifacts, and analytical products are available in this repository.

The recommended evaluation order is:

## 1. Solution Overview

* `docs/challenge/08_solution_adherence_matrix.md`

Challenge adherence matrix containing the complete mapping between challenge requirements, implementation details, and technical evidence.

---

## 2. Architecture

* `docs/architecture/01_solution_architecture.md`
* `docs/architecture/01_medallion_architecture_overview.png`
* `docs/architecture/02_end_to_end_data_flow.png`
* `docs/architecture/03_star_schema_model.png`

Architecture documentation, end-to-end data flow, and dimensional model.

---

## 3. Data

* `docs/data_dictionary/02_data_dictionary.md`
* `docs/data_dictionary/legislative_data_dictionary.xlsx`

Technical data dictionary containing tables, columns, business rules, and metadata.

---

## 4. Operations

* `docs/operations/03_pipeline_orchestration.md`
* `docs/operations/06_runbook.md`

Operational workflow, orchestration strategy, and execution procedures.

---

## 5. Governance

* `docs/governance/04_data_quality_strategy.md`
* `docs/governance/05_traceability.md`

Data quality, traceability, and governance strategies implemented throughout the platform.

---

# Executive Summary

The platform implements a complete Medallion Architecture using Databricks for processing legislative data from the Brazilian Chamber of Deputies.

## Key Deliverables

✅ Complete Medallion Architecture (Bronze, Silver, Gold, and Marts)

✅ Dimensional Model (Star Schema)

✅ 6 Analytical Data Marts

✅ Data Quality Framework

✅ Traceability Framework

✅ Metadata Governance

✅ Incremental Processing

✅ CSV Fallback Strategy

✅ Operational Auditing

✅ Replay and Recovery Mechanisms

✅ Enterprise-Level Documentation

---

# Project Objective

This project was developed for educational purposes, Data Engineering studies, portfolio development, and demonstration of modern data architecture best practices.

The solution simulates patterns commonly found in enterprise Data Engineering environments, including:

* Medallion Architecture
* Data Governance
* Operational Auditing
* Data Quality
* Traceability
* Incremental Processing
* Replay and Recovery
* Dimensional Modeling
* Analytical Data Marts

---

# Solution Architecture

```text
Brazilian Chamber of Deputies API
               │
               ▼
          01_Bronze
               │
               ▼
          02_Silver
               │
               ▼
           03_Gold
               │
               ▼
           04_Marts
               │
               ▼
     05_Quality & Governance
```

## Layers

| Layer   | Purpose                                        |
| ------- | ---------------------------------------------- |
| Bronze  | Raw data ingestion and preservation            |
| Silver  | Data curation, standardization, and enrichment |
| Gold    | Enterprise dimensional model                   |
| Marts   | Specialized analytical products                |
| Quality | Governance, quality, and traceability          |

---

# Technologies Used

| Category        | Technology             |
| --------------- | ---------------------- |
| Platform        | Databricks             |
| Language        | Python                 |
| Processing      | Apache Spark / PySpark |
| Storage         | Delta Lake             |
| Governance      | Unity Catalog          |
| Orchestration   | Databricks Workflows   |
| Version Control | GitHub                 |
| Data Quality    | Custom Framework       |
| Traceability    | Custom Framework       |

---

# Analytical Products

The Marts layer provides the following analytical products:

| Data Mart                          | Purpose                                                 |
| ---------------------------------- | ------------------------------------------------------- |
| am_atlas_frentes                   | Parliamentary caucus composition and diversity analysis |
| am_calendario_eventos_legislativos | Legislative calendar and parliamentary participation    |
| am_correlacao_frentes_votacoes     | Correlation between caucuses and voting behavior        |
| am_visao_geral_despesas_ceap       | Parliamentary expense monitoring                        |
| am_auditoria_cpis                  | CPI auditing and monitoring                             |
| am_monitor_presenca_absenteismo    | Attendance and engagement indicators                    |

---

# Repository Structure

```text
BRAZIL-LEGISLATIVE-ANALYTICS-MEDALLION/
│
├── docs/
├── notebooks/
├── README.md
├── README.pt-BR.md
├── requirements.txt
└── .gitignore
```

## Pipeline Structure

```text
notebooks/
├── 00_setup/
├── 01_bronze/
├── 02_silver/
├── 03_gold/
├── 04_marts/
├── 05_quality/
├── 06_jobs/
└── 99_utils/
```

---

# Documentation Structure

```text
docs/
├── architecture/
├── challenge/
├── data_dictionary/
├── governance/
├── marts/
├── operations/
└── changelog.md
```

---

# Governance and Observability

The platform implements end-to-end governance mechanisms.

## Implemented Features

* Operational Auditing
* Data Quality Framework
* Traceability Framework
* Metadata Governance
* Technical Logging
* Layer-Based Replay
* Reprocessing Controls
* Rejected Records Management
* Data Lineage
* Delta Lake Versioning

---

# Project Links

| Resource                  | Path                                                                 |
| ------------------------- | -------------------------------------------------------------------- |
| GitHub Repository         | https://github.com/claupcosta/brazil-legislative-analytics-medallion |
| Architecture              | docs/architecture                                                    |
| Data Dictionary           | docs/data_dictionary                                                 |
| Governance                | docs/governance                                                      |
| Operations                | docs/operations                                                      |
| Data Marts                | docs/marts                                                           |
| Solution Adherence Matrix | docs/challenge/08_solution_adherence_matrix.md                       |
| Version History           | docs/changelog.md                                                    |

---

## Recommended Documentation Reading Order

For a complete evaluation of the solution, the following reading sequence is recommended:

1. Solution Adherence Matrix
2. Solution Architecture
3. Dimensional Model
4. Data Dictionary
5. Pipeline Orchestration
6. Data Quality Strategy
7. Traceability Strategy
8. Operational Runbook
9. Architectural Decisions
10. Changelog

---
# Author

## Claudia Costa

Data Engineer focused on analytics platforms, Lakehouse architecture, Databricks, data governance, data quality, traceability, and scalable analytical solutions.

Project developed for educational purposes, technical portfolio development, and demonstration of Data Engineering capabilities.

---

# License

This project uses exclusively public data made available by the Brazilian Chamber of Deputies and complementary public data sources.

All analytical artifacts were developed for educational purposes, technical studies, and demonstration of modern data architecture practices.
