# Analytical Data Marts

## Overview

The MAT (Analytical Marts) layer represents the final analytical layer of the Brazil Legislative Analytics Medallion platform.

Its purpose is to provide business-oriented analytical datasets built on top of the Gold dimensional model, enabling reporting, governance, auditing and advanced legislative analytics.

All Data Marts follow common standards for:

* Dimensional modeling
* Data quality validation
* Auditability
* Traceability
* Delta Lake persistence
* CSV export
* Reproducibility

---

## Architecture Position

```text
Brazilian Chamber API
          │
          ▼
       Bronze
          ▼
       Silver
          ▼
        Gold
          ▼
         MAT
          ▼
 Business Analytics
```

The MAT layer consumes dimensions and facts from the Gold layer and delivers business-ready analytical products.

---

## Available Data Marts

| Data Mart                                                                       | Description                                                                    |
| ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| [Atlas das Frentes Parlamentares](README_AM_ATLAS_FRENTES.md)                   | Analysis of parliamentary fronts composition, diversity and representativeness |
| [Calendário de Eventos Legislativos](README_AM_CALENDARIO_EVENTOS.md)           | Legislative events monitoring and participation analysis                       |
| [Correlação entre Frentes e Votações](README_AM_CORRELACAO_FRENTES_VOTACOES.md) | Voting behavior and political alignment analysis                               |
| [Panorama de Despesas CEAP](README_AM_DESPESAS_CEAP.md)                         | Parliamentary expense analytics and anomaly detection                          |
| [Auditoria de CPIs](README_AM_AUDITORIA_CPIS.md)                                | Parliamentary Inquiry Committee monitoring and governance                      |
| [Presença e Absenteísmo Parlamentar](README_AM_PRESENCA_ABSENTEISMO.md)         | Attendance, participation and engagement analytics                             |

---

## Business Glossary

The platform uses several legislative and parliamentary concepts.

For terminology reference, consult:

[Business Glossary](01_business_glossary.md)

---

## Analytical Domains

### Parliamentary Fronts

Provides visibility into:

* Front composition
* Political diversity
* Regional representation
* Leadership structure
* Member participation

Reference:

[README_AM_ATLAS_FRENTES.md](README_AM_ATLAS_FRENTES.md)

---

### Legislative Events

Provides visibility into:

* Legislative calendar
* Event participation
* Parliamentary engagement
* Attendance indicators
* Temporal analysis

Reference:

 [README_AM_CALENDARIO_EVENTOS.md](README_AM_CALENDARIO_EVENTOS.md)

---

### Voting Analytics

Provides visibility into:

* Voting behavior
* Political alignment
* Front cohesion
* Legislative positioning
* Article 17 vote analysis

Reference:

 [README_AM_CORRELACAO_FRENTES_VOTACOES.md](README_AM_CORRELACAO_FRENTES_VOTACOES.md)

---

### Parliamentary Expenses

Provides visibility into:

* CEAP expenditures
* Supplier analysis
* Financial governance
* Expense monitoring
* Anomaly detection

Reference:

 [README_AM_DESPESAS_CEAP.md](README_AM_DESPESAS_CEAP.md)

---

### CPI Monitoring

Provides visibility into:

* Parliamentary Inquiry Committees
* CPI-event relationships
* Activity monitoring
* Audit indicators
* Historical analysis

Reference:

 [README_AM_AUDITORIA_CPIS.md](README_AM_AUDITORIA_CPIS.md)

---

### Attendance Monitoring

Provides visibility into:

* Parliamentary participation
* Attendance indicators
* Absenteeism analysis
* Engagement monitoring
* Temporal participation trends

Reference:

 [README_AM_PRESENCA_ABSENTEISMO.md](README_AM_PRESENCA_ABSENTEISMO.md)

---

## Design Principles

All Data Marts follow the same engineering standards:

### Data Quality

* Completeness validation
* Consistency validation
* Referential integrity checks
* Business rule validation

### Governance

* Auditability
* Lineage
* Traceability
* Metadata standardization

### Reliability

* Delta Lake persistence
* Incremental processing
* Replay support
* Controlled publication

---

## Related Documentation

### Architecture

* [Solution Architecture](../architecture/01_solution_architecture.md)
* [Architectural Decisions](../architecture/07_architectural_decisions.md)

### Data Modeling

* [Data Dictionary](../data_dictionary/02_data_dictionary.md)

### Governance

* [Data Quality Strategy](../governance/04_data_quality_strategy.md)
* [Traceability Strategy](../governance/05_traceability.md)

### Operations

* [Pipeline Orchestration](../operations/03_pipeline_orchestration.md)
* [Operational Runbook](../operations/06_runbook.md)

### Challenge Documentation

* [Challenge Adherence Matrix](../challenge/08_challenge_adherence_matrix.md)

---

## Author

**Claudia Costa**

Brazil Legislative Analytics Medallion

Analytical platform developed using Databricks Lakehouse, Delta Lake and Medallion Architecture principles.
