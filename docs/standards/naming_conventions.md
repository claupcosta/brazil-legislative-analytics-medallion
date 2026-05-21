# Naming Conventions

## Purpose

This document defines the naming standards adopted in the
Brazil Legislative Analytics Medallion project.

The objective is to ensure:

- Consistency
- Readability
- Governance
- Traceability
- Scalability
- Standardized analytical modeling

---

# General Standards

## Language

- Technical naming uses English
- Business terminology may preserve official Portuguese legislative terms when required

Examples:

| Type | Example |
|---|---|
| Technical | `dm_deputy` |
| Official legislative term | `cpi` |

---

# Medallion Layer Prefixes

| Prefix | Layer | Description |
|---|---|---|
| `br_` | Bronze | Raw ingestion tables |
| `sb_` | Silver Base | Standardized technical layer |
| `sc_` | Silver Curated | Business-curated reusable entities |
| `dm_` | Gold Dimension | Dimension tables |
| `ft_` | Gold Fact | Fact tables |
| `am_` | Analytical Mart | Analytical marts |
| `ref_` | Reference | Reference/domain tables |
| `audit_` | Audit | Audit physical tables |
| `vw_` | View | SQL views |

---

# Column Prefix Standards

| Prefix | Meaning |
|---|---|
| `id` | Identifier |
| `cd` | Code |
| `tx` | Text |
| `dt` | Date |
| `ts` | Timestamp |
| `vl` | Numeric value |
| `qt` | Quantity |
| `pc` | Percentage |
| `fl` | Boolean flag |
| `nr` | Numeric sequence or number |

---

# Mnemonic Standards

| Mnemonic | Meaning |
|---|---|
| `dept` | Deputy |
| `prt` | Political Party |
| `uf` | State |
| `leg` | Legislature |
| `frnt` | Parliamentary Front |
| `evt` | Event |
| `vot` | Voting |
| `vote` | Voting Result |
| `dsp` | Expense |
| `forn` | Supplier |
| `cpi` | Parliamentary Inquiry Commission |
| `prop` | Proposition |
| `org` | Organization |
| `aud` | Audit |
| `qlt` | Quality |
| `err` | Error |

---

# Audit Naming Rules

## Audit Columns

The prefix:

```text
aud_
```

is reserved for:

- audit columns
- traceability fields
- ingestion metadata
- execution metadata

Examples:

```text
aud_id_execution
aud_ts_ingestion
aud_ts_processing
aud_tx_source_endpoint
aud_hash_record
```

---

## Audit Tables

The prefix:

```text
audit_
```

is reserved for physical audit tables.

Examples:

```text
audit_pipeline_logs
audit_pipeline_errors
audit_data_quality_logs
```

---

# Table Naming Pattern

## Standard

```text
<layer_prefix>_<mnemonic>_<entity>
```

Examples:

```text
br_dept_deputies
sb_frnt_fronts
sc_prt_parties
```

---

# Gold Layer Standards

## Dimension Tables

Pattern:

```text
dm_<entity>
```

Examples:

```text
dm_deputy
dm_party
dm_state
dm_date
```

---

## Fact Tables

Pattern:

```text
ft_<business_process>
```

Examples:

```text
ft_voting_results
ft_expenses_ceap
ft_event_attendance
```

---

# Analytical Mart Standards

Pattern:

```text
am_<business_subject>
```

Examples:

```text
am_fronts_atlas
am_ceap_expenses_overview
am_cpi_audit
```

---

# Reference Tables

Pattern:

```text
ref_<domain_subject>
```

Examples:

```text
ref_vote_choice
ref_event_type
ref_expense_type
```

---

# Traceability Standards

All ingestion layers must preserve:

| Column |
|---|
| `aud_id_execution` |
| `aud_ts_ingestion` |
| `aud_ts_processing` |
| `aud_tx_source_endpoint` |
| `aud_tx_source_system` |
| `aud_tx_pipeline_version` |
| `aud_hash_record` |

---

# Date Standards

## Date fields

Use:

```text
dt_
```

Example:

```text
dept_dt_start_term
```

---

## Timestamp fields

Use:

```text
ts_
```

Example:

```text
aud_ts_ingestion
```

---

# Boolean Standards

Boolean fields must use:

```text
fl_
```

Examples:

```text
dept_fl_active
vote_fl_party_alignment
```

---

# Naming Restrictions

Avoid:

- spaces
- accents
- special characters
- mixed languages in technical naming
- generic names
- abbreviations without mnemonic documentation

---

# Documentation Standards

All tables and columns must contain:

- business description
- technical description
- comments in Unity Catalog
- traceability metadata

---

# Modeling Standards

The project follows a dimensional modeling approach:

- Star Schema
- Shared dimensions
- Facts connected only through dimensions
- Analytical correlations performed in marts

Direct physical relationships between fact tables are not allowed.

---

# Governance Standards

The project enforces:

- Naming standardization
- Auditability
- Data lineage
- Traceability
- Reusable business entities
- Metadata documentation
- Layer segregation

---

# Final Recommendation

All new tables, columns, marts, dimensions and facts created during project evolution must follow this standard.