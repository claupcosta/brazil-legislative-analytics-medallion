# Naming Conventions — Brazil Legislative Analytics Medallion

## 1. Purpose

This document defines the official naming conventions adopted in the project:

**Brazil Legislative Analytics Medallion**

The purpose of this standard is to ensure:

- Consistency across all layers
- Readability and maintainability
- Traceability between Medallion layers
- Standardized dimensional modeling
- Better governance and scalability
- Easier analytical consumption

---

# 2. General Standards

## Language

All technical objects must be created in:

- English

Including:

- Tables
- Columns
- Views
- Notebooks
- Variables
- Functions
- Comments
- Documentation

---

## Writing Pattern

The project adopts:

```text
snake_case
```

### Examples

```text
dm_deputy
ft_expenses_ceap
am_fronts_atlas
evt_dt_start
dsp_vl_net_amount
```

---

# 3. Table Naming Standards

## 3.1 Bronze Layer

Raw ingestion tables.

### Pattern

```text
bronze_<domain>_raw
```

### Examples

```text
bronze_deputies_raw
bronze_fronts_raw
bronze_front_members_raw
bronze_events_raw
bronze_votings_raw
bronze_voting_results_raw
bronze_expenses_ceap_raw
bronze_cpis_raw
bronze_cpi_events_raw
bronze_propositions_raw
```

---

## 3.2 Silver Base Layer

Technical cleansing and standardization layer.

### Pattern

```text
silver_base_<domain>
```

### Examples

```text
silver_base_deputies
silver_base_fronts
silver_base_events
silver_base_votings
silver_base_voting_results
silver_base_expenses_ceap
```

---

## 3.3 Silver Curated Layer

Reusable and business-oriented entities.

### Pattern

```text
silver_curated_<domain>
```

### Examples

```text
silver_curated_deputies
silver_curated_parties
silver_curated_states
silver_curated_events
silver_curated_votings
silver_curated_voting_results
```

---

## 3.4 Gold Layer — Dimensions

Dimensional entities.

### Pattern

```text
dm_<entity>
```

### Examples

```text
dm_deputy
dm_party
dm_state
dm_date
dm_front
dm_event
dm_voting
dm_supplier
dm_cpi
```

---

## 3.5 Gold Layer — Facts

Transactional analytical facts.

### Pattern

```text
ft_<business_process>
```

### Examples

```text
ft_front_members
ft_event_attendance
ft_voting_results
ft_expenses_ceap
ft_cpi_events
```

---

## 3.6 Analytical Marts

Business-oriented analytical outputs.

### Pattern

```text
am_<business_context>
```

### Examples

```text
am_fronts_atlas
am_legislative_events_calendar
am_fronts_votings_correlation
am_ceap_expenses_overview
am_cpi_audit
am_attendance_absenteeism_monitor
```

---

# 4. Column Naming Standards

## Official Pattern

```text
<mnemonic>_<type>_<description>
```

### Example

```text
dept_tx_name
evt_dt_start
dsp_vl_net_amount
```

---

# 5. Official Data Type Prefixes

| Prefix | Meaning | Example |
|---|---|---|
| id | Identifier | dept_id_deputy |
| cd | Code | prt_cd_party |
| tx | Text | dept_tx_name |
| dt | Date | evt_dt_start |
| ts | Timestamp | aud_ts_ingestion |
| vl | Monetary Value | dsp_vl_net_amount |
| qt | Quantity | evt_qt_attendees |
| fl | Boolean / Flag | vote_fl_party_alignment |
| pc | Percentage | abs_pc_attendance |
| nr | Generic Number | evt_nr_session |

---

# 6. Official Domain Mnemonics

| Domain | Mnemonic |
|---|---|
| Deputy | dept |
| Party | prt |
| State / UF | uf |
| Legislature | leg |
| Front | frnt |
| Event | evt |
| Voting | vot |
| Voting Result | vote |
| Expense / CEAP | dsp |
| Supplier | forn |
| CPI | cpi |
| Proposition | prop |
| Organization | org |
| Date | dt |
| Audit | aud |
| Quality | qlt |

---

# 7. Primary Key Standards

## Pattern

```text
<mnemonic>_id_<entity>
```

### Examples

```text
dept_id_deputy
prt_id_party
evt_id_event
vot_id_voting
vote_id_voting_result
dsp_id_expense
frnt_id_front
cpi_id_cpi
prop_id_proposition
```

---

# 8. Foreign Key Standards

Foreign keys must preserve the exact same name as the referenced primary key.

## Example

### Primary Key

```text
dept_id_deputy
```

### Foreign Key

```text
dept_id_deputy
```

This standard improves:

- Traceability
- Data lineage
- Join readability
- Dimensional consistency

---

# 9. Date and Timestamp Standards

## Business Dates

### Examples

```text
evt_dt_start
evt_dt_end
dsp_dt_document
vote_dt_session
```

---

## Technical Timestamps

### Examples

```text
aud_ts_ingestion
aud_ts_processing
aud_ts_last_update
```

---

# 10. Boolean / Flag Standards

## Pattern

```text
<mnemonic>_fl_<description>
```

### Examples

```text
vote_fl_party_alignment
evt_fl_virtual
dsp_fl_suspect_supplier
qlt_fl_valid_record
```

---

# 11. Quantity Standards

## Pattern

```text
<mnemonic>_qt_<description>
```

### Examples

```text
evt_qt_attendees
frnt_qt_members
```

---

# 12. Percentage Standards

## Pattern

```text
<mnemonic>_pc_<description>
```

### Examples

```text
abs_pc_attendance
vote_pc_alignment
```

---

# 13. Monetary Value Standards

## Pattern

```text
<mnemonic>_vl_<description>
```

### Examples

```text
dsp_vl_document_amount
dsp_vl_net_amount
dsp_vl_reimbursed_amount
```

---

# 14. Audit Table Standards

## Audit Logs

### Examples

```text
aud_id_execution
aud_ts_start
aud_ts_end
aud_tx_status
aud_tx_message
```

---

## Error Logs

### Examples

```text
err_id_record
err_tx_message
err_tx_stacktrace
err_ts_occurrence
```

---

# 15. Quality Table Standards

### Examples

```text
qlt_fl_valid_record
qlt_tx_validation_rule
qlt_tx_quality_status
```

---

# 16. Notebook Naming Standards

## Pattern

```text
<execution_order>_<layer>_<process>
```

### Examples

```text
01_bronze_deputies
02_silver_base_fronts
03_silver_curated_events
04_dm_deputy
05_am_fronts_atlas
```

---

# 17. Engineering Best Practices

The project follows the following engineering principles:

- Medallion Architecture
- Separation of concerns
- Reusable utility modules
- Layer traceability
- Incremental ingestion support
- Data quality validation
- Dimensional modeling
- Governance and auditability
- Standardized naming conventions
- Analytical scalability

---

# 18. Traceability Standards

All layers must preserve traceability metadata.

## Recommended Technical Columns

```text
aud_ts_ingestion
aud_ts_processing
aud_tx_source_endpoint
aud_tx_source_system
aud_id_execution
aud_fl_valid_record
```

---

# 19. Data Quality Standards

Quality validations must be implemented across all layers.

## Examples

- Null validations
- Duplicate validations
- Referential integrity
- Mandatory fields
- Schema validation
- Business rule validation

---

# 20. Final Recommendations

Before starting large-scale development:

- Consolidate all naming conventions
- Validate all PK/FK definitions
- Validate dimensional relationships
- Validate analytical marts
- Validate business traceability
- Ensure naming consistency across all layers

This standard must be followed by all notebooks, tables, views, marts, jobs, and utility modules in the project.

---