# Changelog


## v0.2.0 - Foundation Stabilization and Quality Framework

### Added
- Pipeline utilities framework.
- Centralized API client utilities.
- Pagination utility functions.
- Record hash utility functions.
- Legislature domain utilities.
- Console logger utilities.
- Structured audit table logger.
- Quality validation framework.
- Traceability validation framework.
- Incremental pipeline job structure.
- First Bronze ingestion notebook for deputados.
- API resilience and retry strategy.
- Incremental orchestration foundations.

### Improved
- Standardized naming conventions across all notebooks.
- Unified Medallion architecture structure.
- Improved Databricks execution performance.
- Reduced unnecessary catalog switching.
- Optimized API validation workflow.
- Improved notebook modularization.
- Improved governance and auditability standards.
- Standardized logging structure.
- Standardized pipeline execution status handling.

### Changed
- Silver layer architecture simplified into unified Silver layer.
- Quality framework integrated into orchestration layer.
- Hash generation standardized through utility layer.
- Legislature filtering centralized into domain utility.

### Governance
- Expanded audit logging framework.
- Expanded traceability validation standards.
- Added quality validation execution controls.
- Added pipeline execution metadata standards.
- Added ingestion traceability metadata.

### Technical Notes
- Utilities standardized using reusable modular approach.
- Pipeline foundation prepared for scalable Bronze ingestion.
- Project structure aligned with enterprise Medallion architecture standards.


## v0.1.0 - Setup and Governance Foundation

### Added
- Initial Databricks project structure.
- Medallion architecture folder organization.
- Catalog creation notebook.
- Schema creation notebook.
- Project configuration notebook.
- Audit table creation notebook.
- Naming conventions documentation.
- Governance documentation structure.
- Architecture and dimensional model diagrams.

### Created Schemas
- audit
- bronze
- silver_base
- silver_curated
- gold
- marts

### Created Audit Tables
- audit_pipeline_logs
- audit_pipeline_errors
- audit_data_quality_logs

### Governance Standards
- Mnemonic naming conventions
- Table comments
- Column comments
- Traceability standards
- Logging standards
- Data quality standards


