# Changelog


## v1.3.0 — Bronze Layer Stabilization and Operational Resilience

### Added

- Added complete Bronze ingestion notebooks for:
  - deputados
  - frentes
  - eventos
  - votacoes
  - votos
  - despesas CEAP
  - orgaos
  - orgaos membros
  - proposicoes

- Added CSV fallback ingestion notebooks for high-volume or unstable sources:
  - 04a_bronze_votacoes_csv_fallback
  - 05a_bronze_votos_csv_fallback
  - 06a_bronze_despesas_ceap_csv_fallback
  - 07a_bronze_orgaos_csv_fallback
  - 08a_bronze_orgaos_membros_csv_fallback
  - 09a_bronze_proposicoes_csv_fallback

- Added operational documentation:
  - docs/operations/pipeline_orchestration.md
  - docs/architecture/README.md updates

### Changed

- Updated project configuration with centralized Volume paths for CSV fallback ingestion.
- Updated API client utility to preserve backward compatibility with pagination utilities.
- Updated full pipeline job to prioritize CSV fallback ingestion for unstable API endpoints.
- Updated Bronze notebooks with standardized headers, logging and governance comments.
- Updated API limitation documentation to reflect endpoint instability and timeout behavior.

### Improved

- Improved Bronze execution stability in Databricks Free Edition.
- Improved fallback strategy for unstable Câmara API endpoints.
- Improved traceability through source file lineage and ingestion metadata.
- Improved pipeline resilience by separating API validation from operational fallback ingestion.
- Improved consistency across Bronze notebooks and utility functions.

### Fixed

- Fixed missing compatibility functions in `utils_api_client`.
- Fixed `fetch_camara_api_data` compatibility for `utils_pagination`.
- Fixed missing `extract_response_records` function required by paginated API ingestion.
- Fixed setup validation issue related to audit error table creation.
- Fixed orchestration path for `br_orgaos` by using CSV fallback as the operational source.

### Notes

This release consolidates the Bronze layer and operational foundation of the
Brazil Legislative Analytics Medallion project.

The project is now ready to move forward to the Silver layer implementation,
where normalized, deduplicated and analytically prepared datasets will be created.

Future release:

- v2.0.0 — Silver Layer Implementation and Analytical Standardization

## v1.2.0 - Foundation Stabilization and Quality Framework

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


## v1.1.0 - Setup and Governance Foundation

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


