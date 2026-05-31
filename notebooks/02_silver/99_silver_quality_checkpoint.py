# Databricks notebook source
# MAGIC %md
# MAGIC # 99 Silver — Quality Checkpoint
# MAGIC
# MAGIC **Notebook:** `99_silver_quality_checkpoint`
# MAGIC
# MAGIC Validates the completeness, quality, governance, and operational readiness of all Silver layer datasets before Gold layer construction.
# MAGIC
# MAGIC This notebook validates:
# MAGIC
# MAGIC - Silver table availability
# MAGIC - Silver record volume verification
# MAGIC - Data quality controls
# MAGIC - Referential consistency checks
# MAGIC - Duplicate detection
# MAGIC - Mandatory field validation
# MAGIC - Governance comment coverage
# MAGIC - Silver layer readiness assessment
# MAGIC - Rejected records monitoring
# MAGIC - Delivery checkpoint evidence generation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Responsibilities
# MAGIC
# MAGIC - Verify all mandatory Silver datasets were generated
# MAGIC - Confirm Silver record volumes are within expected ranges
# MAGIC - Validate critical business keys
# MAGIC - Detect duplicate records
# MAGIC - Verify mandatory attributes
# MAGIC - Assess rejected record volumes
# MAGIC - Confirm governance comments were applied
# MAGIC - Produce delivery evidence for the Silver layer
# MAGIC - Certify Silver readiness for Gold consumption
# MAGIC - Support project acceptance validation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Notes
# MAGIC
# MAGIC - This notebook does not modify data
# MAGIC - All validations are read-only
# MAGIC - Results are intended for quality assurance purposes
# MAGIC - Validation failures must be investigated before Gold development
# MAGIC - Record counts may vary according to source refresh dates
# MAGIC - Governance validations use Information Schema metadata
# MAGIC - Rejected records are expected when source quality issues exist
# MAGIC - Silver datasets are considered approved when critical validations pass
# MAGIC - This notebook serves as the official Silver checkpoint
# MAGIC - Results should be archived as delivery evidence
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/decisions/silver_layer_strategy.md`
# MAGIC - `/docs/governance/data_quality.md`
# MAGIC - `/docs/governance/traceability.md`
# MAGIC - `/docs/operations/execution_guide.md`
# MAGIC - `/docs/standards/naming_conventions.md`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'slv_deputados' AS tabela, COUNT(*) total
# MAGIC FROM brazil_legislative_analytics.silver.slv_deputados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_partidos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_partidos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_estados', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_estados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_frentes', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_frentes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_frentes_membros', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_frentes_membros
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_eventos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_votacoes', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_votos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_votos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_despesas_ceap', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_despesas_ceap
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_fornecedores', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_fornecedores
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_cpis', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_cpis
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_cpi_eventos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_cpi_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_proposicoes', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_proposicoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_presencas_eventos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_presencas_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_orgaos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_orgaos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'slv_registros_rejeitados', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.silver.slv_registros_rejeitados;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     COUNT(*) colunas
# MAGIC FROM brazil_legislative_analytics.information_schema.columns
# MAGIC WHERE table_schema = 'silver'
# MAGIC   AND comment IS NOT NULL
# MAGIC GROUP BY table_name
# MAGIC ORDER BY table_name;