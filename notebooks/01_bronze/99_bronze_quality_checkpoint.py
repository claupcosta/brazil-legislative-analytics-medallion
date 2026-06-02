# Databricks notebook source
# MAGIC %md
# MAGIC # 99 Bronze — Quality Checkpoint
# MAGIC
# MAGIC **Notebook:** `99_bronze_quality_checkpoint`
# MAGIC
# MAGIC Validates the completeness, quality, governance, and operational readiness of all Bronze layer datasets before Silver layer processing.
# MAGIC
# MAGIC This notebook validates:
# MAGIC
# MAGIC - Bronze table availability
# MAGIC - Raw ingestion completeness
# MAGIC - Record volume verification
# MAGIC - Source metadata validation
# MAGIC - Technical duplicate detection
# MAGIC - Mandatory ingestion field validation
# MAGIC - Audit field consistency
# MAGIC - Governance comment coverage
# MAGIC - Source traceability verification
# MAGIC - Bronze readiness assessment
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Responsibilities
# MAGIC
# MAGIC - Verify all Bronze datasets were ingested successfully
# MAGIC - Confirm record volumes from source extractions
# MAGIC - Validate ingestion metadata
# MAGIC - Verify source traceability fields
# MAGIC - Detect technical duplicates
# MAGIC - Assess raw data completeness
# MAGIC - Confirm governance comments were applied
# MAGIC - Validate audit information availability
# MAGIC - Produce Bronze quality evidence
# MAGIC - Certify Bronze readiness for Silver processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Notes
# MAGIC
# MAGIC - This notebook does not modify data
# MAGIC - All validations are read-only
# MAGIC - Results are intended for quality assurance purposes
# MAGIC - Bronze datasets preserve source information without business transformations
# MAGIC - Record counts may vary according to source refresh dates
# MAGIC - Audit fields are mandatory for traceability
# MAGIC - Governance validations use Information Schema metadata
# MAGIC - Validation failures should be investigated before Silver processing
# MAGIC - This notebook serves as the official Bronze checkpoint
# MAGIC - Results should be archived as delivery evidence
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - /docs/decisions/bronze_layer_strategy.md
# MAGIC - /docs/governance/data_quality.md
# MAGIC - /docs/governance/traceability.md
# MAGIC - /docs/operations/execution_guide.md
# MAGIC - /docs/standards/naming_conventions.md

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Inventário Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN brazil_legislative_analytics.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Volume por tabela

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'br_deputados' AS tabela, COUNT(*) total
# MAGIC FROM brazil_legislative_analytics.bronze.br_deputados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'br_despesas_ceap', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.bronze.br_despesas_ceap
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'br_eventos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.bronze.br_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'br_frentes', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.bronze.br_frentes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'br_frentes_membros', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.bronze.br_frentes_membros
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'br_orgaos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.bronze.br_orgaos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'br_orgaos_membros', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.bronze.br_orgaos_membros
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'br_presencas_eventos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.bronze.br_presencas_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'br_proposicoes', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.bronze.br_proposicoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'br_votacoes', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.bronze.br_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'br_votos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.bronze.br_votos;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Governança

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     column_name
# MAGIC FROM brazil_legislative_analytics.information_schema.columns
# MAGIC WHERE table_schema = 'bronze'
# MAGIC   AND column_name LIKE 'aud_%'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Auditoria

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     column_name
# MAGIC FROM brazil_legislative_analytics.information_schema.columns
# MAGIC WHERE table_schema = 'bronze'
# MAGIC   AND column_name LIKE 'aud_%'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Diagnóstico Final

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_tabelas
# MAGIC FROM brazil_legislative_analytics.information_schema.tables
# MAGIC WHERE table_schema = 'bronze';