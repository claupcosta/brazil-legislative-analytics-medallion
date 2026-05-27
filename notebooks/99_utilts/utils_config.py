# Databricks notebook source
# MAGIC %md
# MAGIC # Utils Layer — Configuration Loader
# MAGIC
# MAGIC **Notebook:** `utils_config`  
# MAGIC **Layer:** `Utils`  
# MAGIC **Source/Endpoint:** `Project Configuration Objects`  
# MAGIC **Target:** `Reusable global configuration variables and constants`
# MAGIC
# MAGIC Provides lightweight access to centralized project configuration
# MAGIC used across Medallion pipeline notebooks.
# MAGIC
# MAGIC This notebook loads shared configuration objects, governance standards,
# MAGIC table mappings and execution constants required throughout the project.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Load centralized project configuration objects
# MAGIC - Validate required configuration availability
# MAGIC - Provide reusable global constants
# MAGIC - Support shared pipeline configuration standards
# MAGIC - Enable lightweight configuration reuse across notebooks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Optimized for lightweight `%run` execution
# MAGIC - Avoids heavy validation and unnecessary initialization overhead
# MAGIC - Uses fully qualified table names across the pipeline
# MAGIC - Catalog activation is optional and disabled by default
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/standards/naming_conventions.md`
# MAGIC - `/docs/standards/coding_standards.md`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# ============================================================
# LIGHTWEIGHT CONFIGURATION VALIDATION
# ============================================================

REQUIRED_CONFIG_OBJECTS = [
    "PROJECT_NAME",
    "PROJECT_VERSION",
    "PROJECT_ENVIRONMENT",
    "CATALOG_NAME",
    "SCHEMA_AUDIT",
    "SCHEMA_BRONZE",
    "SCHEMA_SILVER",
    "SCHEMA_GOLD",
    "SCHEMA_MARTS",
    "AUD_TB_LOG_EXECUCAO_PIPELINE",
    "AUD_TB_LOG_ERROS_PIPELINE",
    "AUD_TB_LOG_QUALIDADE_DADOS",
    "BRONZE_TABLES",
    "SILVER_TABLES",
    "GOLD_DIMENSION_TABLES",
    "GOLD_FACT_TABLES",
    "MART_TABLES",
    "API_ENDPOINTS",
    "TRACEABILITY_COLUMNS",
    "QUALITY_PASSED",
    "QUALITY_WARNING",
    "QUALITY_FAILED",
    "EXECUTION_STATUS_STARTED",
    "EXECUTION_STATUS_SUCCESS",
    "EXECUTION_STATUS_WARNING",
    "EXECUTION_STATUS_FAILED",
]

missing_config_objects = [
    object_name
    for object_name in REQUIRED_CONFIG_OBJECTS
    if object_name not in globals()
]

if missing_config_objects:

    raise Exception(
        "Project configuration was not loaded correctly. "
        f"Missing objects: {missing_config_objects}"
    )

# COMMAND ----------

# ============================================================
# OPTIONAL CATALOG ACTIVATION
# ============================================================
#
# Disabled by default to reduce overhead when this notebook is
# imported through %run by other notebooks.
#
# Use fully qualified table names across the pipeline instead.
#
# ============================================================

SET_ACTIVE_CATALOG = False

if SET_ACTIVE_CATALOG:
    spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

print("utils_config loaded successfully.")