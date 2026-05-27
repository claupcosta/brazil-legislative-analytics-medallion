# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Layer — Project Setup Validation
# MAGIC
# MAGIC **Notebook:** `90_validate_project_setup`  
# MAGIC **Layer:** `Setup`  
# MAGIC **Source/Endpoint:** `Internal Spark Metadata Validation`  
# MAGIC **Target:** `Setup validation results and audit quality logs`
# MAGIC
# MAGIC Validates the catalog, schemas and audit tables required by the
# MAGIC Brazil Legislative Analytics Medallion project.
# MAGIC
# MAGIC This notebook verifies whether the setup structure was successfully
# MAGIC initialized before executing Bronze, Silver, Gold, Marts or Quality pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Validate catalog existence
# MAGIC - Validate required schema availability
# MAGIC - Validate audit table existence
# MAGIC - Detect deprecated schema usage
# MAGIC - Persist setup validation results into audit tables
# MAGIC - Generate setup validation summary
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Validation results are persisted into audit quality logs
# MAGIC - Supports governance and operational readiness checks
# MAGIC - Deprecated schemas are monitored for architecture compliance
# MAGIC - Failed validations block pipeline execution
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/governance/data_governance.md`
# MAGIC - `/docs/monitoring/observability.md`

# COMMAND ----------

# MAGIC %run ./01_project_config

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("90 - VALIDATE PROJECT SETUP")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print(f"Catalog: {CATALOG_NAME}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# EXPECTED STRUCTURES
# ============================================================

EXPECTED_SCHEMAS = [
    SCHEMA_AUDIT,
    SCHEMA_BRONZE,
    SCHEMA_SILVER,
    SCHEMA_GOLD,
    SCHEMA_MARTS,
]

DEPRECATED_SCHEMAS = [
    "silver_base",
    "silver_curated",
]

EXPECTED_AUDIT_TABLES = [
    AUD_TB_LOG_EXECUCAO_PIPELINE,
    AUD_TB_LOG_ERROS_PIPELINE,
    AUD_TB_LOG_QUALIDADE_DADOS,
]

DATA_QUALITY_LOG_TABLE = (
    f"{CATALOG_NAME}."
    f"{SCHEMA_AUDIT}."
    f"{AUD_TB_LOG_QUALIDADE_DADOS}"
)

validation_results = []

# COMMAND ----------

# ============================================================
# VALIDATION HELPERS
# ============================================================

def register_validation_result(
    validation_group: str,
    object_name: str,
    validation_status: str,
    validation_message: str,
) -> None:
    """
    Stores a setup validation result in memory for
    final reporting and persistence.
    """

    validation_results.append({
        "validation_group": validation_group,
        "object_name": object_name,
        "validation_status": validation_status,
        "validation_message": validation_message,
        "validated_at": datetime.now(),
    })

# COMMAND ----------

def catalog_exists(
    catalog_name: str,
) -> bool:
    """
    Checks whether a catalog exists in the current workspace.
    """

    catalogs_df = spark.sql("SHOW CATALOGS")

    return (
        catalogs_df
        .filter(f"catalog = '{catalog_name}'")
        .count() > 0
    )

# COMMAND ----------

def schema_exists(
    catalog_name: str,
    schema_name: str,
) -> bool:
    """
    Checks whether a schema exists inside a catalog.
    """

    schemas_df = spark.sql(
        f"SHOW SCHEMAS IN {catalog_name}"
    )

    return (
        schemas_df
        .filter(f"databaseName = '{schema_name}'")
        .count() > 0
    )

# COMMAND ----------

def table_exists(
    full_table_name: str,
) -> bool:
    """
    Checks whether a fully qualified table exists.
    """

    return spark.catalog.tableExists(full_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validate Catalog

# COMMAND ----------

if catalog_exists(CATALOG_NAME):

    register_validation_result(
        validation_group="catalog_exists",
        object_name=CATALOG_NAME,
        validation_status="PASSED",
        validation_message="Catalog exists.",
    )

else:

    register_validation_result(
        validation_group="catalog_exists",
        object_name=CATALOG_NAME,
        validation_status="FAILED",
        validation_message="Catalog does not exist.",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate Required Schemas

# COMMAND ----------

for schema_name in EXPECTED_SCHEMAS:

    full_schema_name = (
        f"{CATALOG_NAME}.{schema_name}"
    )

    if schema_exists(CATALOG_NAME, schema_name):

        register_validation_result(
            validation_group="schema_exists",
            object_name=full_schema_name,
            validation_status="PASSED",
            validation_message="Required schema exists.",
        )

    else:

        register_validation_result(
            validation_group="schema_exists",
            object_name=full_schema_name,
            validation_status="FAILED",
            validation_message="Required schema does not exist.",
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Deprecated Schemas

# COMMAND ----------

for schema_name in DEPRECATED_SCHEMAS:

    full_schema_name = (
        f"{CATALOG_NAME}.{schema_name}"
    )

    if schema_exists(CATALOG_NAME, schema_name):

        register_validation_result(
            validation_group="deprecated_schema_check",
            object_name=full_schema_name,
            validation_status="WARNING",
            validation_message=(
                "Deprecated schema still exists "
                "and should not be used by the "
                "simplified architecture."
            ),
        )

    else:

        register_validation_result(
            validation_group="deprecated_schema_check",
            object_name=full_schema_name,
            validation_status="PASSED",
            validation_message=(
                "Deprecated schema does not exist "
                "or is not active."
            ),
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate Audit Tables

# COMMAND ----------

for table_name in EXPECTED_AUDIT_TABLES:

    full_table_name = (
        f"{CATALOG_NAME}."
        f"{SCHEMA_AUDIT}."
        f"{table_name}"
    )

    if table_exists(full_table_name):

        register_validation_result(
            validation_group="audit_table_exists",
            object_name=full_table_name,
            validation_status="PASSED",
            validation_message="Required audit table exists.",
        )

    else:

        register_validation_result(
            validation_group="audit_table_exists",
            object_name=full_table_name,
            validation_status="FAILED",
            validation_message="Required audit table does not exist.",
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Display Validation Results

# COMMAND ----------

validation_df = spark.createDataFrame(
    validation_results
)

display(validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Persist Setup Validation Results

# COMMAND ----------

# ============================================================
# PERSIST SETUP VALIDATION RESULTS
# ============================================================

setup_quality_log_df = (
    validation_df
    .withColumn("qlt_id_log", F.expr("uuid()"))
    .withColumn("aud_id_execucao", F.lit(RUN_ID))
    .withColumn("aud_tx_nome_projeto", F.lit(PROJECT_NAME))
    .withColumn("aud_tx_versao_pipeline", F.lit(PROJECT_VERSION))
    .withColumn("aud_tx_ambiente", F.lit(PROJECT_ENVIRONMENT))
    .withColumn(
        "aud_tx_nome_notebook",
        F.lit("90_validate_project_setup"),
    )
    .withColumn(
        "aud_tx_nome_camada",
        F.lit("setup"),
    )
    .withColumn(
        "aud_tx_nome_entidade",
        F.lit("project_setup"),
    )
    .withColumn(
        "aud_tx_tabela_destino",
        F.lit(DATA_QUALITY_LOG_TABLE),
    )
    .withColumn(
        "qlt_tx_nome_regra",
        F.col("validation_group"),
    )
    .withColumn(
        "qlt_tx_descricao_regra",
        F.col("validation_message"),
    )
    .withColumn(
        "qlt_tx_status_validacao",
        F.col("validation_status"),
    )
    .withColumn(
        "qlt_qt_total_registros",
        F.lit(1).cast(LongType()),
    )
    .withColumn(
        "qlt_qt_registros_invalidos",
        F.when(
            F.col("validation_status") == "FAILED",
            F.lit(1),
        )
        .otherwise(F.lit(0))
        .cast(LongType())
    )
    .withColumn(
        "qlt_pc_registros_invalidos",
        F.when(
            F.col("validation_status") == "FAILED",
            F.lit(100.0),
        )
        .otherwise(F.lit(0.0))
        .cast(DoubleType())
    )
    .withColumn(
        "qlt_dh_validacao",
        F.current_timestamp(),
    )
    .withColumn(
        "qlt_tx_mensagem",
        F.concat(
            F.lit(
                "Setup validation result for object: "
            ),
            F.col("object_name"),
            F.lit(" | "),
            F.col("validation_message"),
        ),
    )
    .select(
        "qlt_id_log",
        "aud_id_execucao",
        "aud_tx_nome_projeto",
        "aud_tx_versao_pipeline",
        "aud_tx_ambiente",
        "aud_tx_nome_notebook",
        "aud_tx_nome_camada",
        "aud_tx_nome_entidade",
        "aud_tx_tabela_destino",
        "qlt_tx_nome_regra",
        "qlt_tx_descricao_regra",
        "qlt_tx_status_validacao",
        "qlt_qt_total_registros",
        "qlt_qt_registros_invalidos",
        "qlt_pc_registros_invalidos",
        "qlt_dh_validacao",
        "qlt_tx_mensagem",
    )
)

setup_quality_log_df.write.mode(
    "append"
).saveAsTable(DATA_QUALITY_LOG_TABLE)

print(
    f"Setup validation results persisted into: "
    f"{DATA_QUALITY_LOG_TABLE}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation Summary

# COMMAND ----------

failed_count = (
    validation_df
    .filter("validation_status = 'FAILED'")
    .count()
)

warning_count = (
    validation_df
    .filter("validation_status = 'WARNING'")
    .count()
)

passed_count = (
    validation_df
    .filter("validation_status = 'PASSED'")
    .count()
)

print("=" * 90)
print("PROJECT SETUP VALIDATION SUMMARY")
print("=" * 90)
print(f"Passed validations: {passed_count}")
print(f"Warning validations: {warning_count}")
print(f"Failed validations: {failed_count}")
print("=" * 90)

# COMMAND ----------

if failed_count > 0:

    raise Exception(
        f"Project setup validation failed with "
        f"{failed_count} failed validation(s). "
        "Please review the validation output "
        "before running the pipeline."
    )

print("PROJECT SETUP VALIDATED SUCCESSFULLY")

if warning_count > 0:

    print(
        f"WARNING: {warning_count} warning(s) found. "
        "Please review deprecated schemas or "
        "non-blocking setup issues."
    )

