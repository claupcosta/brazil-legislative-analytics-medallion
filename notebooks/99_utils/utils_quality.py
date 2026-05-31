# Databricks notebook source
# MAGIC %md
# MAGIC # Utils Layer — Data Quality Utilities
# MAGIC
# MAGIC **Notebook:** `utils_quality`  
# MAGIC **Layer:** `Utils`  
# MAGIC **Source/Endpoint:** `Spark DataFrames`  
# MAGIC **Target:** `Reusable data quality validation and audit functions`
# MAGIC
# MAGIC Provides reusable data quality functions used across Bronze,
# MAGIC Silver, Gold, Marts and Quality notebooks.
# MAGIC
# MAGIC This notebook centralizes validation logic and audit-ready quality
# MAGIC result generation for Medallion pipeline workflows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Validate required columns
# MAGIC - Validate null values
# MAGIC - Validate duplicate records
# MAGIC - Validate allowed domain values
# MAGIC - Calculate invalid record percentages
# MAGIC - Build structured quality validation results
# MAGIC - Support audit quality logging workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Shared utility notebook across Medallion layers
# MAGIC - Validation functions return structured dictionaries or DataFrames
# MAGIC - Supports auditability and traceability workflows
# MAGIC - Quality results can be persisted into audit quality tables
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/governance/data_quality_rules.md`
# MAGIC - `/docs/monitoring/quality_monitoring.md`
# MAGIC - `/docs/architecture/medallion_architecture.md`

# COMMAND ----------

# MAGIC %run ./utils_config

# COMMAND ----------

from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType

# COMMAND ----------

QUALITY_PASSED = "PASSED"
QUALITY_FAILED = "FAILED"
QUALITY_WARNING = "WARNING"

# COMMAND ----------

def validate_dataframe(dataframe: DataFrame) -> None:
    """
    Validates whether the input object is a Spark DataFrame.
    """

    if dataframe is None:
        raise ValueError("Input DataFrame cannot be None for data quality validation.")

    if not hasattr(dataframe, "columns"):
        raise TypeError("Input object must be a Spark DataFrame.")

# COMMAND ----------

def validate_required_columns(
    dataframe: DataFrame,
    required_columns: List[str],
) -> Dict[str, Any]:
    """
    Validates whether required columns exist in a DataFrame.
    """

    validate_dataframe(dataframe)

    required_columns = required_columns or []

    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    validation_status = (
        QUALITY_PASSED
        if len(missing_columns) == 0
        else QUALITY_FAILED
    )

    return {
        "nome_regra": "required_columns_check",
        "descricao_regra": "Validates whether all required columns exist in the DataFrame.",
        "status_validacao": validation_status,
        "total_registros": len(required_columns),
        "registros_invalidos": len(missing_columns),
        "percentual_invalidos": (
            0.0 if len(required_columns) == 0
            else round((len(missing_columns) / len(required_columns)) * 100, 2)
        ),
        "mensagem": f"Missing columns: {missing_columns}",
    }

# COMMAND ----------

def validate_nulls(
    dataframe: DataFrame,
    columns: List[str],
) -> List[Dict[str, Any]]:
    """
    Validates null values for selected columns.
    """

    validate_dataframe(dataframe)

    results = []
    total_records = dataframe.count()

    for column in columns:

        if column not in dataframe.columns:
            results.append({
                "nome_regra": f"null_check_{column}",
                "descricao_regra": f"Validates null values for column {column}.",
                "status_validacao": QUALITY_FAILED,
                "total_registros": total_records,
                "registros_invalidos": total_records,
                "percentual_invalidos": 100.0,
                "mensagem": f"Column does not exist: {column}",
            })
            continue

        null_count = dataframe.filter(F.col(column).isNull()).count()

        null_percentage = (
            0.0 if total_records == 0
            else round((null_count / total_records) * 100, 2)
        )

        validation_status = (
            QUALITY_PASSED
            if null_count == 0
            else QUALITY_WARNING
        )

        results.append({
            "nome_regra": f"null_check_{column}",
            "descricao_regra": f"Validates null values for column {column}.",
            "status_validacao": validation_status,
            "total_registros": total_records,
            "registros_invalidos": null_count,
            "percentual_invalidos": null_percentage,
            "mensagem": f"Null records found: {null_count}",
        })

    return results

# COMMAND ----------

def validate_duplicates(
    dataframe: DataFrame,
    key_columns: List[str],
) -> Dict[str, Any]:
    """
    Validates duplicate records based on business key columns.
    """

    validate_dataframe(dataframe)

    missing_columns = [
        column
        for column in key_columns
        if column not in dataframe.columns
    ]

    total_records = dataframe.count()

    if missing_columns:
        return {
            "nome_regra": "duplicate_key_check",
            "descricao_regra": "Validates duplicated records based on business key columns.",
            "status_validacao": QUALITY_FAILED,
            "total_registros": total_records,
            "registros_invalidos": total_records,
            "percentual_invalidos": 100.0,
            "mensagem": f"Missing key columns: {missing_columns}",
        }

    duplicate_groups = (
        dataframe
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    duplicate_percentage = (
        0.0 if total_records == 0
        else round((duplicate_groups / total_records) * 100, 2)
    )

    validation_status = (
        QUALITY_PASSED
        if duplicate_groups == 0
        else QUALITY_FAILED
    )

    return {
        "nome_regra": "duplicate_key_check",
        "descricao_regra": "Validates duplicated records based on business key columns.",
        "status_validacao": validation_status,
        "total_registros": total_records,
        "registros_invalidos": duplicate_groups,
        "percentual_invalidos": duplicate_percentage,
        "mensagem": f"Duplicated key groups found: {duplicate_groups}",
    }

# COMMAND ----------

def validate_domain(
    dataframe: DataFrame,
    column: str,
    allowed_values: List[str],
) -> Dict[str, Any]:
    """
    Validates whether a column contains only allowed domain values.
    """

    validate_dataframe(dataframe)

    total_records = dataframe.count()

    if column not in dataframe.columns:
        return {
            "nome_regra": f"domain_check_{column}",
            "descricao_regra": f"Validates allowed values for column {column}.",
            "status_validacao": QUALITY_FAILED,
            "total_registros": total_records,
            "registros_invalidos": total_records,
            "percentual_invalidos": 100.0,
            "mensagem": f"Column does not exist: {column}",
        }

    invalid_count = (
        dataframe
        .filter(~F.col(column).isin(allowed_values))
        .count()
    )

    invalid_percentage = (
        0.0 if total_records == 0
        else round((invalid_count / total_records) * 100, 2)
    )

    validation_status = (
        QUALITY_PASSED
        if invalid_count == 0
        else QUALITY_WARNING
    )

    return {
        "nome_regra": f"domain_check_{column}",
        "descricao_regra": f"Validates allowed values for column {column}.",
        "status_validacao": validation_status,
        "total_registros": total_records,
        "registros_invalidos": invalid_count,
        "percentual_invalidos": invalid_percentage,
        "mensagem": f"Invalid values found: {invalid_count}",
    }

# COMMAND ----------

def build_quality_log(
    quality_results: List[Dict[str, Any]],
    execution_id: str,
    notebook_name: str,
    layer_name: str,
    entity_name: str,
    target_table: str,
) -> DataFrame:
    """
    Converts quality validation dictionaries into the audit quality log table format.
    """

    results_df = spark.createDataFrame(quality_results)

    return (
        results_df
        .withColumn("qlt_id_log", F.expr("uuid()"))
        .withColumn("aud_id_execucao", F.lit(execution_id))
        .withColumn("aud_tx_nome_projeto", F.lit(PROJECT_NAME))
        .withColumn("aud_tx_versao_pipeline", F.lit(PIPELINE_VERSION))
        .withColumn("aud_tx_ambiente", F.lit(PROJECT_ENVIRONMENT))
        .withColumn("aud_tx_nome_notebook", F.lit(notebook_name))
        .withColumn("aud_tx_nome_camada", F.lit(layer_name))
        .withColumn("aud_tx_nome_entidade", F.lit(entity_name))
        .withColumn("aud_tx_tabela_destino", F.lit(target_table))
        .withColumn("qlt_tx_nome_regra", F.col("nome_regra"))
        .withColumn("qlt_tx_descricao_regra", F.col("descricao_regra"))
        .withColumn("qlt_tx_status_validacao", F.col("status_validacao"))
        .withColumn("qlt_qt_total_registros", F.col("total_registros").cast(LongType()))
        .withColumn("qlt_qt_registros_invalidos", F.col("registros_invalidos").cast(LongType()))
        .withColumn("qlt_pc_registros_invalidos", F.col("percentual_invalidos").cast(DoubleType()))
        .withColumn("qlt_dh_validacao", F.current_timestamp())
        .withColumn("qlt_tx_mensagem", F.col("mensagem"))
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

# COMMAND ----------

def write_quality_log(
    quality_dataframe: DataFrame,
) -> None:
    """
    Appends quality validation results into the audit quality log table.
    """

    quality_log_table = (
        f"{CATALOG_NAME}.{SCHEMA_AUDIT}.{AUD_TB_LOG_QUALIDADE_DADOS}"
    )

    quality_dataframe.write.mode("append").saveAsTable(
        quality_log_table
    )

# COMMAND ----------

print("utils_quality loaded successfully.")