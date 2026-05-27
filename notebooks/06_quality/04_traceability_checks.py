# Databricks notebook source
# MAGIC %md
# MAGIC # Quality Layer — Traceability Checks
# MAGIC
# MAGIC **Notebook:** `04_traceability_checks`  
# MAGIC **Layer:** `Quality`  
# MAGIC **Source/Endpoint:** `Bronze, Silver and Gold Delta Tables`  
# MAGIC **Target:** `Traceability validation results and audit logs`
# MAGIC
# MAGIC Executes cross-layer traceability validations for the
# MAGIC Brazil Legislative Analytics Medallion project.
# MAGIC
# MAGIC This notebook validates whether pipeline traceability fields are
# MAGIC consistently available and populated across Bronze, Silver and Gold layers.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Validate table existence across Medallion layers
# MAGIC - Validate required traceability columns
# MAGIC - Validate execution identifier availability
# MAGIC - Validate pipeline version availability
# MAGIC - Validate processing timestamp availability
# MAGIC - Validate critical traceability column population
# MAGIC - Persist traceability validation results into audit tables
# MAGIC - Generate traceability validation summary
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Validation results are persisted into audit quality logs
# MAGIC - Supports cross-layer governance and lineage validation
# MAGIC - Failed validations may not block execution during early development
# MAGIC - Pipeline blocking behavior is controlled by `FAIL_ON_ERROR`
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/governance/data_lineage.md`
# MAGIC - `/docs/governance/data_quality_rules.md`
# MAGIC - `/docs/monitoring/quality_monitoring.md`

# COMMAND ----------

# MAGIC %run ../99_utils/utils_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_quality

# COMMAND ----------

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("04 - TRACEABILITY CHECKS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print(f"Catalog: {CATALOG_NAME}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# QUALITY CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "04_traceability_checks"
LAYER_NAME = "quality"

# During early development, some tables may not exist yet.
# Set to True when all layers are active and traceability checks
# must block the pipeline.
FAIL_ON_ERROR = False

DATA_QUALITY_LOG_TABLE = (
    f"{CATALOG_NAME}."
    f"{SCHEMA_AUDIT}."
    f"{AUD_TB_LOG_QUALIDADE_DADOS}"
)

TRACEABILITY_RULES = {
    "bronze": {
        "schema_name": SCHEMA_BRONZE,
        "tables": BRONZE_TABLES,
        "required_columns": [
            "aud_id_execucao",
            "aud_dh_ingestao",
            "aud_tx_endpoint_origem",
            "aud_tx_sistema_origem",
            "aud_tx_versao_pipeline",
            "aud_tx_hash_registro",
        ],
        "critical_columns": [
            "aud_id_execucao",
            "aud_dh_ingestao",
            "aud_tx_endpoint_origem",
            "aud_tx_hash_registro",
        ],
    },
    "silver": {
        "schema_name": SCHEMA_SILVER,
        "tables": SILVER_TABLES,
        "required_columns": [
            "aud_id_execucao",
            "aud_dh_processamento",
            "aud_tx_versao_pipeline",
            "aud_tx_hash_registro",
        ],
        "critical_columns": [
            "aud_id_execucao",
            "aud_dh_processamento",
            "aud_tx_hash_registro",
        ],
    },
    "gold": {
        "schema_name": SCHEMA_GOLD,
        "tables": {
            **GOLD_DIMENSION_TABLES,
            **GOLD_FACT_TABLES,
        },
        "required_columns": [
            "aud_id_execucao",
            "aud_dh_processamento",
            "aud_tx_versao_pipeline",
        ],
        "critical_columns": [
            "aud_id_execucao",
            "aud_dh_processamento",
        ],
    },
}

quality_results = []

# COMMAND ----------

# ============================================================
# QUALITY HELPERS
# ============================================================

def add_quality_result(
    rule_name: str,
    rule_description: str,
    validation_status: str,
    total_records: int,
    invalid_records: int,
    invalid_percentage: float,
    message: str,
    entity_name: str,
    target_table: str,
) -> None:
    """
    Adds a quality validation result to the in-memory result list.
    """

    quality_results.append({
        "nome_regra": rule_name,
        "descricao_regra": rule_description,
        "status_validacao": validation_status,
        "total_registros": int(total_records) if total_records is not None else 0,
        "registros_invalidos": int(invalid_records) if invalid_records is not None else 0,
        "percentual_invalidos": float(invalid_percentage) if invalid_percentage is not None else 0.0,
        "mensagem": message,
        "entity_name": entity_name,
        "target_table": target_table,
    })

# COMMAND ----------

def add_exception_result(
    entity_name: str,
    target_table: str,
    error: Exception,
) -> None:
    """
    Adds a controlled exception result to the quality result list.
    """

    add_quality_result(
        rule_name="traceability_quality_exception",
        rule_description="Captures unexpected errors during traceability validation.",
        validation_status=QUALITY_FAILED,
        total_records=1,
        invalid_records=1,
        invalid_percentage=100.0,
        message=f"Unexpected error during traceability validation: {str(error)}",
        entity_name=entity_name,
        target_table=target_table,
    )

# COMMAND ----------

def table_exists(
    full_table_name: str,
) -> bool:
    """
    Checks whether a fully qualified table exists.
    """

    try:
        return spark.catalog.tableExists(full_table_name)

    except Exception:
        return False

# COMMAND ----------

def get_table_dataframe(
    full_table_name: str,
) -> DataFrame:
    """
    Reads a table into a Spark DataFrame.
    """

    return spark.table(full_table_name)

# COMMAND ----------

def get_full_table_name_by_layer(
    schema_name: str,
    table_name: str,
) -> str:
    """
    Builds a fully qualified table name for any project layer.
    """

    return (
        f"{CATALOG_NAME}."
        f"{schema_name}."
        f"{table_name}"
    )

# COMMAND ----------

def validate_table_exists(
    layer_name: str,
    entity_name: str,
    full_table_name: str,
) -> bool:
    """
    Validates whether a table exists for traceability checks.
    """

    exists = table_exists(full_table_name)

    add_quality_result(
        rule_name="traceability_table_exists",
        rule_description="Validates whether the table exists for traceability checks.",
        validation_status=QUALITY_PASSED if exists else QUALITY_FAILED,
        total_records=1,
        invalid_records=0 if exists else 1,
        invalid_percentage=0.0 if exists else 100.0,
        message=(
            "Table exists."
            if exists
            else "Table does not exist."
        ),
        entity_name=f"{layer_name}.{entity_name}",
        target_table=full_table_name,
    )

    return exists

# COMMAND ----------

def validate_required_traceability_columns(
    dataframe: DataFrame,
    layer_name: str,
    entity_name: str,
    full_table_name: str,
    required_columns: list,
) -> None:
    """
    Validates required traceability columns for a table.
    """

    result = validate_required_columns(
        dataframe=dataframe,
        required_columns=required_columns,
    )

    add_quality_result(
        rule_name="traceability_required_columns",
        rule_description="Validates required traceability columns.",
        validation_status=result["status_validacao"],
        total_records=result["total_registros"],
        invalid_records=result["registros_invalidos"],
        invalid_percentage=result["percentual_invalidos"],
        message=result["mensagem"],
        entity_name=f"{layer_name}.{entity_name}",
        target_table=full_table_name,
    )

# COMMAND ----------

def validate_column_population(
    dataframe: DataFrame,
    layer_name: str,
    entity_name: str,
    full_table_name: str,
    column_name: str,
) -> None:
    """
    Validates whether a traceability column is populated.
    """

    if column_name not in dataframe.columns:

        add_quality_result(
            rule_name=f"traceability_population_{column_name}",
            rule_description="Validates whether a traceability column is populated.",
            validation_status=QUALITY_FAILED,
            total_records=1,
            invalid_records=1,
            invalid_percentage=100.0,
            message=f"Column does not exist: {column_name}",
            entity_name=f"{layer_name}.{entity_name}",
            target_table=full_table_name,
        )

        return

    total_records = dataframe.count()

    invalid_records = (
        dataframe
        .filter(
            F.col(column_name).isNull()
            | (F.trim(F.col(column_name).cast("string")) == "")
        )
        .count()
    )

    invalid_percentage = (
        0.0 if total_records == 0
        else round((invalid_records / total_records) * 100, 2)
    )

    validation_status = (
        QUALITY_PASSED
        if invalid_records == 0
        else QUALITY_WARNING
    )

    add_quality_result(
        rule_name=f"traceability_population_{column_name}",
        rule_description="Validates whether a traceability column is populated.",
        validation_status=validation_status,
        total_records=total_records,
        invalid_records=invalid_records,
        invalid_percentage=invalid_percentage,
        message=f"Records with missing {column_name}: {invalid_records}",
        entity_name=f"{layer_name}.{entity_name}",
        target_table=full_table_name,
    )

# COMMAND ----------

def run_entity_checks(
    layer_name: str,
    schema_name: str,
    entity_name: str,
    table_name: str,
    required_columns: list,
    critical_columns: list,
) -> None:
    """
    Executes traceability checks for a single table.
    """

    full_table_name = get_full_table_name_by_layer(
        schema_name=schema_name,
        table_name=table_name,
    )

    print("=" * 90)
    print(f"Running traceability checks for: {full_table_name}")
    print("=" * 90)

    try:

        if not validate_table_exists(
            layer_name=layer_name,
            entity_name=entity_name,
            full_table_name=full_table_name,
        ):
            return

        dataframe = get_table_dataframe(full_table_name)

        validate_required_traceability_columns(
            dataframe=dataframe,
            layer_name=layer_name,
            entity_name=entity_name,
            full_table_name=full_table_name,
            required_columns=required_columns,
        )

        for column_name in critical_columns:

            validate_column_population(
                dataframe=dataframe,
                layer_name=layer_name,
                entity_name=entity_name,
                full_table_name=full_table_name,
                column_name=column_name,
            )

    except Exception as error:

        add_exception_result(
            entity_name=f"{layer_name}.{entity_name}",
            target_table=full_table_name,
            error=error,
        )

# COMMAND ----------

def build_traceability_quality_log() -> DataFrame:
    """
    Builds the final traceability quality log DataFrame.
    """

    if not quality_results:

        add_quality_result(
            rule_name="traceability_no_results",
            rule_description="Validates whether traceability checks produced results.",
            validation_status=QUALITY_WARNING,
            total_records=0,
            invalid_records=0,
            invalid_percentage=0.0,
            message="No traceability quality results were generated.",
            entity_name="traceability",
            target_table=DATA_QUALITY_LOG_TABLE,
        )

    quality_base_df = spark.createDataFrame(
        quality_results
    )

    return (
        quality_base_df
        .withColumn("qlt_id_log", F.expr("uuid()"))
        .withColumn("aud_id_execucao", F.lit(RUN_ID))
        .withColumn("aud_tx_nome_projeto", F.lit(PROJECT_NAME))
        .withColumn("aud_tx_versao_pipeline", F.lit(PROJECT_VERSION))
        .withColumn("aud_tx_ambiente", F.lit(PROJECT_ENVIRONMENT))
        .withColumn("aud_tx_nome_notebook", F.lit(NOTEBOOK_NAME))
        .withColumn("aud_tx_nome_camada", F.lit(LAYER_NAME))
        .withColumn("aud_tx_nome_entidade", F.col("entity_name"))
        .withColumn("aud_tx_tabela_destino", F.col("target_table"))
        .withColumn("qlt_tx_nome_regra", F.col("nome_regra"))
        .withColumn("qlt_tx_descricao_regra", F.col("descricao_regra"))
        .withColumn("qlt_tx_status_validacao", F.col("status_validacao"))
        .withColumn("qlt_qt_total_registros", F.col("total_registros"))
        .withColumn("qlt_qt_registros_invalidos", F.col("registros_invalidos"))
        .withColumn("qlt_pc_registros_invalidos", F.col("percentual_invalidos"))
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

# MAGIC %md
# MAGIC ## 1. Execute Traceability Checks

# COMMAND ----------

for layer_name, layer_config in TRACEABILITY_RULES.items():

    for entity_name, table_name in layer_config["tables"].items():

        run_entity_checks(
            layer_name=layer_name,
            schema_name=layer_config["schema_name"],
            entity_name=entity_name,
            table_name=table_name,
            required_columns=layer_config["required_columns"],
            critical_columns=layer_config["critical_columns"],
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Persist Quality Results

# COMMAND ----------

quality_log_df = build_traceability_quality_log()

quality_log_df.write.mode(
    "append"
).saveAsTable(DATA_QUALITY_LOG_TABLE)

print(
    f"Traceability quality results persisted into: "
    f"{DATA_QUALITY_LOG_TABLE}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Display Quality Results

# COMMAND ----------

display(quality_log_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quality Summary

# COMMAND ----------

failed_count = (
    quality_log_df
    .filter("qlt_tx_status_validacao = 'FAILED'")
    .count()
)

warning_count = (
    quality_log_df
    .filter("qlt_tx_status_validacao = 'WARNING'")
    .count()
)

passed_count = (
    quality_log_df
    .filter("qlt_tx_status_validacao = 'PASSED'")
    .count()
)

print("=" * 90)
print("TRACEABILITY QUALITY SUMMARY")
print("=" * 90)
print(f"Passed validations: {passed_count}")
print(f"Warning validations: {warning_count}")
print(f"Failed validations: {failed_count}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# QUALITY EXECUTION POLICY
# ============================================================

if failed_count > 0 and FAIL_ON_ERROR:

    raise Exception(
        f"Traceability validation failed with "
        f"{failed_count} failed validation(s)."
    )

if failed_count > 0:

    print(
        f"WARNING: Traceability validation finished with "
        f"{failed_count} failed validation(s). "
        "This is expected if some pipeline tables have not been created yet."
    )

print("TRACEABILITY CHECKS COMPLETED")