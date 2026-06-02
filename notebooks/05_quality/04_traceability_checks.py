# Databricks notebook source
# MAGIC %md
# MAGIC # Quality Layer — Traceability Checks
# MAGIC
# MAGIC **Notebook:** `04_traceability_checks`
# MAGIC **Layer:** `Quality`
# MAGIC **Source:** `Bronze, Silver and Gold Delta Tables`
# MAGIC **Target:** `Traceability validation results and audit logs`
# MAGIC
# MAGIC Updated version:
# MAGIC - Keeps strict Bronze traceability validation.
# MAGIC - Supports current Silver audit naming:
# MAGIC   `aud_id_execucao_silver`, `aud_tx_versao_pipeline_silver`,
# MAGIC   `aud_tx_hash_registro_silver`, `aud_tx_hash_registro_bronze`,
# MAGIC   `aud_dh_processamento`, and entity-specific deduplication keys.
# MAGIC - Supports current Gold audit naming:
# MAGIC   `aud_dh_processamento`, `aud_tx_hash_registro_silver`,
# MAGIC   `aud_tx_hash_registro_gold`, source/lineage fields and business keys.
# MAGIC - Avoids false failures caused by old generic fields such as:
# MAGIC   `aud_id_execucao`, `aud_tx_versao_pipeline`, `aud_tx_hash_registro`.
# MAGIC - Treats optional tables as WARNING instead of FAILED.
# MAGIC - Validates traceability by groups of acceptable alternative columns.

# COMMAND ----------

# MAGIC %run ../99_utils/utils_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_quality

# COMMAND ----------

from datetime import datetime
from typing import Dict, List, Optional

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

# Keep False during development and portfolio validation.
# Set True only when traceability checks must block the pipeline.
FAIL_ON_ERROR = False

DATA_QUALITY_LOG_TABLE = (
    f"{CATALOG_NAME}."
    f"{SCHEMA_AUDIT}."
    f"{AUD_TB_LOG_QUALIDADE_DADOS}"
)

try:
    QUALITY_PASSED
except NameError:
    QUALITY_PASSED = "PASSED"

try:
    QUALITY_WARNING
except NameError:
    QUALITY_WARNING = "WARNING"

try:
    QUALITY_FAILED
except NameError:
    QUALITY_FAILED = "FAILED"

# Tables that may not exist in the current model and should not fail the pipeline.
OPTIONAL_TABLES = {
    "silver.orgaos_membros",
    "silver.cnpj_enriquecido",
}

# Layers allowed to use WARNING when no traceability alternative is found.
# Bronze remains stricter because raw ingestion audit fields are expected.
SOFT_TRACEABILITY_LAYERS = {"silver", "gold"}

# ============================================================
# TRACEABILITY RULES
# ============================================================
# required_groups:
#   A group passes if at least one column in the group exists.
#   The table passes required traceability if all groups pass.
#
# critical_groups:
#   For each group, the first existing column is checked for population.
#   If no column exists in a group, the result is WARNING for silver/gold
#   and FAILED for bronze.

TRACEABILITY_RULES: Dict[str, Dict[str, object]] = {
    "bronze": {
        "schema_name": SCHEMA_BRONZE,
        "tables": BRONZE_TABLES,
        "required_groups": [
            ["aud_id_execucao"],
            ["aud_dh_ingestao"],
            ["aud_tx_endpoint_origem"],
            ["aud_tx_sistema_origem"],
            ["aud_tx_versao_pipeline"],
            ["aud_tx_hash_registro"],
        ],
        "critical_groups": [
            ["aud_id_execucao"],
            ["aud_dh_ingestao"],
            ["aud_tx_endpoint_origem"],
            ["aud_tx_hash_registro"],
        ],
    },
    "silver": {
        "schema_name": SCHEMA_SILVER,
        "tables": SILVER_TABLES,
        "required_groups": [
            ["aud_id_execucao_silver", "aud_id_execucao", "aud_id_execucao_bronze"],
            ["aud_dh_processamento", "aud_dh_processamento_silver", "aud_dh_ingestao_bronze"],
            ["aud_tx_versao_pipeline_silver", "aud_tx_versao_pipeline", "aud_tx_versao_pipeline_bronze"],
            [
                "aud_tx_hash_registro_silver",
                "aud_tx_hash_registro",
                "aud_tx_hash_registro_bronze",
                "tx_hash_registro",
                "hash_registro",
                "dep_tx_chave_deputado_legislatura",
                "desp_tx_chave_deduplicacao",
                "forn_tx_chave_deduplicacao",
                "frm_tx_chave_deduplicacao",
                "frn_tx_chave_deduplicacao",
                "pev_tx_chave_deduplicacao",
            ],
        ],
        "critical_groups": [
            ["aud_dh_processamento", "aud_dh_processamento_silver"],
            [
                "aud_tx_hash_registro_silver",
                "aud_tx_hash_registro_bronze",
                "dep_tx_chave_deputado_legislatura",
                "desp_tx_chave_deduplicacao",
                "forn_tx_chave_deduplicacao",
                "frm_tx_chave_deduplicacao",
                "frn_tx_chave_deduplicacao",
                "pev_tx_chave_deduplicacao",
            ],
        ],
    },
    "gold": {
        "schema_name": SCHEMA_GOLD,
        "tables": {
            **GOLD_DIMENSION_TABLES,
            **GOLD_FACT_TABLES,
        },
        "required_groups": [
            ["aud_dh_processamento", "aud_dh_processamento_gold", "aud_dh_processamento_silver"],
            [
                "aud_tx_hash_registro_gold",
                "aud_tx_hash_registro_silver",
                "aud_tx_hash_registro",
                "tx_hash_registro",
                "hash_registro",
                "dep_tx_chave_deputado_legislatura",
                "desp_tx_chave_deduplicacao",
                "forn_tx_chave_deduplicacao",
                "fdc_tx_business_key",
            ],
            [
                "aud_tx_tabela_origem",
                "aud_tx_camada_origem",
                "aud_tx_tabela_destino",
                "aud_tx_versao_pipeline_gold",
                "aud_tx_versao_pipeline_silver",
                "aud_tx_versao_pipeline",
            ],
        ],
        "critical_groups": [
            ["aud_dh_processamento", "aud_dh_processamento_gold", "aud_dh_processamento_silver"],
        ],
    },
}

quality_results = []

# COMMAND ----------

# ============================================================
# HELPERS
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
    """Adds a quality validation result to the in-memory result list."""

    quality_results.append({
        "nome_regra": rule_name,
        "descricao_regra": rule_description,
        "status_validacao": validation_status,
        "total_registros": int(total_records) if total_records is not None else 0,
        "registros_invalidos": int(invalid_records) if invalid_records is not None else 0,
        "percentual_invalidos": float(invalid_percentage) if invalid_percentage is not None else 0.0,
        "mensagem": str(message),
        "entity_name": entity_name,
        "target_table": target_table,
    })


def add_exception_result(
    entity_name: str,
    target_table: str,
    error: Exception,
) -> None:
    """Adds a controlled exception result to the quality result list."""

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


def table_exists(full_table_name: str) -> bool:
    """Checks whether a fully qualified table exists."""

    try:
        return spark.catalog.tableExists(full_table_name)
    except Exception:
        return False


def get_table_dataframe(full_table_name: str) -> DataFrame:
    """Reads a table into a Spark DataFrame."""

    return spark.table(full_table_name)


def get_full_table_name_by_layer(
    schema_name: str,
    table_name: str,
) -> str:
    """Builds a fully qualified table name for any project layer."""

    return f"{CATALOG_NAME}.{schema_name}.{table_name}"


def calculate_percentage(
    invalid_records: int,
    total_records: int,
) -> float:
    """Calculates invalid percentage safely."""

    if total_records is None or total_records == 0:
        return 0.0

    return round((invalid_records / total_records) * 100, 4)


def first_existing_column(
    dataframe: DataFrame,
    column_group: List[str],
) -> Optional[str]:
    """Returns the first column from a group that exists in the dataframe."""

    dataframe_columns = set(dataframe.columns)

    for column_name in column_group:
        if column_name in dataframe_columns:
            return column_name

    return None


def existing_columns_from_group(
    dataframe: DataFrame,
    column_group: List[str],
) -> List[str]:
    """Returns all existing columns from a group."""

    dataframe_columns = set(dataframe.columns)

    return [
        column_name
        for column_name in column_group
        if column_name in dataframe_columns
    ]


def status_for_missing_traceability(layer_name: str) -> str:
    """Returns the status for missing traceability by layer."""

    if layer_name in SOFT_TRACEABILITY_LAYERS:
        return QUALITY_WARNING

    return QUALITY_FAILED


def invalid_count_for_status(status: str) -> int:
    """Returns invalid count for status."""

    return 1 if status == QUALITY_FAILED else 0


def invalid_percentage_for_status(status: str) -> float:
    """Returns invalid percentage for status."""

    return 100.0 if status == QUALITY_FAILED else 0.0

# COMMAND ----------

# ============================================================
# VALIDATIONS
# ============================================================

def validate_table_exists(
    layer_name: str,
    entity_name: str,
    full_table_name: str,
) -> bool:
    """Validates whether a table exists for traceability checks."""

    exists = table_exists(full_table_name)
    entity_key = f"{layer_name}.{entity_name}"

    if exists:
        status = QUALITY_PASSED
        invalid_records = 0
        invalid_percentage = 0.0
        message = "Table exists."

    elif entity_key in OPTIONAL_TABLES:
        status = QUALITY_WARNING
        invalid_records = 0
        invalid_percentage = 0.0
        message = "Optional table does not exist in the current model."

    else:
        status = QUALITY_FAILED
        invalid_records = 1
        invalid_percentage = 100.0
        message = "Table does not exist."

    add_quality_result(
        rule_name="traceability_table_exists",
        rule_description="Validates whether the table exists for traceability checks.",
        validation_status=status,
        total_records=1,
        invalid_records=invalid_records,
        invalid_percentage=invalid_percentage,
        message=message,
        entity_name=entity_key,
        target_table=full_table_name,
    )

    return exists


def validate_required_traceability_groups(
    dataframe: DataFrame,
    layer_name: str,
    entity_name: str,
    full_table_name: str,
    required_groups: List[List[str]],
) -> None:
    """
    Validates required traceability by alternative column groups.

    A group passes when at least one of its columns exists.
    """

    missing_groups = []
    detected_columns = []

    for column_group in required_groups:
        existing_columns = existing_columns_from_group(
            dataframe=dataframe,
            column_group=column_group,
        )

        if existing_columns:
            detected_columns.extend(existing_columns)
        else:
            missing_groups.append(column_group)

    if not missing_groups:
        status = QUALITY_PASSED
        invalid_records = 0
        invalid_percentage = 0.0
    else:
        status = status_for_missing_traceability(layer_name)
        invalid_records = invalid_count_for_status(status)
        invalid_percentage = invalid_percentage_for_status(status)

    add_quality_result(
        rule_name="traceability_required_columns",
        rule_description="Validates required traceability column groups.",
        validation_status=status,
        total_records=len(required_groups),
        invalid_records=len(missing_groups) if status == QUALITY_FAILED else 0,
        invalid_percentage=(
            round((len(missing_groups) / len(required_groups)) * 100, 2)
            if status == QUALITY_FAILED and required_groups
            else 0.0
        ),
        message=(
            f"Detected traceability columns: {sorted(set(detected_columns))}. "
            f"Missing alternative groups: {missing_groups}"
        ),
        entity_name=f"{layer_name}.{entity_name}",
        target_table=full_table_name,
    )


def validate_column_group_population(
    dataframe: DataFrame,
    layer_name: str,
    entity_name: str,
    full_table_name: str,
    column_group: List[str],
) -> None:
    """
    Validates whether the first available column in a traceability group is populated.
    """

    column_name = first_existing_column(
        dataframe=dataframe,
        column_group=column_group,
    )

    if column_name is None:
        status = status_for_missing_traceability(layer_name)

        add_quality_result(
            rule_name="traceability_population_missing_group",
            rule_description="Validates whether a traceability column group is available.",
            validation_status=status,
            total_records=1,
            invalid_records=invalid_count_for_status(status),
            invalid_percentage=invalid_percentage_for_status(status),
            message=(
                f"No column found for alternative group: {column_group}. "
                f"Layer policy status: {status}."
            ),
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

    invalid_percentage = calculate_percentage(
        invalid_records=invalid_records,
        total_records=total_records,
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
        message=(
            f"Column selected from group: {column_name}. "
            f"Records with missing values: {invalid_records}."
        ),
        entity_name=f"{layer_name}.{entity_name}",
        target_table=full_table_name,
    )


def run_entity_checks(
    layer_name: str,
    schema_name: str,
    entity_name: str,
    table_name: str,
    required_groups: List[List[str]],
    critical_groups: List[List[str]],
) -> None:
    """Executes traceability checks for a single table."""

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

        validate_required_traceability_groups(
            dataframe=dataframe,
            layer_name=layer_name,
            entity_name=entity_name,
            full_table_name=full_table_name,
            required_groups=required_groups,
        )

        for column_group in critical_groups:
            validate_column_group_population(
                dataframe=dataframe,
                layer_name=layer_name,
                entity_name=entity_name,
                full_table_name=full_table_name,
                column_group=column_group,
            )

    except Exception as error:
        add_exception_result(
            entity_name=f"{layer_name}.{entity_name}",
            target_table=full_table_name,
            error=error,
        )

# COMMAND ----------

# ============================================================
# LOG BUILDER
# ============================================================

def build_traceability_quality_log() -> DataFrame:
    """Builds the final traceability quality log DataFrame."""

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

    quality_base_df = spark.createDataFrame(quality_results)

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
            required_groups=layer_config["required_groups"],
            critical_groups=layer_config["critical_groups"],
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Persist Quality Results

# COMMAND ----------

quality_log_df = build_traceability_quality_log()

quality_log_df.write.mode("append").saveAsTable(DATA_QUALITY_LOG_TABLE)

print(f"Traceability quality results persisted into: {DATA_QUALITY_LOG_TABLE}")

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
    .filter(F.col("qlt_tx_status_validacao") == QUALITY_FAILED)
    .count()
)

warning_count = (
    quality_log_df
    .filter(F.col("qlt_tx_status_validacao") == QUALITY_WARNING)
    .count()
)

passed_count = (
    quality_log_df
    .filter(F.col("qlt_tx_status_validacao") == QUALITY_PASSED)
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
        f"{failed_count} failed validation(s). Review the traceability log."
    )

print("TRACEABILITY CHECKS COMPLETED")