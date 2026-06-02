# Databricks notebook source
# MAGIC %md
# MAGIC # Quality Layer — Marts Quality Checks
# MAGIC
# MAGIC **Notebook:** `05_quality_marts_checks`  
# MAGIC **Layer:** `Quality`  
# MAGIC **Source/Endpoint:** `Mart Delta Tables`  
# MAGIC **Target:** `Mart quality validation results and audit logs`
# MAGIC
# MAGIC Executes data quality validations for analytical Mart tables
# MAGIC in the Brazil Legislative Analytics Medallion project.
# MAGIC
# MAGIC This notebook validates whether Mart analytical outputs are available,
# MAGIC traceable and structurally consistent before consumption by dashboards,
# MAGIC reports and downstream analytical products.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Validate Mart table existence
# MAGIC - Validate minimum record availability
# MAGIC - Validate required traceability columns
# MAGIC - Validate expected Mart key / grain columns
# MAGIC - Validate null values in Mart key / grain columns
# MAGIC - Validate duplicated records at the expected Mart grain
# MAGIC - Persist quality validation results into audit tables
# MAGIC - Generate Mart quality validation summary
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Validation results are persisted into audit quality logs
# MAGIC - Supports controlled exception handling per entity
# MAGIC - Failed validations may not block execution during early development
# MAGIC - Pipeline blocking behavior is controlled by `FAIL_ON_ERROR`
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/governance/data_quality_rules.md`
# MAGIC - `/docs/monitoring/quality_monitoring.md`
# MAGIC - `/docs/architecture/medallion_architecture.md`

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
print("05 - QUALITY MARTS CHECKS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Layer: {SCHEMA_MARTS}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# QUALITY CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "05_quality_marts_checks"
LAYER_NAME = "marts"

# During early development, Mart tables may not exist yet.
# Set to True when Mart processing is active and quality checks
# must block the pipeline.
FAIL_ON_ERROR = False

DATA_QUALITY_LOG_TABLE = (
    f"{CATALOG_NAME}."
    f"{SCHEMA_AUDIT}."
    f"{AUD_TB_LOG_QUALIDADE_DADOS}"
)

# Table names aligned with the current data dictionary.
MART_ENTITY_TABLES = {
    "am_atlas_frentes": "am_atlas_frentes",
    "am_calendario_eventos": "am_calendario_eventos",
    "am_correlacao_frentes_votacoes": "am_correlacao_frentes_votacoes",
    "am_despesas_ceap": "am_despesas_ceap",
    "am_auditoria_cpis": "am_auditoria_cpis",
    "am_presenca_absenteismo": "am_presenca_absenteismo",
}

# Marts audit columns use the _marts suffix in the current dictionary.
MART_REQUIRED_COLUMNS = [
    "aud_id_execucao_marts",
    "aud_dh_processamento_marts",
    "aud_tx_versao_pipeline_marts",
    "aud_tx_hash_registro_marts",
]

# Expected grain/key columns by analytical mart, aligned with current dictionary.
# The first column in each list is the mart surrogate/business grain used for uniqueness.
MART_KEY_COLUMNS = {
    "am_atlas_frentes": [
        "atl_sk_atlas_frente",
        "frn_sk_frente",
    ],
    "am_calendario_eventos": [
        "cal_sk_calendario_evento",
        "evt_sk_evento",
    ],
    "am_correlacao_frentes_votacoes": [
        "cfv_sk_correlacao_frente_votacao",
        "frn_sk_frente",
        "vot_sk_votacao",
    ],
    "am_despesas_ceap": [
        "dce_sk_despesa_ceap_mart",
        "dce_tx_business_key",
        "dep_sk_deputado",
        "forn_sk_fornecedor",
        "dat_sk_data",
    ],
    "am_auditoria_cpis": [
        "acp_sk_auditoria_cpi",
        "acp_tx_business_key",
        "cpi_sk_cpi",
    ],
    "am_presenca_absenteismo": [
        "pab_sk_presenca_absenteismo",
        "pab_tx_business_key",
        "dep_sk_deputado",
    ],
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
        rule_name="marts_quality_exception",
        rule_description="Captures unexpected errors during Mart quality validation.",
        validation_status=QUALITY_FAILED,
        total_records=1,
        invalid_records=1,
        invalid_percentage=100.0,
        message=f"Unexpected error during Mart quality validation: {str(error)}",
        entity_name=entity_name,
        target_table=target_table,
    )

# COMMAND ----------

def get_marts_table(
    table_name: str,
) -> str:
    """
    Builds a fully qualified Mart table name.
    """

    return f"{CATALOG_NAME}.{SCHEMA_MARTS}.{table_name}"

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

def count_records(
    dataframe: DataFrame,
) -> int:
    """
    Counts records from a Spark DataFrame.
    """

    return dataframe.count()

# COMMAND ----------

def validate_table_exists(
    entity_name: str,
    full_table_name: str,
) -> bool:
    """
    Validates whether a Mart table exists.
    """

    exists = table_exists(full_table_name)

    add_quality_result(
        rule_name="marts_table_exists",
        rule_description="Validates whether the Mart table exists.",
        validation_status=QUALITY_PASSED if exists else QUALITY_FAILED,
        total_records=1,
        invalid_records=0 if exists else 1,
        invalid_percentage=0.0 if exists else 100.0,
        message=(
            "Mart table exists."
            if exists
            else "Mart table does not exist."
        ),
        entity_name=entity_name,
        target_table=full_table_name,
    )

    return exists

# COMMAND ----------

def validate_minimum_records(
    dataframe: DataFrame,
    entity_name: str,
    full_table_name: str,
) -> int:
    """
    Validates whether a Mart table contains at least one record.
    """

    total_records = count_records(dataframe)
    invalid_records = 0 if total_records > 0 else 1

    add_quality_result(
        rule_name="marts_minimum_records",
        rule_description="Validates whether the Mart table contains at least one record.",
        validation_status=QUALITY_PASSED if total_records > 0 else QUALITY_WARNING,
        total_records=total_records,
        invalid_records=invalid_records,
        invalid_percentage=0.0 if total_records > 0 else 100.0,
        message=f"Mart table record count: {total_records}",
        entity_name=entity_name,
        target_table=full_table_name,
    )

    return total_records

# COMMAND ----------

def validate_traceability_columns(
    dataframe: DataFrame,
    entity_name: str,
    full_table_name: str,
) -> None:
    """
    Validates required Mart traceability columns.
    """

    result = validate_required_columns(
        dataframe=dataframe,
        required_columns=MART_REQUIRED_COLUMNS,
    )

    add_quality_result(
        rule_name="marts_required_traceability_columns",
        rule_description="Validates required Mart traceability columns.",
        validation_status=result["status_validacao"],
        total_records=result["total_registros"],
        invalid_records=result["registros_invalidos"],
        invalid_percentage=result["percentual_invalidos"],
        message=result["mensagem"],
        entity_name=entity_name,
        target_table=full_table_name,
    )

# COMMAND ----------

def validate_key_columns(
    dataframe: DataFrame,
    entity_name: str,
    full_table_name: str,
) -> None:
    """
    Validates whether expected Mart grain/key columns exist.
    """

    key_columns = MART_KEY_COLUMNS.get(entity_name, [])

    if not key_columns:
        add_quality_result(
            rule_name="marts_key_mapping",
            rule_description="Validates whether the Mart entity has a configured key mapping.",
            validation_status=QUALITY_WARNING,
            total_records=1,
            invalid_records=0,
            invalid_percentage=0.0,
            message=f"No key mapping configured for entity: {entity_name}",
            entity_name=entity_name,
            target_table=full_table_name,
        )
        return

    result = validate_required_columns(
        dataframe=dataframe,
        required_columns=key_columns,
    )

    add_quality_result(
        rule_name="marts_key_columns",
        rule_description="Validates expected Mart grain/key columns.",
        validation_status=result["status_validacao"],
        total_records=result["total_registros"],
        invalid_records=result["registros_invalidos"],
        invalid_percentage=result["percentual_invalidos"],
        message=result["mensagem"],
        entity_name=entity_name,
        target_table=full_table_name,
    )

# COMMAND ----------

def validate_key_nulls(
    dataframe: DataFrame,
    entity_name: str,
    full_table_name: str,
) -> None:
    """
    Validates null values in Mart grain/key columns.
    """

    key_columns = [
        column
        for column in MART_KEY_COLUMNS.get(entity_name, [])
        if column in dataframe.columns
    ]

    if not key_columns:
        return

    results = validate_nulls(
        dataframe=dataframe,
        columns=key_columns,
    )

    for result in results:
        add_quality_result(
            rule_name=f"marts_{result['nome_regra']}",
            rule_description=result["descricao_regra"],
            validation_status=result["status_validacao"],
            total_records=result["total_registros"],
            invalid_records=result["registros_invalidos"],
            invalid_percentage=result["percentual_invalidos"],
            message=result["mensagem"],
            entity_name=entity_name,
            target_table=full_table_name,
        )

# COMMAND ----------

def validate_key_duplicates(
    dataframe: DataFrame,
    entity_name: str,
    full_table_name: str,
) -> None:
    """
    Validates duplicated records based on configured Mart grain/key columns.
    """

    key_columns = [
        column
        for column in MART_KEY_COLUMNS.get(entity_name, [])
        if column in dataframe.columns
    ]

    if not key_columns:
        return

    result = validate_duplicates(
        dataframe=dataframe,
        key_columns=key_columns,
    )

    add_quality_result(
        rule_name="marts_key_duplicate_check",
        rule_description="Validates duplicated records based on configured Mart grain/key columns.",
        validation_status=result["status_validacao"],
        total_records=result["total_registros"],
        invalid_records=result["registros_invalidos"],
        invalid_percentage=result["percentual_invalidos"],
        message=result["mensagem"],
        entity_name=entity_name,
        target_table=full_table_name,
    )

# COMMAND ----------

def run_entity_checks(
    entity_name: str,
    table_name: str,
) -> None:
    """
    Executes all Mart quality checks for a single entity.
    """

    full_table_name = get_marts_table(table_name)

    print("=" * 90)
    print(f"Running Mart quality checks for: {full_table_name}")
    print("=" * 90)

    try:

        if not validate_table_exists(
            entity_name=entity_name,
            full_table_name=full_table_name,
        ):
            return

        dataframe = get_table_dataframe(full_table_name)

        validate_minimum_records(
            dataframe=dataframe,
            entity_name=entity_name,
            full_table_name=full_table_name,
        )

        validate_traceability_columns(
            dataframe=dataframe,
            entity_name=entity_name,
            full_table_name=full_table_name,
        )

        validate_key_columns(
            dataframe=dataframe,
            entity_name=entity_name,
            full_table_name=full_table_name,
        )

        validate_key_nulls(
            dataframe=dataframe,
            entity_name=entity_name,
            full_table_name=full_table_name,
        )

        validate_key_duplicates(
            dataframe=dataframe,
            entity_name=entity_name,
            full_table_name=full_table_name,
        )

    except Exception as error:

        add_exception_result(
            entity_name=entity_name,
            target_table=full_table_name,
            error=error,
        )

# COMMAND ----------

def build_marts_quality_log() -> DataFrame:
    """
    Builds the final Mart quality log DataFrame.
    """

    if not quality_results:

        add_quality_result(
            rule_name="marts_quality_no_results",
            rule_description="Validates whether Mart quality checks produced results.",
            validation_status=QUALITY_WARNING,
            total_records=0,
            invalid_records=0,
            invalid_percentage=0.0,
            message="No Mart quality results were generated.",
            entity_name="marts",
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
# MAGIC ## 1. Execute Mart Quality Checks

# COMMAND ----------

for entity_name, table_name in MART_ENTITY_TABLES.items():

    run_entity_checks(
        entity_name=entity_name,
        table_name=table_name,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Persist Quality Results

# COMMAND ----------

quality_log_df = build_marts_quality_log()

quality_log_df.write.mode(
    "append"
).saveAsTable(DATA_QUALITY_LOG_TABLE)

print(
    f"Mart quality results persisted into: "
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
print("MARTS QUALITY SUMMARY")
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
        f"Mart quality validation failed with "
        f"{failed_count} failed validation(s)."
    )

if failed_count > 0:

    print(
        f"WARNING: Mart quality validation finished with "
        f"{failed_count} failed validation(s). "
        "This is expected if Mart tables have not been created yet."
    )

print("MARTS QUALITY CHECKS COMPLETED")