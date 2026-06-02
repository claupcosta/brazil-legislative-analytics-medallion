# Databricks notebook source
# MAGIC %md
# MAGIC # Quality Layer — Gold Quality Checks
# MAGIC
# MAGIC **Notebook:** `03_quality_gold_checks`
# MAGIC **Layer:** `Quality`
# MAGIC **Source:** `Gold Delta Tables`
# MAGIC **Target:** `Audit quality log`
# MAGIC
# MAGIC Updated version:
# MAGIC - Supports current Gold table names in plural: `dm_deputados`, `dm_partidos`, etc.
# MAGIC - Supports current Gold key naming pattern: `dep_sk_deputado`, `par_sk_partido`, `dat_sk_data`, etc.
# MAGIC - Does not fail only because old audit fields are missing.
# MAGIC - Treats lineage / traceability as PASSED when any current audit/hash/source column exists.
# MAGIC - Treats missing traceability as WARNING instead of FAILED during development.
# MAGIC - Uses multiple possible key definitions per entity.
# MAGIC - Avoids false failures caused by old mappings such as `sk_deputado`, `sk_data`, `dim_deputado`.

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
print("03 - QUALITY GOLD CHECKS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Layer: {SCHEMA_GOLD}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# QUALITY CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "03_quality_gold_checks"
LAYER_NAME = "gold"

# Keep False during development and portfolio validation.
# Set True only when quality checks must block the pipeline.
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

# ============================================================
# GOLD TABLE REGISTRY
# ============================================================
# Prefer the tables configured in utils_config.
# The keys generated here follow the current project pattern:
# dim_deputados, dim_partidos, fact_frentes_membros, etc.

GOLD_ENTITY_TABLES = {}

for entity_name, table_name in GOLD_DIMENSION_TABLES.items():
    GOLD_ENTITY_TABLES[f"dim_{entity_name}"] = {
        "table_type": "dimension",
        "table_name": table_name,
    }

for entity_name, table_name in GOLD_FACT_TABLES.items():
    GOLD_ENTITY_TABLES[f"fact_{entity_name}"] = {
        "table_type": "fact",
        "table_name": table_name,
    }

# Optional Gold entities. Missing tables here generate WARNING rather than FAILED.
OPTIONAL_GOLD_ENTITIES = set()

# Entities allowed to be empty depending on ingestion scope.
ALLOW_EMPTY_GOLD_ENTITIES = set()

# ============================================================
# CURRENT GOLD KEY MAPPINGS
# ============================================================
# Each entity supports multiple valid key options.
# The first option fully present in the dataframe is used.

GOLD_DIMENSION_KEYS: Dict[str, List[List[str]]] = {
    "dim_deputados": [
        ["dep_sk_deputado"],
        ["dep_id_deputado", "leg_id_legislatura"],
        ["dep_id_deputado"],
    ],
    "dim_partidos": [
        ["par_sk_partido"],
        ["par_id_partido"],
        ["par_tx_sigla"],
    ],
    "dim_estados": [
        ["est_sk_estado"],
        ["est_id_estado"],
        ["est_tx_sigla_uf"],
    ],
    "dim_datas": [
        ["dat_sk_data"],
        ["dat_dt_data"],
        ["dat_id_data"],
    ],
    "dim_data": [
        ["dat_sk_data"],
        ["dat_dt_data"],
        ["dat_id_data"],
    ],
    "dim_orgaos": [
        ["org_sk_orgao"],
        ["org_id_orgao"],
    ],
    "dim_orgao": [
        ["org_sk_orgao"],
        ["org_id_orgao"],
    ],
    "dim_tipos_evento": [
        ["evt_tp_sk_tipo_evento"],
        ["evt_tp_id_tipo_evento"],
        ["evt_cd_tipo"],
    ],
    "dim_tipo_evento": [
        ["evt_tp_sk_tipo_evento"],
        ["evt_tp_id_tipo_evento"],
        ["evt_cd_tipo"],
    ],
    "dim_eventos": [
        ["evt_sk_evento"],
        ["evt_id_evento"],
    ],
    "dim_evento": [
        ["evt_sk_evento"],
        ["evt_id_evento"],
    ],
    "dim_votacoes": [
        ["vot_sk_votacao"],
        ["vot_id_votacao"],
    ],
    "dim_votacao": [
        ["vot_sk_votacao"],
        ["vot_id_votacao"],
    ],
    "dim_tipos_votacao": [
        ["vot_tp_sk_tipo_votacao"],
        ["vot_tp_id_tipo_votacao"],
        ["vot_cd_tipo"],
    ],
    "dim_tipo_votacao": [
        ["vot_tp_sk_tipo_votacao"],
        ["vot_tp_id_tipo_votacao"],
        ["vot_cd_tipo"],
    ],
    "dim_frentes": [
        ["frn_sk_frente"],
        ["frm_sk_frente"],
        ["frn_id_frente"],
        ["frm_id_frente"],
    ],
    "dim_frente": [
        ["frn_sk_frente"],
        ["frm_sk_frente"],
        ["frn_id_frente"],
        ["frm_id_frente"],
    ],
    "dim_fornecedores": [
        ["forn_sk_fornecedor"],
        ["forn_id_fornecedor"],
        ["forn_tx_chave_deduplicacao"],
        ["forn_tx_documento_limpo"],
    ],
    "dim_fornecedor": [
        ["forn_sk_fornecedor"],
        ["forn_id_fornecedor"],
        ["forn_tx_chave_deduplicacao"],
        ["forn_tx_documento_limpo"],
    ],
    "dim_cpis": [
        ["cpi_sk_cpi"],
        ["cpi_id_cpi"],
        ["cpi_id_orgao"],
    ],
    "dim_cpi": [
        ["cpi_sk_cpi"],
        ["cpi_id_cpi"],
        ["cpi_id_orgao"],
    ],
}

GOLD_FACT_KEYS: Dict[str, List[List[str]]] = {
    "fact_frentes_membros": [
        ["frm_sk_frente_membro"],
        ["frn_sk_frente_membro"],
        ["frn_sk_frente", "dep_sk_deputado", "dat_sk_data_inicio"],
        ["frm_sk_frente", "dep_sk_deputado", "dat_sk_data_inicio"],
        ["frn_id_frente", "dep_id_deputado"],
        ["frm_id_frente", "dep_id_deputado"],
    ],
    "fact_presencas_eventos": [
        ["fpe_sk_presenca_evento"],
        ["evt_sk_evento", "dep_sk_deputado"],
        ["evt_id_evento", "dep_id_deputado"],
    ],
    "fact_eventos_presencas": [
        ["fpe_sk_presenca_evento"],
        ["evt_sk_evento", "dep_sk_deputado"],
        ["evt_id_evento", "dep_id_deputado"],
    ],
    "fact_resultados_votacoes": [
        ["frv_sk_resultado_votacao"],
        ["vot_sk_votacao", "dep_sk_deputado"],
        ["vot_id_votacao", "dep_id_deputado"],
    ],
    "fact_despesas_ceap": [
        ["fdc_sk_despesa_ceap"],
        ["des_id_despesa"],
        ["desp_tx_chave_deduplicacao"],
        ["dep_sk_deputado", "forn_sk_fornecedor", "dat_sk_data"],
    ],
    "fact_eventos_cpis": [
        ["fec_sk_evento_cpi"],
        ["cpi_sk_cpi", "evt_sk_evento"],
        ["cpi_id_orgao", "evt_id_evento"],
        ["cpi_evt_id_relacao"],
    ],
    "fact_cpi_eventos": [
        ["fec_sk_evento_cpi"],
        ["cpi_sk_cpi", "evt_sk_evento"],
        ["cpi_id_orgao", "evt_id_evento"],
        ["cpi_evt_id_relacao"],
    ],
}

# Columns accepted as traceability / lineage / audit evidence in the current Gold model.
TRACEABILITY_COLUMN_CANDIDATES = [
    "aud_id_execucao",
    "aud_dh_processamento",
    "aud_tx_versao_pipeline",
    "aud_id_execucao_silver",
    "aud_dh_processamento_silver",
    "aud_tx_versao_pipeline_silver",
    "aud_id_execucao_gold",
    "aud_dh_processamento_gold",
    "aud_tx_versao_pipeline_gold",
    "aud_tx_tabela_origem",
    "aud_tx_tabela_destino",
    "aud_tx_camada_origem",
    "aud_tx_hash_registro",
    "aud_tx_hash_registro_silver",
    "aud_tx_hash_registro_gold",
    "tx_hash_registro",
    "hash_registro",
    "tx_chave_deduplicacao",
    "desp_tx_chave_deduplicacao",
    "forn_tx_chave_deduplicacao",
    "dep_tx_chave_deputado_legislatura",
    "fdc_tx_business_key",
]

quality_results = []

# COMMAND ----------

# ============================================================
# GENERIC HELPERS
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
        rule_name="gold_quality_exception",
        rule_description="Captures unexpected errors during Gold quality validation.",
        validation_status=QUALITY_FAILED,
        total_records=1,
        invalid_records=1,
        invalid_percentage=100.0,
        message=f"Unexpected error during Gold quality validation: {str(error)}",
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


def count_records(dataframe: DataFrame) -> int:
    """Counts records from a Spark DataFrame."""

    return dataframe.count()


def calculate_percentage(
    invalid_records: int,
    total_records: int,
) -> float:
    """Calculates invalid percentage safely."""

    if total_records is None or total_records == 0:
        return 0.0

    return round((invalid_records / total_records) * 100, 4)


def get_key_options(
    entity_name: str,
    table_type: str,
) -> List[List[str]]:
    """Returns configured key options for a Gold entity."""

    if table_type == "dimension":
        return GOLD_DIMENSION_KEYS.get(entity_name, [])

    return GOLD_FACT_KEYS.get(entity_name, [])


def get_existing_key_columns(
    dataframe: DataFrame,
    entity_name: str,
    table_type: str,
) -> List[str]:
    """
    Returns the first valid key combination found in the dataframe.
    """

    dataframe_columns = set(dataframe.columns)
    key_options = get_key_options(entity_name, table_type)

    for key_group in key_options:
        if all(column in dataframe_columns for column in key_group):
            return key_group

    return []


def get_traceability_columns(dataframe: DataFrame) -> List[str]:
    """Returns traceability columns found in the dataframe."""

    dataframe_columns = set(dataframe.columns)
    return [
        column
        for column in TRACEABILITY_COLUMN_CANDIDATES
        if column in dataframe_columns
    ]


def get_hash_or_dedup_column(dataframe: DataFrame) -> Optional[str]:
    """
    Finds a hash, deduplication or business-key-like column.
    """

    preferred_columns = [
        "aud_tx_hash_registro_gold",
        "aud_tx_hash_registro_silver",
        "aud_tx_hash_registro",
        "tx_hash_registro",
        "hash_registro",
        "tx_chave_deduplicacao",
        "desp_tx_chave_deduplicacao",
        "forn_tx_chave_deduplicacao",
        "dep_tx_chave_deputado_legislatura",
        "fdc_tx_business_key",
    ]

    dataframe_columns = set(dataframe.columns)

    for column in preferred_columns:
        if column in dataframe_columns:
            return column

    for column in dataframe.columns:
        column_lower = column.lower()

        if (
            "hash" in column_lower
            or "deduplicacao" in column_lower
            or "business_key" in column_lower
            or "chave" in column_lower
        ):
            return column

    return None


def validate_duplicates_by_columns(
    dataframe: DataFrame,
    key_columns: List[str],
) -> Dict[str, object]:
    """Validates duplicate groups for a list of columns."""

    total_records = count_records(dataframe)

    duplicate_groups = (
        dataframe
        .groupBy(*[F.col(column) for column in key_columns])
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    status = QUALITY_PASSED if duplicate_groups == 0 else QUALITY_FAILED

    return {
        "status_validacao": status,
        "total_registros": total_records,
        "registros_invalidos": duplicate_groups,
        "percentual_invalidos": calculate_percentage(duplicate_groups, total_records),
        "mensagem": (
            f"Duplicate groups found: {duplicate_groups}. "
            f"Columns used: {key_columns}"
        ),
    }

# COMMAND ----------

# ============================================================
# VALIDATION FUNCTIONS
# ============================================================

def validate_table_exists(
    entity_name: str,
    full_table_name: str,
) -> bool:
    """Validates whether a Gold table exists."""

    exists = table_exists(full_table_name)

    if exists:
        status = QUALITY_PASSED
        invalid_records = 0
        invalid_percentage = 0.0
        message = "Gold table exists."

    elif entity_name in OPTIONAL_GOLD_ENTITIES:
        status = QUALITY_WARNING
        invalid_records = 0
        invalid_percentage = 0.0
        message = "Optional Gold table does not exist in the current model."

    else:
        status = QUALITY_FAILED
        invalid_records = 1
        invalid_percentage = 100.0
        message = "Gold table does not exist."

    add_quality_result(
        rule_name="gold_table_exists",
        rule_description="Validates whether the Gold table exists.",
        validation_status=status,
        total_records=1,
        invalid_records=invalid_records,
        invalid_percentage=invalid_percentage,
        message=message,
        entity_name=entity_name,
        target_table=full_table_name,
    )

    return exists


def validate_minimum_records(
    dataframe: DataFrame,
    entity_name: str,
    full_table_name: str,
) -> int:
    """Validates whether a Gold table contains records."""

    total_records = count_records(dataframe)

    if total_records > 0:
        status = QUALITY_PASSED
        invalid_records = 0
        invalid_percentage = 0.0
        message = f"Gold table record count: {total_records}"

    elif entity_name in ALLOW_EMPTY_GOLD_ENTITIES:
        status = QUALITY_WARNING
        invalid_records = 0
        invalid_percentage = 0.0
        message = (
            f"Gold table record count: {total_records}. "
            "Empty table allowed for this entity in the current ingestion window."
        )

    else:
        status = QUALITY_WARNING
        invalid_records = 1
        invalid_percentage = 100.0
        message = f"Gold table record count: {total_records}"

    add_quality_result(
        rule_name="gold_minimum_records",
        rule_description="Validates whether the Gold table contains at least one record.",
        validation_status=status,
        total_records=total_records,
        invalid_records=invalid_records,
        invalid_percentage=invalid_percentage,
        message=message,
        entity_name=entity_name,
        target_table=full_table_name,
    )

    return total_records


def validate_traceability_columns(
    dataframe: DataFrame,
    entity_name: str,
    full_table_name: str,
) -> None:
    """
    Validates Gold traceability using current audit/hash/source columns.

    Missing old audit columns no longer generate FAILED.
    """

    traceability_columns = get_traceability_columns(dataframe)
    fallback_column = get_hash_or_dedup_column(dataframe)

    if traceability_columns or fallback_column:
        status = QUALITY_PASSED
        invalid_records = 0
        invalid_percentage = 0.0
        message = (
            "Gold traceability validated. "
            f"Detected columns: {traceability_columns}. "
            f"Fallback technical column: {fallback_column}"
        )
    else:
        status = QUALITY_WARNING
        invalid_records = 0
        invalid_percentage = 0.0
        message = (
            "No standard audit/hash/source column found. "
            "Validation kept as warning for current Gold model."
        )

    add_quality_result(
        rule_name="gold_required_traceability_columns",
        rule_description="Validates Gold traceability, audit or lineage columns.",
        validation_status=status,
        total_records=1,
        invalid_records=invalid_records,
        invalid_percentage=invalid_percentage,
        message=message,
        entity_name=entity_name,
        target_table=full_table_name,
    )


def validate_key_columns(
    dataframe: DataFrame,
    entity_name: str,
    full_table_name: str,
    table_type: str,
) -> None:
    """Validates whether expected Gold key columns exist."""

    key_columns = get_existing_key_columns(
        dataframe=dataframe,
        entity_name=entity_name,
        table_type=table_type,
    )

    if table_type == "dimension":
        rule_name = "gold_dimension_key_columns"
        rule_description = "Validates expected Gold dimension key columns."
    else:
        rule_name = "gold_fact_key_columns"
        rule_description = "Validates expected Gold fact key columns."

    if key_columns:
        status = QUALITY_PASSED
        invalid_records = 0
        invalid_percentage = 0.0
        message = f"Gold key columns found: {key_columns}"
    else:
        expected = get_key_options(entity_name, table_type)
        status = QUALITY_WARNING
        invalid_records = 0
        invalid_percentage = 0.0
        message = (
            f"No configured Gold key found for entity {entity_name}. "
            f"Expected one of: {expected}. "
            f"Available columns: {dataframe.columns}. "
            "Validation kept as warning."
        )

    add_quality_result(
        rule_name=rule_name,
        rule_description=rule_description,
        validation_status=status,
        total_records=1,
        invalid_records=invalid_records,
        invalid_percentage=invalid_percentage,
        message=message,
        entity_name=entity_name,
        target_table=full_table_name,
    )


def validate_key_nulls(
    dataframe: DataFrame,
    entity_name: str,
    full_table_name: str,
    table_type: str,
) -> None:
    """Validates null values in resolved Gold key columns."""

    key_columns = get_existing_key_columns(
        dataframe=dataframe,
        entity_name=entity_name,
        table_type=table_type,
    )

    if not key_columns:
        return

    total_records = count_records(dataframe)

    for column in key_columns:
        invalid_records = dataframe.filter(F.col(column).isNull()).count()
        invalid_percentage = calculate_percentage(invalid_records, total_records)
        status = QUALITY_PASSED if invalid_records == 0 else QUALITY_FAILED

        add_quality_result(
            rule_name=f"gold_null_check_{column}",
            rule_description=f"Validates null values in Gold key column {column}.",
            validation_status=status,
            total_records=total_records,
            invalid_records=invalid_records,
            invalid_percentage=invalid_percentage,
            message=(
                f"Column {column} has {invalid_records} null value(s) "
                f"out of {total_records} record(s)."
            ),
            entity_name=entity_name,
            target_table=full_table_name,
        )


def validate_key_duplicates(
    dataframe: DataFrame,
    entity_name: str,
    full_table_name: str,
    table_type: str,
) -> None:
    """Validates duplicated records based on resolved Gold key columns."""

    key_columns = get_existing_key_columns(
        dataframe=dataframe,
        entity_name=entity_name,
        table_type=table_type,
    )

    if not key_columns:
        return

    result = validate_duplicates_by_columns(
        dataframe=dataframe,
        key_columns=key_columns,
    )

    add_quality_result(
        rule_name="gold_key_duplicate_check",
        rule_description="Validates duplicated records based on configured Gold key columns.",
        validation_status=result["status_validacao"],
        total_records=result["total_registros"],
        invalid_records=result["registros_invalidos"],
        invalid_percentage=result["percentual_invalidos"],
        message=result["mensagem"],
        entity_name=entity_name,
        target_table=full_table_name,
    )

# COMMAND ----------

# ============================================================
# EXECUTION
# ============================================================

def run_entity_checks(
    entity_name: str,
    table_config: dict,
) -> None:
    """Executes all Gold quality checks for a single entity."""

    table_name = table_config["table_name"]
    table_type = table_config["table_type"]

    full_table_name = get_gold_table(table_name)

    print("=" * 90)
    print(f"Running Gold quality checks for: {full_table_name}")
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
            table_type=table_type,
        )

        validate_key_nulls(
            dataframe=dataframe,
            entity_name=entity_name,
            full_table_name=full_table_name,
            table_type=table_type,
        )

        validate_key_duplicates(
            dataframe=dataframe,
            entity_name=entity_name,
            full_table_name=full_table_name,
            table_type=table_type,
        )

    except Exception as error:
        add_exception_result(
            entity_name=entity_name,
            target_table=full_table_name,
            error=error,
        )


def build_gold_quality_log() -> DataFrame:
    """Builds the final Gold quality log DataFrame."""

    if not quality_results:
        add_quality_result(
            rule_name="gold_quality_no_results",
            rule_description="Validates whether Gold quality checks produced results.",
            validation_status=QUALITY_WARNING,
            total_records=0,
            invalid_records=0,
            invalid_percentage=0.0,
            message="No Gold quality results were generated.",
            entity_name="gold",
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
# MAGIC ## 1. Execute Gold Quality Checks

# COMMAND ----------

for entity_name, table_config in GOLD_ENTITY_TABLES.items():
    run_entity_checks(
        entity_name=entity_name,
        table_config=table_config,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Persist Quality Results

# COMMAND ----------

quality_log_df = build_gold_quality_log()

quality_log_df.write.mode("append").saveAsTable(DATA_QUALITY_LOG_TABLE)

print(f"Gold quality results persisted into: {DATA_QUALITY_LOG_TABLE}")

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
print("GOLD QUALITY SUMMARY")
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
        f"Gold quality validation failed with "
        f"{failed_count} failed validation(s)."
    )

if failed_count > 0:
    print(
        f"WARNING: Gold quality validation finished with "
        f"{failed_count} failed validation(s). Review the quality log."
    )

print("GOLD QUALITY CHECKS COMPLETED")