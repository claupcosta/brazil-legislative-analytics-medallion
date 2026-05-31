# Databricks notebook source
# MAGIC %md
# MAGIC # Utils — Rejected Records Functions
# MAGIC
# MAGIC **Notebook:** `utils_rejected_records`
# MAGIC
# MAGIC Provides reusable rejected-record handling functions used across
# MAGIC Silver, Gold, Quality and Job notebooks.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Standard rejected record schema helpers
# MAGIC - Mandatory validation rejection builders
# MAGIC - Technical duplicate rejection builders
# MAGIC - Rejected record persistence helpers
# MAGIC - Rejected record cleanup helpers
# MAGIC - Rejected record profiling helpers
# MAGIC - Reusable rejected-record governance utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Centralize rejected-record logic
# MAGIC - Standardize rejected-record metadata across Silver notebooks
# MAGIC - Register records rejected by mandatory validation rules
# MAGIC - Register records discarded by technical deduplication
# MAGIC - Preserve source payloads for traceability
# MAGIC - Support Bronze-to-Silver lineage
# MAGIC - Reduce duplicated rejection logic across notebooks
# MAGIC - Improve auditability and troubleshooting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Rejected records are not necessarily bad source records
# MAGIC - Some records are technically discarded due to deduplication rules
# MAGIC - Mandatory validation rejections should use severity `ERROR`
# MAGIC - Technical deduplication discards should use severity `WARNING`
# MAGIC - Original payloads should be preserved whenever available
# MAGIC - Naming conventions follow Portuguese mnemonic standards
# MAGIC - Comments and documentation are written in English
# MAGIC - This notebook must be executed with `%run` before calling rejected-record utilities
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/decisions/silver_layer_strategy.md`
# MAGIC - `/docs/governance/data_quality.md`
# MAGIC - `/docs/governance/traceability.md`
# MAGIC - `/docs/operations/execution_guide.md`
# MAGIC - `/docs/standards/naming_conventions.md`

# COMMAND ----------

# ==========================================================================================
# Utils — Rejected Records Functions
# Notebook: utils_rejected_records
# ==========================================================================================

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ==========================================================================================
# Spark Session
# ==========================================================================================

spark = SparkSession.getActiveSession()

if spark is None:
    spark = SparkSession.builder.getOrCreate()


# ==========================================================================================
# Rejected Records Configuration
# ==========================================================================================

REJECTION_SEVERITY_ERROR = "ERROR"
REJECTION_SEVERITY_WARNING = "WARNING"

REJECTION_OBSERVATION_MANDATORY_VALIDATION = (
    "Record rejected due to mandatory validation rule."
)

REJECTION_OBSERVATION_TECHNICAL_DUPLICATE = (
    "Record discarded because a newer or preferred record was kept "
    "according to the technical deduplication rule."
)

REJECTED_RECORD_REQUIRED_COLUMNS = [
    "rej_id_registro",
    "aud_id_execucao_silver",
    "aud_dh_processamento",
    "aud_tx_camada_origem",
    "aud_tx_tabela_origem",
    "aud_tx_tabela_destino",
    "aud_tx_versao_pipeline_silver",
    "rej_tx_entidade",
    "rej_tx_id_registro",
    "rej_tx_regra_validacao",
    "rej_tx_motivo_rejeicao",
    "rej_tx_severidade",
    "rej_tx_payload_json",
    "rej_tx_observacao",
]


# ==========================================================================================
# Internal Helpers
# ==========================================================================================

def _get_spark_session() -> SparkSession:
    """
    Returns an active Spark session.
    """

    global spark

    spark_session = SparkSession.getActiveSession()

    if spark_session is None:
        spark_session = SparkSession.builder.getOrCreate()

    spark = spark_session

    return spark_session


def _column_or_null(dataframe: DataFrame, column_name: str):
    """
    Returns an existing column or a NULL string column.
    """

    if column_name in dataframe.columns:
        return F.col(column_name).cast(StringType())

    return F.lit(None).cast(StringType())


def _normalize_rejected_dataframe(dataframe: DataFrame) -> DataFrame:
    """
    Ensures the rejected dataframe contains the required columns.
    """

    normalized_df = dataframe

    for column_name in REJECTED_RECORD_REQUIRED_COLUMNS:
        if column_name not in normalized_df.columns:
            normalized_df = normalized_df.withColumn(
                column_name,
                F.lit(None).cast(StringType()),
            )

    return normalized_df.select(*REJECTED_RECORD_REQUIRED_COLUMNS)


# ==========================================================================================
# Mandatory Validation Rejected Records
# ==========================================================================================

def build_mandatory_rejected_records(
    dataframe: DataFrame,
    execution_id: str,
    source_table: str,
    target_table: str,
    project_version: str,
    entity_name: str,
    record_id_column: str,
    validation_rule_column: str,
    payload_column: Optional[str] = None,
    valid_flag_column: Optional[str] = None,
) -> DataFrame:
    """
    Builds standardized rejected records for mandatory validation failures.
    """

    _get_spark_session()

    source_df = dataframe

    if valid_flag_column and valid_flag_column in source_df.columns:
        source_df = source_df.filter(F.col(valid_flag_column) == False)

    payload_expression = (
        F.col(payload_column).cast(StringType())
        if payload_column and payload_column in source_df.columns
        else F.to_json(F.struct(*[F.col(c) for c in source_df.columns]))
    )

    rejected_df = (
        source_df
        .withColumn("rej_id_registro", F.expr("uuid()"))
        .withColumn("aud_id_execucao_silver", F.lit(execution_id))
        .withColumn("aud_dh_processamento", F.current_timestamp())
        .withColumn("aud_tx_camada_origem", F.lit("silver"))
        .withColumn("aud_tx_tabela_origem", F.lit(source_table))
        .withColumn("aud_tx_tabela_destino", F.lit(target_table))
        .withColumn("aud_tx_versao_pipeline_silver", F.lit(project_version))
        .withColumn("rej_tx_entidade", F.lit(entity_name))
        .withColumn(
            "rej_tx_id_registro",
            _column_or_null(source_df, record_id_column),
        )
        .withColumn(
            "rej_tx_regra_validacao",
            _column_or_null(source_df, validation_rule_column),
        )
        .withColumn(
            "rej_tx_motivo_rejeicao",
            _column_or_null(source_df, validation_rule_column),
        )
        .withColumn("rej_tx_severidade", F.lit(REJECTION_SEVERITY_ERROR))
        .withColumn("rej_tx_payload_json", payload_expression)
        .withColumn(
            "rej_tx_observacao",
            F.lit(REJECTION_OBSERVATION_MANDATORY_VALIDATION),
        )
    )

    return _normalize_rejected_dataframe(rejected_df)


# ==========================================================================================
# Duplicate Rejected Records
# ==========================================================================================

def build_duplicate_rejected_records(
    dataframe: DataFrame,
    execution_id: str,
    source_table: str,
    target_table: str,
    project_version: str,
    entity_name: str,
    record_id_column: str,
    payload_column: Optional[str],
    dedup_rank_column: str,
    duplicate_rule_code: str,
    observation: Optional[str] = None,
) -> DataFrame:
    """
    Builds standardized rejected records for technical duplicate records.
    """

    _get_spark_session()

    if dedup_rank_column not in dataframe.columns:
        raise ValueError(f"Dedup rank column not found: {dedup_rank_column}")

    duplicate_df = dataframe.filter(F.col(dedup_rank_column) > 1)

    payload_expression = (
        F.col(payload_column).cast(StringType())
        if payload_column and payload_column in duplicate_df.columns
        else F.to_json(F.struct(*[F.col(c) for c in duplicate_df.columns]))
    )

    rejected_df = (
        duplicate_df
        .withColumn("rej_id_registro", F.expr("uuid()"))
        .withColumn("aud_id_execucao_silver", F.lit(execution_id))
        .withColumn("aud_dh_processamento", F.current_timestamp())
        .withColumn("aud_tx_camada_origem", F.lit("silver"))
        .withColumn("aud_tx_tabela_origem", F.lit(source_table))
        .withColumn("aud_tx_tabela_destino", F.lit(target_table))
        .withColumn("aud_tx_versao_pipeline_silver", F.lit(project_version))
        .withColumn("rej_tx_entidade", F.lit(entity_name))
        .withColumn(
            "rej_tx_id_registro",
            _column_or_null(duplicate_df, record_id_column),
        )
        .withColumn("rej_tx_regra_validacao", F.lit(duplicate_rule_code))
        .withColumn("rej_tx_motivo_rejeicao", F.lit(duplicate_rule_code))
        .withColumn("rej_tx_severidade", F.lit(REJECTION_SEVERITY_WARNING))
        .withColumn("rej_tx_payload_json", payload_expression)
        .withColumn(
            "rej_tx_observacao",
            F.lit(observation or REJECTION_OBSERVATION_TECHNICAL_DUPLICATE),
        )
    )

    return _normalize_rejected_dataframe(rejected_df)


# ==========================================================================================
# Union Rejected Records
# ==========================================================================================

def union_rejected_records(
    mandatory_rejected_dataframe: Optional[DataFrame] = None,
    duplicate_rejected_dataframe: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Unions mandatory and duplicate rejected records.
    """

    spark_session = _get_spark_session()

    empty_df = spark_session.createDataFrame(
        [],
        schema=",".join([f"{c} string" for c in REJECTED_RECORD_REQUIRED_COLUMNS]),
    )

    result_df = empty_df

    if mandatory_rejected_dataframe is not None:
        result_df = result_df.unionByName(
            _normalize_rejected_dataframe(mandatory_rejected_dataframe),
            allowMissingColumns=True,
        )

    if duplicate_rejected_dataframe is not None:
        result_df = result_df.unionByName(
            _normalize_rejected_dataframe(duplicate_rejected_dataframe),
            allowMissingColumns=True,
        )

    return result_df


# ==========================================================================================
# Clean Rejected Records
# ==========================================================================================

def clean_rejected_records_for_entity(
    rejected_table: str,
    entity_name: str,
    target_table: Optional[str] = None,
) -> None:
    """
    Deletes previous rejected records for the same entity and target table.
    """

    spark_session = _get_spark_session()

    if not rejected_table:
        raise ValueError("Rejected table cannot be empty.")

    if not entity_name:
        raise ValueError("Entity name cannot be empty.")

    if target_table:
        spark_session.sql(f"""
            DELETE FROM {rejected_table}
            WHERE rej_tx_entidade = '{entity_name}'
              AND aud_tx_tabela_destino = '{target_table}'
        """)
    else:
        spark_session.sql(f"""
            DELETE FROM {rejected_table}
            WHERE rej_tx_entidade = '{entity_name}'
        """)


# ==========================================================================================
# Persist Rejected Records
# ==========================================================================================

def persist_rejected_records(
    rejected_dataframe: DataFrame,
    rejected_table: str,
    mode: str = "append",
) -> None:
    """
    Persists rejected records into the rejected records table.
    """

    _get_spark_session()

    if rejected_dataframe is None:
        return

    normalized_df = _normalize_rejected_dataframe(rejected_dataframe)

    if normalized_df.count() == 0:
        return

    (
        normalized_df
        .write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .saveAsTable(rejected_table)
    )


# ==========================================================================================
# Clean and Persist Rejected Records
# ==========================================================================================

def clean_and_persist_rejected_records(
    rejected_dataframe: DataFrame,
    rejected_table: str,
    entity_name: str,
    target_table: Optional[str] = None,
    mode: str = "append",
) -> None:
    """
    Cleans previous rejected records for the entity and persists the current batch.
    """

    _get_spark_session()

    clean_rejected_records_for_entity(
        rejected_table=rejected_table,
        entity_name=entity_name,
        target_table=target_table,
    )

    persist_rejected_records(
        rejected_dataframe=rejected_dataframe,
        rejected_table=rejected_table,
        mode=mode,
    )


# ==========================================================================================
# Summarize Rejected Records
# ==========================================================================================

def summarize_rejected_records(
    rejected_table: str,
    entity_name: Optional[str] = None,
    target_table: Optional[str] = None,
) -> DataFrame:
    """
    Returns rejected record totals grouped by entity, validation rule and severity.
    """

    spark_session = _get_spark_session()

    rejected_df = spark_session.table(rejected_table)

    if entity_name:
        rejected_df = rejected_df.filter(F.col("rej_tx_entidade") == entity_name)

    if target_table:
        rejected_df = rejected_df.filter(
            F.col("aud_tx_tabela_destino") == target_table
        )

    return (
        rejected_df
        .groupBy(
            "rej_tx_entidade",
            "rej_tx_regra_validacao",
            "rej_tx_motivo_rejeicao",
            "rej_tx_severidade",
        )
        .agg(F.count("*").alias("total_registros"))
        .orderBy(F.col("total_registros").desc())
    )


# ==========================================================================================
# Count Rejected Records
# ==========================================================================================

def count_rejected_records(
    rejected_table: str,
    entity_name: Optional[str] = None,
    target_table: Optional[str] = None,
) -> int:
    """
    Counts rejected records for an entity and optionally for a target table.
    """

    spark_session = _get_spark_session()

    rejected_df = spark_session.table(rejected_table)

    if entity_name:
        rejected_df = rejected_df.filter(F.col("rej_tx_entidade") == entity_name)

    if target_table:
        rejected_df = rejected_df.filter(
            F.col("aud_tx_tabela_destino") == target_table
        )

    return rejected_df.count()


print("utils_rejected_records loaded successfully.")
print(
    "Available functions: "
    "build_mandatory_rejected_records, "
    "build_duplicate_rejected_records, "
    "union_rejected_records, "
    "clean_rejected_records_for_entity, "
    "persist_rejected_records, "
    "clean_and_persist_rejected_records, "
    "summarize_rejected_records, "
    "count_rejected_records"
)