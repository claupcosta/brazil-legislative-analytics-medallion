# Databricks notebook source
# MAGIC %md
# MAGIC # 17 Silver — Temas Standardization
# MAGIC
# MAGIC **Notebook:** `17_silver_temas`
# MAGIC
# MAGIC Standardizes legislative themes identified in propositions and persists a curated theme dimension into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Legislative theme reference standardization
# MAGIC - Theme name normalization
# MAGIC - Theme dimension creation
# MAGIC - Deterministic theme identifier generation
# MAGIC - Theme quality validation rules
# MAGIC - Technical duplicate prevention
# MAGIC - Traceability and lineage preservation
# MAGIC - Rejected record registration using global utilities
# MAGIC - Silver Delta persistence logic
# MAGIC - Governance comments using global utilities
# MAGIC
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Build the legislative theme dimension
# MAGIC - Standardize theme identifiers
# MAGIC - Standardize theme names
# MAGIC - Preserve original theme references
# MAGIC - Generate deterministic identifiers when source identifiers are unavailable
# MAGIC - Validate mandatory dimension attributes
# MAGIC - Eliminate technical duplicates
# MAGIC - Preserve execution lineage metadata
# MAGIC - Register rejected records for traceability
# MAGIC - Persist curated Silver dimension table
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - This notebook is a reference dimension builder
# MAGIC - Records are derived from proposition thematic classifications
# MAGIC - One record is generated for each unique legislative theme
# MAGIC - The resulting grain is one row per legislative theme
# MAGIC - Theme identifiers are deterministic
# MAGIC - Original theme identifiers are preserved whenever available
# MAGIC - Theme names are standardized before persistence
# MAGIC - Silver validation rules guarantee dimension consistency
# MAGIC - Rejected records are redirected to `slv_registros_rejeitados`
# MAGIC - Global utility notebooks are used to reduce duplicated logic
# MAGIC - Documentation and governance comments are written in English
# MAGIC - Naming conventions follow Portuguese mnemonic standards
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/decisions/silver_layer_strategy.md`
# MAGIC - `/docs/governance/data_quality.md`
# MAGIC - `/docs/governance/traceability.md`
# MAGIC - `/docs/operations/execution_guide.md`
# MAGIC - `/docs/standards/naming_conventions.md`
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_legislature

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_text

# COMMAND ----------

# MAGIC %run ../99_utils/utils_comments

# COMMAND ----------

# MAGIC %run ../99_utils/utils_rejected_records

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------


from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    when,
    coalesce,
    concat_ws,
    current_timestamp,
    regexp_replace,
    regexp_extract,
    get_json_object,
    row_number,
    sha2,
)
from pyspark.sql.types import StringType, BooleanType
from pyspark.sql.window import Window

# ==========================================================================================
# Initialize Spark Session
# ==========================================================================================

spark = SparkSession.getActiveSession()

if spark is None:
    spark = SparkSession.builder.getOrCreate()

globals()["spark"] = spark

write_pipeline_log.__globals__["spark"] = spark

clean_rejected_records_for_entity.__globals__["spark"] = spark
persist_rejected_records.__globals__["spark"] = spark
clean_and_persist_rejected_records.__globals__["spark"] = spark
build_mandatory_rejected_records.__globals__["spark"] = spark
build_duplicate_rejected_records.__globals__["spark"] = spark
union_rejected_records.__globals__["spark"] = spark

apply_table_comment.__globals__["spark"] = spark
apply_column_comment.__globals__["spark"] = spark
apply_column_comments.__globals__["spark"] = spark
apply_governance_comments.__globals__["spark"] = spark

# ==========================================================================================
# Execution Header
# ==========================================================================================

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("17 - SILVER TEMAS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# ==========================================================================================
# 1. Global Configuration
# ==========================================================================================

NOTEBOOK_NAME = "17_silver_temas"
LAYER_NAME = "silver"
ENTITY_NAME = "temas"

SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES.get("proposicoes", "br_proposicoes")
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES.get("temas", "slv_temas")
)

REJECTED_TABLE = get_silver_table(
    SILVER_TABLES["registros_rejeitados"]
)

LOAD_TYPE = LOAD_TYPE_FULL

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

APPLY_GOVERNANCE_COMMENTS = True

records_read = 0
records_written = 0
records_rejected = 0

# ==========================================================================================
# 2. Helper Functions
# ==========================================================================================

def column_exists(dataframe, column_name: str) -> bool:
    return column_name in dataframe.columns


def safe_col(dataframe, column_name: str, default_value=None):
    if column_exists(dataframe, column_name):
        return col(column_name)

    return lit(default_value)


def clean_string(column_expression):
    return trim(
        regexp_replace(
            column_expression.cast("string"),
            r"\s+",
            " ",
        )
    )


def clean_upper(column_expression):
    return upper(clean_string(column_expression))


def clean_identifier_from_text(column_expression):
    return regexp_replace(
        regexp_replace(
            upper(clean_string(column_expression)),
            r"[^A-Z0-9]+",
            "_",
        ),
        r"(^_+|_+$)",
        "",
    )


def get_first_available_json_payload(dataframe):
    payload_candidates = [
        "prp_tx_payload_json",
        "prop_tx_payload_json",
        "pro_tx_payload_json",
        "tem_tx_payload_json",
    ]

    for column_name in payload_candidates:
        if column_exists(dataframe, column_name):
            return col(column_name).cast(StringType())

    return lit(None).cast(StringType())

# ==========================================================================================
# 3. Start Pipeline Log
# ==========================================================================================

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver temas derivation started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver temas derivation from proposicoes source.",
)

# ==========================================================================================
# 4. Read Bronze Proposicoes Source
# ==========================================================================================

try:

    source_df = spark.table(SOURCE_TABLE)

    records_read = source_df.count()

    log_info(
        pipeline_logger=logger,
        message=(
            "Bronze proposicoes table loaded successfully "
            f"| records_read={records_read}"
        ),
    )

except Exception as error:

    finished_at = datetime.now()
    duration_seconds = (finished_at - started_at).total_seconds()

    write_pipeline_log(
        log_id=str(uuid.uuid4()),
        execution_id=execution_id,
        notebook_name=NOTEBOOK_NAME,
        layer_name=LAYER_NAME,
        entity_name=ENTITY_NAME,
        target_table=TARGET_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=f"Failed reading Bronze proposicoes table | error={str(error)}",
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=None,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed reading Bronze proposicoes table.",
        error=error,
    )

    raise error

# ==========================================================================================
# 5. Derive Theme Candidates
# ==========================================================================================

payload_column = get_first_available_json_payload(source_df)

temas_candidate_df = (
    source_df
    .select(
        coalesce(
            safe_col(source_df, "tem_id_tema").cast(StringType()),
            safe_col(source_df, "prp_id_tema").cast(StringType()),
            safe_col(source_df, "prop_id_tema").cast(StringType()),
            get_json_object(payload_column, "$.idTema"),
            get_json_object(payload_column, "$.codTema"),
            get_json_object(payload_column, "$.tema.id"),
            get_json_object(payload_column, "$.tema.codTema"),
            lit(None).cast(StringType()),
        ).alias("tem_id_tema_raw"),

        coalesce(
            safe_col(source_df, "tem_tx_nome").cast(StringType()),
            safe_col(source_df, "tem_tx_tema").cast(StringType()),
            safe_col(source_df, "prp_tx_tema").cast(StringType()),
            safe_col(source_df, "prp_tx_temas").cast(StringType()),
            safe_col(source_df, "prop_tx_tema").cast(StringType()),
            safe_col(source_df, "prop_tx_temas").cast(StringType()),
            get_json_object(payload_column, "$.tema"),
            get_json_object(payload_column, "$.temas"),
            get_json_object(payload_column, "$.nomeTema"),
            get_json_object(payload_column, "$.descricaoTema"),
            get_json_object(payload_column, "$.tema.nome"),
            get_json_object(payload_column, "$.tema.descricao"),
            lit(None).cast(StringType()),
        ).alias("tem_tx_nome_raw"),

        coalesce(
            safe_col(source_df, "prp_id_proposicao").cast(StringType()),
            safe_col(source_df, "prop_id_proposicao").cast(StringType()),
            safe_col(source_df, "id").cast(StringType()),
            get_json_object(payload_column, "$.id"),
            get_json_object(payload_column, "$.idProposicao"),
            lit(None).cast(StringType()),
        ).alias("prp_id_proposicao_referencia"),

        payload_column.alias("tem_tx_payload_json"),

        safe_col(source_df, "aud_id_execucao").cast(StringType()).alias("aud_id_execucao_bronze"),
        safe_col(source_df, "aud_dh_ingestao").alias("aud_dh_ingestao_bronze"),
        safe_col(source_df, "aud_tx_endpoint_origem").cast(StringType()).alias("aud_tx_endpoint_origem_bronze"),
        safe_col(source_df, "aud_tx_sistema_origem").cast(StringType()).alias("aud_tx_sistema_origem_bronze"),
        safe_col(source_df, "aud_tx_versao_pipeline").cast(StringType()).alias("aud_tx_versao_pipeline_bronze"),
        safe_col(source_df, "aud_tx_tipo_carga").cast(StringType()).alias("aud_tx_tipo_carga_bronze"),
        safe_col(source_df, "aud_tx_hash_registro").cast(StringType()).alias("aud_tx_hash_registro_bronze"),
    )
)

# ==========================================================================================
# 6. Normalize Theme Attributes
# ==========================================================================================

temas_base_df = (
    temas_candidate_df
    .withColumn(
        "tem_tx_nome",
        clean_upper(col("tem_tx_nome_raw")),
    )
    .withColumn(
        "tem_id_tema",
        when(
            col("tem_id_tema_raw").isNotNull()
            & (clean_string(col("tem_id_tema_raw")) != ""),
            concat_ws("_", lit("TEM"), clean_identifier_from_text(col("tem_id_tema_raw"))),
        )
        .when(
            col("tem_tx_nome").isNotNull()
            & (col("tem_tx_nome") != ""),
            concat_ws(
                "_",
                lit("TEM"),
                regexp_extract(
                    sha2(col("tem_tx_nome"), 256),
                    r"^(.{16})",
                    1,
                ),
            ),
        )
        .otherwise(lit(None).cast(StringType())),
    )
    .withColumn(
        "tem_tx_descricao",
        col("tem_tx_nome"),
    )
    .withColumn(
        "tem_tx_origem_registro",
        lit("derived_from_bronze_proposicoes"),
    )
    .withColumn(
        "tem_fl_registro_derivado",
        lit(True).cast(BooleanType()),
    )
    .withColumn(
        "tem_fl_id_original_informado",
        (
            col("tem_id_tema_raw").isNotNull()
            & (clean_string(col("tem_id_tema_raw")) != "")
        ).cast(BooleanType()),
    )
    .drop(
        "tem_id_tema_raw",
        "tem_tx_nome_raw",
    )
)

# ==========================================================================================
# 7. Apply Theme Quality Rules
# ==========================================================================================

temas_quality_df = (
    temas_base_df
    .withColumn(
        "tem_fl_id_valido",
        (
            col("tem_id_tema").isNotNull()
            & (col("tem_id_tema") != "")
        ).cast(BooleanType()),
    )
    .withColumn(
        "tem_fl_nome_valido",
        (
            col("tem_tx_nome").isNotNull()
            & (col("tem_tx_nome") != "")
        ).cast(BooleanType()),
    )
    .withColumn(
        "tem_fl_registro_valido_silver",
        (
            col("tem_fl_id_valido")
            & col("tem_fl_nome_valido")
        ).cast(BooleanType()),
    )
    .withColumn(
        "tem_tx_motivo_rejeicao",
        when(
            ~col("tem_fl_nome_valido"),
            lit("TEM_NOME_NULO_OU_VAZIO"),
        )
        .when(
            ~col("tem_fl_id_valido"),
            lit("TEM_ID_NULO_OU_VAZIO"),
        )
        .otherwise(lit(None).cast(StringType())),
    )
)

# ==========================================================================================
# 8. Build Mandatory Rejected Records
# ==========================================================================================

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=temas_quality_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="tem_id_tema",
    validation_rule_column="tem_tx_motivo_rejeicao",
    payload_column="tem_tx_payload_json",
    valid_flag_column="tem_fl_registro_valido_silver",
)

# ==========================================================================================
# 9. Keep Valid Records
# ==========================================================================================

valid_df = (
    temas_quality_df
    .filter(col("tem_fl_registro_valido_silver") == True)
    .drop("tem_tx_motivo_rejeicao")
)

# ==========================================================================================
# 10. Identify Technical Duplicates
# ==========================================================================================

dedup_window = (
    Window
    .partitionBy("tem_id_tema")
    .orderBy(
        col("tem_fl_id_original_informado").desc_nulls_last(),
        col("aud_dh_ingestao_bronze").desc_nulls_last(),
        col("aud_tx_hash_registro_bronze").asc_nulls_last(),
    )
)

ranked_df = (
    valid_df
    .withColumn(
        "rn_deduplicacao",
        row_number().over(dedup_window),
    )
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=ranked_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="tem_id_tema",
    payload_column="tem_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="TEM_REGISTRO_DUPLICADO_TECNICO",
    observation=(
        "Theme record kept only once by deterministic theme identifier. "
        "When original theme identifier is unavailable, identifier is derived from the standardized theme name."
    ),
)

temas_dedup_df = (
    ranked_df
    .filter(col("rn_deduplicacao") == 1)
    .drop("rn_deduplicacao")
)

# ==========================================================================================
# 11. Persist Rejected and Discarded Records
# ==========================================================================================

rejected_df = union_rejected_records(
    mandatory_rejected_dataframe=mandatory_rejected_df,
    duplicate_rejected_dataframe=duplicate_rejected_df,
)

records_rejected = rejected_df.count()

try:

    clean_and_persist_rejected_records(
        rejected_dataframe=rejected_df,
        rejected_table=REJECTED_TABLE,
        entity_name=ENTITY_NAME,
        target_table=TARGET_TABLE,
        mode="append",
    )

    log_info(
        pipeline_logger=logger,
        message=(
            "Rejected and discarded theme records persisted "
            f"| records_rejected={records_rejected}"
        ),
    )

except Exception as error:

    finished_at = datetime.now()
    duration_seconds = (finished_at - started_at).total_seconds()

    write_pipeline_log(
        log_id=str(uuid.uuid4()),
        execution_id=execution_id,
        notebook_name=NOTEBOOK_NAME,
        layer_name=LAYER_NAME,
        entity_name=ENTITY_NAME,
        target_table=REJECTED_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=f"Failed writing rejected theme records | error={str(error)}",
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=records_read,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed writing rejected theme records.",
        error=error,
    )

    raise error

# ==========================================================================================
# 12. Add Silver Traceability Columns
# ==========================================================================================

silver_df = (
    temas_dedup_df
    .withColumn("aud_id_execucao_silver", lit(execution_id))
    .withColumn("aud_dh_processamento", current_timestamp())
    .withColumn("aud_tx_camada_origem", lit("bronze"))
    .withColumn("aud_tx_tabela_origem", lit(SOURCE_TABLE))
    .withColumn("aud_tx_tabela_destino", lit(TARGET_TABLE))
    .withColumn("aud_tx_versao_pipeline_silver", lit(PROJECT_VERSION))
    .withColumn(
        "aud_tx_regra_derivacao",
        lit(
            "Theme dimension derived from theme attributes available in Bronze proposicoes common fields and JSON payload."
        ),
    )
)

# ==========================================================================================
# 13. Add Silver Record Hash
# ==========================================================================================

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "tem_id_tema",
        "tem_tx_nome",
        "tem_tx_descricao",
        "tem_tx_origem_registro",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# ==========================================================================================
# 14. Select Final Silver Columns
# ==========================================================================================

final_columns = [
    "tem_id_tema",
    "tem_tx_nome",
    "tem_tx_descricao",
    "tem_tx_origem_registro",
    "prp_id_proposicao_referencia",

    "tem_fl_id_valido",
    "tem_fl_nome_valido",
    "tem_fl_id_original_informado",
    "tem_fl_registro_derivado",
    "tem_fl_registro_valido_silver",

    "tem_tx_payload_json",

    "aud_id_execucao_bronze",
    "aud_dh_ingestao_bronze",
    "aud_tx_endpoint_origem_bronze",
    "aud_tx_sistema_origem_bronze",
    "aud_tx_versao_pipeline_bronze",
    "aud_tx_tipo_carga_bronze",
    "aud_tx_hash_registro_bronze",

    "aud_id_execucao_silver",
    "aud_dh_processamento",
    "aud_tx_camada_origem",
    "aud_tx_tabela_origem",
    "aud_tx_tabela_destino",
    "aud_tx_versao_pipeline_silver",
    "aud_tx_regra_derivacao",
    "aud_tx_hash_registro_silver",
]

silver_df = silver_df.select(*final_columns)

# ==========================================================================================
# 15. Persist Silver Table
# ==========================================================================================

records_written = silver_df.count()

try:

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    log_info(
        pipeline_logger=logger,
        message=(
            "Silver temas table persisted successfully "
            f"| records_written={records_written}"
        ),
    )

except Exception as error:

    finished_at = datetime.now()
    duration_seconds = (finished_at - started_at).total_seconds()

    write_pipeline_log(
        log_id=str(uuid.uuid4()),
        execution_id=execution_id,
        notebook_name=NOTEBOOK_NAME,
        layer_name=LAYER_NAME,
        entity_name=ENTITY_NAME,
        target_table=TARGET_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=f"Failed writing Silver temas table | error={str(error)}",
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=records_read,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed writing Silver temas table.",
        error=error,
    )

    raise error

# ==========================================================================================
# 16. Apply Governance Comments
# ==========================================================================================

table_comment = """
Derived legislative theme dimension in the Silver layer.

This table contains standardized theme records derived from available
proposition theme attributes in Bronze proposicoes. When the original theme
identifier is unavailable, a deterministic identifier is generated from the
standardized theme name.

Main characteristics:
- one row per standardized legislative theme
- deterministic theme identifier
- theme name standardization
- support for common fields and JSON payload extraction
- rejected and duplicate records tracked separately
- preserved Bronze lineage
- deterministic Silver record hash
"""

column_comments = {
    "tem_id_tema": "Deterministic legislative theme identifier.",
    "tem_tx_nome": "Standardized legislative theme name.",
    "tem_tx_descricao": "Standardized legislative theme description.",
    "tem_tx_origem_registro": "Source description used to derive the theme record.",
    "prp_id_proposicao_referencia": "Reference proposition identifier from which the theme was derived.",

    "tem_fl_id_valido": "Flag indicating whether the theme identifier is valid.",
    "tem_fl_nome_valido": "Flag indicating whether the theme name is valid.",
    "tem_fl_id_original_informado": "Flag indicating whether the original source theme identifier was available.",
    "tem_fl_registro_derivado": "Flag indicating whether the theme record was derived from another source table.",
    "tem_fl_registro_valido_silver": "Flag indicating whether the theme record passed Silver validation rules.",

    "tem_tx_payload_json": "Original Bronze payload preserved for traceability.",

    "aud_id_execucao_bronze": "Bronze execution identifier inherited from source ingestion.",
    "aud_dh_ingestao_bronze": "Bronze ingestion timestamp inherited from source ingestion.",
    "aud_tx_endpoint_origem_bronze": "Bronze source endpoint inherited from source ingestion.",
    "aud_tx_sistema_origem_bronze": "Bronze source system inherited from source ingestion.",
    "aud_tx_versao_pipeline_bronze": "Bronze pipeline version inherited from source ingestion.",
    "aud_tx_tipo_carga_bronze": "Bronze load type inherited from source ingestion.",
    "aud_tx_hash_registro_bronze": "Bronze deterministic record hash inherited for traceability.",

    "aud_id_execucao_silver": "Execution identifier for Silver theme processing.",
    "aud_dh_processamento": "Timestamp when the theme record was processed in Silver.",
    "aud_tx_camada_origem": "Source Medallion layer used during processing.",
    "aud_tx_tabela_origem": "Source Bronze table used during theme derivation.",
    "aud_tx_tabela_destino": "Target Silver table used during theme derivation.",
    "aud_tx_versao_pipeline_silver": "Pipeline version used during Silver theme processing.",
    "aud_tx_regra_derivacao": "Description of the Silver theme derivation rule applied.",
    "aud_tx_hash_registro_silver": "Deterministic Silver theme record hash.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# ==========================================================================================
# 17. Final Pipeline Log
# ==========================================================================================

finished_at = datetime.now()
duration_seconds = (finished_at - started_at).total_seconds()

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_SUCCESS,
    message=(
        "Silver temas derivation completed successfully "
        "| grain=one legislative theme per deterministic theme identifier"
    ),
    started_at=started_at,
    finished_at=finished_at,
    duration_seconds=duration_seconds,
    records_read=records_read,
    records_written=records_written,
)

log_success(
    pipeline_logger=logger,
    message=(
        f"Silver temas derivation completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER TEMAS COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print("Grain: one legislative theme per deterministic theme identifier")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)

