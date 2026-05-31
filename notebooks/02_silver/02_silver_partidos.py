# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver — Partidos Standardization
# MAGIC
# MAGIC **Notebook:** `02_silver_partidos`
# MAGIC
# MAGIC Derives standardized Brazilian political party records from available Bronze sources and persists validated, deduplicated and analytics-ready party records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Party derivation from `br_despesas_ceap`
# MAGIC - Party acronym standardization rules
# MAGIC - Deterministic derived party identifier
# MAGIC - Quality validation rules
# MAGIC - Technical duplicate detection
# MAGIC - Rejected records tracking using global utilities
# MAGIC - Bronze-to-Silver traceability
# MAGIC - Silver Delta persistence logic
# MAGIC - Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read party references from Bronze CEAP expenses
# MAGIC - Derive party records from deputy party acronym
# MAGIC - Standardize party acronyms
# MAGIC - Generate deterministic party identifiers
# MAGIC - Validate mandatory party fields
# MAGIC - Remove technical duplicate records
# MAGIC - Preserve Bronze ingestion lineage
# MAGIC - Register rejected and discarded records for traceability
# MAGIC - Persist curated Silver party dimension
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - There is no dedicated Bronze party source table in the current lakehouse
# MAGIC - `slv_partidos` is a derived Silver dimension
# MAGIC - Party name is not available in the current source and is temporarily filled with the party acronym
# MAGIC - `par_fl_nome_informado` indicates whether an official party name was available
# MAGIC - Future enrichment may replace derived party names using an official party source
# MAGIC - Comments and documentation are written in English
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

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

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
    row_number,
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
print("02 - SILVER PARTIDOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)



# ==========================================================================================
# 1. Global Configuration
# ==========================================================================================

NOTEBOOK_NAME = "02_silver_partidos"
LAYER_NAME = "silver"
ENTITY_NAME = "partidos"

SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES.get("despesas_ceap", "br_despesas_ceap")
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES.get("partidos", "slv_partidos")
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
    message="Silver partidos derivation started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver partidos derivation from CEAP source.",
)



# ==========================================================================================
# 4. Read Bronze CEAP Source
# ==========================================================================================

try:

    source_df = spark.table(SOURCE_TABLE)

    records_read = source_df.count()

    log_info(
        pipeline_logger=logger,
        message=(
            "Bronze CEAP table loaded successfully "
            f"| records_read={records_read}"
        ),
    )

except Exception as error:

    finished_at = datetime.now()

    duration_seconds = (
        finished_at - started_at
    ).total_seconds()

    write_pipeline_log(
        log_id=str(uuid.uuid4()),
        execution_id=execution_id,
        notebook_name=NOTEBOOK_NAME,
        layer_name=LAYER_NAME,
        entity_name=ENTITY_NAME,
        target_table=TARGET_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed reading Bronze CEAP table "
            f"| error={str(error)}"
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=None,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed reading Bronze CEAP table.",
        error=error,
    )

    raise error



# ==========================================================================================
# 5. Derive Party Candidates from CEAP
# ==========================================================================================

partidos_candidate_df = (
    source_df
    .select(
        clean_upper(
            safe_col(source_df, "dep_tx_sigla_partido")
        ).alias("par_tx_sigla"),

        safe_col(source_df, "dep_id_deputado").cast(StringType()).alias("dep_id_deputado_referencia"),

        safe_col(source_df, "aud_id_execucao").cast(StringType()).alias("aud_id_execucao_bronze"),
        safe_col(source_df, "aud_dh_ingestao").alias("aud_dh_ingestao_bronze"),
        safe_col(source_df, "aud_tx_endpoint_origem").cast(StringType()).alias("aud_tx_endpoint_origem_bronze"),
        safe_col(source_df, "aud_tx_sistema_origem").cast(StringType()).alias("aud_tx_sistema_origem_bronze"),
        safe_col(source_df, "aud_tx_versao_pipeline").cast(StringType()).alias("aud_tx_versao_pipeline_bronze"),
        safe_col(source_df, "aud_tx_tipo_carga").cast(StringType()).alias("aud_tx_tipo_carga_bronze"),
        safe_col(source_df, "aud_tx_hash_registro").cast(StringType()).alias("aud_tx_hash_registro_bronze"),

        coalesce(
            safe_col(source_df, "desp_tx_payload_json").cast(StringType()),
            safe_col(source_df, "ceap_tx_payload_json").cast(StringType()),
            lit(None).cast(StringType()),
        ).alias("par_tx_payload_json"),
    )
)



# ==========================================================================================
# 6. Normalize Derived Party Attributes
# ==========================================================================================

partidos_base_df = (
    partidos_candidate_df
    .withColumn(
        "par_id_partido",
        when(
            col("par_tx_sigla").isNotNull()
            & (col("par_tx_sigla") != ""),
            concat_ws(
                "_",
                lit("PAR"),
                regexp_replace(
                    col("par_tx_sigla"),
                    r"[^A-Z0-9]+",
                    "_",
                ),
            ),
        )
        .otherwise(lit(None).cast(StringType())),
    )
    .withColumn(
        "par_id_partido",
        regexp_replace(col("par_id_partido"), r"_+$", "")
    )
    .withColumn(
        "par_tx_nome",
        col("par_tx_sigla"),
    )
    .withColumn(
        "par_tx_uri",
        lit(None).cast(StringType()),
    )
    .withColumn(
        "par_tx_origem_registro",
        lit("derived_from_bronze_ceap"),
    )
    .withColumn(
        "par_fl_registro_derivado",
        lit(True).cast(BooleanType()),
    )
    .withColumn(
        "par_fl_nome_informado",
        lit(False).cast(BooleanType()),
    )
)


# ==========================================================================================
# 7. Apply Party Quality Rules
# ==========================================================================================

partidos_quality_df = (
    partidos_base_df
    .withColumn(
        "par_fl_id_valido",
        (
            col("par_id_partido").isNotNull()
            & (col("par_id_partido") != "")
        ).cast(BooleanType()),
    )
    .withColumn(
        "par_fl_sigla_valida",
        (
            col("par_tx_sigla").isNotNull()
            & (col("par_tx_sigla") != "")
        ).cast(BooleanType()),
    )
    .withColumn(
        "par_fl_nome_valido",
        (
            col("par_tx_nome").isNotNull()
            & (col("par_tx_nome") != "")
        ).cast(BooleanType()),
    )
    .withColumn(
        "par_fl_registro_valido_silver",
        (
            col("par_fl_id_valido")
            & col("par_fl_sigla_valida")
            & col("par_fl_nome_valido")
        ).cast(BooleanType()),
    )
    .withColumn(
        "par_tx_motivo_rejeicao",
        when(
            ~col("par_fl_sigla_valida"),
            lit("PAR_SIGLA_NULA_OU_VAZIA"),
        )
        .when(
            ~col("par_fl_id_valido"),
            lit("PAR_ID_NULO_OU_VAZIO"),
        )
        .when(
            ~col("par_fl_nome_valido"),
            lit("PAR_NOME_NULO_OU_VAZIO"),
        )
        .otherwise(
            lit(None).cast(StringType())
        ),
    )
)



# ==========================================================================================
# 8. Build Mandatory Rejected Records
# ==========================================================================================

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=partidos_quality_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="par_id_partido",
    validation_rule_column="par_tx_motivo_rejeicao",
    payload_column="par_tx_payload_json",
    valid_flag_column="par_fl_registro_valido_silver",
)



# ==========================================================================================
# 9. Keep Valid Derived Party Records
# ==========================================================================================

valid_df = (
    partidos_quality_df
    .filter(
        col("par_fl_registro_valido_silver") == True
    )
    .drop("par_tx_motivo_rejeicao")
)



# ==========================================================================================
# 10. Identify Technical Duplicates
# ==========================================================================================

dedup_window = (
    Window
    .partitionBy(
        "par_id_partido"
    )
    .orderBy(
        col("aud_dh_ingestao_bronze").desc_nulls_last(),
        col("aud_tx_hash_registro_bronze").asc_nulls_last(),
    )
)

partidos_ranked_df = (
    valid_df
    .withColumn(
        "rn_deduplicacao",
        row_number().over(dedup_window),
    )
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=partidos_ranked_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="par_id_partido",
    payload_column="par_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="PAR_REGISTRO_DUPLICADO_TECNICO",
    observation=(
        "Derived party record kept only once by party acronym. "
        "Deduplication order uses latest Bronze ingestion timestamp."
    ),
)

partidos_dedup_df = (
    partidos_ranked_df
    .filter(
        col("rn_deduplicacao") == 1
    )
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
            "Rejected and discarded derived party records persisted "
            f"| records_rejected={records_rejected}"
        ),
    )

except Exception as error:

    finished_at = datetime.now()

    duration_seconds = (
        finished_at - started_at
    ).total_seconds()

    write_pipeline_log(
        log_id=str(uuid.uuid4()),
        execution_id=execution_id,
        notebook_name=NOTEBOOK_NAME,
        layer_name=LAYER_NAME,
        entity_name=ENTITY_NAME,
        target_table=REJECTED_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed writing rejected derived party records "
            f"| error={str(error)}"
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=records_read,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed writing rejected derived party records.",
        error=error,
    )

    raise error



# ==========================================================================================
# 12. Add Silver Traceability Columns
# ==========================================================================================

silver_df = (
    partidos_dedup_df
    .withColumn(
        "aud_id_execucao_silver",
        lit(execution_id),
    )
    .withColumn(
        "aud_dh_processamento",
        current_timestamp(),
    )
    .withColumn(
        "aud_tx_camada_origem",
        lit("bronze"),
    )
    .withColumn(
        "aud_tx_tabela_origem",
        lit(SOURCE_TABLE),
    )
    .withColumn(
        "aud_tx_tabela_destino",
        lit(TARGET_TABLE),
    )
    .withColumn(
        "aud_tx_versao_pipeline_silver",
        lit(PROJECT_VERSION),
    )
    .withColumn(
        "aud_tx_regra_derivacao",
        lit(
            "Party dimension derived from distinct deputy party acronyms available in Bronze CEAP expenses."
        ),
    )
)



# ==========================================================================================
# 13. Add Silver Record Hash
# ==========================================================================================

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "par_id_partido",
        "par_tx_sigla",
        "par_tx_nome",
        "par_tx_origem_registro",
    ],
    hash_column="aud_tx_hash_registro_silver",
)



# ==========================================================================================
# 14. Select Final Silver Columns
# ==========================================================================================

final_columns = [
    "par_id_partido",
    "par_tx_sigla",
    "par_tx_nome",
    "par_tx_uri",
    "par_tx_origem_registro",

    "par_fl_id_valido",
    "par_fl_sigla_valida",
    "par_fl_nome_valido",
    "par_fl_nome_informado",
    "par_fl_registro_derivado",
    "par_fl_registro_valido_silver",

    "par_tx_payload_json",

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

silver_df = silver_df.select(
    *final_columns
)



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
            "Silver partidos table persisted successfully "
            f"| records_written={records_written}"
        ),
    )

except Exception as error:

    finished_at = datetime.now()

    duration_seconds = (
        finished_at - started_at
    ).total_seconds()

    write_pipeline_log(
        log_id=str(uuid.uuid4()),
        execution_id=execution_id,
        notebook_name=NOTEBOOK_NAME,
        layer_name=LAYER_NAME,
        entity_name=ENTITY_NAME,
        target_table=TARGET_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed writing Silver partidos table "
            f"| error={str(error)}"
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=records_read,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed writing Silver partidos table.",
        error=error,
    )

    raise error



# ==========================================================================================
# 16. Apply Governance Comments
# ==========================================================================================

table_comment = """
Derived political party dimension in the Silver layer.

This table contains standardized Brazilian political party records derived from
party acronyms available in Bronze CEAP expenses. There is no dedicated Bronze
party source table in the current lakehouse.

Main characteristics:
- one row per derived party acronym
- deterministic party identifier generated from party acronym
- party name temporarily filled with party acronym
- derived-source flag available for governance
- preserved Bronze lineage
- deterministic Silver record hash
"""

column_comments = {
    "par_id_partido": "Deterministic political party identifier generated from the party acronym.",
    "par_tx_sigla": "Standardized political party acronym.",
    "par_tx_nome": "Political party name. Temporarily filled with party acronym when official name is unavailable.",
    "par_tx_uri": "Political party URI. Null when not available in the current source.",
    "par_tx_origem_registro": "Source description used to derive the party record.",

    "par_fl_id_valido": "Flag indicating whether the party identifier is valid.",
    "par_fl_sigla_valida": "Flag indicating whether the party acronym is valid.",
    "par_fl_nome_valido": "Flag indicating whether the party name field is populated.",
    "par_fl_nome_informado": "Flag indicating whether the official party name was available in the source.",
    "par_fl_registro_derivado": "Flag indicating whether the party record was derived from another source table.",
    "par_fl_registro_valido_silver": "Flag indicating whether the party record passed Silver validation rules.",

    "par_tx_payload_json": "Original Bronze payload preserved for traceability when available.",

    "aud_id_execucao_bronze": "Bronze execution identifier inherited from source ingestion.",
    "aud_dh_ingestao_bronze": "Bronze ingestion timestamp inherited from source ingestion.",
    "aud_tx_endpoint_origem_bronze": "Bronze source endpoint inherited from source ingestion.",
    "aud_tx_sistema_origem_bronze": "Bronze source system inherited from source ingestion.",
    "aud_tx_versao_pipeline_bronze": "Bronze pipeline version inherited from source ingestion.",
    "aud_tx_tipo_carga_bronze": "Bronze load type inherited from source ingestion.",
    "aud_tx_hash_registro_bronze": "Bronze deterministic record hash inherited for traceability.",

    "aud_id_execucao_silver": "Execution identifier for Silver party processing.",
    "aud_dh_processamento": "Timestamp when party record was processed in Silver.",
    "aud_tx_camada_origem": "Source Medallion layer used during processing.",
    "aud_tx_tabela_origem": "Source Bronze table used during party derivation.",
    "aud_tx_tabela_destino": "Target Silver table used during party derivation.",
    "aud_tx_versao_pipeline_silver": "Pipeline version used during Silver party processing.",
    "aud_tx_regra_derivacao": "Description of the Silver derivation rule applied.",
    "aud_tx_hash_registro_silver": "Deterministic Silver party record hash.",
    
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

duration_seconds = (
    finished_at - started_at
).total_seconds()

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_SUCCESS,
    message=(
        "Silver partidos derivation completed successfully "
        "| grain=one political party per derived party acronym"
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
        f"Silver partidos derivation completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER PARTIDOS COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print("Grain: one political party per derived party acronym")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)

