# Databricks notebook source
# MAGIC %md
# MAGIC # 16 Silver — Órgãos Standardization
# MAGIC
# MAGIC **Notebook:** `16_silver_orgaos`
# MAGIC
# MAGIC Standardizes legislative body records from the Bronze layer and persists
# MAGIC validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Legislative body schema normalization rules
# MAGIC - Safe extraction from Bronze JSON payload
# MAGIC - Legislative body identifier standardization logic
# MAGIC - Text normalization using global utilities
# MAGIC - Quality validation rules
# MAGIC - Rejected records tracking using global utilities
# MAGIC - Technical duplicate tracking
# MAGIC - Silver Delta persistence logic
# MAGIC - Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read legislative body data from Bronze layer
# MAGIC - Standardize body identifiers, acronyms, names, types and URIs
# MAGIC - Extract missing attributes from `org_tx_payload_json` when necessary
# MAGIC - Normalize textual fields
# MAGIC - Validate mandatory legislative body fields
# MAGIC - Remove technical duplicate records
# MAGIC - Preserve Bronze ingestion lineage
# MAGIC - Register rejected and discarded records for traceability
# MAGIC - Persist curated Delta table
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves raw source values
# MAGIC - Silver standardizes, validates and deduplicates records
# MAGIC - Legislative body identifier and name are mandatory for analytical use
# MAGIC - Missing structured columns may be recovered from the Bronze JSON payload
# MAGIC - Invalid records are redirected to `slv_registros_rejeitados`
# MAGIC - Technical duplicates are also registered as discarded records
# MAGIC - Global utility notebooks are used to reduce duplicated logic
# MAGIC - Comments and documentation are written in English
# MAGIC - Naming conventions follow Portuguese mnemonic standards
# MAGIC
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

# Databricks notebook source

# MAGIC %md
# MAGIC # 16 Silver — Órgãos Standardization
# MAGIC
# MAGIC **Notebook:** `16_silver_orgaos`
# MAGIC **Layer:** `Silver`
# MAGIC **Source:** `brazil_legislative_analytics.bronze.br_orgaos`
# MAGIC **Target:** `brazil_legislative_analytics.silver.slv_orgaos`
# MAGIC
# MAGIC Standardizes legislative body records from the Bronze layer and persists
# MAGIC validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook also creates analytical organization classification fields
# MAGIC to support Gold dimensions and downstream marts.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read legislative body data from Bronze layer
# MAGIC - Standardize body identifiers, acronyms, names, types and URIs
# MAGIC - Normalize textual fields
# MAGIC - Validate mandatory legislative body fields
# MAGIC - Create analytical organization type classification
# MAGIC - Create analytical flags for plenary, committee, board and parliamentary front
# MAGIC - Remove technical duplicate records
# MAGIC - Preserve Bronze ingestion lineage
# MAGIC - Register rejected and discarded records for traceability
# MAGIC - Persist Silver Delta table
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves raw source values
# MAGIC - Silver standardizes, validates and deduplicates records
# MAGIC - Legislative body identifier and name are mandatory for analytical use
# MAGIC - Invalid records are redirected to `slv_registros_rejeitados`
# MAGIC - Technical duplicates are also registered as discarded records
# MAGIC - Classification flags support Gold dimensional modeling
# MAGIC - Comments and documentation are written in English
# MAGIC - Naming conventions follow Portuguese mnemonic standards

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_hash

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

from pyspark.sql import functions as F

from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    current_timestamp,
    row_number,
    when,
)

from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("16 - SILVER ORGAOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "16_silver_orgaos"
LAYER_NAME = "silver"
ENTITY_NAME = "orgaos"

SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES["orgaos"]
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES["orgaos"]
)

REJECTED_TABLE = get_silver_table(
    SILVER_TABLES["registros_rejeitados"]
)

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

APPLY_GOVERNANCE_COMMENTS = True

records_read = None
records_written = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start Pipeline Log

# COMMAND ----------

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver orgaos standardization started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver orgaos standardization.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze Table

# COMMAND ----------

source_df = spark.table(SOURCE_TABLE)

records_read = source_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze orgaos table loaded successfully "
        f"| records_read={records_read}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper Functions

# COMMAND ----------

def clean_text(column_name):
    """
    Cleans textual columns by trimming and normalizing spaces.
    """

    return (
        when(
            col(column_name).isNull(),
            lit(None).cast(StringType()),
        )
        .otherwise(
            trim(
                F.regexp_replace(
                    col(column_name).cast("string"),
                    r"\s+",
                    " ",
                )
            )
        )
    )


def clean_upper_text(column_name):
    """
    Cleans textual columns and converts values to uppercase.
    """

    return upper(
        clean_text(column_name)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize Base Attributes

# COMMAND ----------

orgaos_base_df = (
    source_df
    .select(

        clean_text("org_id_orgao")
            .alias("org_id_orgao"),

        clean_upper_text("org_tx_sigla")
            .alias("org_tx_sigla"),

        clean_upper_text("org_tx_nome")
            .alias("org_tx_nome"),

        clean_upper_text("org_tx_apelido")
            .alias("org_tx_apelido"),

        clean_upper_text("org_tx_tipo_orgao")
            .alias("org_tx_tipo_orgao"),

        clean_upper_text("org_tx_sigla_tipo_orgao")
            .alias("org_tx_sigla_tipo_orgao"),

        clean_upper_text("org_tx_situacao")
            .alias("org_tx_situacao"),

        clean_text("org_dt_inicio")
            .alias("org_dt_inicio"),

        clean_text("org_dt_fim")
            .alias("org_dt_fim"),

        clean_text("org_tx_uri")
            .alias("org_tx_uri"),

        clean_text("org_tx_payload_json")
            .alias("org_tx_payload_json"),

        col("aud_id_execucao")
            .alias("aud_id_execucao_bronze"),

        col("aud_dh_ingestao")
            .alias("aud_dh_ingestao_bronze"),

        col("aud_tx_endpoint_origem")
            .alias("aud_tx_endpoint_origem_bronze"),

        col("aud_tx_sistema_origem")
            .alias("aud_tx_sistema_origem_bronze"),

        col("aud_tx_versao_pipeline")
            .alias("aud_tx_versao_pipeline_bronze"),

        col("aud_tx_tipo_carga")
            .alias("aud_tx_tipo_carga_bronze"),

        col("aud_tx_hash_registro")
            .alias("aud_tx_hash_registro_bronze"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Apply Quality Rules and Analytical Classification

# COMMAND ----------

orgaos_quality_df = (
    orgaos_base_df

    .withColumn(
        "org_fl_id_valido",
        (
            col("org_id_orgao").isNotNull()
            & (trim(col("org_id_orgao")) != "")
        )
    )

    .withColumn(
        "org_fl_nome_valido",
        (
            col("org_tx_nome").isNotNull()
            & (trim(col("org_tx_nome")) != "")
        )
    )

    .withColumn(
        "org_fl_sigla_informada",
        (
            col("org_tx_sigla").isNotNull()
            & (trim(col("org_tx_sigla")) != "")
        )
    )

    .withColumn(
        "org_fl_tipo_informado",
        (
            col("org_tx_tipo_orgao").isNotNull()
            & (trim(col("org_tx_tipo_orgao")) != "")
        )
    )

    .withColumn(
        "org_fl_uri_informada",
        (
            col("org_tx_uri").isNotNull()
            & (trim(col("org_tx_uri")) != "")
        )
    )

    .withColumn(
        "org_tx_tipo_curado",
        when(
            upper(col("org_tx_tipo_orgao")).contains("PLEN"),
            lit("PLENARIO")
        )
        .when(
            upper(col("org_tx_tipo_orgao")).contains("COMISS"),
            lit("COMISSAO")
        )
        .when(
            upper(col("org_tx_tipo_orgao")).contains("MESA"),
            lit("MESA DIRETORA")
        )
        .when(
            upper(col("org_tx_tipo_orgao")).contains("FRENTE"),
            lit("FRENTE PARLAMENTAR")
        )
        .otherwise(col("org_tx_tipo_orgao"))
    )

    .withColumn(
        "org_fl_plenario",
        when(
            upper(col("org_tx_tipo_orgao")).contains("PLEN"),
            lit(1)
        ).otherwise(lit(0))
    )

    .withColumn(
        "org_fl_comissao",
        when(
            upper(col("org_tx_tipo_orgao")).contains("COMISS"),
            lit(1)
        ).otherwise(lit(0))
    )

    .withColumn(
        "org_fl_mesa",
        when(
            upper(col("org_tx_tipo_orgao")).contains("MESA"),
            lit(1)
        ).otherwise(lit(0))
    )

    .withColumn(
        "org_fl_frente_parlamentar",
        when(
            upper(col("org_tx_tipo_orgao")).contains("FRENTE"),
            lit(1)
        ).otherwise(lit(0))
    )

    .withColumn(
        "org_fl_registro_valido_silver",
        (
            col("org_fl_id_valido")
            & col("org_fl_nome_valido")
        )
    )

    .withColumn(
        "org_tx_motivo_rejeicao",
        when(
            ~col("org_fl_id_valido"),
            lit("ORG_ID_NULO_OU_VAZIO"),
        )
        .when(
            ~col("org_fl_nome_valido"),
            lit("ORG_NOME_NULO_OU_VAZIO"),
        )
        .otherwise(lit(None))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Build Mandatory Rejected Records

# COMMAND ----------

mandatory_rejected_source_df = (
    orgaos_quality_df
    .filter(
        col("org_fl_registro_valido_silver") == False
    )
)

mandatory_rejected_df = (
    build_mandatory_rejected_records(
        dataframe=mandatory_rejected_source_df,
        execution_id=execution_id,
        source_table=SOURCE_TABLE,
        target_table=TARGET_TABLE,
        project_version=PROJECT_VERSION,
        entity_name=ENTITY_NAME,
        record_id_column="org_id_orgao",
        validation_rule_column="org_tx_motivo_rejeicao",
        payload_column="org_tx_payload_json",
        valid_flag_column="org_fl_registro_valido_silver",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Keep Valid Records

# COMMAND ----------

valid_df = (
    orgaos_quality_df
    .filter(
        col("org_fl_registro_valido_silver") == True
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Deduplicate Records

# COMMAND ----------

dedup_window = (
    Window
    .partitionBy("org_id_orgao")
    .orderBy(
        col("aud_dh_ingestao_bronze").desc_nulls_last()
    )
)

dedup_df = (
    valid_df
    .withColumn(
        "rn_deduplicacao",
        row_number().over(dedup_window)
    )
)

duplicate_rejected_df = (
    build_duplicate_rejected_records(
        dataframe=dedup_df,
        execution_id=execution_id,
        source_table=SOURCE_TABLE,
        target_table=TARGET_TABLE,
        project_version=PROJECT_VERSION,
        entity_name=ENTITY_NAME,
        record_id_column="org_id_orgao",
        payload_column="org_tx_payload_json",
        dedup_rank_column="rn_deduplicacao",
        duplicate_rule_code="ORG_REGISTRO_DUPLICADO",
        observation=(
            "Duplicate orgao records removed "
            "keeping latest Bronze ingestion."
        ),
    )
)

silver_df = (
    dedup_df
    .filter(col("rn_deduplicacao") == 1)
    .drop("rn_deduplicacao")
    .drop("org_tx_motivo_rejeicao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Persist Rejected Records

# COMMAND ----------

rejected_df = union_rejected_records(
    mandatory_rejected_dataframe=mandatory_rejected_df,
    duplicate_rejected_dataframe=duplicate_rejected_df,
)

clean_and_persist_rejected_records(
    rejected_dataframe=rejected_df,
    rejected_table=REJECTED_TABLE,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    mode="append",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Add Silver Traceability Columns

# COMMAND ----------

silver_df = (
    silver_df

    .withColumn(
        "aud_id_execucao_silver",
        lit(execution_id)
    )

    .withColumn(
        "aud_dh_processamento",
        current_timestamp()
    )

    .withColumn(
        "aud_tx_camada_origem",
        lit("bronze")
    )

    .withColumn(
        "aud_tx_tabela_origem",
        lit(SOURCE_TABLE)
    )

    .withColumn(
        "aud_tx_tabela_destino",
        lit(TARGET_TABLE)
    )

    .withColumn(
        "aud_tx_versao_pipeline_silver",
        lit(PROJECT_VERSION)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Add Silver Hash

# COMMAND ----------

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "org_id_orgao",
        "org_tx_sigla",
        "org_tx_nome",
        "org_tx_tipo_orgao",
        "org_tx_tipo_curado",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Select Final Columns

# COMMAND ----------

final_columns = [

    "org_id_orgao",
    "org_tx_sigla",
    "org_tx_nome",
    "org_tx_apelido",
    "org_tx_tipo_orgao",
    "org_tx_tipo_curado",
    "org_tx_sigla_tipo_orgao",
    "org_tx_situacao",
    "org_dt_inicio",
    "org_dt_fim",
    "org_tx_uri",

    "org_fl_plenario",
    "org_fl_comissao",
    "org_fl_mesa",
    "org_fl_frente_parlamentar",

    "org_fl_id_valido",
    "org_fl_nome_valido",
    "org_fl_sigla_informada",
    "org_fl_tipo_informado",
    "org_fl_uri_informada",
    "org_fl_registro_valido_silver",

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
    "aud_tx_hash_registro_silver",
]

silver_df = silver_df.select(*final_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Persist Silver Table

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS {TARGET_TABLE}
""")

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

records_written = spark.table(
    TARGET_TABLE
).count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Silver orgaos table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Apply Governance Comments

# COMMAND ----------

table_comment = """
Standardized legislative bodies table in the Silver layer.

This table contains validated, deduplicated and analytically classified
legislative body records used to support Gold dimensions and analytical marts.

The table includes organization type curation and analytical flags for plenary,
committee, board and parliamentary front classification.
"""

column_comments = {

    "org_id_orgao":
        "Legislative body identifier.",

    "org_tx_sigla":
        "Standardized legislative body acronym.",

    "org_tx_nome":
        "Standardized legislative body name.",

    "org_tx_apelido":
        "Legislative body nickname.",

    "org_tx_tipo_orgao":
        "Original legislative body type description.",

    "org_tx_tipo_curado":
        "Curated legislative body type used for analytical classification.",

    "org_tx_sigla_tipo_orgao":
        "Legislative body type acronym.",

    "org_tx_situacao":
        "Legislative body operational status.",

    "org_dt_inicio":
        "Legislative body start date.",

    "org_dt_fim":
        "Legislative body end date.",

    "org_tx_uri":
        "Legislative body URI.",

    "org_fl_plenario":
        "Analytical flag indicating whether the organization is a plenary body.",

    "org_fl_comissao":
        "Analytical flag indicating whether the organization is a committee.",

    "org_fl_mesa":
        "Analytical flag indicating whether the organization is a board or directing table.",

    "org_fl_frente_parlamentar":
        "Analytical flag indicating whether the organization is a parliamentary front.",

    "org_fl_id_valido":
        "Flag indicating valid legislative body identifier.",

    "org_fl_nome_valido":
        "Flag indicating valid legislative body name.",

    "org_fl_sigla_informada":
        "Flag indicating acronym availability.",

    "org_fl_tipo_informado":
        "Flag indicating body type availability.",

    "org_fl_uri_informada":
        "Flag indicating URI availability.",

    "org_fl_registro_valido_silver":
        "Flag indicating whether record passed Silver validation.",

    "aud_id_execucao_bronze":
        "Bronze execution identifier.",

    "aud_dh_ingestao_bronze":
        "Bronze ingestion timestamp.",

    "aud_tx_endpoint_origem_bronze":
        "Bronze source endpoint or CSV fallback source.",

    "aud_tx_sistema_origem_bronze":
        "Bronze source system.",

    "aud_tx_versao_pipeline_bronze":
        "Bronze pipeline version.",

    "aud_tx_tipo_carga_bronze":
        "Bronze load type.",

    "aud_tx_hash_registro_bronze":
        "Bronze deterministic record hash.",

    "aud_id_execucao_silver":
        "Silver execution identifier.",

    "aud_dh_processamento":
        "Silver processing timestamp.",

    "aud_tx_camada_origem":
        "Source Medallion layer used during processing.",

    "aud_tx_tabela_origem":
        "Source Bronze table used during processing.",

    "aud_tx_tabela_destino":
        "Target Silver table.",

    "aud_tx_versao_pipeline_silver":
        "Silver pipeline version.",

    "aud_tx_hash_registro_silver":
        "Deterministic Silver hash used for traceability.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Final Pipeline Log

# COMMAND ----------

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
        f"Silver orgaos standardization completed successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written}"
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
        f"Silver orgaos standardization completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER ORGAOS COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)