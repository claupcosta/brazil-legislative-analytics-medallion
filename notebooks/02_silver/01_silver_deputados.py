# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Silver — Deputados Standardization
# MAGIC
# MAGIC **Notebook:** `01_silver_deputados`
# MAGIC
# MAGIC Standardizes deputy records from the Bronze layer and persists validated,
# MAGIC deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Schema normalization rules
# MAGIC - Deputy standardization logic
# MAGIC - Text normalization using global utilities
# MAGIC - Quality validation rules
# MAGIC - Rejected records tracking using global utilities
# MAGIC - Technical duplicate tracking by deputy and legislature
# MAGIC - Silver Delta persistence logic
# MAGIC - Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read deputy data from Bronze layer
# MAGIC - Standardize deputy attributes
# MAGIC - Normalize textual fields
# MAGIC - Validate mandatory deputy fields
# MAGIC - Preserve one record per deputy per legislature
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
# MAGIC - The grain of this table is one deputy per legislature
# MAGIC - Invalid records are redirected to `slv_registros_rejeitados`
# MAGIC - Technical duplicates are also registered as discarded records
# MAGIC - Global utility notebooks are used to reduce duplicated logic
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

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_datetime

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

# ==========================================================================================
# 01 Silver — Deputados Standardization
# Notebook: 01_silver_deputados
# ==========================================================================================
#
# Standardizes deputy records from the Bronze layer and persists validated, deduplicated
# and analytics-ready records into the Silver layer.
#
# This notebook defines:
# - Deputy schema normalization rules
# - One record per deputy per legislature
# - Party enrichment fallback from JSON payload and Bronze CEAP data when available
# - Latest legislature flag for downstream dimensional joins
# - Text normalization rules
# - Quality validation rules
# - Rejected records tracking using global utilities
# - Technical duplicate tracking by deputy and legislature
# - Silver Delta persistence logic
# - Governance comments using global utilities
#
# Responsibilities:
# - Read deputy data from Bronze layer
# - Standardize deputy attributes
# - Preserve one record per deputy per legislature
# - Enrich party and UF from auxiliary CEAP data when available
# - Identify the most recent legislature per deputy
# - Validate mandatory deputy fields
# - Remove technical duplicate records
# - Preserve Bronze ingestion lineage
# - Register rejected and discarded records for traceability
# - Persist curated Delta table
# - Apply governance comments to table and columns
#
# Notes:
# - Bronze preserves raw source values
# - Silver standardizes, validates and deduplicates records
# - The grain of this table is one deputy per legislature
# - dep_id_deputado is not unique by itself
# - Use dep_id_deputado + dep_id_legislatura for historical joins
# - Use dep_fl_legislatura_mais_recente for current-dimension joins when legislature is unavailable
# - Party is informative and does not invalidate the deputy record
# - Invalid records are redirected to slv_registros_rejeitados
# - Comments and documentation are written in English
# - Naming conventions follow Portuguese mnemonic standards
# ==========================================================================================



# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    lower,
    regexp_replace,
    current_timestamp,
    row_number,
    dense_rank,
    when,
    coalesce,
    concat_ws,
    get_json_object,
    count,
    desc,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, TimestampType

# COMMAND ----------

# ==========================================================================================
# Initialize Spark session explicitly for utility notebooks
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
generate_missing_comment_report.__globals__["spark"] = spark

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("01 - SILVER DEPUTADOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ==========================================================================================
# 1. Global Configuration
# ==========================================================================================

NOTEBOOK_NAME = "01_silver_deputados"
LAYER_NAME = "silver"
ENTITY_NAME = "deputados"

SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES["deputados"]
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES["deputados"]
)

REJECTED_TABLE = get_silver_table(
    SILVER_TABLES["registros_rejeitados"]
)

AUX_DESPESAS_TABLE = get_bronze_table(
    BRONZE_TABLES["despesas_ceap"]
)

LOAD_TYPE = LOAD_TYPE_FULL

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

APPLY_GOVERNANCE_COMMENTS = True

records_read = None
records_written = None
records_rejected = None

# COMMAND ----------

# ==========================================================================================
# 2. Helper Functions
# ==========================================================================================

def table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def column_exists(dataframe, column_name: str) -> bool:
    return column_name in dataframe.columns


def safe_col(dataframe, column_name: str, default_value=None):
    if column_exists(dataframe, column_name):
        return col(column_name)

    return lit(default_value)


def clean_upper(column_expression):
    return trim(
        regexp_replace(
            upper(column_expression.cast("string")),
            r"\s+",
            " ",
        )
    )


def clean_lower(column_expression):
    return trim(
        regexp_replace(
            lower(column_expression.cast("string")),
            r"\s+",
            " ",
        )
    )


def json_value(json_column: str, json_path: str):
    return get_json_object(col(json_column), json_path)

# COMMAND ----------

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
    message="Silver deputados standardization started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver deputados standardization.",
)

# COMMAND ----------

# ==========================================================================================
# 4. Read Bronze Deputados
# ==========================================================================================

bronze_df = spark.table(
    SOURCE_TABLE
)

records_read = bronze_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze deputados table loaded successfully "
        f"| records_read={records_read}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 5. Build Auxiliary Party Lookup from Bronze CEAP
# ==========================================================================================

if table_exists(AUX_DESPESAS_TABLE):

    despesas_df = spark.table(AUX_DESPESAS_TABLE)

    despesas_party_source_df = (
        despesas_df
        .select(
            safe_col(despesas_df, "dep_id_deputado").cast("string").alias("dep_id_deputado_aux"),
            safe_col(despesas_df, "dep_tx_sigla_partido").cast("string").alias("dep_tx_sigla_partido_aux"),
            safe_col(despesas_df, "dep_tx_sigla_uf").cast("string").alias("dep_tx_sigla_uf_aux"),
        )
        .filter(col("dep_id_deputado_aux").isNotNull())
        .filter(trim(col("dep_id_deputado_aux")) != "")
        .filter(col("dep_tx_sigla_partido_aux").isNotNull())
        .filter(trim(col("dep_tx_sigla_partido_aux")) != "")
        .withColumn("dep_tx_sigla_partido_aux", clean_upper(col("dep_tx_sigla_partido_aux")))
        .withColumn("dep_tx_sigla_uf_aux", clean_upper(col("dep_tx_sigla_uf_aux")))
    )

    party_rank_window = (
        Window
        .partitionBy("dep_id_deputado_aux")
        .orderBy(
            desc("qt_ocorrencias"),
            col("dep_tx_sigla_partido_aux").asc(),
        )
    )

    party_lookup_df = (
        despesas_party_source_df
        .groupBy(
            "dep_id_deputado_aux",
            "dep_tx_sigla_partido_aux",
            "dep_tx_sigla_uf_aux",
        )
        .agg(count(lit(1)).alias("qt_ocorrencias"))
        .withColumn("rn_partido", row_number().over(party_rank_window))
        .filter(col("rn_partido") == 1)
        .drop("rn_partido", "qt_ocorrencias")
    )

else:
    party_lookup_df = None

# COMMAND ----------

# ==========================================================================================
# 6. Standardize Bronze Source Fields
# ==========================================================================================

silver_base_df = (
    bronze_df
    .select(
        safe_col(bronze_df, "dep_id_deputado").cast(StringType()).alias("dep_id_deputado"),

        safe_col(bronze_df, "dep_tx_uri").cast(StringType()).alias("dep_tx_uri"),

        safe_col(bronze_df, "dep_tx_nome").cast(StringType()).alias("dep_tx_nome"),

        coalesce(
            safe_col(bronze_df, "dep_tx_sigla_partido").cast(StringType()),
            json_value("dep_tx_payload_json", "$.siglaPartido").cast(StringType()),
            json_value("dep_tx_payload_json", "$.ultimoStatus.siglaPartido").cast(StringType()),
        ).alias("dep_tx_sigla_partido"),

        coalesce(
            safe_col(bronze_df, "dep_tx_uri_partido").cast(StringType()),
            json_value("dep_tx_payload_json", "$.uriPartido").cast(StringType()),
            json_value("dep_tx_payload_json", "$.ultimoStatus.uriPartido").cast(StringType()),
        ).alias("dep_tx_uri_partido"),

        coalesce(
            safe_col(bronze_df, "dep_tx_sigla_uf").cast(StringType()),
            json_value("dep_tx_payload_json", "$.siglaUf").cast(StringType()),
            json_value("dep_tx_payload_json", "$.siglaUF").cast(StringType()),
            json_value("dep_tx_payload_json", "$.ultimoStatus.siglaUf").cast(StringType()),
        ).alias("dep_tx_sigla_uf"),

        coalesce(
            safe_col(bronze_df, "dep_id_legislatura").cast(StringType()),
            json_value("dep_tx_payload_json", "$.idLegislatura").cast(StringType()),
            json_value("dep_tx_payload_json", "$.ultimoStatus.idLegislatura").cast(StringType()),
        ).alias("dep_id_legislatura"),

        coalesce(
            safe_col(bronze_df, "dep_id_legislatura_referencia").cast(StringType()),
            safe_col(bronze_df, "dep_id_legislatura").cast(StringType()),
            json_value("dep_tx_payload_json", "$.idLegislatura").cast(StringType()),
            json_value("dep_tx_payload_json", "$.ultimoStatus.idLegislatura").cast(StringType()),
        ).alias("dep_id_legislatura_referencia"),

        coalesce(
            safe_col(bronze_df, "dep_tx_url_foto").cast(StringType()),
            json_value("dep_tx_payload_json", "$.urlFoto").cast(StringType()),
            json_value("dep_tx_payload_json", "$.ultimoStatus.urlFoto").cast(StringType()),
        ).alias("dep_tx_url_foto"),

        coalesce(
            safe_col(bronze_df, "dep_tx_email").cast(StringType()),
            json_value("dep_tx_payload_json", "$.email").cast(StringType()),
            json_value("dep_tx_payload_json", "$.ultimoStatus.email").cast(StringType()),
        ).alias("dep_tx_email"),

        safe_col(bronze_df, "dep_tx_payload_json").cast(StringType()).alias("dep_tx_payload_json"),

        safe_col(bronze_df, "aud_id_execucao").cast(StringType()).alias("aud_id_execucao_bronze"),
        safe_col(bronze_df, "aud_dh_ingestao").cast(TimestampType()).alias("aud_dh_ingestao_bronze"),
        safe_col(bronze_df, "aud_tx_endpoint_origem").cast(StringType()).alias("aud_tx_endpoint_origem"),
        safe_col(bronze_df, "aud_tx_sistema_origem").cast(StringType()).alias("aud_tx_sistema_origem"),
        safe_col(bronze_df, "aud_tx_versao_pipeline").cast(StringType()).alias("aud_tx_versao_pipeline_bronze"),
        safe_col(bronze_df, "aud_tx_tipo_carga").cast(StringType()).alias("aud_tx_tipo_carga_bronze"),
        safe_col(bronze_df, "aud_tx_hash_registro").cast(StringType()).alias("aud_tx_hash_registro_bronze"),
    )
)

# COMMAND ----------

# ==========================================================================================
# 7. Enrich Party and UF from Auxiliary Sources
# ==========================================================================================

if party_lookup_df is not None:

    silver_enriched_df = (
        silver_base_df.alias("dep")
        .join(
            party_lookup_df.alias("aux"),
            col("dep.dep_id_deputado") == col("aux.dep_id_deputado_aux"),
            "left",
        )
        .withColumn(
            "dep_tx_sigla_partido",
            coalesce(
                when(trim(col("dep.dep_tx_sigla_partido")) != "", col("dep.dep_tx_sigla_partido")),
                when(trim(col("aux.dep_tx_sigla_partido_aux")) != "", col("aux.dep_tx_sigla_partido_aux")),
            )
        )
        .withColumn(
            "dep_tx_sigla_uf",
            coalesce(
                when(trim(col("dep.dep_tx_sigla_uf")) != "", col("dep.dep_tx_sigla_uf")),
                when(trim(col("aux.dep_tx_sigla_uf_aux")) != "", col("aux.dep_tx_sigla_uf_aux")),
            )
        )
        .drop(
            "dep_id_deputado_aux",
            "dep_tx_sigla_partido_aux",
            "dep_tx_sigla_uf_aux",
        )
    )

else:
    silver_enriched_df = silver_base_df

# COMMAND ----------

# ==========================================================================================
# 8. Normalize Text Fields
# ==========================================================================================

silver_clean_df = (
    silver_enriched_df
    .withColumn(
        "dep_tx_nome",
        clean_upper(col("dep_tx_nome")),
    )
    .withColumn(
        "dep_tx_sigla_partido",
        clean_upper(col("dep_tx_sigla_partido")),
    )
    .withColumn(
        "dep_tx_sigla_uf",
        clean_upper(col("dep_tx_sigla_uf")),
    )
    .withColumn(
        "dep_tx_email",
        clean_lower(col("dep_tx_email")),
    )
    .withColumn(
        "dep_tx_chave_deputado_legislatura",
        concat_ws(
            "||",
            col("dep_id_deputado"),
            col("dep_id_legislatura"),
        ),
    )
)

# COMMAND ----------

# ==========================================================================================
# 9. Apply Latest Legislature Flag
# ==========================================================================================

latest_legislature_window = (
    Window
    .partitionBy(
        "dep_id_deputado"
    )
    .orderBy(
        col("dep_id_legislatura").cast("int").desc_nulls_last()
    )
)

silver_context_df = (
    silver_clean_df
    .withColumn(
        "dep_nr_rank_legislatura",
        dense_rank().over(latest_legislature_window),
    )
    .withColumn(
        "dep_fl_legislatura_mais_recente",
        when(
            col("dep_nr_rank_legislatura") == 1,
            lit(True),
        ).otherwise(
            lit(False)
        ),
    )
    .drop(
        "dep_nr_rank_legislatura"
    )
)

# COMMAND ----------

# ==========================================================================================
# 10. Apply Silver Quality Flags
# ==========================================================================================

silver_quality_df = (
    silver_context_df
    .withColumn(
        "dep_fl_id_valido",
        (
            col("dep_id_deputado").isNotNull()
            & (trim(col("dep_id_deputado")) != "")
        ).cast("boolean"),
    )
    .withColumn(
        "dep_fl_legislatura_valida",
        (
            col("dep_id_legislatura").isNotNull()
            & (trim(col("dep_id_legislatura")) != "")
        ).cast("boolean"),
    )
    .withColumn(
        "dep_fl_nome_valido",
        (
            col("dep_tx_nome").isNotNull()
            & (trim(col("dep_tx_nome")) != "")
        ).cast("boolean"),
    )
    .withColumn(
        "dep_fl_partido_informado",
        (
            col("dep_tx_sigla_partido").isNotNull()
            & (trim(col("dep_tx_sigla_partido")) != "")
        ).cast("boolean"),
    )
    .withColumn(
        "dep_fl_uf_informada",
        (
            col("dep_tx_sigla_uf").isNotNull()
            & (trim(col("dep_tx_sigla_uf")) != "")
        ).cast("boolean"),
    )
    .withColumn(
        "dep_fl_email_informado",
        (
            col("dep_tx_email").isNotNull()
            & (trim(col("dep_tx_email")) != "")
        ).cast("boolean"),
    )
    .withColumn(
        "dep_fl_registro_valido_silver",
        (
            col("dep_fl_id_valido")
            & col("dep_fl_legislatura_valida")
            & col("dep_fl_nome_valido")
        ).cast("boolean"),
    )
    .withColumn(
        "dep_tx_motivo_rejeicao",
        when(
            ~col("dep_fl_id_valido"),
            lit("DEP_ID_NULO_OU_VAZIO"),
        )
        .when(
            ~col("dep_fl_legislatura_valida"),
            lit("DEP_LEGISLATURA_NULA_OU_VAZIA"),
        )
        .when(
            ~col("dep_fl_nome_valido"),
            lit("DEP_NOME_NULO_OU_VAZIO"),
        )
        .otherwise(
            lit(None).cast(StringType())
        ),
    )
)

# COMMAND ----------

# ==========================================================================================
# 11. Build Mandatory Rejected Records
# ==========================================================================================

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=silver_quality_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="dep_tx_chave_deputado_legislatura",
    validation_rule_column="dep_tx_motivo_rejeicao",
    payload_column="dep_tx_payload_json",
    valid_flag_column="dep_fl_registro_valido_silver",
)

# COMMAND ----------

# ==========================================================================================
# 12. Identify Technical Duplicates
# ==========================================================================================

valid_df = (
    silver_quality_df
    .filter(
        col("dep_fl_registro_valido_silver") == True
    )
    .drop(
        "dep_tx_motivo_rejeicao"
    )
)

dedup_window = (
    Window
    .partitionBy(
        "dep_id_deputado",
        "dep_id_legislatura",
    )
    .orderBy(
        col("aud_dh_ingestao_bronze").desc_nulls_last()
    )
)

valid_ranked_df = (
    valid_df
    .withColumn(
        "rn_deduplicacao",
        row_number().over(dedup_window),
    )
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=valid_ranked_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="dep_tx_chave_deputado_legislatura",
    payload_column="dep_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="DEP_REGISTRO_DUPLICADO_TECNICO",
    observation=(
        "Record kept only once by dep_id_deputado and dep_id_legislatura. "
        "Deduplication order uses latest Bronze ingestion timestamp."
    ),
)

silver_dedup_df = (
    valid_ranked_df
    .filter(
        col("rn_deduplicacao") == 1
    )
    .drop(
        "rn_deduplicacao"
    )
)

# COMMAND ----------

# ==========================================================================================
# 13. Persist Rejected and Discarded Records
# ==========================================================================================

rejected_df = union_rejected_records(
    mandatory_rejected_dataframe=mandatory_rejected_df,
    duplicate_rejected_dataframe=duplicate_rejected_df,
)

records_rejected = rejected_df.count()

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
        f"Rejected and discarded deputados records persisted "
        f"| records_rejected={records_rejected}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 14. Add Silver Traceability Columns
# ==========================================================================================

silver_df = (
    silver_dedup_df
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
            "Deputy standardized at deputy plus legislature grain. "
            "Party and UF are enriched from JSON payload and CEAP auxiliary source when available. "
            "Latest legislature flag is provided for downstream dimensional joins."
        ),
    )
)

# COMMAND ----------

# ==========================================================================================
# 15. Add Silver Record Hash
# ==========================================================================================

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "dep_id_deputado",
        "dep_id_legislatura",
        "dep_tx_nome",
        "dep_tx_sigla_partido",
        "dep_tx_sigla_uf",
        "dep_fl_legislatura_mais_recente",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# ==========================================================================================
# 16. Select Final Silver Columns
# ==========================================================================================

final_columns = [
    "dep_tx_chave_deputado_legislatura",
    "dep_id_deputado",
    "dep_tx_uri",
    "dep_tx_nome",
    "dep_tx_sigla_partido",
    "dep_tx_uri_partido",
    "dep_tx_sigla_uf",
    "dep_id_legislatura",
    "dep_id_legislatura_referencia",
    "dep_fl_legislatura_mais_recente",
    "dep_tx_url_foto",
    "dep_tx_email",
    "dep_fl_id_valido",
    "dep_fl_legislatura_valida",
    "dep_fl_nome_valido",
    "dep_fl_partido_informado",
    "dep_fl_uf_informada",
    "dep_fl_email_informado",
    "dep_fl_registro_valido_silver",
    "dep_tx_payload_json",
    "aud_id_execucao_bronze",
    "aud_dh_ingestao_bronze",
    "aud_tx_endpoint_origem",
    "aud_tx_sistema_origem",
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

# COMMAND ----------

# ==========================================================================================
# 17. Persist Silver Table
# ==========================================================================================

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

records_written = spark.table(TARGET_TABLE).count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Silver deputados table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 18. Apply Governance Comments
# ==========================================================================================

table_comment = """
Standardized deputies table in the Silver layer.

This table contains cleaned, validated, deduplicated and analytics-ready deputy
records derived from the Bronze ingestion layer.

Main characteristics:
- one record per deputy per legislature
- normalized deputy names
- standardized UF information
- party enrichment from source payload and auxiliary CEAP data when available
- latest legislature flag for downstream dimensional joins
- mandatory Silver validation flags
- rejected records persisted separately
- technical duplicate tracking by deputy and legislature
- preserved Bronze lineage
- deterministic Silver record hash

Silver layer note:
- The grain preserves historical legislative context.
- dep_id_deputado is not unique by itself in this table.
- Use dep_id_deputado plus dep_id_legislatura for historical joins.
- Use dep_fl_legislatura_mais_recente when joining sources that do not provide legislature.
- Business aggregations and dimensional modeling are handled in the Gold layer.
"""

column_comments = {
    "dep_tx_chave_deputado_legislatura":
        "Deterministic natural key composed by deputy identifier and legislature identifier.",

    "dep_id_deputado":
        "Deputy identifier as provided by the Câmara API.",

    "dep_tx_uri":
        "Deputy source URI.",

    "dep_tx_nome":
        "Standardized deputy name.",

    "dep_tx_sigla_partido":
        "Standardized political party acronym when available from source or auxiliary CEAP data.",

    "dep_tx_uri_partido":
        "Political party URI when available from source data.",

    "dep_tx_sigla_uf":
        "Standardized Brazilian state acronym.",

    "dep_id_legislatura":
        "Legislature identifier associated with the deputy record.",

    "dep_id_legislatura_referencia":
        "Reference legislature used during extraction.",

    "dep_fl_legislatura_mais_recente":
        "Flag indicating whether this is the most recent legislature record for the deputy.",

    "dep_tx_url_foto":
        "Deputy photo URL.",

    "dep_tx_email":
        "Deputy institutional email.",

    "dep_fl_id_valido":
        "Flag indicating whether deputy identifier is valid.",

    "dep_fl_legislatura_valida":
        "Flag indicating whether legislature identifier is valid.",

    "dep_fl_nome_valido":
        "Flag indicating whether deputy name is valid.",

    "dep_fl_partido_informado":
        "Flag indicating whether political party information exists.",

    "dep_fl_uf_informada":
        "Flag indicating whether UF information exists.",

    "dep_fl_email_informado":
        "Flag indicating whether email information exists.",

    "dep_fl_registro_valido_silver":
        "Flag indicating whether the record passed Silver validation.",

    "dep_tx_payload_json":
        "Original Bronze JSON payload preserved for traceability.",

    "aud_id_execucao_bronze":
        "Execution identifier from Bronze ingestion.",

    "aud_dh_ingestao_bronze":
        "Bronze ingestion timestamp.",

    "aud_tx_endpoint_origem":
        "Source endpoint used during Bronze ingestion.",

    "aud_tx_sistema_origem":
        "Source system identified during Bronze ingestion.",

    "aud_tx_versao_pipeline_bronze":
        "Pipeline version used during Bronze ingestion.",

    "aud_tx_tipo_carga_bronze":
        "Load type applied during Bronze ingestion.",

    "aud_tx_hash_registro_bronze":
        "Deterministic Bronze record hash.",

    "aud_id_execucao_silver":
        "Execution identifier for Silver transformation.",

    "aud_dh_processamento":
        "Timestamp when the record was processed in Silver.",

    "aud_tx_camada_origem":
        "Source Medallion layer used during processing.",

    "aud_tx_tabela_origem":
        "Source table used during processing.",

    "aud_tx_tabela_destino":
        "Target Silver table.",

    "aud_tx_versao_pipeline_silver":
        "Pipeline version used during Silver transformation.",

    "aud_tx_regra_derivacao":
        "Textual description of deputy standardization and enrichment rules.",

    "aud_tx_hash_registro_silver":
        "Deterministic Silver record hash.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# ==========================================================================================
# 19. Final Pipeline Log
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
        f"Silver deputados standardization completed successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| records_rejected={records_rejected} "
        f"| grain=one deputy per legislature"
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
        f"Silver deputados standardization completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER DEPUTADOS COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print("Grain: one deputy per legislature")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)