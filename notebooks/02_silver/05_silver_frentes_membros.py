# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Silver — Frentes Membros Standardization
# MAGIC
# MAGIC **Notebook:** `05_silver_frentes_membros`
# MAGIC
# MAGIC Standardizes parliamentary front membership records from the Bronze layer and persists validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Parliamentary front membership schema normalization rules
# MAGIC - Integration with `slv_frentes`
# MAGIC - Integration with `slv_deputados`
# MAGIC - Deputy identifier and membership attribute standardization
# MAGIC - Text normalization using global utilities
# MAGIC - Quality validation rules
# MAGIC - Technical duplicate detection and removal
# MAGIC - Rejected records tracking using global utilities
# MAGIC - Silver Delta persistence logic
# MAGIC - Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read parliamentary front membership records from Bronze layer
# MAGIC - Standardize front and deputy identifiers
# MAGIC - Enrich deputy information from Silver dimensions
# MAGIC - Normalize party, federation unit and membership attributes
# MAGIC - Validate mandatory membership fields
# MAGIC - Validate front and deputy existence in project scope
# MAGIC - Remove technical duplicate records
# MAGIC - Preserve Bronze lineage and audit attributes
# MAGIC - Register rejected records for traceability
# MAGIC - Persist curated Silver table
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves raw source values
# MAGIC - Silver standardizes, validates and deduplicates records
# MAGIC - Front scope is inherited from `slv_frentes`
# MAGIC - Deputy enrichment is inherited from `slv_deputados`
# MAGIC - Only memberships linked to project-supported parliamentary fronts are kept
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

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_comments

# COMMAND ----------

# MAGIC %run ../99_utils/utils_rejected_records

# COMMAND ----------

# MAGIC %run ../99_utils/utils_text

# COMMAND ----------

# ==========================================================================================
# 05 Silver — Frentes Membros Standardization
# Notebook: 05_silver_frentes_membros
# ==========================================================================================
#
# Standardizes parliamentary front membership records from the Bronze layer and persists
# validated, deduplicated and analytics-ready relationship records into the Silver layer.
#
# Responsibilities:
# - Read parliamentary front member records from Bronze
# - Standardize front, deputy and membership attributes
# - Use JSON payload as fallback for missing attributes
# - Enrich members with Silver fronts and deputies when available
# - Validate mandatory front-member relationship fields
# - Remove technical duplicate records
# - Register rejected and discarded records for traceability
# - Persist curated Delta table
# - Apply governance comments to table and columns
#
# Notes:
# - Front attributes use the frn_ prefix
# - Deputy attributes use the dep_ prefix
# - Front-member relationship attributes use the frm_ prefix
# - The relationship grain is front + deputy
# - Missing party or UF does not invalidate the record
# - Missing front ID or deputy ID invalidates the relationship
# - Comments and documentation are written in English
# - Naming conventions follow Portuguese mnemonic standards
# ==========================================================================================


from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    regexp_replace,
    current_timestamp,
    row_number,
    when,
    coalesce,
    concat_ws,
    sha2,
    to_json,
    struct,
    get_json_object,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_extract

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
print("05 - SILVER FRENTES MEMBROS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ==========================================================================================
# 1. Global Configuration
# ==========================================================================================

NOTEBOOK_NAME = "05_silver_frentes_membros"
LAYER_NAME = "silver"
ENTITY_NAME = "frentes_membros"

SOURCE_TABLE = get_bronze_table(BRONZE_TABLES["frentes_membros"])
TARGET_TABLE = get_silver_table(SILVER_TABLES["frentes_membros"])
REJECTED_TABLE = get_silver_table(SILVER_TABLES["registros_rejeitados"])

SOURCE_FRENTES_TABLE = get_silver_table(SILVER_TABLES["frentes"])
SOURCE_DEPUTADOS_TABLE = get_silver_table(SILVER_TABLES["deputados"])

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
# 2. Start Pipeline Log
# ==========================================================================================

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver parliamentary front members standardization started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver parliamentary front members standardization.",
)

# COMMAND ----------

# ==========================================================================================
# 3. Helper Functions
# ==========================================================================================

def column_exists(dataframe, column_name):
    return column_name in dataframe.columns


def safe_col(dataframe, column_name, default_value=None):
    if column_exists(dataframe, column_name):
        return col(column_name)

    return lit(default_value)


def clean_text(column_expression):
    return trim(
        regexp_replace(
            upper(column_expression.cast("string")),
            r"\s+",
            " ",
        )
    )


def json_value(json_column, json_path):
    return get_json_object(col(json_column), json_path)


def first_payload_column(dataframe):
    candidate_columns = [
        "frm_tx_payload_json",
        "frn_mem_tx_payload_json",
        "fre_mem_tx_payload_json",
        "mem_tx_payload_json",
        "frn_tx_payload_json",
        "aud_tx_payload_json",
        "payload_json",
    ]

    for candidate_column in candidate_columns:
        if candidate_column in dataframe.columns:
            return candidate_column

    return None

# COMMAND ----------

# ==========================================================================================
# 4. Read Source Tables
# ==========================================================================================

bronze_df = spark.table(SOURCE_TABLE)
frentes_df = spark.table(SOURCE_FRENTES_TABLE)
deputados_df = spark.table(SOURCE_DEPUTADOS_TABLE)

records_read = bronze_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze parliamentary front members table loaded successfully "
        f"| records_read={records_read}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 5. Standardize Bronze Source Fields
# ==========================================================================================

payload_column = first_payload_column(bronze_df)

bronze_standardized_df = (
    bronze_df
    .withColumn(
        "frn_id_frente_raw",
        coalesce(
            safe_col(bronze_df, "frn_id_frente").cast("string"),
            safe_col(bronze_df, "frm_id_frente").cast("string"),
            safe_col(bronze_df, "idFrente").cast("string"),
            safe_col(bronze_df, "id_frente").cast("string"),
            safe_col(bronze_df, "frente_id").cast("string"),

            regexp_extract(
                safe_col(bronze_df, "aud_tx_endpoint_origem").cast("string"),
                r"/frentes/([0-9]+)/membros",
                1
            ),

            regexp_extract(
                safe_col(bronze_df, "aud_tx_arquivo_origem").cast("string"),
                r"frentes[_/-]([0-9]+)[_/-]membros",
                1
            ),

            regexp_extract(
                safe_col(bronze_df, "aud_tx_arquivo_origem").cast("string"),
                r"frentes_membros[_/-]([0-9]+)",
                1
            ),

            json_value(payload_column, "$.idFrente").cast("string") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.id_frente").cast("string") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.frente.id").cast("string") if payload_column else lit(None).cast("string")
        )
    )
    .withColumn(
        "frn_tx_titulo_raw",
        coalesce(
            safe_col(bronze_df, "frn_tx_titulo").cast("string"),
            safe_col(bronze_df, "frn_tx_nome").cast("string"),
            safe_col(bronze_df, "tituloFrente").cast("string"),
            safe_col(bronze_df, "nomeFrente").cast("string"),
            safe_col(bronze_df, "nome_frente").cast("string"),
            json_value(payload_column, "$.tituloFrente") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.nomeFrente") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.frente.titulo") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.frente.nome") if payload_column else lit(None).cast("string")
        )
    )
    .withColumn(
        "dep_id_deputado_raw",
        coalesce(
            safe_col(bronze_df, "dep_id_deputado").cast("string"),
            safe_col(bronze_df, "frm_id_deputado").cast("string"),
            safe_col(bronze_df, "frn_mem_id_deputado").cast("string"),
            safe_col(bronze_df, "mem_id_deputado").cast("string"),
            safe_col(bronze_df, "idDeputado").cast("string"),
            safe_col(bronze_df, "id_deputado").cast("string"),
            safe_col(bronze_df, "deputado_id").cast("string"),
            safe_col(bronze_df, "id").cast("string"),

            json_value(payload_column, "$.idDeputado").cast("string") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.id_deputado").cast("string") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.id").cast("string") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.deputado.id").cast("string") if payload_column else lit(None).cast("string")
        )
    )
    .withColumn(
        "dep_tx_nome_raw",
        coalesce(
            safe_col(bronze_df, "dep_tx_nome").cast("string"),
            safe_col(bronze_df, "dep_tx_nome_parlamentar").cast("string"),
            safe_col(bronze_df, "nome").cast("string"),
            safe_col(bronze_df, "nomeDeputado").cast("string"),
            safe_col(bronze_df, "nome_deputado").cast("string"),

            json_value(payload_column, "$.nomeDeputado") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.nome") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.deputado.nome") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.deputado.nomeParlamentar") if payload_column else lit(None).cast("string")
        )
    )
    .withColumn(
        "dep_tx_sigla_partido_raw",
        coalesce(
            safe_col(bronze_df, "dep_tx_sigla_partido").cast("string"),
            safe_col(bronze_df, "siglaPartido").cast("string"),
            safe_col(bronze_df, "sigla_partido").cast("string"),

            json_value(payload_column, "$.siglaPartido") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.sigla_partido") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.deputado.siglaPartido") if payload_column else lit(None).cast("string")
        )
    )
    .withColumn(
        "dep_tx_sigla_uf_raw",
        coalesce(
            safe_col(bronze_df, "dep_tx_sigla_uf").cast("string"),
            safe_col(bronze_df, "siglaUf").cast("string"),
            safe_col(bronze_df, "siglaUF").cast("string"),
            safe_col(bronze_df, "sigla_uf").cast("string"),

            json_value(payload_column, "$.siglaUf") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.siglaUF") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.sigla_uf") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.deputado.siglaUf") if payload_column else lit(None).cast("string")
        )
    )
    .withColumn(
        "frm_tx_cargo_raw",
        coalesce(
            safe_col(bronze_df, "frm_tx_cargo").cast("string"),
            safe_col(bronze_df, "frn_mem_tx_cargo").cast("string"),
            safe_col(bronze_df, "cargo").cast("string"),

            json_value(payload_column, "$.cargo") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.deputado.cargo") if payload_column else lit(None).cast("string")
        )
    )
    .withColumn(
        "frm_tx_condicao_raw",
        coalesce(
            safe_col(bronze_df, "frm_tx_condicao").cast("string"),
            safe_col(bronze_df, "frn_mem_tx_condicao").cast("string"),
            safe_col(bronze_df, "condicao").cast("string"),
            safe_col(bronze_df, "situacao").cast("string"),

            json_value(payload_column, "$.condicao") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.situacao") if payload_column else lit(None).cast("string"),
            json_value(payload_column, "$.deputado.condicao") if payload_column else lit(None).cast("string")
        )
    )
    .withColumn(
        "frm_tx_payload_json",
        (
            col(payload_column).cast("string")
            if payload_column
            else to_json(struct(*[col(c) for c in bronze_df.columns]))
        )
    )
    .withColumn(
        "aud_id_execucao_bronze",
        safe_col(bronze_df, "aud_id_execucao").cast("string")
    )
    .withColumn(
        "aud_dh_ingestao_bronze",
        safe_col(bronze_df, "aud_dh_ingestao")
    )
    .withColumn(
        "aud_tx_endpoint_origem_bronze",
        safe_col(bronze_df, "aud_tx_endpoint_origem").cast("string")
    )
    .withColumn(
        "aud_tx_sistema_origem_bronze",
        safe_col(bronze_df, "aud_tx_sistema_origem").cast("string")
    )
    .withColumn(
        "aud_tx_versao_pipeline_bronze",
        safe_col(bronze_df, "aud_tx_versao_pipeline").cast("string")
    )
    .withColumn(
        "aud_tx_tipo_carga_bronze",
        safe_col(bronze_df, "aud_tx_tipo_carga").cast("string")
    )
    .withColumn(
        "aud_tx_arquivo_origem_bronze",
        safe_col(bronze_df, "aud_tx_arquivo_origem").cast("string")
    )
    .withColumn(
        "aud_tx_hash_registro_bronze",
        safe_col(bronze_df, "aud_tx_hash_registro").cast("string")
    )
)

# COMMAND ----------

# ==========================================================================================
# 6. Normalize Textual and Key Fields
# ==========================================================================================

normalized_df = (
    bronze_standardized_df
    .withColumn("frn_id_frente", trim(col("frn_id_frente_raw").cast("string")))
    .withColumn("frn_tx_titulo", clean_text(col("frn_tx_titulo_raw")))
    .withColumn("dep_id_deputado", trim(col("dep_id_deputado_raw").cast("string")))
    .withColumn("dep_tx_nome", clean_text(col("dep_tx_nome_raw")))
    .withColumn("dep_tx_sigla_partido", clean_text(col("dep_tx_sigla_partido_raw")))
    .withColumn("dep_tx_sigla_uf", clean_text(col("dep_tx_sigla_uf_raw")))
    .withColumn("frm_tx_cargo", clean_text(col("frm_tx_cargo_raw")))
    .withColumn("frm_tx_condicao", clean_text(col("frm_tx_condicao_raw")))
    .withColumn(
        "frm_tx_cargo",
        when(
            col("frm_tx_cargo").isNull()
            | (trim(col("frm_tx_cargo")) == ""),
            lit("MEMBRO")
        ).otherwise(col("frm_tx_cargo"))
    )
    .withColumn(
        "frm_tx_condicao",
        when(
            col("frm_tx_condicao").isNull()
            | (trim(col("frm_tx_condicao")) == ""),
            lit("NAO_INFORMADO")
        ).otherwise(col("frm_tx_condicao"))
    )
)

# COMMAND ----------

# ==========================================================================================
# 7. Enrich with Silver Parliamentary Fronts
# ==========================================================================================

frentes_enrichment_df = (
    frentes_df
    .select(
        col("frn_id_frente").cast("string").alias("frn_id_frente_ref"),
        safe_col(frentes_df, "frn_tx_titulo", None).cast("string").alias("frn_tx_titulo_ref"),
        safe_col(frentes_df, "frn_tx_nome", None).cast("string").alias("frn_tx_nome_ref"),
        safe_col(frentes_df, "frn_tx_situacao", None).cast("string").alias("frn_tx_situacao"),
        safe_col(frentes_df, "frn_dt_criacao", None).alias("frn_dt_criacao"),
        safe_col(frentes_df, "frn_fl_registro_valido_silver", True).alias("frn_fl_registro_valido_silver"),
    )
    .dropDuplicates(["frn_id_frente_ref"])
)

front_enriched_df = (
    normalized_df.alias("mem")
    .join(
        frentes_enrichment_df.alias("frn"),
        col("mem.frn_id_frente") == col("frn.frn_id_frente_ref"),
        "left",
    )
    .withColumn(
        "frn_tx_titulo",
        coalesce(
            col("mem.frn_tx_titulo"),
            col("frn.frn_tx_titulo_ref"),
            col("frn.frn_tx_nome_ref"),
        )
    )
    .drop(
        "frn_id_frente_ref",
        "frn_tx_titulo_ref",
        "frn_tx_nome_ref",
    )
)

# COMMAND ----------

# ==========================================================================================
# 8. Enrich with Silver Deputies
# ==========================================================================================

deputados_enrichment_df = (
    deputados_df
    .select(
        col("dep_id_deputado").cast("string").alias("dep_id_deputado_ref"),
        safe_col(deputados_df, "dep_tx_nome", None).cast("string").alias("dep_tx_nome_ref"),
        safe_col(deputados_df, "dep_tx_nome_parlamentar", None).cast("string").alias("dep_tx_nome_parlamentar_ref"),
        safe_col(deputados_df, "dep_tx_sigla_partido", None).cast("string").alias("dep_tx_sigla_partido_ref"),
        safe_col(deputados_df, "dep_tx_sigla_uf", None).cast("string").alias("dep_tx_sigla_uf_ref"),
        safe_col(deputados_df, "dep_fl_registro_valido_silver", True).alias("dep_fl_registro_valido_silver"),
    )
    .dropDuplicates(["dep_id_deputado_ref"])
)

enriched_df = (
    front_enriched_df.alias("mem")
    .join(
        deputados_enrichment_df.alias("dep"),
        col("mem.dep_id_deputado") == col("dep.dep_id_deputado_ref"),
        "left",
    )
    .withColumn(
        "dep_tx_nome",
        coalesce(
            col("mem.dep_tx_nome"),
            col("dep.dep_tx_nome_ref"),
            col("dep.dep_tx_nome_parlamentar_ref"),
        )
    )
    .withColumn(
        "dep_tx_sigla_partido",
        coalesce(
            col("mem.dep_tx_sigla_partido"),
            col("dep.dep_tx_sigla_partido_ref"),
        )
    )
    .withColumn(
        "dep_tx_sigla_uf",
        coalesce(
            col("mem.dep_tx_sigla_uf"),
            col("dep.dep_tx_sigla_uf_ref"),
        )
    )
    .drop(
        "dep_id_deputado_ref",
        "dep_tx_nome_ref",
        "dep_tx_nome_parlamentar_ref",
        "dep_tx_sigla_partido_ref",
        "dep_tx_sigla_uf_ref",
    )
)

# COMMAND ----------

# ==========================================================================================
# 9. Analytical Derivations
# ==========================================================================================

derived_df = (
    enriched_df
    .withColumn(
        "frm_id_relacao",
        sha2(
            concat_ws(
                "||",
                coalesce(col("frn_id_frente"), lit("UNKNOWN_FRONT")),
                coalesce(col("dep_id_deputado"), lit("UNKNOWN_DEPUTY")),
            ),
            256,
        )
    )
    .withColumn(
        "frm_tx_tipo_participacao",
        when(col("frm_tx_cargo").like("%COORDEN%"), lit("COORDENADOR"))
        .when(col("frm_tx_cargo").like("%PRESIDENT%"), lit("PRESIDENTE"))
        .when(col("frm_tx_cargo").like("%VICE%"), lit("VICE"))
        .when(col("frm_tx_cargo").like("%SECRET%"), lit("SECRETARIO"))
        .otherwise(lit("MEMBRO"))
    )
    .withColumn(
        "frm_fl_coordenador",
        when(col("frm_tx_tipo_participacao") == "COORDENADOR", lit(True)).otherwise(lit(False))
    )
    .withColumn(
        "frm_fl_lideranca",
        when(
            col("frm_tx_tipo_participacao").isin(
                "COORDENADOR",
                "PRESIDENTE",
                "VICE",
                "SECRETARIO",
            ),
            lit(True),
        ).otherwise(lit(False))
    )
    .withColumn(
        "frm_fl_partido_informado",
        when(
            col("dep_tx_sigla_partido").isNotNull()
            & (trim(col("dep_tx_sigla_partido")) != ""),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "frm_fl_uf_informada",
        when(
            col("dep_tx_sigla_uf").isNotNull()
            & (trim(col("dep_tx_sigla_uf")) != ""),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "frm_fl_frente_identificada",
        when(
            col("frn_id_frente").isNotNull()
            & (trim(col("frn_id_frente")) != ""),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "frm_fl_deputado_identificado",
        when(
            col("dep_id_deputado").isNotNull()
            & (trim(col("dep_id_deputado")) != ""),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "frm_fl_frente_encontrada_silver",
        when(col("frn_fl_registro_valido_silver") == True, lit(True)).otherwise(lit(False))
    )
    .withColumn(
        "frm_fl_deputado_encontrado_silver",
        when(col("dep_fl_registro_valido_silver") == True, lit(True)).otherwise(lit(False))
    )
)

# COMMAND ----------

# ==========================================================================================
# 10. Quality Rules
# ==========================================================================================

quality_df = (
    derived_df
    .withColumn(
        "frm_fl_id_relacao_valido",
        when(
            col("frm_id_relacao").isNotNull()
            & (trim(col("frm_id_relacao")) != ""),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "frm_fl_registro_valido_silver",
        (
            col("frm_fl_id_relacao_valido")
            & col("frm_fl_frente_identificada")
            & col("frm_fl_deputado_identificado")
        )
    )
    .withColumn(
        "frm_tx_motivo_rejeicao",
        when(
            ~col("frm_fl_frente_identificada"),
            lit("FRM_FRENTE_NULA_OU_VAZIA")
        )
        .when(
            ~col("frm_fl_deputado_identificado"),
            lit("FRM_DEPUTADO_NULO_OU_VAZIO")
        )
        .when(
            ~col("frm_fl_id_relacao_valido"),
            lit("FRM_ID_RELACAO_INVALIDO")
        )
        .otherwise(lit(None).cast(StringType()))
    )
)

# COMMAND ----------

# ==========================================================================================
# 11. Build Mandatory Rejected Records
# ==========================================================================================

mandatory_rejected_source_df = (
    quality_df
    .filter(col("frm_fl_registro_valido_silver") == False)
)

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=mandatory_rejected_source_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="frm_id_relacao",
    validation_rule_column="frm_tx_motivo_rejeicao",
    payload_column="frm_tx_payload_json",
    valid_flag_column="frm_fl_registro_valido_silver",
)

# COMMAND ----------

# ==========================================================================================
# 12. Keep Valid Records
# ==========================================================================================

valid_df = (
    quality_df
    .filter(col("frm_fl_registro_valido_silver") == True)
)

# COMMAND ----------

# ==========================================================================================
# 13. Technical Deduplication
# ==========================================================================================

dedup_window = (
    Window
    .partitionBy("frm_id_relacao")
    .orderBy(
        col("aud_dh_ingestao_bronze").desc_nulls_last()
    )
)

dedup_df = (
    valid_df
    .withColumn("rn_deduplicacao", row_number().over(dedup_window))
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=dedup_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="frm_id_relacao",
    payload_column="frm_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="FRM_REGISTRO_DUPLICADO",
    observation="Duplicate parliamentary front member relationship removed keeping latest available record.",
)

silver_df = (
    dedup_df
    .filter(col("rn_deduplicacao") == 1)
    .drop("rn_deduplicacao")
    .drop("frm_tx_motivo_rejeicao")
)

# COMMAND ----------

# ==========================================================================================
# 14. Persist Rejected Records
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
        f"Rejected and discarded parliamentary front member records persisted "
        f"| records_rejected={records_rejected}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 15. Add Silver Traceability Columns
# ==========================================================================================

silver_df = (
    silver_df
    .withColumn("aud_id_execucao_silver", lit(execution_id))
    .withColumn("aud_dh_processamento", current_timestamp())
    .withColumn("aud_tx_camada_origem", lit("bronze"))
    .withColumn("aud_tx_tabela_origem", lit(SOURCE_TABLE))
    .withColumn("aud_tx_tabela_destino", lit(TARGET_TABLE))
    .withColumn("aud_tx_versao_pipeline_silver", lit(PROJECT_VERSION))
    .withColumn(
        "aud_tx_regra_derivacao",
        lit(
            "Parliamentary front membership relationship standardized from Bronze, "
            "enriched with Silver fronts and deputies when available, validated at front plus deputy grain."
        )
    )
)

# COMMAND ----------

# ==========================================================================================
# 16. Add Silver Hash
# ==========================================================================================

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "frm_id_relacao",
        "frn_id_frente",
        "dep_id_deputado",
        "dep_tx_sigla_partido",
        "dep_tx_sigla_uf",
        "frm_tx_tipo_participacao",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# ==========================================================================================
# 17. Select Final Columns
# ==========================================================================================

final_columns = [
    "frm_id_relacao",

    "frn_id_frente",
    "frn_tx_titulo",
    "frn_tx_situacao",
    "frn_dt_criacao",

    "dep_id_deputado",
    "dep_tx_nome",
    "dep_tx_sigla_partido",
    "dep_tx_sigla_uf",

    "frm_tx_cargo",
    "frm_tx_condicao",
    "frm_tx_tipo_participacao",

    "frm_fl_coordenador",
    "frm_fl_lideranca",
    "frm_fl_partido_informado",
    "frm_fl_uf_informada",
    "frm_fl_frente_identificada",
    "frm_fl_deputado_identificado",
    "frm_fl_frente_encontrada_silver",
    "frm_fl_deputado_encontrado_silver",
    "frm_fl_id_relacao_valido",
    "frm_fl_registro_valido_silver",

    "frm_tx_payload_json",

    "aud_id_execucao_bronze",
    "aud_dh_ingestao_bronze",
    "aud_tx_endpoint_origem_bronze",
    "aud_tx_sistema_origem_bronze",
    "aud_tx_versao_pipeline_bronze",
    "aud_tx_tipo_carga_bronze",
    "aud_tx_arquivo_origem_bronze",
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

existing_final_columns = [
    column_name
    for column_name in final_columns
    if column_name in silver_df.columns
]

silver_df = silver_df.select(*existing_final_columns)

# COMMAND ----------

# ==========================================================================================
# 18. Persist Silver Table
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
        f"Silver parliamentary front members table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 19. Apply Governance Comments
# ==========================================================================================

table_comment = """
Standardized parliamentary front members table in the Silver layer.

This table contains validated and deduplicated relationships between parliamentary fronts
and deputies. It enriches member records with front and deputy contextual attributes when
available, preserves Bronze lineage, and supports downstream analytics for parliamentary
front participation.

Known modeling note:
- The table does not contain deputy legislature.
- Deputy enrichment uses the most recent available deputy dimension record when applicable.
- Some historical deputies may not exist in slv_deputados and must be handled analytically
  in the Gold layer when broader historical coverage is required.
"""

column_comments = {
    "frm_id_relacao": "Deterministic relationship identifier between parliamentary front and deputy.",
    "frn_id_frente": "Parliamentary front identifier.",
    "frn_tx_titulo": "Standardized parliamentary front title.",
    "dep_id_deputado": "Deputy identifier associated with the parliamentary front membership.",
    "dep_tx_nome": "Standardized deputy name when available.",
    "dep_tx_sigla_partido": "Deputy political party acronym when available from the deputy dimension.",
    "dep_tx_sigla_uf": "Deputy federation unit acronym.",
    "frm_tx_tipo_participacao": "Type of participation in the parliamentary front.",
    "frm_fl_frente_identificada": "Flag indicating whether the front identifier was available.",
    "frm_fl_deputado_identificado": "Flag indicating whether the deputy identifier was available.",
    "frm_fl_frente_encontrada_silver": "Flag indicating whether the front was found in slv_frentes.",
    "frm_fl_deputado_encontrado_silver": "Flag indicating whether the deputy was found in slv_deputados.",
    "frm_fl_registro_valido_silver": "Flag indicating whether the record passed Silver validation.",
    "frm_tx_payload_json": "Original Bronze payload preserved for traceability.",
    "aud_id_execucao_bronze": "Bronze execution identifier.",
    "aud_dh_ingestao_bronze": "Bronze ingestion timestamp.",
    "aud_tx_endpoint_origem_bronze": "Bronze source endpoint.",
    "aud_tx_sistema_origem_bronze": "Bronze source system.",
    "aud_tx_versao_pipeline_bronze": "Bronze pipeline version.",
    "aud_tx_tipo_carga_bronze": "Bronze load type.",
    "aud_tx_arquivo_origem_bronze": "Bronze source file when applicable.",
    "aud_tx_hash_registro_bronze": "Bronze deterministic record hash.",
    "aud_id_execucao_silver": "Silver execution identifier.",
    "aud_dh_processamento": "Silver processing timestamp.",
    "aud_tx_camada_origem": "Source Medallion layer.",
    "aud_tx_tabela_origem": "Source Bronze table.",
    "aud_tx_tabela_destino": "Target Silver table.",
    "aud_tx_versao_pipeline_silver": "Silver pipeline version.",
    "aud_tx_regra_derivacao": "Description of the derivation and enrichment rule applied.",
    "aud_tx_hash_registro_silver": "Silver deterministic record hash.",

    "frn_tx_situacao":"Current status of the parliamentary front as provided by the source.",
    "frn_dt_criacao":  "Original creation date of the parliamentary front.",
    "frm_tx_cargo": "Role held by the deputy within the parliamentary front.",
    "frm_tx_condicao": "Membership condition or status within the parliamentary front.",
    "frm_fl_coordenador": "Flag indicating whether the deputy acts as parliamentary front coordinator.",
    "frm_fl_lideranca": "Flag indicating whether the deputy holds a leadership position in the parliamentary front.",
    "frm_fl_partido_informado": "Flag indicating whether political party information was available in the source record.",
    "frm_fl_uf_informada": "Flag indicating whether federation unit information was available in the source record.",
    "frm_fl_id_relacao_valido":  "Flag indicating whether the deterministic membership relationship identifier was successfully generated."
}

apply_governance_comments(
    table_name="brazil_legislative_analytics.silver.slv_frentes_membros",
    table_comment=table_comment,
    column_comments=column_comments,
)

# COMMAND ----------

# ==========================================================================================
# 20. Final Pipeline Log
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
        f"Silver parliamentary front members standardization completed successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| records_rejected={records_rejected}"
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
        f"Silver parliamentary front members standardization completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER FRENTES MEMBROS COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)