# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Silver — Frentes Parlamentares Standardization
# MAGIC
# MAGIC **Notebook:** `04_silver_frentes`
# MAGIC
# MAGIC Standardizes parliamentary front records from the Bronze layer and persists
# MAGIC validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Parliamentary front schema normalization rules
# MAGIC - Front identifier standardization logic
# MAGIC - Legislature extraction from Bronze JSON payload
# MAGIC - Project legislature scope validation using global utilities
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
# MAGIC - Read parliamentary front data from Bronze layer
# MAGIC - Standardize front identifiers, titles, URI and legislature attributes
# MAGIC - Extract legislature identifier from `frn_tx_payload_json`
# MAGIC - Keep only legislatures supported by the project scope
# MAGIC - Normalize textual fields
# MAGIC - Validate mandatory parliamentary front fields
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
# MAGIC - Legislature is extracted from the Bronze payload JSON field `idLegislatura`
# MAGIC - Only legislatures configured in `utils_legislature` are kept in Silver
# MAGIC - Front identifier, title and supported legislature are mandatory for analytical use
# MAGIC - Records outside the project legislature scope are redirected to `slv_registros_rejeitados`
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

# ==========================================================================================
# 04 Silver — Frentes Parlamentares Standardization
# Notebook: 04_silver_frentes
# ==========================================================================================
#
# Standardizes parliamentary front records from the Bronze layer and persists validated,
# deduplicated and analytics-ready records into the Silver layer.
#
# This notebook defines:
# - Parliamentary front schema normalization rules
# - Front identifier standardization logic
# - Complementary minimal front derivation from Bronze front members
# - Referential coverage support for slv_frentes_membros
# - Text normalization rules
# - Quality validation rules
# - Rejected records tracking using global utilities
# - Technical duplicate tracking
# - Silver Delta persistence logic
# - Governance comments using global utilities
#
# Notes:
# - The main source is br_frentes
# - Additional front identifiers may be derived from br_frentes_membros
# - Derived records are kept with a controlled placeholder title
# - Derived records are flagged as incomplete source records
# - This prevents valid member relationships from becoming orphan references
# ==========================================================================================




# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    upper,
    lower,
    trim,
    when,
    coalesce,
    to_json,
    struct,
    row_number,
    concat_ws,
    regexp_replace,
    regexp_extract,
    count,
    desc,
)
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.window import Window

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
print("04 - SILVER FRENTES")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ==========================================================================================
# 1. Global Configuration
# ==========================================================================================

NOTEBOOK_NAME = "04_silver_frentes"
LAYER_NAME = "silver"
ENTITY_NAME = "frentes"

SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES["frentes"]
)

MEMBERS_SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES.get("frentes_membros", "br_frentes_membros")
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES["frentes"]
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
records_members_read = None
records_written = None
records_rejected = None
records_derived = None

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
    message="Silver frentes standardization started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver frentes standardization.",
)

# COMMAND ----------

# ==========================================================================================
# 4. Read Bronze Tables
# ==========================================================================================

bronze_df = spark.table(SOURCE_TABLE)

records_read = bronze_df.count()

if table_exists(MEMBERS_SOURCE_TABLE):
    bronze_members_df = spark.table(MEMBERS_SOURCE_TABLE)
    records_members_read = bronze_members_df.count()
else:
    bronze_members_df = None
    records_members_read = 0

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze frentes loaded successfully "
        f"| records_read={records_read} "
        f"| members_records_read={records_members_read}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 5. Standardize Main Front Records
# ==========================================================================================

main_frentes_df = (
    bronze_df
    .select(
        trim(safe_col(bronze_df, "frn_id_frente").cast(StringType())).alias("frn_id_frente"),

        clean_upper(
            coalesce(
                safe_col(bronze_df, "frn_tx_titulo").cast(StringType()),
                safe_col(bronze_df, "frn_tx_nome").cast(StringType()),
            )
        ).alias("frn_tx_titulo"),

        trim(safe_col(bronze_df, "frn_tx_uri").cast(StringType())).alias("frn_tx_uri"),

        trim(
            coalesce(
                safe_col(bronze_df, "frn_id_legislatura").cast(StringType()),
                safe_col(bronze_df, "leg_id_legislatura").cast(StringType()),
            )
        ).alias("leg_id_legislatura"),

        clean_upper(safe_col(bronze_df, "frn_tx_situacao").cast(StringType())).alias("frn_tx_situacao"),

        trim(safe_col(bronze_df, "frn_id_coordenador").cast(StringType())).alias("frn_id_coordenador"),
        trim(safe_col(bronze_df, "frn_tx_uri_coordenador").cast(StringType())).alias("frn_tx_uri_coordenador"),
        clean_upper(safe_col(bronze_df, "frn_tx_nome_coordenador").cast(StringType())).alias("frn_tx_nome_coordenador"),
        clean_upper(safe_col(bronze_df, "frn_tx_sigla_partido_coordenador").cast(StringType())).alias("frn_tx_sigla_partido_coordenador"),
        trim(safe_col(bronze_df, "frn_tx_uri_partido_coordenador").cast(StringType())).alias("frn_tx_uri_partido_coordenador"),
        clean_upper(safe_col(bronze_df, "frn_tx_sigla_uf_coordenador").cast(StringType())).alias("frn_tx_sigla_uf_coordenador"),
        trim(safe_col(bronze_df, "frn_id_legislatura_coordenador").cast(StringType())).alias("frn_id_legislatura_coordenador"),
        trim(safe_col(bronze_df, "frn_tx_url_foto_coordenador").cast(StringType())).alias("frn_tx_url_foto_coordenador"),

        clean_lower(safe_col(bronze_df, "frn_tx_email").cast(StringType())).alias("frn_tx_email"),
        trim(safe_col(bronze_df, "frn_tx_telefone").cast(StringType())).alias("frn_tx_telefone"),
        clean_upper(safe_col(bronze_df, "frn_tx_keywords").cast(StringType())).alias("frn_tx_keywords"),
        trim(safe_col(bronze_df, "frn_tx_url_website").cast(StringType())).alias("frn_tx_url_website"),
        trim(safe_col(bronze_df, "frn_tx_url_documento").cast(StringType())).alias("frn_tx_url_documento"),

        safe_col(bronze_df, "frn_tx_payload_json").cast(StringType()).alias("frn_tx_payload_json"),

        safe_col(bronze_df, "aud_id_execucao").cast(StringType()).alias("aud_id_execucao_bronze"),
        safe_col(bronze_df, "aud_dh_ingestao").cast(TimestampType()).alias("aud_dh_ingestao_bronze"),
        safe_col(bronze_df, "aud_tx_endpoint_origem").cast(StringType()).alias("aud_tx_endpoint_origem_bronze"),
        safe_col(bronze_df, "aud_tx_sistema_origem").cast(StringType()).alias("aud_tx_sistema_origem_bronze"),
        safe_col(bronze_df, "aud_tx_versao_pipeline").cast(StringType()).alias("aud_tx_versao_pipeline_bronze"),
        safe_col(bronze_df, "aud_tx_tipo_carga").cast(StringType()).alias("aud_tx_tipo_carga_bronze"),
        safe_col(bronze_df, "aud_tx_hash_registro").cast(StringType()).alias("aud_tx_hash_registro_bronze"),
    )
    .withColumn("frn_fl_registro_derivado_membros", lit(False))
    .withColumn("frn_fl_cadastro_completo", lit(True))
    .withColumn("frn_tx_origem_registro", lit("br_frentes"))
)

# COMMAND ----------

# ==========================================================================================
# 6. Derive Missing Front Records from Bronze Front Members
# ==========================================================================================

if bronze_members_df is not None:

    member_front_ids_df = (
        bronze_members_df
        .withColumn(
            "frn_id_frente",
            coalesce(
                trim(safe_col(bronze_members_df, "frn_id_frente").cast(StringType())),
                trim(safe_col(bronze_members_df, "frm_id_frente").cast(StringType())),
                trim(safe_col(bronze_members_df, "idFrente").cast(StringType())),
                trim(safe_col(bronze_members_df, "id_frente").cast(StringType())),
                regexp_extract(
                    safe_col(bronze_members_df, "aud_tx_endpoint_origem").cast(StringType()),
                    r"/frentes/([0-9]+)/membros",
                    1,
                ),
                regexp_extract(
                    safe_col(bronze_members_df, "aud_tx_arquivo_origem").cast(StringType()),
                    r"frentes[_/-]([0-9]+)[_/-]membros",
                    1,
                ),
            )
        )
        .filter(col("frn_id_frente").isNotNull())
        .filter(trim(col("frn_id_frente")) != "")
        .select("frn_id_frente")
        .distinct()
    )

    existing_front_ids_df = (
        main_frentes_df
        .select("frn_id_frente")
        .filter(col("frn_id_frente").isNotNull())
        .filter(trim(col("frn_id_frente")) != "")
        .distinct()
    )

    derived_frentes_df = (
        member_front_ids_df.alias("mem")
        .join(
            existing_front_ids_df.alias("frn"),
            col("mem.frn_id_frente") == col("frn.frn_id_frente"),
            "left_anti",
        )
        .withColumn(
            "frn_tx_titulo",
            concat_ws(
                "",
                lit("FRENTE PARLAMENTAR "),
                col("frn_id_frente"),
                lit(" - CADASTRO NÃO LOCALIZADO NA FONTE DE FRENTES"),
            )
        )
        .withColumn(
            "frn_tx_uri",
            concat_ws(
                "",
                lit("https://dadosabertos.camara.leg.br/api/v2/frentes/"),
                col("frn_id_frente"),
            )
        )
        .withColumn("leg_id_legislatura", lit(None).cast(StringType()))
        .withColumn("frn_tx_situacao", lit(None).cast(StringType()))
        .withColumn("frn_id_coordenador", lit(None).cast(StringType()))
        .withColumn("frn_tx_uri_coordenador", lit(None).cast(StringType()))
        .withColumn("frn_tx_nome_coordenador", lit(None).cast(StringType()))
        .withColumn("frn_tx_sigla_partido_coordenador", lit(None).cast(StringType()))
        .withColumn("frn_tx_uri_partido_coordenador", lit(None).cast(StringType()))
        .withColumn("frn_tx_sigla_uf_coordenador", lit(None).cast(StringType()))
        .withColumn("frn_id_legislatura_coordenador", lit(None).cast(StringType()))
        .withColumn("frn_tx_url_foto_coordenador", lit(None).cast(StringType()))
        .withColumn("frn_tx_email", lit(None).cast(StringType()))
        .withColumn("frn_tx_telefone", lit(None).cast(StringType()))
        .withColumn("frn_tx_keywords", lit(None).cast(StringType()))
        .withColumn("frn_tx_url_website", lit(None).cast(StringType()))
        .withColumn("frn_tx_url_documento", lit(None).cast(StringType()))
        .withColumn("frn_tx_payload_json", to_json(struct(col("frn_id_frente"))))
        .withColumn("aud_id_execucao_bronze", lit(None).cast(StringType()))
        .withColumn("aud_dh_ingestao_bronze", lit(None).cast(TimestampType()))
        .withColumn("aud_tx_endpoint_origem_bronze", lit(MEMBERS_SOURCE_TABLE))
        .withColumn("aud_tx_sistema_origem_bronze", lit("derived_from_front_members"))
        .withColumn("aud_tx_versao_pipeline_bronze", lit(None).cast(StringType()))
        .withColumn("aud_tx_tipo_carga_bronze", lit("DERIVED_REFERENCE"))
        .withColumn("aud_tx_hash_registro_bronze", lit(None).cast(StringType()))
        .withColumn("frn_fl_registro_derivado_membros", lit(True))
        .withColumn("frn_fl_cadastro_completo", lit(False))
        .withColumn("frn_tx_origem_registro", lit("derived_from_br_frentes_membros"))
        .select(main_frentes_df.columns)
    )

else:

    derived_frentes_df = spark.createDataFrame(
        [],
        main_frentes_df.schema,
    )

records_derived = derived_frentes_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Derived missing front references from members "
        f"| records_derived={records_derived}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 7. Union Main and Derived Front Records
# ==========================================================================================

standardized_df = (
    main_frentes_df
    .unionByName(
        derived_frentes_df,
        allowMissingColumns=True,
    )
)

# COMMAND ----------

# ==========================================================================================
# 8. Apply Quality Rules and Thematic Flags
# ==========================================================================================

validated_df = (
    standardized_df
    .withColumn(
        "frn_fl_id_valido",
        (
            col("frn_id_frente").isNotNull()
            & (trim(col("frn_id_frente")) != "")
        ).cast("boolean"),
    )
    .withColumn(
        "frn_fl_titulo_valido",
        (
            col("frn_tx_titulo").isNotNull()
            & (trim(col("frn_tx_titulo")) != "")
        ).cast("boolean"),
    )
    .withColumn(
        "frn_fl_legislatura_informada",
        (
            col("leg_id_legislatura").isNotNull()
            & (trim(col("leg_id_legislatura")) != "")
        ).cast("boolean"),
    )
    .withColumn(
        "frn_fl_legislatura_escopo_projeto",
        when(
            col("leg_id_legislatura").isin([str(x) for x in REFERENCE_LEGISLATURES]),
            lit(True),
        )
        .when(
            col("frn_fl_registro_derivado_membros") == True,
            lit(False),
        )
        .otherwise(lit(False))
        .cast("boolean"),
    )
    .withColumn(
        "frn_fl_uri_informada",
        (
            col("frn_tx_uri").isNotNull()
            & (trim(col("frn_tx_uri")) != "")
        ).cast("boolean"),
    )
    .withColumn(
        "frn_fl_registro_valido_silver",
        (
            col("frn_fl_id_valido")
            & col("frn_fl_titulo_valido")
            & col("frn_fl_uri_informada")
        ).cast("boolean"),
    )
    .withColumn(
        "frn_tx_chave_deduplicacao",
        col("frn_id_frente"),
    )
    .withColumn(
        "frn_tx_motivo_rejeicao",
        when(~col("frn_fl_id_valido"), lit("FRN_ID_NULO_OU_VAZIO"))
        .when(~col("frn_fl_titulo_valido"), lit("FRN_TITULO_NULO_OU_VAZIO"))
        .when(~col("frn_fl_uri_informada"), lit("FRN_URI_NULA_OU_VAZIA"))
        .otherwise(lit(None).cast(StringType())),
    )
)

theme_text_col = concat_ws(
    " ",
    col("frn_tx_titulo"),
    col("frn_tx_keywords"),
)

validated_df = (
    validated_df
    .withColumn(
        "frn_fl_tema_saude",
        when(
            theme_text_col.contains("SAÚDE")
            | theme_text_col.contains("SAUDE"),
            lit(1),
        ).otherwise(lit(0)),
    )
    .withColumn(
        "frn_fl_tema_educacao",
        when(
            theme_text_col.contains("EDUCA"),
            lit(1),
        ).otherwise(lit(0)),
    )
    .withColumn(
        "frn_fl_tema_seguranca",
        when(
            theme_text_col.contains("SEGURAN"),
            lit(1),
        ).otherwise(lit(0)),
    )
    .withColumn(
        "frn_fl_tema_agro",
        when(
            theme_text_col.contains("AGRO")
            | theme_text_col.contains("RURAL"),
            lit(1),
        ).otherwise(lit(0)),
    )
    .withColumn(
        "frn_fl_tema_mulher",
        when(
            theme_text_col.contains("MULHER"),
            lit(1),
        ).otherwise(lit(0)),
    )
    .withColumn(
        "frn_fl_tema_meio_ambiente",
        when(
            theme_text_col.contains("AMBIENT")
            | theme_text_col.contains("SUSTENT"),
            lit(1),
        ).otherwise(lit(0)),
    )
)

# COMMAND ----------

# ==========================================================================================
# 9. Build Mandatory Rejected Records
# ==========================================================================================

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=validated_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="frn_tx_chave_deduplicacao",
    validation_rule_column="frn_tx_motivo_rejeicao",
    payload_column="frn_tx_payload_json",
    valid_flag_column="frn_fl_registro_valido_silver",
)

# COMMAND ----------

# ==========================================================================================
# 10. Identify Technical Duplicates
# ==========================================================================================

valid_candidates_df = (
    validated_df
    .filter(col("frn_fl_registro_valido_silver") == True)
    .drop("frn_tx_motivo_rejeicao")
)

dedup_window = (
    Window
    .partitionBy("frn_id_frente")
    .orderBy(
        col("frn_fl_cadastro_completo").desc(),
        col("aud_dh_ingestao_bronze").desc_nulls_last(),
    )
)

valid_ranked_df = (
    valid_candidates_df
    .withColumn("rn_deduplicacao", row_number().over(dedup_window))
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=valid_ranked_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="frn_tx_chave_deduplicacao",
    payload_column="frn_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="FRN_REGISTRO_DUPLICADO",
    observation=(
        "Record kept only once by frn_id_frente. "
        "Complete br_frentes records are prioritized over derived member references."
    ),
)

silver_dedup_df = (
    valid_ranked_df
    .filter(col("rn_deduplicacao") == 1)
    .drop("rn_deduplicacao")
)

# COMMAND ----------

# ==========================================================================================
# 11. Persist Rejected Records
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
        f"Rejected and discarded frentes records persisted "
        f"| records_rejected={records_rejected}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 12. Add Silver Traceability Columns
# ==========================================================================================

silver_df = (
    silver_dedup_df
    .withColumn("aud_id_execucao_silver", lit(execution_id))
    .withColumn("aud_dh_processamento", current_timestamp())
    .withColumn("aud_tx_camada_origem", lit("bronze"))
    .withColumn("aud_tx_tabela_origem", lit(SOURCE_TABLE))
    .withColumn("aud_tx_tabela_destino", lit(TARGET_TABLE))
    .withColumn("aud_tx_versao_pipeline_silver", lit(PROJECT_VERSION))
    .withColumn(
        "aud_tx_regra_derivacao",
        when(
            col("frn_fl_registro_derivado_membros") == True,
            lit(
                "Minimal parliamentary front reference derived from Bronze front members "
                "to preserve referential coverage for member relationships."
            ),
        ).otherwise(
            lit(
                "Parliamentary front standardized from Bronze front source with complete "
                "source attributes when available."
            )
        ),
    )
)

# COMMAND ----------

# ==========================================================================================
# 13. Add Silver Record Hash
# ==========================================================================================

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "frn_id_frente",
        "frn_tx_titulo",
        "frn_tx_uri",
        "leg_id_legislatura",
        "frn_tx_situacao",
        "frn_id_coordenador",
        "frn_fl_registro_derivado_membros",
        "frn_fl_cadastro_completo",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# ==========================================================================================
# 14. Select Final Silver Columns
# ==========================================================================================

final_columns = [
    "frn_id_frente",
    "frn_tx_titulo",
    "frn_tx_uri",
    "leg_id_legislatura",
    "frn_tx_situacao",
    "frn_id_coordenador",
    "frn_tx_uri_coordenador",
    "frn_tx_nome_coordenador",
    "frn_tx_sigla_partido_coordenador",
    "frn_tx_uri_partido_coordenador",
    "frn_tx_sigla_uf_coordenador",
    "frn_id_legislatura_coordenador",
    "frn_tx_url_foto_coordenador",
    "frn_tx_email",
    "frn_tx_telefone",
    "frn_tx_keywords",
    "frn_tx_url_website",
    "frn_tx_url_documento",
    "frn_fl_id_valido",
    "frn_fl_titulo_valido",
    "frn_fl_legislatura_informada",
    "frn_fl_legislatura_escopo_projeto",
    "frn_fl_uri_informada",
    "frn_fl_registro_derivado_membros",
    "frn_fl_cadastro_completo",
    "frn_fl_registro_valido_silver",
    "frn_fl_tema_saude",
    "frn_fl_tema_educacao",
    "frn_fl_tema_seguranca",
    "frn_fl_tema_agro",
    "frn_fl_tema_mulher",
    "frn_fl_tema_meio_ambiente",
    "frn_tx_origem_registro",
    "frn_tx_payload_json",
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

# COMMAND ----------

# ==========================================================================================
# 15. Persist Silver Table
# ==========================================================================================

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

records_written = spark.table(TARGET_TABLE).count()

log_success(
    pipeline_logger=logger,
    message=(
        f"Silver frentes persisted successfully "
        f"| records_written={records_written} "
        f"| records_rejected={records_rejected} "
        f"| records_derived={records_derived}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 16. Apply Governance Comments
# ==========================================================================================

table_comment = """
Standardized parliamentary fronts table in the Silver layer.

This table contains validated, deduplicated and enriched parliamentary front records.
It preserves source front attributes, thematic analytical flags and Bronze-to-Silver
traceability metadata.

This table also includes minimal derived front references from Bronze front-member
relationships when a front identifier exists in members but is missing from the main
front source table. These derived records are explicitly flagged as incomplete source
records and are kept to preserve referential coverage for downstream analytics.
"""

column_comments = {
    "frn_id_frente": "Parliamentary front identifier.",
    "frn_tx_titulo": "Standardized parliamentary front title or controlled placeholder for derived records.",
    "frn_tx_uri": "Parliamentary front URI.",
    "leg_id_legislatura": "Legislature identifier associated with the parliamentary front when available.",
    "frn_tx_situacao": "Parliamentary front status description.",
    "frn_id_coordenador": "Coordinator deputy identifier.",
    "frn_tx_uri_coordenador": "Coordinator deputy URI.",
    "frn_tx_nome_coordenador": "Coordinator deputy name.",
    "frn_tx_sigla_partido_coordenador": "Coordinator deputy political party acronym.",
    "frn_tx_uri_partido_coordenador": "Coordinator deputy political party URI.",
    "frn_tx_sigla_uf_coordenador": "Coordinator deputy federation unit acronym.",
    "frn_id_legislatura_coordenador": "Coordinator deputy legislature identifier.",
    "frn_tx_url_foto_coordenador": "Coordinator deputy photo URL.",
    "frn_tx_email": "Parliamentary front contact email.",
    "frn_tx_telefone": "Parliamentary front contact phone number.",
    "frn_tx_keywords": "Keywords associated with the parliamentary front.",
    "frn_tx_url_website": "Parliamentary front website URL.",
    "frn_tx_url_documento": "Parliamentary front document URL.",
    "frn_fl_id_valido": "Flag indicating whether the parliamentary front identifier is valid.",
    "frn_fl_titulo_valido": "Flag indicating whether the parliamentary front title is valid.",
    "frn_fl_legislatura_informada": "Flag indicating whether the legislature is informed.",
    "frn_fl_legislatura_escopo_projeto": "Flag indicating whether the legislature belongs to the project scope.",
    "frn_fl_uri_informada": "Flag indicating whether the parliamentary front URI is informed.",
    "frn_fl_registro_derivado_membros": "Flag indicating whether the front was minimally derived from front-member relationships.",
    "frn_fl_cadastro_completo": "Flag indicating whether the front came from the complete front source table.",
    "frn_fl_registro_valido_silver": "Flag indicating whether the record passed Silver validation rules.",
    "frn_fl_tema_saude": "Analytical flag indicating whether the front is related to health.",
    "frn_fl_tema_educacao": "Analytical flag indicating whether the front is related to education.",
    "frn_fl_tema_seguranca": "Analytical flag indicating whether the front is related to security.",
    "frn_fl_tema_agro": "Analytical flag indicating whether the front is related to agriculture or rural topics.",
    "frn_fl_tema_mulher": "Analytical flag indicating whether the front is related to women topics.",
    "frn_fl_tema_meio_ambiente": "Analytical flag indicating whether the front is related to environment or sustainability.",
    "frn_tx_origem_registro": "Record origin classification within Silver transformation.",
    "frn_tx_payload_json": "Original raw payload preserved from Bronze or generated minimal payload for derived records.",
    "aud_id_execucao_bronze": "Bronze execution identifier.",
    "aud_dh_ingestao_bronze": "Bronze ingestion timestamp.",
    "aud_tx_endpoint_origem_bronze": "Bronze source endpoint or CSV fallback source.",
    "aud_tx_sistema_origem_bronze": "Bronze source system.",
    "aud_tx_versao_pipeline_bronze": "Bronze pipeline version.",
    "aud_tx_tipo_carga_bronze": "Bronze load type.",
    "aud_tx_hash_registro_bronze": "Bronze deterministic record hash.",
    "aud_id_execucao_silver": "Silver execution identifier.",
    "aud_dh_processamento": "Silver processing timestamp.",
    "aud_tx_camada_origem": "Source layer name.",
    "aud_tx_tabela_origem": "Source table name.",
    "aud_tx_tabela_destino": "Target table name.",
    "aud_tx_versao_pipeline_silver": "Silver pipeline version.",
    "aud_tx_regra_derivacao": "Textual description of the derivation rule used for the record.",
    "aud_tx_hash_registro_silver": "Silver deterministic record hash.",
}

if APPLY_GOVERNANCE_COMMENTS:
    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

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
        f"Silver frentes completed successfully "
        f"| records_read={records_read} "
        f"| members_records_read={records_members_read} "
        f"| records_derived={records_derived} "
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
        f"Silver frentes standardization completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER FRENTES COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Members Source Table: {MEMBERS_SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"Records Read: {records_read}")
print(f"Members Records Read: {records_members_read}")
print(f"Records Derived From Members: {records_derived}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)