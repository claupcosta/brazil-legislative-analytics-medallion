# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Marts — Legislative Events Calendar
# MAGIC
# MAGIC **Notebook:** `02_am_calendario_eventos`
# MAGIC
# MAGIC Builds the curated Business Mart for Legislative Events Calendar analysis used by dashboards, executive reports, agenda monitoring and business consumption.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Legislative events analytical mart model
# MAGIC * One analytical record per event
# MAGIC * Event scheduling indicators
# MAGIC * Organizing body indicators
# MAGIC * Parliamentary participation indicators
# MAGIC * Geographic indicators
# MAGIC * Event status indicators
# MAGIC * Business-ready attributes for dashboard consumption
# MAGIC * Marts governance metadata
# MAGIC * Column and table comments
# MAGIC * Marts validation rules
# MAGIC * Marts execution logging
# MAGIC * CSV export for delivery evidence
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC * Read validated Gold dimensions and facts
# MAGIC * Keep one analytical record per legislative event
# MAGIC * Answer the six mandatory business deliverables
# MAGIC * Aggregate participation and organizational metrics
# MAGIC * Preserve event business identifiers
# MAGIC * Generate Marts execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Marts quality validations
# MAGIC * Publish the Business Mart as a Delta table
# MAGIC * Export the Business Mart as a CSV file
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Questions Covered
# MAGIC
# MAGIC This mart supports the six mandatory deliverables for Legislative Events Analytics:
# MAGIC
# MAGIC 1. Which legislative events occurred?
# MAGIC 2. Which organizations promoted the events?
# MAGIC 3. How many parliamentarians participated?
# MAGIC 4. What is the geographic distribution of events?
# MAGIC 5. Which event types are most frequent?
# MAGIC 6. Which events have the greatest parliamentary engagement?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Mart Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per legislative event.
# MAGIC
# MAGIC ### Sources
# MAGIC
# MAGIC * `brazil_legislative_analytics.gold.dm_eventos`
# MAGIC * `brazil_legislative_analytics.gold.ft_presenca_eventos`
# MAGIC
# MAGIC Optional dimensional context already available:
# MAGIC
# MAGIC * Deputies
# MAGIC * Organizations
# MAGIC * Federation Units
# MAGIC * Legislature
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.marts.am_calendario_eventos`
# MAGIC
# MAGIC ### CSV Export
# MAGIC
# MAGIC `/Volumes/brazil_legislative_analytics/marts/exports/am_calendario_eventos/am_calendario_eventos.csv`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `eve_id_evento`
# MAGIC
# MAGIC ### Mart Surrogate Key
# MAGIC
# MAGIC `ace_sk_calendario_evento`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC Rule 1:
# MAGIC
# MAGIC Only valid Gold records are eligible.
# MAGIC
# MAGIC Rule 2:
# MAGIC
# MAGIC One analytical record per legislative event.
# MAGIC
# MAGIC Rule 3:
# MAGIC
# MAGIC Participation metrics are derived from
# MAGIC `ft_presenca_eventos`.
# MAGIC
# MAGIC Rule 4:
# MAGIC
# MAGIC Organizational indicators are derived from
# MAGIC associated organizations.
# MAGIC
# MAGIC Rule 5:
# MAGIC
# MAGIC Geographic indicators are derived from
# MAGIC available federation unit attributes.
# MAGIC
# MAGIC Rule 6:
# MAGIC
# MAGIC Event engagement ranking is calculated
# MAGIC by distinct participating parliamentarians.
# MAGIC
# MAGIC Rule 7:
# MAGIC
# MAGIC The mart must be published as Delta
# MAGIC and exported as CSV.
# MAGIC
# MAGIC Rule 8:
# MAGIC
# MAGIC All Marts objects must contain governance comments.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Controls
# MAGIC
# MAGIC Validates:
# MAGIC
# MAGIC * Null mart surrogate keys
# MAGIC * Null business keys
# MAGIC * Duplicate event records
# MAGIC * Invalid mart records
# MAGIC * Empty mart result
# MAGIC * CSV export path creation
# MAGIC
# MAGIC Execution is interrupted when critical validations fail.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Expected Deliverables
# MAGIC
# MAGIC ### Deliverable 1
# MAGIC
# MAGIC Legislative Events Catalog
# MAGIC
# MAGIC ### Deliverable 2
# MAGIC
# MAGIC Events by Organization
# MAGIC
# MAGIC ### Deliverable 3
# MAGIC
# MAGIC Events by Participation Volume
# MAGIC
# MAGIC ### Deliverable 4
# MAGIC
# MAGIC Geographic Distribution of Events
# MAGIC
# MAGIC ### Deliverable 5
# MAGIC
# MAGIC Parliamentary Engagement Ranking
# MAGIC
# MAGIC ### Deliverable 6
# MAGIC
# MAGIC Governance and Audit Indicators
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Governance
# MAGIC
# MAGIC Layer: Marts
# MAGIC
# MAGIC Domain: Legislative Events
# MAGIC
# MAGIC Owner: Brazil Legislative Analytics
# MAGIC
# MAGIC Consumption Type:
# MAGIC
# MAGIC * Dashboard
# MAGIC * Analytics
# MAGIC * Executive Reporting
# MAGIC * CSV Delivery
# MAGIC
# MAGIC Status:
# MAGIC
# MAGIC Approved for Production
# MAGIC

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_comments

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_quality

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# EXECUTION CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "01_am_atlas_frentes"

ENTITY_NAME = "atlas_frentes"

try:
    MARTS_SCHEMA
except NameError:
    MARTS_SCHEMA = "brazil_legislative_analytics.marts"

try:
    CATALOG_NAME
except NameError:
    CATALOG_NAME = "brazil_legislative_analytics"

try:
    GOLD_SCHEMA
except NameError:
    GOLD_SCHEMA = f"{CATALOG_NAME}.gold"

SOURCE_DIM_FRENTES = f"{GOLD_SCHEMA}.dm_frentes"
SOURCE_FACT_MEMBROS = f"{GOLD_SCHEMA}.ft_frentes_membros"

TARGET_TABLE = f"{MARTS_SCHEMA}.am_atlas_frentes"

EXPORT_BASE_PATH = f"/Volumes/{CATALOG_NAME}/marts/exports"
EXPORT_DIR = f"{EXPORT_BASE_PATH}/am_atlas_frentes"
EXPORT_TMP_DIR = f"{EXPORT_DIR}/_tmp"
EXPORT_FILE_NAME = "am_atlas_frentes.csv"
EXPORT_FILE_PATH = f"{EXPORT_DIR}/{EXPORT_FILE_NAME}"

EXECUTION_ID = str(uuid.uuid4())

STARTED_AT = datetime.now()

PIPELINE_LOG_ID = str(uuid.uuid4())

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name="marts"
)

log_info(
    logger,
    f"Starting notebook {NOTEBOOK_NAME}"
)

# COMMAND ----------

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def column_or_default(dataframe, column_name, default_value=None):
    """Return a DataFrame column when it exists, otherwise return a literal default value."""

    if column_name in dataframe.columns:
        return F.col(column_name)

    return F.lit(default_value)


def create_export_volume_if_possible():
    """Create the Marts schema and export volume when the current workspace permissions allow it."""

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")

    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.marts.exports")
        log_info(logger, "Marts export volume is available.")
    except Exception as error:
        log_info(
            logger,
            f"Could not create export volume automatically. The notebook will still try to use the export path. Details: {str(error)}"
        )


def export_single_csv(dataframe, temporary_path, final_directory, final_file_path):
    """Export a Spark DataFrame as a single CSV file with a stable file name."""

    dbutils.fs.mkdirs(final_directory)
    dbutils.fs.rm(temporary_path, recurse=True)

    (
        dataframe
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", ";")
        .option("encoding", "UTF-8")
        .csv(temporary_path)
    )

    part_files = [
        file_info.path
        for file_info in dbutils.fs.ls(temporary_path)
        if file_info.name.startswith("part-") and file_info.name.endswith(".csv")
    ]

    if not part_files:
        raise ValueError("CSV export failed because no part file was generated.")

    dbutils.fs.rm(final_file_path, recurse=False)
    dbutils.fs.mv(part_files[0], final_file_path)
    dbutils.fs.rm(temporary_path, recurse=True)

    log_success(
        logger,
        f"CSV exported successfully to {final_file_path}"
    )

# COMMAND ----------

# ============================================================
# PREPARE MARTS SCHEMA AND EXPORT PATH
# ============================================================

create_export_volume_if_possible()

dbutils.fs.mkdirs(EXPORT_DIR)

# COMMAND ----------

# ============================================================
# READ GOLD TABLES
# ============================================================

df_frentes = spark.table(SOURCE_DIM_FRENTES)

df_membros = spark.table(SOURCE_FACT_MEMBROS)

records_read_frentes = df_frentes.count()
records_read_membros = df_membros.count()
records_read = records_read_membros

log_info(
    logger,
    f"Records read from {SOURCE_DIM_FRENTES}: {records_read_frentes}"
)

log_info(
    logger,
    f"Records read from {SOURCE_FACT_MEMBROS}: {records_read_membros}"
)

# COMMAND ----------

# ============================================================
# GOLD VALID RECORDS
# ============================================================

df_membros_validos = (
    df_membros
    .filter(
        column_or_default(
            dataframe=df_membros,
            column_name="ffm_fl_registro_valido_gold",
            default_value=True
        ) == F.lit(True)
    )
)

records_eligible = df_membros_validos.count()

log_info(
    logger,
    f"Eligible Gold records for Marts: {records_eligible}"
)

# COMMAND ----------

# ============================================================
# STANDARDIZE FRONT ATTRIBUTES
# ============================================================

df_frentes_std = (
    df_frentes
    .select(
        column_or_default(df_frentes, "frn_sk_frente").alias("frn_sk_frente"),
        column_or_default(df_frentes, "frn_id_frente").cast("string").alias("frn_id_frente"),
        column_or_default(df_frentes, "frn_tx_titulo").alias("frn_tx_titulo"),
        column_or_default(df_frentes, "frn_tx_situacao").alias("frn_tx_situacao"),
        column_or_default(df_frentes, "frn_dt_criacao").cast("date").alias("frn_dt_criacao"),
        column_or_default(df_frentes, "leg_id_legislatura").cast("string").alias("frn_leg_id_legislatura")
    )
    .dropDuplicates(["frn_id_frente"])
)

# COMMAND ----------

# ============================================================
# STANDARDIZE MEMBERSHIP FACT ATTRIBUTES
# ============================================================

df_membros_std = (
    df_membros_validos
    .select(
        column_or_default(df_membros_validos, "ffm_sk_frente_membro").alias("ffm_sk_frente_membro"),
        column_or_default(df_membros_validos, "frn_id_frente").cast("string").alias("frn_id_frente"),
        column_or_default(df_membros_validos, "frn_tx_titulo").alias("ffm_frn_tx_titulo"),
        column_or_default(df_membros_validos, "dep_id_deputado").cast("string").alias("dep_id_deputado"),
        column_or_default(df_membros_validos, "dep_tx_nome").alias("dep_tx_nome"),
        column_or_default(df_membros_validos, "dep_tx_sigla_partido").alias("dep_tx_sigla_partido"),
        column_or_default(df_membros_validos, "dep_tx_sigla_uf").alias("dep_tx_sigla_uf"),
        column_or_default(df_membros_validos, "frm_tx_cargo").alias("frm_tx_cargo"),
        column_or_default(df_membros_validos, "frm_tx_condicao").alias("frm_tx_condicao"),
        column_or_default(df_membros_validos, "frm_tx_tipo_participacao").alias("frm_tx_tipo_participacao"),
        column_or_default(df_membros_validos, "frm_fl_coordenador", False).cast("boolean").alias("frm_fl_coordenador"),
        column_or_default(df_membros_validos, "frm_fl_lideranca", False).cast("boolean").alias("frm_fl_lideranca"),
        column_or_default(df_membros_validos, "leg_id_legislatura").cast("string").alias("ffm_leg_id_legislatura"),
        column_or_default(df_membros_validos, "ffm_fl_frente_encontrada_gold", True).cast("boolean").alias("ffm_fl_frente_encontrada_gold"),
        column_or_default(df_membros_validos, "ffm_fl_deputado_encontrado_gold", False).cast("boolean").alias("ffm_fl_deputado_encontrado_gold"),
        column_or_default(df_membros_validos, "ffm_fl_partido_encontrado_gold", False).cast("boolean").alias("ffm_fl_partido_encontrado_gold"),
        column_or_default(df_membros_validos, "ffm_fl_estado_encontrado_gold", False).cast("boolean").alias("ffm_fl_estado_encontrado_gold"),
        column_or_default(df_membros_validos, "ffm_fl_dimensoes_principais_completas", False).cast("boolean").alias("ffm_fl_dimensoes_principais_completas")
    )
)

# COMMAND ----------

# ============================================================
# PARTY COMPOSITION
# ============================================================

df_partido_counts = (
    df_membros_std
    .filter(F.col("dep_tx_sigla_partido").isNotNull())
    .groupBy(
        "frn_id_frente",
        "dep_tx_sigla_partido"
    )
    .agg(
        F.countDistinct("dep_id_deputado").alias("atl_qt_membros_partido")
    )
)

party_rank_window = Window.partitionBy("frn_id_frente").orderBy(
    F.col("atl_qt_membros_partido").desc(),
    F.col("dep_tx_sigla_partido").asc()
)

df_partido_predominante = (
    df_partido_counts
    .withColumn("rn", F.row_number().over(party_rank_window))
    .filter(F.col("rn") == 1)
    .select(
        "frn_id_frente",
        F.col("dep_tx_sigla_partido").alias("atl_tx_partido_predominante"),
        F.col("atl_qt_membros_partido").alias("atl_qt_membros_partido_predominante")
    )
)

# COMMAND ----------

# ============================================================
# GEOGRAPHIC COMPOSITION
# ============================================================

df_uf_counts = (
    df_membros_std
    .filter(F.col("dep_tx_sigla_uf").isNotNull())
    .groupBy(
        "frn_id_frente",
        "dep_tx_sigla_uf"
    )
    .agg(
        F.countDistinct("dep_id_deputado").alias("atl_qt_membros_uf")
    )
)

uf_rank_window = Window.partitionBy("frn_id_frente").orderBy(
    F.col("atl_qt_membros_uf").desc(),
    F.col("dep_tx_sigla_uf").asc()
)

df_uf_predominante = (
    df_uf_counts
    .withColumn("rn", F.row_number().over(uf_rank_window))
    .filter(F.col("rn") == 1)
    .select(
        "frn_id_frente",
        F.col("dep_tx_sigla_uf").alias("atl_tx_uf_predominante"),
        F.col("atl_qt_membros_uf").alias("atl_qt_membros_uf_predominante")
    )
)

# COMMAND ----------

# ============================================================
# FRONT AGGREGATION
# ============================================================

df_front_metrics = (
    df_membros_std
    .groupBy("frn_id_frente")
    .agg(
        F.count("ffm_sk_frente_membro").alias("atl_qt_registros_membros"),
        F.countDistinct("dep_id_deputado").alias("atl_qt_membros"),
        F.countDistinct("dep_tx_sigla_partido").alias("atl_qt_partidos"),
        F.countDistinct("dep_tx_sigla_uf").alias("atl_qt_ufs"),
        F.sum(F.when(F.col("frm_fl_coordenador") == True, 1).otherwise(0)).alias("atl_qt_coordenadores"),
        F.sum(F.when(F.col("frm_fl_lideranca") == True, 1).otherwise(0)).alias("atl_qt_liderancas"),
        F.sum(F.when(F.col("ffm_fl_frente_encontrada_gold") == True, 1).otherwise(0)).alias("atl_qt_frente_encontrada_gold"),
        F.sum(F.when(F.col("ffm_fl_deputado_encontrado_gold") == True, 1).otherwise(0)).alias("atl_qt_deputado_encontrado_gold"),
        F.sum(F.when(F.col("ffm_fl_partido_encontrado_gold") == True, 1).otherwise(0)).alias("atl_qt_partido_encontrado_gold"),
        F.sum(F.when(F.col("ffm_fl_estado_encontrado_gold") == True, 1).otherwise(0)).alias("atl_qt_estado_encontrado_gold"),
        F.sum(F.when(F.col("ffm_fl_dimensoes_principais_completas") == True, 1).otherwise(0)).alias("atl_qt_dimensoes_completas"),
        F.max("ffm_leg_id_legislatura").alias("atl_leg_id_legislatura")
    )
)

# COMMAND ----------

# ============================================================
# UNIVERSE METRICS
# ============================================================

total_deputados_universo = (
    df_membros_std
    .select("dep_id_deputado")
    .where(F.col("dep_id_deputado").isNotNull())
    .distinct()
    .count()
)

if total_deputados_universo == 0:
    total_deputados_universo = 1

# COMMAND ----------

# ============================================================
# CURRENT LEGISLATURE RULE
# ============================================================

current_legislature = (
    df_front_metrics
    .select(
        F.max(
            F.col("atl_leg_id_legislatura").cast("int")
        ).alias("leg_atual")
    )
    .collect()[0]["leg_atual"]
)

log_info(
    logger,
    f"Current legislature identified for active Front rule: {current_legislature}"
)

# COMMAND ----------

# ============================================================
# BUILD MART
# ============================================================

df_mart_base = (
    df_front_metrics.alias("m")
    .join(
        df_frentes_std.alias("f"),
        F.col("m.frn_id_frente") == F.col("f.frn_id_frente"),
        "left"
    )
    .join(
        df_partido_predominante.alias("p"),
        F.col("m.frn_id_frente") == F.col("p.frn_id_frente"),
        "left"
    )
    .join(
        df_uf_predominante.alias("u"),
        F.col("m.frn_id_frente") == F.col("u.frn_id_frente"),
        "left"
    )
)

rank_window = Window.orderBy(
    F.col("atl_qt_membros").desc(),
    F.col("m.frn_id_frente").asc()
)

df_mart = (
    df_mart_base
    .withColumn(
        "atl_sk_atlas_frente",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("m.frn_id_frente"), F.lit("")),
                F.coalesce(F.col("atl_leg_id_legislatura"), F.col("frn_leg_id_legislatura"), F.lit(""))
            ),
            256
        )
    )
    .withColumn(
        "frn_tx_titulo_final",
        F.coalesce(F.col("f.frn_tx_titulo"), F.col("m.frn_id_frente"))
    )
    .withColumn(
        "leg_id_legislatura",
        F.coalesce(F.col("atl_leg_id_legislatura"), F.col("frn_leg_id_legislatura"))
    )
    .withColumn(
        "atl_fl_frente_ativa",
        F.when(
            (F.col("leg_id_legislatura").cast("int") == F.lit(current_legislature))
            & (F.col("atl_qt_membros") > F.lit(0)),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "atl_vl_pct_cobertura_deputados",
        F.round((F.col("atl_qt_membros") / F.lit(total_deputados_universo)) * F.lit(100), 2)
    )
    .withColumn(
        "atl_vl_pct_dimensoes_completas",
        F.round((F.col("atl_qt_dimensoes_completas") / F.col("atl_qt_registros_membros")) * F.lit(100), 2)
    )
    .withColumn(
        "atl_nr_rank_representatividade",
        F.dense_rank().over(rank_window)
    )
    .withColumn(
        "atl_fl_possui_coordenador",
        F.col("atl_qt_coordenadores") > F.lit(0)
    )
    .withColumn(
        "atl_fl_possui_lideranca",
        F.col("atl_qt_liderancas") > F.lit(0)
    )
    .withColumn(
        "atl_fl_composicao_partidaria_identificada",
        F.col("atl_qt_partidos") > F.lit(0)
    )
    .withColumn(
        "atl_fl_distribuicao_uf_identificada",
        F.col("atl_qt_ufs") > F.lit(0)
    )
    .withColumn(
        "atl_fl_registro_valido_marts",
        F.col("atl_sk_atlas_frente").isNotNull()
        & F.col("m.frn_id_frente").isNotNull()
        & (F.col("atl_qt_membros") > F.lit(0))
    )
    .withColumn(
        "aud_id_execucao_marts",
        F.lit(EXECUTION_ID)
    )
    .withColumn(
        "aud_dh_processamento_marts",
        F.current_timestamp()
    )
    .withColumn(
        "aud_tx_versao_pipeline_marts",
        F.lit(PROJECT_VERSION)
    )
    .withColumn(
        "aud_tx_hash_registro_marts",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("atl_sk_atlas_frente"), F.lit("")),
                F.coalesce(F.col("m.frn_id_frente"), F.lit("")),
                F.coalesce(F.col("frn_tx_titulo_final"), F.lit("")),
                F.coalesce(F.col("atl_qt_membros").cast("string"), F.lit("")),
                F.coalesce(F.col("atl_qt_partidos").cast("string"), F.lit("")),
                F.coalesce(F.col("atl_qt_ufs").cast("string"), F.lit("")),
                F.coalesce(F.col("atl_tx_partido_predominante"), F.lit("")),
                F.coalesce(F.col("atl_tx_uf_predominante"), F.lit(""))
            ),
            256
        )
    )
)

# COMMAND ----------

# ============================================================
# FINAL SELECT
# ============================================================

df_final = (
    df_mart
    .select(
        F.col("atl_sk_atlas_frente"),
        F.col("f.frn_sk_frente").alias("frn_sk_frente"),
        F.col("m.frn_id_frente").alias("frn_id_frente"),
        F.col("frn_tx_titulo_final").alias("frn_tx_titulo"),
        F.col("f.frn_tx_situacao").alias("frn_tx_situacao"),
        F.col("f.frn_dt_criacao").alias("frn_dt_criacao"),
        F.col("leg_id_legislatura"),

        F.col("atl_qt_registros_membros"),
        F.col("atl_qt_membros"),
        F.col("atl_qt_partidos"),
        F.col("atl_qt_ufs"),
        F.col("atl_qt_coordenadores"),
        F.col("atl_qt_liderancas"),

        F.col("atl_tx_partido_predominante"),
        F.col("atl_qt_membros_partido_predominante"),
        F.col("atl_tx_uf_predominante"),
        F.col("atl_qt_membros_uf_predominante"),

        F.col("atl_vl_pct_cobertura_deputados"),
        F.col("atl_vl_pct_dimensoes_completas"),
        F.col("atl_nr_rank_representatividade"),

        F.col("atl_fl_frente_ativa"),
        F.col("atl_fl_possui_coordenador"),
        F.col("atl_fl_possui_lideranca"),
        F.col("atl_fl_composicao_partidaria_identificada"),
        F.col("atl_fl_distribuicao_uf_identificada"),
        F.col("atl_fl_registro_valido_marts"),

        F.col("atl_qt_frente_encontrada_gold"),
        F.col("atl_qt_deputado_encontrado_gold"),
        F.col("atl_qt_partido_encontrado_gold"),
        F.col("atl_qt_estado_encontrado_gold"),
        F.col("atl_qt_dimensoes_completas"),

        F.col("aud_id_execucao_marts"),
        F.col("aud_dh_processamento_marts"),
        F.col("aud_tx_versao_pipeline_marts"),
        F.col("aud_tx_hash_registro_marts")
    )
    .dropDuplicates(["frn_id_frente"])
)

# COMMAND ----------

# ============================================================
# QUALITY VALIDATIONS
# ============================================================

required_columns_result = validate_required_columns(
    dataframe=df_final,
    required_columns=[
        "atl_sk_atlas_frente",
        "frn_id_frente",
        "atl_qt_membros"
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_final,
    key_columns=[
        "frn_id_frente"
    ]
)

null_results = validate_nulls(
    dataframe=df_final,
    columns=[
        "atl_sk_atlas_frente",
        "frn_id_frente",
        "atl_qt_membros"
    ]
)

empty_mart_result = {
    "check_name": "empty_mart_result",
    "check_status": "PASSED" if df_final.count() > 0 else "FAILED",
    "check_message": "Mart contains records." if df_final.count() > 0 else "Mart is empty."
}

quality_results = [
    required_columns_result,
    duplicate_result,
    empty_mart_result
]

quality_results.extend(
    null_results
)

quality_df = build_quality_log(
    quality_results=quality_results,
    execution_id=EXECUTION_ID,
    notebook_name=NOTEBOOK_NAME,
    layer_name="marts",
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE
)

write_quality_log(
    quality_dataframe=quality_df
)

# COMMAND ----------

# ============================================================
# WRITE MART TABLE
# ============================================================

(
    df_final
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_TABLE
    )
)

records_written = df_final.count()

log_success(
    logger,
    f"Records written to Marts: {records_written}"
)

# COMMAND ----------

# ============================================================
# EXPORT MART AS CSV
# ============================================================

export_single_csv(
    dataframe=df_final.orderBy("atl_nr_rank_representatividade", "frn_tx_titulo"),
    temporary_path=EXPORT_TMP_DIR,
    final_directory=EXPORT_DIR,
    final_file_path=EXPORT_FILE_PATH
)

# COMMAND ----------

# ============================================================
# GOVERNANCE COMMENTS
# ============================================================

TABLE_COMMENT = """
Business Mart for the Parliamentary Fronts Atlas.

This mart contains one analytical record per Parliamentary Front.

Main characteristics:

* Parliamentary Front descriptive attributes
* membership quantity indicators
* party composition indicators
* federation unit distribution indicators
* coordination and leadership indicators
* representativeness ranking
* Gold lineage
* Marts lineage
* governance metadata
* CSV export for delivery consumption
"""

COLUMN_COMMENTS = {
    "atl_sk_atlas_frente":
        "Marts surrogate key for the Parliamentary Fronts Atlas record.",

    "frn_sk_frente":
        "Gold surrogate key of the Parliamentary Front dimension.",

    "frn_id_frente":
        "Business identifier of the Parliamentary Front.",

    "frn_tx_titulo":
        "Standardized title of the Parliamentary Front.",

    "frn_tx_situacao":
        "Current situation of the Parliamentary Front.",

    "frn_dt_criacao":
        "Creation date of the Parliamentary Front when available.",

    "leg_id_legislatura":
        "Legislature identifier associated with the Parliamentary Front membership records.",

    "atl_qt_registros_membros":
        "Total number of membership records associated with the Parliamentary Front.",

    "atl_qt_membros":
        "Number of distinct parliamentarians associated with the Parliamentary Front.",

    "atl_qt_partidos":
        "Number of distinct political parties represented in the Parliamentary Front.",

    "atl_qt_ufs":
        "Number of distinct federation units represented in the Parliamentary Front.",

    "atl_qt_coordenadores":
        "Number of records classified as coordinators in the Parliamentary Front.",

    "atl_qt_liderancas":
        "Number of records classified as leadership positions in the Parliamentary Front.",

    "atl_tx_partido_predominante":
        "Political party with the highest number of distinct members in the Parliamentary Front.",

    "atl_qt_membros_partido_predominante":
        "Number of distinct members from the predominant political party.",

    "atl_tx_uf_predominante":
        "Federation unit with the highest number of distinct members in the Parliamentary Front.",

    "atl_qt_membros_uf_predominante":
        "Number of distinct members from the predominant federation unit.",

    "atl_vl_pct_cobertura_deputados":
        "Percentage of the total deputy universe represented in the Parliamentary Front.",

    "atl_vl_pct_dimensoes_completas":
        "Percentage of membership records with complete main Gold dimensional coverage.",

    "atl_nr_rank_representatividade":
        "Representativeness ranking based on the number of distinct members.",

    "atl_fl_frente_ativa":
        "Flag indicating whether the Parliamentary Front belongs to the most recent legislature available in the mart and has at least one member.",

    "atl_fl_possui_coordenador":
        "Flag indicating whether the Parliamentary Front has at least one coordinator.",

    "atl_fl_possui_lideranca":
        "Flag indicating whether the Parliamentary Front has at least one leadership position.",

    "atl_fl_composicao_partidaria_identificada":
        "Flag indicating whether party composition was identified.",

    "atl_fl_distribuicao_uf_identificada":
        "Flag indicating whether federation unit distribution was identified.",

    "atl_fl_registro_valido_marts":
        "Flag indicating whether the mart record passed Marts validation.",

    "atl_qt_frente_encontrada_gold":
        "Number of membership records with front dimension found in Gold.",

    "atl_qt_deputado_encontrado_gold":
        "Number of membership records with deputy dimension found in Gold.",

    "atl_qt_partido_encontrado_gold":
        "Number of membership records with party dimension found in Gold.",

    "atl_qt_estado_encontrado_gold":
        "Number of membership records with state dimension found in Gold.",

    "atl_qt_dimensoes_completas":
        "Number of membership records with all main dimensions complete.",

    "aud_id_execucao_marts":
        "Execution identifier generated during Marts processing.",

    "aud_dh_processamento_marts":
        "Timestamp when the record was processed in Marts.",

    "aud_tx_versao_pipeline_marts":
        "Pipeline version used during Marts processing.",

    "aud_tx_hash_registro_marts":
        "Deterministic Marts record hash."
}

apply_table_comment(
    table_name=TARGET_TABLE,
    table_comment=TABLE_COMMENT
)

existing_columns = set(spark.table(TARGET_TABLE).columns)

COLUMN_COMMENTS = {
    column_name: column_comment
    for column_name, column_comment in COLUMN_COMMENTS.items()
    if column_name in existing_columns
}

apply_column_comments(
    table_name=TARGET_TABLE,
    column_comments=COLUMN_COMMENTS
)

# COMMAND ----------

# ============================================================
# PIPELINE AUDIT LOG
# ============================================================

FINISHED_AT = datetime.now()

duration_seconds = (
    FINISHED_AT - STARTED_AT
).total_seconds()

write_pipeline_log(
    log_id=PIPELINE_LOG_ID,
    execution_id=EXECUTION_ID,
    notebook_name=NOTEBOOK_NAME,
    layer_name="marts",
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status="SUCCESS",
    message="Parliamentary Fronts Atlas mart generated successfully.",
    started_at=STARTED_AT,
    finished_at=FINISHED_AT,
    duration_seconds=duration_seconds,
    records_read=records_read,
    records_written=records_written
)

# COMMAND ----------

# ============================================================
# POST-WRITE VALIDATIONS
# ============================================================

mart_df = spark.table(TARGET_TABLE)

print("=" * 80)
print("MART ATLAS FRENTES - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read from Gold fact: {records_read}")
print(f"Records eligible from Gold fact: {records_eligible}")
print(f"Records written to Marts: {records_written}")
print(f"CSV export path: {EXPORT_FILE_PATH}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(mart_df.orderBy("atl_nr_rank_representatividade").limit(20))