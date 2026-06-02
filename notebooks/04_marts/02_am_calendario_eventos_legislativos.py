# Databricks notebook source
# MAGIC  %md
# MAGIC  # 02 Marts — Legislative Events Calendar
# MAGIC
# MAGIC  **Notebook:** `02_am_calendario_eventos`
# MAGIC
# MAGIC  Builds the curated Business Mart for the Legislative Events Calendar used by analytical dashboards, executive reports, agenda monitoring, and business consumption.
# MAGIC
# MAGIC  This notebook defines:
# MAGIC
# MAGIC  * Legislative events analytical mart model
# MAGIC  * One analytical record per legislative event
# MAGIC  * Event calendar attributes
# MAGIC  * Legislative body and organization indicators
# MAGIC  * Parliamentary participation indicators
# MAGIC  * Attendance indicators
# MAGIC  * Party composition indicators
# MAGIC  * Geographic distribution indicators
# MAGIC  * Event engagement ranking
# MAGIC  * Business-ready attributes for dashboard consumption
# MAGIC  * Marts governance metadata
# MAGIC  * Column and table comments
# MAGIC  * Marts validation rules
# MAGIC  * Marts execution logging
# MAGIC  * CSV export for delivery evidence
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ## Responsibilities
# MAGIC
# MAGIC  * Read validated Gold dimensions and facts
# MAGIC  * Keep one analytical record per legislative event
# MAGIC  * Answer the six mandatory business deliverables for the Legislative Events Calendar
# MAGIC  * Aggregate participation, attendance, party, federation unit and organization indicators
# MAGIC  * Preserve event business identifiers and descriptive attributes
# MAGIC  * Generate Marts execution metadata
# MAGIC  * Apply governance comments
# MAGIC  * Execute Marts quality validations
# MAGIC  * Publish the Business Mart as a Delta table
# MAGIC  * Export the Business Mart as a CSV file
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ## Business Questions Covered
# MAGIC
# MAGIC  This mart supports the six mandatory deliverables for Legislative Events Analytics:
# MAGIC
# MAGIC  1. Which legislative events exist in the analytical calendar?
# MAGIC  2. Which legislative bodies or organizations promoted the events?
# MAGIC  3. How many parliamentarians participated in each event?
# MAGIC  4. What is the party and geographic distribution of participants?
# MAGIC  5. Which event types and statuses are most frequent?
# MAGIC  6. Which events have the highest parliamentary engagement?
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ## Mart Model
# MAGIC
# MAGIC  ### Grain
# MAGIC
# MAGIC  One record per legislative event.
# MAGIC
# MAGIC  ### Sources
# MAGIC
# MAGIC  * `brazil_legislative_analytics.gold.dm_eventos`
# MAGIC  * `brazil_legislative_analytics.gold.ft_presencas_eventos`
# MAGIC
# MAGIC  Optional dimensional context already available in the Gold fact:
# MAGIC
# MAGIC  * Deputy attributes
# MAGIC  * Party attributes
# MAGIC  * Federation unit attributes
# MAGIC  * Legislature attributes
# MAGIC  * Date attributes
# MAGIC
# MAGIC  ### Target
# MAGIC
# MAGIC  `brazil_legislative_analytics.marts.am_calendario_eventos`
# MAGIC
# MAGIC  ### CSV Export
# MAGIC
# MAGIC  `/Volumes/brazil_legislative_analytics/marts/exports/am_calendario_eventos/am_calendario_eventos.csv`
# MAGIC
# MAGIC  ### Business Key
# MAGIC
# MAGIC  `evt_id_evento`
# MAGIC
# MAGIC  ### Mart Surrogate Key
# MAGIC
# MAGIC  `cal_sk_calendario_evento`
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ## Business Rules
# MAGIC
# MAGIC  Rule 1:
# MAGIC
# MAGIC  Only valid Gold legislative event records are eligible for the mart.
# MAGIC
# MAGIC  Rule 2:
# MAGIC
# MAGIC  One analytical record is maintained per legislative event.
# MAGIC
# MAGIC  Rule 3:
# MAGIC
# MAGIC  Legislative event descriptive attributes are primarily sourced from `dm_eventos`.
# MAGIC
# MAGIC  Rule 4:
# MAGIC
# MAGIC  Parliamentary participation and attendance metrics are derived from `ft_presencas_eventos`.
# MAGIC
# MAGIC  Rule 5:
# MAGIC
# MAGIC  Party and federation unit composition metrics are calculated from available Gold attendance fact attributes.
# MAGIC
# MAGIC  Rule 6:
# MAGIC
# MAGIC  Event engagement ranking is calculated by the number of distinct deputies associated with each event.
# MAGIC
# MAGIC  Rule 7:
# MAGIC
# MAGIC  Events without attendance records are preserved in the calendar with zero participation metrics.
# MAGIC
# MAGIC  Rule 8:
# MAGIC
# MAGIC  The mart must be published as a Delta table and exported as CSV for delivery consumption.
# MAGIC
# MAGIC  Rule 9:
# MAGIC
# MAGIC  All Marts objects must contain governance comments.
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ## Data Quality Controls
# MAGIC
# MAGIC  Validates:
# MAGIC
# MAGIC  * Null mart surrogate keys
# MAGIC  * Null legislative event business keys
# MAGIC  * Duplicate legislative event records
# MAGIC  * Invalid mart records
# MAGIC  * Empty mart result
# MAGIC  * Negative analytical metrics
# MAGIC  * CSV export path creation
# MAGIC
# MAGIC  Execution is interrupted when critical validations fail.
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ## Expected Deliverables
# MAGIC
# MAGIC  ### Deliverable 1
# MAGIC
# MAGIC  Legislative Events Catalog
# MAGIC
# MAGIC  ### Deliverable 2
# MAGIC
# MAGIC  Events by Legislative Body or Organization
# MAGIC
# MAGIC  ### Deliverable 3
# MAGIC
# MAGIC  Events by Participation Volume
# MAGIC
# MAGIC  ### Deliverable 4
# MAGIC
# MAGIC  Party and Geographic Distribution of Participants
# MAGIC
# MAGIC  ### Deliverable 5
# MAGIC
# MAGIC  Event Type and Status Monitoring
# MAGIC
# MAGIC  ### Deliverable 6
# MAGIC
# MAGIC  Governance, Audit and CSV Delivery Evidence
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ## Governance
# MAGIC
# MAGIC  Layer: Marts
# MAGIC
# MAGIC  Domain: Legislative Events
# MAGIC
# MAGIC  Owner: Brazil Legislative Analytics
# MAGIC
# MAGIC  Consumption Type:
# MAGIC
# MAGIC  * Dashboard
# MAGIC  * Analytics
# MAGIC  * Executive Reporting
# MAGIC  * CSV Delivery
# MAGIC
# MAGIC  Status:
# MAGIC
# MAGIC  Ready for validation

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

NOTEBOOK_NAME = "02_am_calendario_eventos"
ENTITY_NAME = "calendario_eventos"

try:
    CATALOG_NAME
except NameError:
    CATALOG_NAME = "brazil_legislative_analytics"

try:
    MARTS_SCHEMA
except NameError:
    MARTS_SCHEMA = f"{CATALOG_NAME}.marts"

try:
    GOLD_SCHEMA
except NameError:
    GOLD_SCHEMA = f"{CATALOG_NAME}.gold"

try:
    PROJECT_VERSION
except NameError:
    PROJECT_VERSION = "1.0.0"

SOURCE_DIM_EVENTOS = f"{GOLD_SCHEMA}.dm_eventos"
SOURCE_FACT_PRESENCAS = f"{GOLD_SCHEMA}.ft_presencas_eventos"

TARGET_TABLE = f"{MARTS_SCHEMA}.am_calendario_eventos"

EXPORT_BASE_PATH = f"/Volumes/{CATALOG_NAME}/marts/exports"
EXPORT_DIR = f"{EXPORT_BASE_PATH}/am_calendario_eventos"
EXPORT_TMP_DIR = f"{EXPORT_DIR}/_tmp"
EXPORT_FILE_NAME = "am_calendario_eventos.csv"
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
            "Could not create export volume automatically. "
            f"The notebook will still try to use the export path. Details: {str(error)}"
        )


def export_single_csv(dataframe, temporary_path, final_directory, final_file_path):
    """Export a Spark DataFrame as a single CSV file with a stable file name."""
    dbutils.fs.mkdirs(final_directory)

    try:
        dbutils.fs.rm(temporary_path, recurse=True)
    except Exception:
        pass

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

    try:
        dbutils.fs.rm(final_file_path, recurse=False)
    except Exception:
        pass

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

df_eventos = spark.table(SOURCE_DIM_EVENTOS)
df_presencas = spark.table(SOURCE_FACT_PRESENCAS)

records_read_eventos = df_eventos.count()
records_read_presencas = df_presencas.count()
records_read = records_read_eventos + records_read_presencas

log_info(
    logger,
    f"Records read from {SOURCE_DIM_EVENTOS}: {records_read_eventos}"
)

log_info(
    logger,
    f"Records read from {SOURCE_FACT_PRESENCAS}: {records_read_presencas}"
)

# COMMAND ----------

# ============================================================
# GOLD VALID RECORDS
# ============================================================

df_eventos_validos = (
    df_eventos
    .filter(
        column_or_default(
            dataframe=df_eventos,
            column_name="evt_fl_registro_valido_gold",
            default_value=True
        ) == F.lit(True)
    )
)

df_presencas_validas = (
    df_presencas
    .filter(
        column_or_default(
            dataframe=df_presencas,
            column_name="fpe_fl_registro_valido_gold",
            default_value=True
        ) == F.lit(True)
    )
)

records_eligible_eventos = df_eventos_validos.count()
records_eligible_presencas = df_presencas_validas.count()
records_eligible = records_eligible_eventos + records_eligible_presencas

log_info(
    logger,
    f"Eligible Gold event records for Marts: {records_eligible_eventos}"
)

log_info(
    logger,
    f"Eligible Gold attendance records for Marts: {records_eligible_presencas}"
)

# COMMAND ----------

# ============================================================
# STANDARDIZE EVENT DIMENSION ATTRIBUTES
# ============================================================

df_eventos_std = (
    df_eventos_validos
    .select(
        column_or_default(df_eventos_validos, "evt_sk_evento").alias("evt_sk_evento"),
        column_or_default(df_eventos_validos, "evt_id_evento").cast("string").alias("evt_id_evento"),
        column_or_default(df_eventos_validos, "evt_tx_uri").alias("evt_tx_uri"),
        column_or_default(df_eventos_validos, "evt_dh_inicio").cast("timestamp").alias("evt_dh_inicio"),
        column_or_default(df_eventos_validos, "evt_dh_fim").cast("timestamp").alias("evt_dh_fim"),
        column_or_default(df_eventos_validos, "evt_dt_inicio").cast("date").alias("evt_dt_inicio"),
        column_or_default(df_eventos_validos, "evt_dt_fim").cast("date").alias("evt_dt_fim"),
        column_or_default(df_eventos_validos, "evt_nr_ano").cast("int").alias("evt_nr_ano"),
        column_or_default(df_eventos_validos, "evt_nr_mes").cast("int").alias("evt_nr_mes"),
        column_or_default(df_eventos_validos, "leg_id_legislatura").cast("string").alias("leg_id_legislatura"),
        column_or_default(df_eventos_validos, "evt_tx_situacao").alias("evt_tx_situacao"),
        column_or_default(df_eventos_validos, "evt_tx_titulo").alias("evt_tx_titulo"),
        column_or_default(df_eventos_validos, "evt_tx_tipo_evento").alias("evt_tx_tipo_evento"),
        column_or_default(df_eventos_validos, "evt_tx_local").alias("evt_tx_local"),
        column_or_default(df_eventos_validos, "evt_id_orgao").cast("string").alias("evt_id_orgao"),
        column_or_default(df_eventos_validos, "evt_tx_sigla_orgao").alias("evt_tx_sigla_orgao"),
        column_or_default(df_eventos_validos, "evt_tx_nome_orgao").alias("evt_tx_nome_orgao"),
        column_or_default(df_eventos_validos, "evt_tx_tipo_orgao").alias("evt_tx_tipo_orgao"),
        column_or_default(df_eventos_validos, "evt_fl_data_inicio_informada", False).cast("boolean").alias("evt_fl_data_inicio_informada"),
        column_or_default(df_eventos_validos, "evt_fl_orgao_informado", False).cast("boolean").alias("evt_fl_orgao_informado"),
        column_or_default(df_eventos_validos, "evt_fl_tipo_evento_informado", False).cast("boolean").alias("evt_fl_tipo_evento_informado"),
        column_or_default(df_eventos_validos, "evt_fl_titulo_informado", False).cast("boolean").alias("evt_fl_titulo_informado"),
        column_or_default(df_eventos_validos, "evt_fl_periodo_valido", False).cast("boolean").alias("evt_fl_periodo_valido"),
        column_or_default(df_eventos_validos, "evt_fl_legislatura_identificada", False).cast("boolean").alias("evt_fl_legislatura_identificada")
    )
    .dropDuplicates(["evt_id_evento"])
)

# COMMAND ----------

# ============================================================
# STANDARDIZE EVENT ATTENDANCE FACT ATTRIBUTES
# ============================================================

df_presencas_std = (
    df_presencas_validas
    .select(
        column_or_default(df_presencas_validas, "fpe_sk_presenca_evento").alias("fpe_sk_presenca_evento"),
        column_or_default(df_presencas_validas, "evt_sk_evento").alias("evt_sk_evento"),
        column_or_default(df_presencas_validas, "evt_id_evento").cast("string").alias("evt_id_evento"),
        column_or_default(df_presencas_validas, "dep_id_deputado").cast("string").alias("dep_id_deputado"),
        column_or_default(df_presencas_validas, "dep_tx_nome").alias("dep_tx_nome"),
        column_or_default(df_presencas_validas, "dep_tx_sigla_partido").alias("dep_tx_sigla_partido"),
        column_or_default(df_presencas_validas, "dep_tx_sigla_uf").alias("dep_tx_sigla_uf"),
        column_or_default(df_presencas_validas, "pev_fl_presenca", False).cast("boolean").alias("pev_fl_presenca"),
        column_or_default(df_presencas_validas, "fpe_qt_registro_presenca", 1).cast("int").alias("fpe_qt_registro_presenca"),
        column_or_default(df_presencas_validas, "fpe_qt_presenca", 0).cast("int").alias("fpe_qt_presenca"),
        column_or_default(df_presencas_validas, "fpe_qt_ausencia", 0).cast("int").alias("fpe_qt_ausencia"),
        column_or_default(df_presencas_validas, "fpe_fl_evento_encontrado_gold", False).cast("boolean").alias("fpe_fl_evento_encontrado_gold"),
        column_or_default(df_presencas_validas, "fpe_fl_deputado_encontrado_gold", False).cast("boolean").alias("fpe_fl_deputado_encontrado_gold"),
        column_or_default(df_presencas_validas, "fpe_fl_partido_encontrado_gold", False).cast("boolean").alias("fpe_fl_partido_encontrado_gold"),
        column_or_default(df_presencas_validas, "fpe_fl_estado_encontrado_gold", False).cast("boolean").alias("fpe_fl_estado_encontrado_gold"),
        column_or_default(df_presencas_validas, "fpe_fl_data_encontrada_gold", False).cast("boolean").alias("fpe_fl_data_encontrada_gold"),
        column_or_default(df_presencas_validas, "fpe_fl_dimensoes_principais_completas", False).cast("boolean").alias("fpe_fl_dimensoes_principais_completas")
    )
)

# COMMAND ----------

# ============================================================
# PARTY COMPOSITION
# ============================================================

df_partido_counts = (
    df_presencas_std
    .filter(F.col("dep_tx_sigla_partido").isNotNull())
    .groupBy(
        "evt_id_evento",
        "dep_tx_sigla_partido"
    )
    .agg(
        F.countDistinct("dep_id_deputado").alias("cal_qt_deputados_partido")
    )
)

party_rank_window = Window.partitionBy("evt_id_evento").orderBy(
    F.col("cal_qt_deputados_partido").desc(),
    F.col("dep_tx_sigla_partido").asc()
)

df_partido_predominante = (
    df_partido_counts
    .withColumn("rn", F.row_number().over(party_rank_window))
    .filter(F.col("rn") == 1)
    .select(
        "evt_id_evento",
        F.col("dep_tx_sigla_partido").alias("cal_tx_partido_predominante"),
        F.col("cal_qt_deputados_partido").alias("cal_qt_deputados_partido_predominante")
    )
)

# COMMAND ----------

# ============================================================
# GEOGRAPHIC COMPOSITION
# ============================================================

df_uf_counts = (
    df_presencas_std
    .filter(F.col("dep_tx_sigla_uf").isNotNull())
    .groupBy(
        "evt_id_evento",
        "dep_tx_sigla_uf"
    )
    .agg(
        F.countDistinct("dep_id_deputado").alias("cal_qt_deputados_uf")
    )
)

uf_rank_window = Window.partitionBy("evt_id_evento").orderBy(
    F.col("cal_qt_deputados_uf").desc(),
    F.col("dep_tx_sigla_uf").asc()
)

df_uf_predominante = (
    df_uf_counts
    .withColumn("rn", F.row_number().over(uf_rank_window))
    .filter(F.col("rn") == 1)
    .select(
        "evt_id_evento",
        F.col("dep_tx_sigla_uf").alias("cal_tx_uf_predominante"),
        F.col("cal_qt_deputados_uf").alias("cal_qt_deputados_uf_predominante")
    )
)

# COMMAND ----------

# ============================================================
# EVENT PARTICIPATION AGGREGATION
# ============================================================

df_event_metrics = (
    df_presencas_std
    .groupBy("evt_id_evento")
    .agg(
        F.sum("fpe_qt_registro_presenca").alias("cal_qt_registros_presenca"),
        F.countDistinct("dep_id_deputado").alias("cal_qt_deputados"),
        F.countDistinct(F.when(F.col("pev_fl_presenca") == True, F.col("dep_id_deputado"))).alias("cal_qt_deputados_presentes"),
        F.countDistinct(F.when(F.col("pev_fl_presenca") == False, F.col("dep_id_deputado"))).alias("cal_qt_deputados_ausentes"),
        F.sum("fpe_qt_presenca").alias("cal_qt_presencas"),
        F.sum("fpe_qt_ausencia").alias("cal_qt_ausencias"),
        F.countDistinct("dep_tx_sigla_partido").alias("cal_qt_partidos"),
        F.countDistinct("dep_tx_sigla_uf").alias("cal_qt_ufs"),
        F.sum(F.when(F.col("fpe_fl_evento_encontrado_gold") == True, 1).otherwise(0)).alias("cal_qt_evento_encontrado_gold"),
        F.sum(F.when(F.col("fpe_fl_deputado_encontrado_gold") == True, 1).otherwise(0)).alias("cal_qt_deputado_encontrado_gold"),
        F.sum(F.when(F.col("fpe_fl_partido_encontrado_gold") == True, 1).otherwise(0)).alias("cal_qt_partido_encontrado_gold"),
        F.sum(F.when(F.col("fpe_fl_estado_encontrado_gold") == True, 1).otherwise(0)).alias("cal_qt_estado_encontrado_gold"),
        F.sum(F.when(F.col("fpe_fl_data_encontrada_gold") == True, 1).otherwise(0)).alias("cal_qt_data_encontrada_gold"),
        F.sum(F.when(F.col("fpe_fl_dimensoes_principais_completas") == True, 1).otherwise(0)).alias("cal_qt_dimensoes_completas")
    )
)

# COMMAND ----------

# ============================================================
# BUILD MART
# ============================================================

df_mart_base = (
    df_eventos_std.alias("e")
    .join(
        df_event_metrics.alias("m"),
        F.col("e.evt_id_evento") == F.col("m.evt_id_evento"),
        "left"
    )
    .join(
        df_partido_predominante.alias("p"),
        F.col("e.evt_id_evento") == F.col("p.evt_id_evento"),
        "left"
    )
    .join(
        df_uf_predominante.alias("u"),
        F.col("e.evt_id_evento") == F.col("u.evt_id_evento"),
        "left"
    )
)

rank_window = Window.orderBy(
    F.col("cal_qt_deputados").desc(),
    F.col("e.evt_id_evento").asc()
)

df_mart = (
    df_mart_base
    .withColumn("cal_qt_registros_presenca", F.coalesce(F.col("cal_qt_registros_presenca"), F.lit(0)))
    .withColumn("cal_qt_deputados", F.coalesce(F.col("cal_qt_deputados"), F.lit(0)))
    .withColumn("cal_qt_deputados_presentes", F.coalesce(F.col("cal_qt_deputados_presentes"), F.lit(0)))
    .withColumn("cal_qt_deputados_ausentes", F.coalesce(F.col("cal_qt_deputados_ausentes"), F.lit(0)))
    .withColumn("cal_qt_presencas", F.coalesce(F.col("cal_qt_presencas"), F.lit(0)))
    .withColumn("cal_qt_ausencias", F.coalesce(F.col("cal_qt_ausencias"), F.lit(0)))
    .withColumn("cal_qt_partidos", F.coalesce(F.col("cal_qt_partidos"), F.lit(0)))
    .withColumn("cal_qt_ufs", F.coalesce(F.col("cal_qt_ufs"), F.lit(0)))
    .withColumn("cal_qt_evento_encontrado_gold", F.coalesce(F.col("cal_qt_evento_encontrado_gold"), F.lit(0)))
    .withColumn("cal_qt_deputado_encontrado_gold", F.coalesce(F.col("cal_qt_deputado_encontrado_gold"), F.lit(0)))
    .withColumn("cal_qt_partido_encontrado_gold", F.coalesce(F.col("cal_qt_partido_encontrado_gold"), F.lit(0)))
    .withColumn("cal_qt_estado_encontrado_gold", F.coalesce(F.col("cal_qt_estado_encontrado_gold"), F.lit(0)))
    .withColumn("cal_qt_data_encontrada_gold", F.coalesce(F.col("cal_qt_data_encontrada_gold"), F.lit(0)))
    .withColumn("cal_qt_dimensoes_completas", F.coalesce(F.col("cal_qt_dimensoes_completas"), F.lit(0)))
    .withColumn("cal_qt_deputados_partido_predominante", F.coalesce(F.col("cal_qt_deputados_partido_predominante"), F.lit(0)))
    .withColumn("cal_qt_deputados_uf_predominante", F.coalesce(F.col("cal_qt_deputados_uf_predominante"), F.lit(0)))
    .withColumn(
        "cal_sk_calendario_evento",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("e.evt_id_evento"), F.lit("")),
                F.coalesce(F.col("e.leg_id_legislatura"), F.lit("")),
                F.coalesce(F.col("e.evt_dt_inicio").cast("string"), F.lit(""))
            ),
            256
        )
    )
    .withColumn(
        "cal_vl_pct_presenca",
        F.when(
            F.col("cal_qt_registros_presenca") > F.lit(0),
            F.round((F.col("cal_qt_presencas") / F.col("cal_qt_registros_presenca")) * F.lit(100), 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "cal_vl_pct_dimensoes_completas",
        F.when(
            F.col("cal_qt_registros_presenca") > F.lit(0),
            F.round((F.col("cal_qt_dimensoes_completas") / F.col("cal_qt_registros_presenca")) * F.lit(100), 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "cal_nr_rank_engajamento",
        F.dense_rank().over(rank_window)
    )
    .withColumn(
        "cal_fl_possui_presenca",
        F.col("cal_qt_registros_presenca") > F.lit(0)
    )
    .withColumn(
        "cal_fl_possui_participacao_deputados",
        F.col("cal_qt_deputados") > F.lit(0)
    )
    .withColumn(
        "cal_fl_composicao_partidaria_identificada",
        F.col("cal_qt_partidos") > F.lit(0)
    )
    .withColumn(
        "cal_fl_distribuicao_uf_identificada",
        F.col("cal_qt_ufs") > F.lit(0)
    )
    .withColumn(
        "cal_fl_evento_realizado",
        F.when(
            F.col("e.evt_dt_inicio").isNotNull()
            & (F.col("e.evt_dt_inicio") <= F.current_date()),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "cal_fl_registro_valido_marts",
        F.col("cal_sk_calendario_evento").isNotNull()
        & F.col("e.evt_id_evento").isNotNull()
        & F.col("e.evt_tx_titulo").isNotNull()
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
                F.coalesce(F.col("cal_sk_calendario_evento"), F.lit("")),
                F.coalesce(F.col("e.evt_id_evento"), F.lit("")),
                F.coalesce(F.col("e.evt_tx_titulo"), F.lit("")),
                F.coalesce(F.col("e.evt_dt_inicio").cast("string"), F.lit("")),
                F.coalesce(F.col("cal_qt_deputados").cast("string"), F.lit("")),
                F.coalesce(F.col("cal_qt_presencas").cast("string"), F.lit("")),
                F.coalesce(F.col("cal_qt_partidos").cast("string"), F.lit("")),
                F.coalesce(F.col("cal_qt_ufs").cast("string"), F.lit(""))
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
        F.col("cal_sk_calendario_evento"),
        F.col("e.evt_sk_evento").alias("evt_sk_evento"),
        F.col("e.evt_id_evento").alias("evt_id_evento"),
        F.col("e.evt_tx_uri").alias("evt_tx_uri"),
        F.col("e.evt_dh_inicio").alias("evt_dh_inicio"),
        F.col("e.evt_dh_fim").alias("evt_dh_fim"),
        F.col("e.evt_dt_inicio").alias("evt_dt_inicio"),
        F.col("e.evt_dt_fim").alias("evt_dt_fim"),
        F.col("e.evt_nr_ano").alias("evt_nr_ano"),
        F.col("e.evt_nr_mes").alias("evt_nr_mes"),
        F.col("e.leg_id_legislatura").alias("leg_id_legislatura"),
        F.col("e.evt_tx_situacao").alias("evt_tx_situacao"),
        F.col("e.evt_tx_titulo").alias("evt_tx_titulo"),
        F.col("e.evt_tx_tipo_evento").alias("evt_tx_tipo_evento"),
        F.col("e.evt_tx_local").alias("evt_tx_local"),
        F.col("e.evt_id_orgao").alias("evt_id_orgao"),
        F.col("e.evt_tx_sigla_orgao").alias("evt_tx_sigla_orgao"),
        F.col("e.evt_tx_nome_orgao").alias("evt_tx_nome_orgao"),
        F.col("e.evt_tx_tipo_orgao").alias("evt_tx_tipo_orgao"),
        F.col("cal_qt_registros_presenca"),
        F.col("cal_qt_deputados"),
        F.col("cal_qt_deputados_presentes"),
        F.col("cal_qt_deputados_ausentes"),
        F.col("cal_qt_presencas"),
        F.col("cal_qt_ausencias"),
        F.col("cal_vl_pct_presenca"),
        F.col("cal_qt_partidos"),
        F.col("cal_tx_partido_predominante"),
        F.col("cal_qt_deputados_partido_predominante"),
        F.col("cal_qt_ufs"),
        F.col("cal_tx_uf_predominante"),
        F.col("cal_qt_deputados_uf_predominante"),
        F.col("cal_vl_pct_dimensoes_completas"),
        F.col("cal_nr_rank_engajamento"),
        F.col("cal_fl_possui_presenca"),
        F.col("cal_fl_possui_participacao_deputados"),
        F.col("cal_fl_composicao_partidaria_identificada"),
        F.col("cal_fl_distribuicao_uf_identificada"),
        F.col("cal_fl_evento_realizado"),
        F.col("cal_fl_registro_valido_marts"),
        F.col("e.evt_fl_data_inicio_informada").alias("evt_fl_data_inicio_informada"),
        F.col("e.evt_fl_orgao_informado").alias("evt_fl_orgao_informado"),
        F.col("e.evt_fl_tipo_evento_informado").alias("evt_fl_tipo_evento_informado"),
        F.col("e.evt_fl_titulo_informado").alias("evt_fl_titulo_informado"),
        F.col("e.evt_fl_periodo_valido").alias("evt_fl_periodo_valido"),
        F.col("e.evt_fl_legislatura_identificada").alias("evt_fl_legislatura_identificada"),
        F.col("cal_qt_evento_encontrado_gold"),
        F.col("cal_qt_deputado_encontrado_gold"),
        F.col("cal_qt_partido_encontrado_gold"),
        F.col("cal_qt_estado_encontrado_gold"),
        F.col("cal_qt_data_encontrada_gold"),
        F.col("cal_qt_dimensoes_completas"),
        F.col("aud_id_execucao_marts"),
        F.col("aud_dh_processamento_marts"),
        F.col("aud_tx_versao_pipeline_marts"),
        F.col("aud_tx_hash_registro_marts")
    )
    .dropDuplicates(["evt_id_evento"])
)

# COMMAND ----------

# ============================================================
# QUALITY VALIDATIONS
# ============================================================

required_columns_result = validate_required_columns(
    dataframe=df_final,
    required_columns=[
        "cal_sk_calendario_evento",
        "evt_id_evento",
        "evt_tx_titulo"
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_final,
    key_columns=[
        "evt_id_evento"
    ]
)

null_results = validate_nulls(
    dataframe=df_final,
    columns=[
        "cal_sk_calendario_evento",
        "evt_id_evento"
    ]
)

empty_mart_result = {
    "check_name": "empty_mart_result",
    "check_status": "PASSED" if df_final.count() > 0 else "FAILED",
    "check_message": "Mart contains records." if df_final.count() > 0 else "Mart is empty."
}

negative_metrics_count = (
    df_final
    .filter(
        (F.col("cal_qt_registros_presenca") < 0)
        | (F.col("cal_qt_deputados") < 0)
        | (F.col("cal_qt_deputados_presentes") < 0)
        | (F.col("cal_qt_deputados_ausentes") < 0)
        | (F.col("cal_qt_presencas") < 0)
        | (F.col("cal_qt_ausencias") < 0)
        | (F.col("cal_qt_partidos") < 0)
        | (F.col("cal_qt_ufs") < 0)
    )
    .count()
)

negative_metrics_result = {
    "check_name": "negative_mart_metrics",
    "check_status": "PASSED" if negative_metrics_count == 0 else "FAILED",
    "check_message": "No negative mart metrics found." if negative_metrics_count == 0 else f"Found {negative_metrics_count} records with negative metrics."
}

quality_results = [
    required_columns_result,
    duplicate_result,
    empty_mart_result,
    negative_metrics_result
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
    dataframe=df_final.orderBy("evt_dt_inicio", "evt_tx_titulo"),
    temporary_path=EXPORT_TMP_DIR,
    final_directory=EXPORT_DIR,
    final_file_path=EXPORT_FILE_PATH
)

# COMMAND ----------

# ============================================================
# GOVERNANCE COMMENTS
# ============================================================

TABLE_COMMENT = """Business Mart for the Legislative Events Calendar.

This mart contains one analytical record per legislative event.

Main characteristics:
* legislative event descriptive attributes
* event calendar attributes
* legislative body and organization indicators
* parliamentary participation indicators
* attendance indicators
* party composition indicators
* federation unit distribution indicators
* engagement ranking
* Gold lineage
* Marts lineage
* governance metadata
* CSV export for delivery consumption
"""

COLUMN_COMMENTS = {
    "cal_sk_calendario_evento":
        "Marts surrogate key for the Legislative Events Calendar record.",
    "evt_sk_evento":
        "Gold surrogate key of the legislative event dimension.",
    "evt_id_evento":
        "Business identifier of the legislative event.",
    "evt_tx_uri":
        "Legislative event URI from the source system.",
    "evt_dh_inicio":
        "Event start timestamp.",
    "evt_dh_fim":
        "Event end timestamp.",
    "evt_dt_inicio":
        "Event start date.",
    "evt_dt_fim":
        "Event end date.",
    "evt_nr_ano":
        "Event year.",
    "evt_nr_mes":
        "Event month number.",
    "leg_id_legislatura":
        "Legislature identifier associated with the event.",
    "evt_tx_situacao":
        "Legislative event status.",
    "evt_tx_titulo":
        "Legislative event title.",
    "evt_tx_tipo_evento":
        "Legislative event type.",
    "evt_tx_local":
        "Legislative event location.",
    "evt_id_orgao":
        "Legislative body identifier associated with the event.",
    "evt_tx_sigla_orgao":
        "Legislative body acronym associated with the event.",
    "evt_tx_nome_orgao":
        "Legislative body name associated with the event.",
    "evt_tx_tipo_orgao":
        "Legislative body type associated with the event.",
    "cal_qt_registros_presenca":
        "Total number of attendance records associated with the event.",
    "cal_qt_deputados":
        "Number of distinct deputies associated with the event.",
    "cal_qt_deputados_presentes":
        "Number of distinct deputies marked as present in the event.",
    "cal_qt_deputados_ausentes":
        "Number of distinct deputies marked as absent in the event.",
    "cal_qt_presencas":
        "Total additive presence count for the event.",
    "cal_qt_ausencias":
        "Total additive absence count for the event.",
    "cal_vl_pct_presenca":
        "Percentage of presence records over total attendance records for the event.",
    "cal_qt_partidos":
        "Number of distinct political parties represented in the event attendance records.",
    "cal_tx_partido_predominante":
        "Political party with the highest number of distinct deputies in the event.",
    "cal_qt_deputados_partido_predominante":
        "Number of distinct deputies from the predominant party in the event.",
    "cal_qt_ufs":
        "Number of distinct federation units represented in the event attendance records.",
    "cal_tx_uf_predominante":
        "Federation unit with the highest number of distinct deputies in the event.",
    "cal_qt_deputados_uf_predominante":
        "Number of distinct deputies from the predominant federation unit in the event.",
    "cal_vl_pct_dimensoes_completas":
        "Percentage of attendance records with complete main Gold dimensional coverage.",
    "cal_nr_rank_engajamento":
        "Event engagement ranking based on the number of distinct deputies.",
    "cal_fl_possui_presenca":
        "Flag indicating whether the event has at least one attendance record.",
    "cal_fl_possui_participacao_deputados":
        "Flag indicating whether the event has at least one associated deputy.",
    "cal_fl_composicao_partidaria_identificada":
        "Flag indicating whether party composition was identified for the event.",
    "cal_fl_distribuicao_uf_identificada":
        "Flag indicating whether federation unit distribution was identified for the event.",
    "cal_fl_evento_realizado":
        "Flag indicating whether the event start date is not later than the processing date.",
    "cal_fl_registro_valido_marts":
        "Flag indicating whether the mart record passed Marts validation.",
    "evt_fl_data_inicio_informada":
        "Flag indicating whether event start datetime is available.",
    "evt_fl_orgao_informado":
        "Flag indicating whether event legislative body identifier is available.",
    "evt_fl_tipo_evento_informado":
        "Flag indicating whether event type is available.",
    "evt_fl_titulo_informado":
        "Flag indicating whether event title is available.",
    "evt_fl_periodo_valido":
        "Flag indicating whether event start and end timestamps form a valid period.",
    "evt_fl_legislatura_identificada":
        "Flag indicating whether event legislature was derived from event year.",
    "cal_qt_evento_encontrado_gold":
        "Number of attendance records with event dimension found in Gold.",
    "cal_qt_deputado_encontrado_gold":
        "Number of attendance records with deputy dimension found in Gold.",
    "cal_qt_partido_encontrado_gold":
        "Number of attendance records with party dimension found in Gold.",
    "cal_qt_estado_encontrado_gold":
        "Number of attendance records with state dimension found in Gold.",
    "cal_qt_data_encontrada_gold":
        "Number of attendance records with date dimension found in Gold.",
    "cal_qt_dimensoes_completas":
        "Number of attendance records with all main dimensions complete.",
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
    message="Legislative Events Calendar mart generated successfully.",
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
print("MART CALENDARIO EVENTOS - RESUMO EXECUÇÃO")
print("=" * 80)
print(f"Records read from Gold event dimension: {records_read_eventos}")
print(f"Records read from Gold attendance fact: {records_read_presencas}")
print(f"Records eligible from Gold event dimension: {records_eligible_eventos}")
print(f"Records eligible from Gold attendance fact: {records_eligible_presencas}")
print(f"Records written to Marts: {records_written}")
print(f"CSV export path: {EXPORT_FILE_PATH}")
print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(mart_df.orderBy("evt_dt_inicio", "evt_tx_titulo").limit(20))
