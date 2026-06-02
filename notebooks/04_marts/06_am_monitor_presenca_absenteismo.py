# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Marts — Registered Attendance Coverage
# MAGIC
# MAGIC **Notebook:** `06_am_presenca_absenteismo`
# MAGIC
# MAGIC Builds the curated Business Mart for deputy registered attendance coverage in legislative events.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Attendance coverage mart model
# MAGIC * One analytical record per deputy, year, month and legislature
# MAGIC * Registered attendance metrics
# MAGIC * Attendance rate indicators
# MAGIC * Compatibility absenteeism fields
# MAGIC * Source limitation documentation
# MAGIC * Marts governance metadata
# MAGIC * Column and table comments
# MAGIC * Marts validation rules
# MAGIC * Marts execution logging
# MAGIC * CSV export for delivery evidence
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Important Source Limitation
# MAGIC
# MAGIC The Gold source `brazil_legislative_analytics.gold.ft_presencas_eventos` is built from the Câmara dos Deputados source file/API `eventosPresencaDeputados`.
# MAGIC
# MAGIC The original source contains only records of deputies with confirmed presence in legislative events. It does not provide a complete expected attendance list and does not provide explicit absence records.
# MAGIC
# MAGIC Consequences for this mart:
# MAGIC
# MAGIC * `pab_qt_ausencias` is expected to be zero.
# MAGIC * `pab_vl_pct_absenteismo` is expected to be zero.
# MAGIC * `pab_fl_alto_absenteismo` is expected to be false.
# MAGIC * `pab_fl_baixo_absenteismo` is expected to be true when the record has attendance records.
# MAGIC * Absenteeism fields are preserved for compatibility with the analytical model, but they should not be interpreted as complete parliamentary absenteeism.
# MAGIC
# MAGIC This mart should be interpreted as **registered attendance coverage**, not as a complete absence/absenteeism mart.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC * Read validated Gold attendance fact records
# MAGIC * Keep one analytical record per deputy, year, month and legislature
# MAGIC * Preserve deputy, party, state and legislature identifiers
# MAGIC * Calculate registered attendance indicators
# MAGIC * Preserve compatibility absenteeism indicators with documented source limitation
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
# MAGIC 1. Which deputies have registered attendance in legislative events by period?
# MAGIC 2. How many legislative events are represented by deputy and month?
# MAGIC 3. What is the registered attendance volume by party, state and legislature?
# MAGIC 4. Which deputies have the highest registered attendance volume?
# MAGIC 5. What is the dimensional coverage of the attendance mart?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Mart Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per deputy, year, month and legislature.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC * `brazil_legislative_analytics.gold.ft_presencas_eventos`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.marts.am_presenca_absenteismo`
# MAGIC
# MAGIC ### CSV Export
# MAGIC
# MAGIC `/Volumes/brazil_legislative_analytics/marts/exports/am_presenca_absenteismo/am_presenca_absenteismo.csv`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC * `dep_id_deputado`
# MAGIC * `leg_id_legislatura`
# MAGIC * `pab_nr_ano`
# MAGIC * `pab_nr_mes`
# MAGIC
# MAGIC ### Mart Surrogate Key
# MAGIC
# MAGIC `pab_sk_presenca_absenteismo`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC Rule 1:
# MAGIC
# MAGIC Only valid Gold attendance records are eligible.
# MAGIC
# MAGIC Rule 2:
# MAGIC
# MAGIC One analytical record is maintained per deputy, year, month and legislature.
# MAGIC
# MAGIC Rule 3:
# MAGIC
# MAGIC Registered attendance metrics are derived from `ft_presencas_eventos`.
# MAGIC
# MAGIC Rule 4:
# MAGIC
# MAGIC Because the source only provides confirmed presence records, absence and absenteeism metrics are compatibility fields and are expected to remain zero.
# MAGIC
# MAGIC Rule 5:
# MAGIC
# MAGIC The mart must preserve dimensional coverage indicators for deputy, party and state dimensions.
# MAGIC
# MAGIC Rule 6:
# MAGIC
# MAGIC The mart must be published as Delta and exported as CSV.
# MAGIC
# MAGIC Rule 7:
# MAGIC
# MAGIC All Marts objects must contain governance comments.
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

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import uuid

# COMMAND ----------

# ============================================================
# EXECUTION CONFIGURATION
# ============================================================

try:
    CATALOG_NAME
except NameError:
    CATALOG_NAME = "brazil_legislative_analytics"

try:
    GOLD_SCHEMA
except NameError:
    GOLD_SCHEMA = f"{CATALOG_NAME}.gold"

try:
    MARTS_SCHEMA
except NameError:
    MARTS_SCHEMA = f"{CATALOG_NAME}.marts"

SOURCE_FACT_ATTENDANCE = f"{GOLD_SCHEMA}.ft_presencas_eventos"
TARGET_TABLE = f"{MARTS_SCHEMA}.am_presenca_absenteismo"
EXPORT_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/marts/exports/am_presenca_absenteismo"
EXPORT_CSV_PATH = f"{EXPORT_VOLUME_PATH}/am_presenca_absenteismo.csv"

PIPELINE_VERSION = "marts_v1.1_registered_attendance_coverage"
EXECUTION_ID = str(uuid.uuid4())
PROCESSING_TS = F.current_timestamp()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.marts.exports")
dbutils.fs.mkdirs(EXPORT_VOLUME_PATH)

# COMMAND ----------

# ============================================================
# READ GOLD SOURCE
# ============================================================

attendance_raw_df = spark.table(SOURCE_FACT_ATTENDANCE)
records_read_attendance = attendance_raw_df.count()

# COMMAND ----------

# ============================================================
# SOURCE LIMITATION VALIDATION
# ============================================================
# The source eventosPresencaDeputados contains only confirmed presence records.
# This validation is intentionally informational and must not fail the mart.

source_profile_df = (
    attendance_raw_df
    .agg(
        F.count(F.lit(1)).alias("total_registros"),
        F.sum(F.col("fpe_qt_registro_presenca")).alias("total_registros_presenca"),
        F.sum(F.col("fpe_qt_presenca")).alias("total_presencas"),
        F.sum(F.col("fpe_qt_ausencia")).alias("total_ausencias"),
        F.sum(F.when(F.col("pev_fl_presenca") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).alias("registros_flag_presenca_true"),
        F.sum(F.when(F.col("pev_fl_presenca") == F.lit(False), F.lit(1)).otherwise(F.lit(0))).alias("registros_flag_presenca_false")
    )
)

source_profile = source_profile_df.collect()[0].asDict()
source_total_absences = source_profile.get("total_ausencias") or 0
source_flag_false = source_profile.get("registros_flag_presenca_false") or 0

# COMMAND ----------

# ============================================================
# FILTER VALID GOLD RECORDS
# ============================================================

attendance_valid_df = (
    attendance_raw_df
    .filter(F.col("fpe_fl_registro_valido_gold") == F.lit(True))
)

records_eligible_attendance = attendance_valid_df.count()

# COMMAND ----------

# ============================================================
# STANDARDIZE ATTENDANCE FACT
# Explicit select prevents ambiguous columns in downstream transformations.
# ============================================================

attendance_df = (
    attendance_valid_df
    .select(
        F.col("fpe_sk_presenca_evento"),
        F.col("evt_sk_evento"),
        F.col("dep_sk_deputado"),
        F.col("par_sk_partido"),
        F.col("est_sk_estado"),
        F.col("dat_sk_data"),
        F.col("pev_id_presenca_evento"),
        F.col("evt_id_evento"),
        F.col("evt_dt_inicio"),
        F.col("evt_nr_ano"),
        F.col("evt_nr_mes"),
        F.col("evt_tx_tipo_evento"),
        F.col("evt_tx_sigla_orgao"),
        F.col("leg_id_legislatura"),
        F.col("dep_id_deputado"),
        F.col("dep_tx_nome"),
        F.col("dep_tx_sigla_partido"),
        F.col("dep_tx_sigla_uf"),
        F.col("leg_id_legislatura_deputado"),
        F.col("pev_fl_presenca"),
        F.col("pev_fl_presenca_origem"),
        F.col("fpe_qt_registro_presenca"),
        F.col("fpe_qt_presenca"),
        F.col("fpe_qt_ausencia"),
        F.col("fpe_fl_evento_encontrado_gold"),
        F.col("fpe_fl_deputado_encontrado_gold"),
        F.col("fpe_fl_partido_encontrado_gold"),
        F.col("fpe_fl_estado_encontrado_gold"),
        F.col("fpe_fl_data_encontrada_gold"),
        F.col("fpe_fl_dimensoes_principais_completas"),
        F.col("aud_id_execucao_gold"),
        F.col("aud_dh_processamento_gold")
    )
)

# COMMAND ----------

# ============================================================
# AGGREGATE MART GRAIN
# One record per deputy, year, month and legislature.
# ============================================================

group_cols = [
    "dep_sk_deputado",
    "par_sk_partido",
    "est_sk_estado",
    "dep_id_deputado",
    "dep_tx_nome",
    "dep_tx_sigla_partido",
    "dep_tx_sigla_uf",
    "leg_id_legislatura",
    "leg_id_legislatura_deputado",
    "evt_nr_ano",
    "evt_nr_mes"
]

agg_df = (
    attendance_df
    .groupBy(*group_cols)
    .agg(
        F.min("evt_dt_inicio").alias("pab_dt_primeiro_evento"),
        F.max("evt_dt_inicio").alias("pab_dt_ultimo_evento"),
        F.countDistinct("evt_id_evento").alias("pab_qt_eventos_distintos"),
        F.countDistinct("evt_tx_tipo_evento").alias("pab_qt_tipos_evento"),
        F.countDistinct("evt_tx_sigla_orgao").alias("pab_qt_orgaos_distintos"),
        F.sum("fpe_qt_registro_presenca").alias("pab_qt_registros_presenca"),
        F.sum("fpe_qt_presenca").alias("pab_qt_presencas"),
        F.sum("fpe_qt_ausencia").alias("pab_qt_ausencias"),
        F.sum(F.when(F.col("fpe_fl_evento_encontrado_gold") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).alias("pab_qt_evento_encontrado_gold"),
        F.sum(F.when(F.col("fpe_fl_deputado_encontrado_gold") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).alias("pab_qt_deputado_encontrado_gold"),
        F.sum(F.when(F.col("fpe_fl_partido_encontrado_gold") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).alias("pab_qt_partido_encontrado_gold"),
        F.sum(F.when(F.col("fpe_fl_estado_encontrado_gold") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).alias("pab_qt_estado_encontrado_gold"),
        F.sum(F.when(F.col("fpe_fl_data_encontrada_gold") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).alias("pab_qt_data_encontrada_gold"),
        F.sum(F.when(F.col("fpe_fl_dimensoes_principais_completas") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).alias("pab_qt_dimensoes_completas"),
        F.countDistinct("aud_id_execucao_gold").alias("pab_qt_execucoes_gold"),
        F.max("aud_dh_processamento_gold").alias("pab_dh_ultimo_processamento_gold")
    )
    .withColumnRenamed("evt_nr_ano", "pab_nr_ano")
    .withColumnRenamed("evt_nr_mes", "pab_nr_mes")
)

# COMMAND ----------

# ============================================================
# DERIVED INDICATORS
# ============================================================

metrics_df = (
    agg_df
    .withColumn(
        "pab_tx_ano_mes",
        F.concat_ws("-", F.col("pab_nr_ano"), F.lpad(F.col("pab_nr_mes").cast("string"), 2, "0"))
    )
    .withColumn(
        "pab_vl_pct_presenca",
        F.when(
            F.col("pab_qt_registros_presenca") > 0,
            F.round((F.col("pab_qt_presencas") / F.col("pab_qt_registros_presenca")) * F.lit(100.0), 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "pab_vl_pct_absenteismo",
        F.when(
            F.col("pab_qt_registros_presenca") > 0,
            F.round((F.col("pab_qt_ausencias") / F.col("pab_qt_registros_presenca")) * F.lit(100.0), 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "pab_vl_pct_dimensoes_completas",
        F.when(
            F.col("pab_qt_registros_presenca") > 0,
            F.round((F.col("pab_qt_dimensoes_completas") / F.col("pab_qt_registros_presenca")) * F.lit(100.0), 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn("pab_fl_possui_presenca", F.col("pab_qt_presencas") > 0)
    .withColumn("pab_fl_possui_ausencia", F.col("pab_qt_ausencias") > 0)
    .withColumn("pab_fl_alto_absenteismo", F.col("pab_vl_pct_absenteismo") >= F.lit(30.0))
    .withColumn("pab_fl_baixo_absenteismo", F.col("pab_vl_pct_absenteismo") <= F.lit(10.0))
    .withColumn(
        "pab_fl_dimensoes_principais_completas",
        F.col("pab_qt_dimensoes_completas") == F.col("pab_qt_registros_presenca")
    )
)

# COMMAND ----------

# ============================================================
# RANKINGS
# ============================================================

rank_presence_window = Window.partitionBy("pab_nr_ano", "pab_nr_mes").orderBy(
    F.col("pab_vl_pct_presenca").desc(),
    F.col("pab_qt_presencas").desc(),
    F.col("dep_id_deputado").asc()
)

# Compatibility ranking. Because the source does not provide absences, this ranking is informational only.
rank_absenteeism_window = Window.partitionBy("pab_nr_ano", "pab_nr_mes").orderBy(
    F.col("pab_vl_pct_absenteismo").desc(),
    F.col("pab_qt_ausencias").desc(),
    F.col("dep_id_deputado").asc()
)

ranked_df = (
    metrics_df
    .withColumn("pab_nr_rank_presenca_periodo", F.row_number().over(rank_presence_window))
    .withColumn("pab_nr_rank_absenteismo_periodo", F.row_number().over(rank_absenteeism_window))
)

# COMMAND ----------

# ============================================================
# BUSINESS KEY, SURROGATE KEY AND AUDIT
# ============================================================

final_base_df = (
    ranked_df
    .withColumn(
        "pab_tx_business_key",
        F.concat_ws(
            "|",
            F.coalesce(F.col("dep_id_deputado"), F.lit("NA")),
            F.coalesce(F.col("leg_id_legislatura").cast("string"), F.lit("NA")),
            F.coalesce(F.col("pab_nr_ano").cast("string"), F.lit("NA")),
            F.coalesce(F.col("pab_nr_mes").cast("string"), F.lit("NA"))
        )
    )
    .withColumn("pab_sk_presenca_absenteismo", F.sha2(F.col("pab_tx_business_key"), 256))
    .withColumn(
        "pab_fl_registro_valido_marts",
        F.col("pab_sk_presenca_absenteismo").isNotNull()
        & F.col("pab_tx_business_key").isNotNull()
        & F.col("dep_id_deputado").isNotNull()
        & F.col("pab_nr_ano").isNotNull()
        & F.col("pab_nr_mes").isNotNull()
        & (F.col("pab_qt_registros_presenca") >= 0)
        & (F.col("pab_qt_presencas") >= 0)
        & (F.col("pab_qt_ausencias") >= 0)
        & (F.col("pab_vl_pct_presenca") >= 0)
        & (F.col("pab_vl_pct_presenca") <= 100)
        & (F.col("pab_vl_pct_absenteismo") >= 0)
        & (F.col("pab_vl_pct_absenteismo") <= 100)
    )
    .withColumn("aud_id_execucao_marts", F.lit(EXECUTION_ID))
    .withColumn("aud_dh_processamento_marts", PROCESSING_TS)
    .withColumn("aud_tx_versao_pipeline_marts", F.lit(PIPELINE_VERSION))
)

hash_cols = [
    "pab_sk_presenca_absenteismo",
    "pab_tx_business_key",
    "dep_sk_deputado",
    "par_sk_partido",
    "est_sk_estado",
    "dep_id_deputado",
    "dep_tx_nome",
    "dep_tx_sigla_partido",
    "dep_tx_sigla_uf",
    "leg_id_legislatura",
    "leg_id_legislatura_deputado",
    "pab_nr_ano",
    "pab_nr_mes",
    "pab_tx_ano_mes",
    "pab_dt_primeiro_evento",
    "pab_dt_ultimo_evento",
    "pab_qt_eventos_distintos",
    "pab_qt_tipos_evento",
    "pab_qt_orgaos_distintos",
    "pab_qt_registros_presenca",
    "pab_qt_presencas",
    "pab_qt_ausencias",
    "pab_vl_pct_presenca",
    "pab_vl_pct_absenteismo",
    "pab_vl_pct_dimensoes_completas",
    "pab_nr_rank_presenca_periodo",
    "pab_nr_rank_absenteismo_periodo",
    "pab_fl_possui_presenca",
    "pab_fl_possui_ausencia",
    "pab_fl_alto_absenteismo",
    "pab_fl_baixo_absenteismo",
    "pab_fl_dimensoes_principais_completas",
    "pab_fl_registro_valido_marts",
    "pab_qt_evento_encontrado_gold",
    "pab_qt_deputado_encontrado_gold",
    "pab_qt_partido_encontrado_gold",
    "pab_qt_estado_encontrado_gold",
    "pab_qt_data_encontrada_gold",
    "pab_qt_dimensoes_completas",
    "pab_qt_execucoes_gold",
    "pab_dh_ultimo_processamento_gold"
]

final_df = (
    final_base_df
    .withColumn(
        "aud_tx_hash_registro_marts",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]), 256)
    )
    .select(
        "pab_sk_presenca_absenteismo",
        "pab_tx_business_key",
        "dep_sk_deputado",
        "par_sk_partido",
        "est_sk_estado",
        "dep_id_deputado",
        "dep_tx_nome",
        "dep_tx_sigla_partido",
        "dep_tx_sigla_uf",
        "leg_id_legislatura",
        "leg_id_legislatura_deputado",
        "pab_nr_ano",
        "pab_nr_mes",
        "pab_tx_ano_mes",
        "pab_dt_primeiro_evento",
        "pab_dt_ultimo_evento",
        "pab_qt_eventos_distintos",
        "pab_qt_tipos_evento",
        "pab_qt_orgaos_distintos",
        "pab_qt_registros_presenca",
        "pab_qt_presencas",
        "pab_qt_ausencias",
        "pab_vl_pct_presenca",
        "pab_vl_pct_absenteismo",
        "pab_vl_pct_dimensoes_completas",
        "pab_nr_rank_presenca_periodo",
        "pab_nr_rank_absenteismo_periodo",
        "pab_fl_possui_presenca",
        "pab_fl_possui_ausencia",
        "pab_fl_alto_absenteismo",
        "pab_fl_baixo_absenteismo",
        "pab_fl_dimensoes_principais_completas",
        "pab_fl_registro_valido_marts",
        "pab_qt_evento_encontrado_gold",
        "pab_qt_deputado_encontrado_gold",
        "pab_qt_partido_encontrado_gold",
        "pab_qt_estado_encontrado_gold",
        "pab_qt_data_encontrada_gold",
        "pab_qt_dimensoes_completas",
        "pab_qt_execucoes_gold",
        "pab_dh_ultimo_processamento_gold",
        "aud_id_execucao_marts",
        "aud_dh_processamento_marts",
        "aud_tx_versao_pipeline_marts",
        "aud_tx_hash_registro_marts"
    )
)

# COMMAND ----------

# ============================================================
# MARTS VALIDATIONS
# ============================================================

records_written = final_df.count()

if records_written == 0:
    raise ValueError("Marts validation failed: target dataframe is empty.")

business_key_duplicates = (
    final_df
    .groupBy("pab_tx_business_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if business_key_duplicates > 0:
    raise ValueError(f"Marts validation failed: {business_key_duplicates} duplicated business keys found.")

invalid_records = final_df.filter(F.col("pab_fl_registro_valido_marts") != F.lit(True)).count()

if invalid_records > 0:
    raise ValueError(f"Marts validation failed: {invalid_records} invalid records found.")

negative_metric_errors = final_df.filter(
    (F.col("pab_qt_eventos_distintos") < 0)
    | (F.col("pab_qt_tipos_evento") < 0)
    | (F.col("pab_qt_orgaos_distintos") < 0)
    | (F.col("pab_qt_registros_presenca") < 0)
    | (F.col("pab_qt_presencas") < 0)
    | (F.col("pab_qt_ausencias") < 0)
    | (F.col("pab_qt_evento_encontrado_gold") < 0)
    | (F.col("pab_qt_deputado_encontrado_gold") < 0)
    | (F.col("pab_qt_partido_encontrado_gold") < 0)
    | (F.col("pab_qt_estado_encontrado_gold") < 0)
    | (F.col("pab_qt_data_encontrada_gold") < 0)
    | (F.col("pab_qt_dimensoes_completas") < 0)
).count()

if negative_metric_errors > 0:
    raise ValueError(f"Marts validation failed: {negative_metric_errors} records have invalid negative metrics.")

percentage_errors = final_df.filter(
    (F.col("pab_vl_pct_presenca") < 0)
    | (F.col("pab_vl_pct_presenca") > 100)
    | (F.col("pab_vl_pct_absenteismo") < 0)
    | (F.col("pab_vl_pct_absenteismo") > 100)
    | (F.col("pab_vl_pct_dimensoes_completas") < 0)
    | (F.col("pab_vl_pct_dimensoes_completas") > 100)
).count()

if percentage_errors > 0:
    raise ValueError(f"Marts validation failed: {percentage_errors} records have percentage values outside expected range.")

attendance_math_errors = final_df.filter(
    F.col("pab_qt_registros_presenca") != (F.col("pab_qt_presencas") + F.col("pab_qt_ausencias"))
).count()

if attendance_math_errors > 0:
    raise ValueError(f"Marts validation failed: {attendance_math_errors} records have invalid attendance arithmetic.")

# Informational validation only: absence is not expected from this source.
source_limitation_note = (
    "The source contains only confirmed presence records. "
    "Absence and absenteeism metrics are kept for compatibility and are expected to be zero."
)

# COMMAND ----------

# ============================================================
# WRITE DELTA TABLE
# ============================================================

(
    final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

# ============================================================
# APPLY GOVERNANCE COMMENTS
# ============================================================

spark.sql(f"COMMENT ON TABLE {TARGET_TABLE} IS 'Business Mart for registered attendance coverage in legislative events. Absenteeism fields are preserved for compatibility but the source currently provides only confirmed presence records.'")

column_comments = {
    "pab_sk_presenca_absenteismo": "Marts surrogate key for the attendance coverage analytical mart record.",
    "pab_tx_business_key": "Business key identifying one attendance coverage mart aggregation grain.",
    "dep_sk_deputado": "Gold surrogate key of the deputy dimension.",
    "par_sk_partido": "Gold surrogate key of the political party dimension.",
    "est_sk_estado": "Gold surrogate key of the state dimension.",
    "dep_id_deputado": "Deputy business identifier associated with attendance records.",
    "dep_tx_nome": "Deputy name associated with attendance records.",
    "dep_tx_sigla_partido": "Deputy political party acronym associated with attendance records.",
    "dep_tx_sigla_uf": "Deputy federation unit acronym associated with attendance records.",
    "leg_id_legislatura": "Legislature identifier associated with the legislative event.",
    "leg_id_legislatura_deputado": "Legislature identifier associated with the deputy record when available.",
    "pab_nr_ano": "Attendance reference year derived from the event year.",
    "pab_nr_mes": "Attendance reference month derived from the event month.",
    "pab_tx_ano_mes": "Year-month reference label for analytical consumption.",
    "pab_dt_primeiro_evento": "First event date represented in the aggregation group.",
    "pab_dt_ultimo_evento": "Latest event date represented in the aggregation group.",
    "pab_qt_eventos_distintos": "Number of distinct legislative events represented in the aggregation group.",
    "pab_qt_tipos_evento": "Number of distinct event types represented in the aggregation group.",
    "pab_qt_orgaos_distintos": "Number of distinct legislative bodies represented in the aggregation group.",
    "pab_qt_registros_presenca": "Total number of attendance records in the aggregation group. The source provides confirmed presence records only.",
    "pab_qt_presencas": "Total number of confirmed presence records in the aggregation group.",
    "pab_qt_ausencias": "Compatibility absence metric. Currently expected to be zero because the source does not provide explicit absence records.",
    "pab_vl_pct_presenca": "Percentage of confirmed presence records over total attendance records.",
    "pab_vl_pct_absenteismo": "Compatibility absenteeism percentage. Currently expected to be zero because the source does not provide explicit absence records.",
    "pab_vl_pct_dimensoes_completas": "Percentage of attendance records with complete main Gold dimensional coverage.",
    "pab_nr_rank_presenca_periodo": "Ranking by registered attendance rate within the year and month period.",
    "pab_nr_rank_absenteismo_periodo": "Compatibility ranking by absenteeism rate. Informational only while absence is unavailable in the source.",
    "pab_fl_possui_presenca": "Flag indicating whether the deputy has at least one confirmed presence in the period.",
    "pab_fl_possui_ausencia": "Compatibility flag indicating whether the deputy has at least one absence in the period. Expected false with current source.",
    "pab_fl_alto_absenteismo": "Compatibility flag for absenteeism greater than or equal to 30 percent. Expected false with current source.",
    "pab_fl_baixo_absenteismo": "Compatibility flag for absenteeism less than or equal to 10 percent. Expected true with current source when attendance records exist.",
    "pab_fl_dimensoes_principais_completas": "Flag indicating whether all records in the group have complete main Gold dimensions.",
    "pab_fl_registro_valido_marts": "Flag indicating whether the mart record passed Marts validation.",
    "pab_qt_evento_encontrado_gold": "Number of records with event dimension found in Gold.",
    "pab_qt_deputado_encontrado_gold": "Number of records with deputy dimension found in Gold.",
    "pab_qt_partido_encontrado_gold": "Number of records with party dimension found in Gold.",
    "pab_qt_estado_encontrado_gold": "Number of records with state dimension found in Gold.",
    "pab_qt_data_encontrada_gold": "Number of records with date dimension found in Gold.",
    "pab_qt_dimensoes_completas": "Number of records with all main dimensions complete.",
    "pab_qt_execucoes_gold": "Number of Gold executions represented in the aggregation group.",
    "pab_dh_ultimo_processamento_gold": "Latest Gold processing timestamp represented in the aggregation group.",
    "aud_id_execucao_marts": "Execution identifier generated during Marts processing.",
    "aud_dh_processamento_marts": "Timestamp when the record was processed in Marts.",
    "aud_tx_versao_pipeline_marts": "Pipeline version used during Marts processing.",
    "aud_tx_hash_registro_marts": "Deterministic Marts record hash."
}

for column_name, comment_text in column_comments.items():
    spark.sql(f"ALTER TABLE {TARGET_TABLE} ALTER COLUMN {column_name} COMMENT '{comment_text}'")

# COMMAND ----------

# ============================================================
# EXPORT CSV
# ============================================================

export_tmp_path = f"{EXPORT_VOLUME_PATH}/_tmp_export"

(
    final_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(export_tmp_path)
)

csv_files = [f.path for f in dbutils.fs.ls(export_tmp_path) if f.path.endswith(".csv")]

if csv_files:
    dbutils.fs.rm(EXPORT_CSV_PATH, True)
    dbutils.fs.mv(csv_files[0], EXPORT_CSV_PATH)
    dbutils.fs.rm(export_tmp_path, True)

# COMMAND ----------

# ============================================================
# EXECUTION SUMMARY
# ============================================================

print("=" * 80)
print("MART REGISTERED ATTENDANCE COVERAGE - RESUMO EXECUCAO")
print("=" * 80)
print(f"Records read from Gold attendance fact: {records_read_attendance}")
print(f"Records eligible from Gold attendance fact: {records_eligible_attendance}")
print(f"Records written to Marts: {records_written}")
print(f"Source total attendance records: {source_profile.get('total_registros_presenca')}")
print(f"Source total confirmed presences: {source_profile.get('total_presencas')}")
print(f"Source total explicit absences: {source_profile.get('total_ausencias')}")
print("Source limitation: eventosPresencaDeputados provides only confirmed presence records.")
print("Absenteeism fields are compatibility metrics and are expected to remain zero with the current source.")
print(f"CSV export path: {EXPORT_CSV_PATH}")
print("STATUS: SUCCESS")
print("=" * 80)


