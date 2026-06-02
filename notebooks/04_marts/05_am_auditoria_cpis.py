# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Marts — CPI Audit
# MAGIC
# MAGIC **Notebook:** `05_am_auditoria_cpis`
# MAGIC
# MAGIC Builds the curated Business Mart for Parliamentary Inquiry Committee (CPI) audit analysis used by analytical dashboards, executive reports, governance checks and delivery evidence.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * CPI audit mart model
# MAGIC * One analytical record per CPI
# MAGIC * CPI registration and status indicators
# MAGIC * CPI temporal consistency indicators
# MAGIC * CPI-event relationship coverage indicators
# MAGIC * CPI event confidence indicators
# MAGIC * Direct and semantic relationship indicators
# MAGIC * Gold dimensional coverage indicators
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
# MAGIC * Read validated Gold CPI dimension records
# MAGIC * Read validated Gold CPI-event relationship records
# MAGIC * Keep one analytical audit record per CPI
# MAGIC * Preserve CPI business identifiers and descriptive attributes
# MAGIC * Calculate CPI status, period and duration indicators
# MAGIC * Calculate event relationship, confidence and realization indicators
# MAGIC * Preserve Gold coverage and lineage indicators
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
# MAGIC This mart supports CPI governance and audit analysis:
# MAGIC
# MAGIC 1. Which CPIs exist in the curated Gold layer?
# MAGIC 2. Which CPIs are active or inactive?
# MAGIC 3. Which CPIs have valid temporal periods?
# MAGIC 4. How many CPI-event relationships were identified for each CPI?
# MAGIC 5. Which CPI-event relationships are direct or semantic?
# MAGIC 6. Which CPIs have high-confidence event relationships?
# MAGIC 7. Which CPIs have events already realized?
# MAGIC 8. Which CPIs have incomplete Gold event or date coverage?
# MAGIC 9. Which CPIs are historical and outside the currently supported legislature derivation window?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Mart Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per CPI available in the Gold CPI dimension.
# MAGIC
# MAGIC ### Sources
# MAGIC
# MAGIC * `brazil_legislative_analytics.gold.dm_cpis`
# MAGIC * `brazil_legislative_analytics.gold.ft_eventos_cpis`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.marts.am_auditoria_cpis`
# MAGIC
# MAGIC ### CSV Export
# MAGIC
# MAGIC `/Volumes/brazil_legislative_analytics/marts/exports/am_auditoria_cpis/am_auditoria_cpis.csv`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC * `cpi_id_orgao`
# MAGIC
# MAGIC ### Mart Surrogate Key
# MAGIC
# MAGIC `acp_sk_auditoria_cpi`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC Rule 1:
# MAGIC
# MAGIC Only valid Gold CPI records are eligible.
# MAGIC
# MAGIC Rule 2:
# MAGIC
# MAGIC One analytical audit record is maintained per CPI.
# MAGIC
# MAGIC Rule 3:
# MAGIC
# MAGIC CPI descriptive and status attributes are derived from `dm_cpis`.
# MAGIC
# MAGIC Rule 4:
# MAGIC
# MAGIC CPI-event relationship indicators are derived from `ft_eventos_cpis`.
# MAGIC
# MAGIC Rule 5:
# MAGIC
# MAGIC Direct, semantic and high-confidence relationship categories are preserved as explicit metrics.
# MAGIC
# MAGIC Rule 6:
# MAGIC
# MAGIC CPIs without event relationships must be preserved for governance visibility.
# MAGIC
# MAGIC Rule 7:
# MAGIC
# MAGIC Historical CPIs outside the currently modeled legislature window must be preserved with explicit classification instead of being treated as invalid records.
# MAGIC
# MAGIC Rule 8:
# MAGIC
# MAGIC The mart must be published as Delta and exported as CSV.
# MAGIC
# MAGIC Rule 9:
# MAGIC
# MAGIC All Marts objects must contain governance comments.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Known Historical Coverage Note
# MAGIC
# MAGIC The analytical legislature derivation currently covers the 56th and 57th legislatures. CPIs outside this supported window are preserved for auditability and classified as historical records outside the current legislature window. They are not considered data quality failures.
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

NOTEBOOK_NAME = "05_am_auditoria_cpis"
ENTITY_NAME = "auditoria_cpis"

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

SOURCE_DIM_CPIS = f"{GOLD_SCHEMA}.dm_cpis"
SOURCE_FACT_EVENTOS_CPIS = f"{GOLD_SCHEMA}.ft_eventos_cpis"

TARGET_TABLE = f"{MARTS_SCHEMA}.am_auditoria_cpis"
EXPORT_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/marts/exports/am_auditoria_cpis"
EXPORT_CSV_PATH = f"{EXPORT_VOLUME_PATH}/am_auditoria_cpis.csv"

PIPELINE_VERSION = "marts_v1.0_cpi_audit"
EXECUTION_ID = str(uuid.uuid4())
PROCESSING_TS = F.current_timestamp()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.marts.exports")
dbutils.fs.mkdirs(EXPORT_VOLUME_PATH)

# COMMAND ----------

# ============================================================
# READ GOLD SOURCES
# ============================================================

cpis_raw_df = spark.table(SOURCE_DIM_CPIS)
events_cpis_raw_df = spark.table(SOURCE_FACT_EVENTOS_CPIS)

records_read_cpis = cpis_raw_df.count()
records_read_events_cpis = events_cpis_raw_df.count()

# COMMAND ----------

# ============================================================
# STANDARDIZE CPI DIMENSION
# Avoid duplicated columns before joining aggregated facts.
# ============================================================

cpis_dim_df = (
    cpis_raw_df
    .filter(F.col("cpi_fl_registro_valido_gold") == F.lit(True))
    .select(
        F.col("cpi_sk_cpi").alias("cpi_sk_cpi"),
        F.col("cpi_id_orgao").alias("cpi_id_orgao"),
        F.col("cpi_tx_sigla").alias("cpi_tx_sigla"),
        F.col("cpi_tx_nome").alias("cpi_tx_nome"),
        F.col("cpi_tx_apelido").alias("cpi_tx_apelido"),
        F.col("cpi_tx_tipo").alias("cpi_tx_tipo"),
        F.col("cpi_tx_tipo_descricao").alias("cpi_tx_tipo_descricao"),
        F.col("cpi_tx_tipo_orgao").alias("cpi_tx_tipo_orgao"),
        F.col("cpi_tx_abrangencia").alias("cpi_tx_abrangencia"),
        F.col("cpi_tx_situacao_origem").alias("cpi_tx_situacao_origem"),
        F.col("cpi_tx_status_analitico").alias("cpi_tx_status_analitico"),
        F.col("cpi_dt_inicio").alias("cpi_dt_inicio"),
        F.col("cpi_dt_fim").alias("cpi_dt_fim"),
        F.col("cpi_nr_ano_inicio").alias("cpi_nr_ano_inicio"),
        F.col("leg_id_legislatura").cast("string").alias("leg_id_legislatura"),
        F.col("cpi_tx_uri").alias("cpi_tx_uri"),
        F.col("cpi_fl_mista").alias("cpi_fl_mista"),
        F.col("cpi_fl_ativa").alias("cpi_fl_ativa"),
        F.col("cpi_fl_data_inicio_informada").alias("cpi_fl_data_inicio_informada"),
        F.col("cpi_fl_data_fim_informada").alias("cpi_fl_data_fim_informada"),
        F.col("cpi_fl_legislatura_identificada").alias("cpi_fl_legislatura_identificada"),
        F.col("cpi_fl_id_valido").alias("cpi_fl_id_valido"),
        F.col("cpi_fl_nome_informado").alias("cpi_fl_nome_informado"),
        F.col("cpi_fl_tipo_cpi_valido").alias("cpi_fl_tipo_cpi_valido"),
        F.col("cpi_fl_periodo_valido").alias("cpi_fl_periodo_valido"),
        F.col("aud_id_execucao_gold").alias("cpi_aud_id_execucao_gold"),
        F.col("aud_dh_processamento_gold").alias("cpi_aud_dh_processamento_gold"),
        F.col("aud_tx_hash_registro_gold").alias("cpi_aud_tx_hash_registro_gold")
    )
    .dropDuplicates(["cpi_id_orgao"])
)

records_eligible_cpis = cpis_dim_df.count()

# COMMAND ----------

# ============================================================
# STANDARDIZE CPI-EVENT FACT
# Only columns required for aggregation are selected.
# ============================================================

events_cpis_valid_df = (
    events_cpis_raw_df
    .filter(F.col("fec_fl_registro_valido_gold") == F.lit(True))
    .select(
        F.col("fec_sk_evento_cpi").alias("fec_sk_evento_cpi"),
        F.col("cpi_evt_id_relacao").alias("cpi_evt_id_relacao"),
        F.col("cpi_id_orgao").alias("cpi_id_orgao"),
        F.col("evt_sk_evento").alias("evt_sk_evento"),
        F.col("evt_id_evento").alias("evt_id_evento"),
        F.col("evt_dt_evento").alias("evt_dt_evento"),
        F.col("dat_sk_data").alias("dat_sk_data"),
        F.col("leg_id_legislatura").cast("string").alias("evt_leg_id_legislatura"),
        F.col("cpi_evt_tx_tipo_relacao").alias("cpi_evt_tx_tipo_relacao"),
        F.col("cpi_evt_tx_nivel_confianca").alias("cpi_evt_tx_nivel_confianca"),
        F.col("fec_qt_relacao_direta").cast("long").alias("fec_qt_relacao_direta"),
        F.col("fec_qt_relacao_semantica").cast("long").alias("fec_qt_relacao_semantica"),
        F.col("fec_qt_alta_confianca").cast("long").alias("fec_qt_alta_confianca"),
        F.col("fec_qt_evento_realizado").cast("long").alias("fec_qt_evento_realizado"),
        F.col("cpi_evt_fl_alta_confianca").alias("cpi_evt_fl_alta_confianca"),
        F.col("fec_fl_cpi_encontrada_gold").alias("fec_fl_cpi_encontrada_gold"),
        F.col("fec_fl_evento_encontrado_gold").alias("fec_fl_evento_encontrado_gold"),
        F.col("fec_fl_data_encontrada_gold").alias("fec_fl_data_encontrada_gold"),
        F.col("fec_fl_dimensoes_principais_completas").alias("fec_fl_dimensoes_principais_completas"),
        F.col("aud_id_execucao_gold").alias("fec_aud_id_execucao_gold"),
        F.col("aud_dh_processamento_gold").alias("fec_aud_dh_processamento_gold")
    )
)

records_eligible_events_cpis = events_cpis_valid_df.count()

# COMMAND ----------

# ============================================================
# BUILD EVENT RELATIONSHIP METRICS BY CPI
# ============================================================

events_metrics_df = (
    events_cpis_valid_df
    .groupBy("cpi_id_orgao")
    .agg(
        F.count("*").cast("long").alias("acp_qt_relacoes_eventos"),
        F.countDistinct("cpi_evt_id_relacao").cast("long").alias("acp_qt_relacoes_distintas"),
        F.countDistinct("evt_id_evento").cast("long").alias("acp_qt_eventos_distintos"),
        F.min("evt_dt_evento").alias("acp_dt_primeiro_evento"),
        F.max("evt_dt_evento").alias("acp_dt_ultimo_evento"),
        F.sum(F.coalesce(F.col("fec_qt_relacao_direta"), F.lit(0))).cast("long").alias("acp_qt_relacoes_diretas"),
        F.sum(F.coalesce(F.col("fec_qt_relacao_semantica"), F.lit(0))).cast("long").alias("acp_qt_relacoes_semanticas"),
        F.sum(F.coalesce(F.col("fec_qt_alta_confianca"), F.lit(0))).cast("long").alias("acp_qt_alta_confianca"),
        F.sum(F.coalesce(F.col("fec_qt_evento_realizado"), F.lit(0))).cast("long").alias("acp_qt_eventos_realizados"),
        F.sum(F.when(F.col("cpi_evt_tx_nivel_confianca") == "HIGH", 1).otherwise(0)).cast("long").alias("acp_qt_relacoes_high"),
        F.sum(F.when(F.col("cpi_evt_tx_nivel_confianca") == "MEDIUM", 1).otherwise(0)).cast("long").alias("acp_qt_relacoes_medium"),
        F.sum(F.when(F.col("fec_fl_cpi_encontrada_gold") == F.lit(True), 1).otherwise(0)).cast("long").alias("acp_qt_cpi_encontrada_gold"),
        F.sum(F.when(F.col("fec_fl_evento_encontrado_gold") == F.lit(True), 1).otherwise(0)).cast("long").alias("acp_qt_evento_encontrado_gold"),
        F.sum(F.when(F.col("fec_fl_data_encontrada_gold") == F.lit(True), 1).otherwise(0)).cast("long").alias("acp_qt_data_encontrada_gold"),
        F.sum(F.when(F.col("fec_fl_dimensoes_principais_completas") == F.lit(True), 1).otherwise(0)).cast("long").alias("acp_qt_dimensoes_completas"),
        F.countDistinct("fec_aud_id_execucao_gold").cast("long").alias("acp_qt_execucoes_gold_eventos"),
        F.max("fec_aud_dh_processamento_gold").alias("acp_dh_ultimo_processamento_gold_eventos")
    )
)

# COMMAND ----------

# ============================================================
# BUILD BASE MART DATAFRAME
# Explicit post-join select prevents ambiguous column references.
# ============================================================

base_mart_df = (
    cpis_dim_df.alias("cpi")
    .join(
        events_metrics_df.alias("evt"),
        on=F.col("cpi.cpi_id_orgao") == F.col("evt.cpi_id_orgao"),
        how="left"
    )
    .select(
        F.col("cpi.cpi_sk_cpi"),
        F.col("cpi.cpi_id_orgao"),
        F.col("cpi.cpi_tx_sigla"),
        F.col("cpi.cpi_tx_nome"),
        F.col("cpi.cpi_tx_apelido"),
        F.col("cpi.cpi_tx_tipo"),
        F.col("cpi.cpi_tx_tipo_descricao"),
        F.col("cpi.cpi_tx_tipo_orgao"),
        F.col("cpi.cpi_tx_abrangencia"),
        F.col("cpi.cpi_tx_situacao_origem"),
        F.col("cpi.cpi_tx_status_analitico"),
        F.col("cpi.cpi_dt_inicio"),
        F.col("cpi.cpi_dt_fim"),
        F.col("cpi.cpi_nr_ano_inicio"),
        F.col("cpi.leg_id_legislatura"),
        F.col("cpi.cpi_tx_uri"),
        F.col("cpi.cpi_fl_mista"),
        F.col("cpi.cpi_fl_ativa"),
        F.col("cpi.cpi_fl_data_inicio_informada"),
        F.col("cpi.cpi_fl_data_fim_informada"),
        F.col("cpi.cpi_fl_legislatura_identificada"),
        F.col("cpi.cpi_fl_id_valido"),
        F.col("cpi.cpi_fl_nome_informado"),
        F.col("cpi.cpi_fl_tipo_cpi_valido"),
        F.col("cpi.cpi_fl_periodo_valido"),
        F.col("cpi.cpi_aud_id_execucao_gold"),
        F.col("cpi.cpi_aud_dh_processamento_gold"),
        F.col("cpi.cpi_aud_tx_hash_registro_gold"),
        F.coalesce(F.col("evt.acp_qt_relacoes_eventos"), F.lit(0)).cast("long").alias("acp_qt_relacoes_eventos"),
        F.coalesce(F.col("evt.acp_qt_relacoes_distintas"), F.lit(0)).cast("long").alias("acp_qt_relacoes_distintas"),
        F.coalesce(F.col("evt.acp_qt_eventos_distintos"), F.lit(0)).cast("long").alias("acp_qt_eventos_distintos"),
        F.col("evt.acp_dt_primeiro_evento"),
        F.col("evt.acp_dt_ultimo_evento"),
        F.coalesce(F.col("evt.acp_qt_relacoes_diretas"), F.lit(0)).cast("long").alias("acp_qt_relacoes_diretas"),
        F.coalesce(F.col("evt.acp_qt_relacoes_semanticas"), F.lit(0)).cast("long").alias("acp_qt_relacoes_semanticas"),
        F.coalesce(F.col("evt.acp_qt_alta_confianca"), F.lit(0)).cast("long").alias("acp_qt_alta_confianca"),
        F.coalesce(F.col("evt.acp_qt_eventos_realizados"), F.lit(0)).cast("long").alias("acp_qt_eventos_realizados"),
        F.coalesce(F.col("evt.acp_qt_relacoes_high"), F.lit(0)).cast("long").alias("acp_qt_relacoes_high"),
        F.coalesce(F.col("evt.acp_qt_relacoes_medium"), F.lit(0)).cast("long").alias("acp_qt_relacoes_medium"),
        F.coalesce(F.col("evt.acp_qt_cpi_encontrada_gold"), F.lit(0)).cast("long").alias("acp_qt_cpi_encontrada_gold"),
        F.coalesce(F.col("evt.acp_qt_evento_encontrado_gold"), F.lit(0)).cast("long").alias("acp_qt_evento_encontrado_gold"),
        F.coalesce(F.col("evt.acp_qt_data_encontrada_gold"), F.lit(0)).cast("long").alias("acp_qt_data_encontrada_gold"),
        F.coalesce(F.col("evt.acp_qt_dimensoes_completas"), F.lit(0)).cast("long").alias("acp_qt_dimensoes_completas"),
        F.coalesce(F.col("evt.acp_qt_execucoes_gold_eventos"), F.lit(0)).cast("long").alias("acp_qt_execucoes_gold_eventos"),
        F.col("evt.acp_dh_ultimo_processamento_gold_eventos")
    )
)

# COMMAND ----------

# ============================================================
# DERIVE ANALYTICAL INDICATORS
# ============================================================

mart_metrics_df = (
    base_mart_df
    .withColumn(
        "acp_qt_dias_duracao_cpi",
        F.when(
            F.col("cpi_dt_inicio").isNotNull() & F.col("cpi_dt_fim").isNotNull(),
            F.datediff(F.col("cpi_dt_fim"), F.col("cpi_dt_inicio"))
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn(
        "acp_qt_dias_entre_primeiro_ultimo_evento",
        F.when(
            F.col("acp_dt_primeiro_evento").isNotNull() & F.col("acp_dt_ultimo_evento").isNotNull(),
            F.datediff(F.col("acp_dt_ultimo_evento"), F.col("acp_dt_primeiro_evento"))
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn(
        "acp_vl_pct_relacoes_alta_confianca",
        F.when(
            F.col("acp_qt_relacoes_eventos") > 0,
            F.round((F.col("acp_qt_alta_confianca") / F.col("acp_qt_relacoes_eventos")) * 100, 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "acp_vl_pct_relacoes_diretas",
        F.when(
            F.col("acp_qt_relacoes_eventos") > 0,
            F.round((F.col("acp_qt_relacoes_diretas") / F.col("acp_qt_relacoes_eventos")) * 100, 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "acp_vl_pct_eventos_realizados",
        F.when(
            F.col("acp_qt_relacoes_eventos") > 0,
            F.round((F.col("acp_qt_eventos_realizados") / F.col("acp_qt_relacoes_eventos")) * 100, 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "acp_vl_pct_dimensoes_completas",
        F.when(
            F.col("acp_qt_relacoes_eventos") > 0,
            F.round((F.col("acp_qt_dimensoes_completas") / F.col("acp_qt_relacoes_eventos")) * 100, 2)
        ).otherwise(F.lit(100.0))
    )
    .withColumn("acp_fl_possui_evento_relacionado", F.col("acp_qt_relacoes_eventos") > 0)
    .withColumn("acp_fl_possui_relacao_direta", F.col("acp_qt_relacoes_diretas") > 0)
    .withColumn("acp_fl_possui_relacao_semantica", F.col("acp_qt_relacoes_semanticas") > 0)
    .withColumn("acp_fl_possui_alta_confianca", F.col("acp_qt_alta_confianca") > 0)
    .withColumn("acp_fl_possui_evento_realizado", F.col("acp_qt_eventos_realizados") > 0)
    .withColumn(
        "acp_fl_dimensoes_principais_completas",
        F.when(F.col("acp_qt_relacoes_eventos") == 0, F.lit(True))
        .otherwise(F.col("acp_qt_dimensoes_completas") == F.col("acp_qt_relacoes_eventos"))
    )
    .withColumn(
        "acp_fl_periodo_cpi_consistente",
        F.coalesce(F.col("cpi_fl_periodo_valido"), F.lit(False))
    )
    .withColumn(
        "acp_tx_cobertura_legislatura",
        F.when(F.col("leg_id_legislatura").isNotNull(), F.lit("LEGISLATURA_IDENTIFICADA"))
        .when(F.year(F.col("cpi_dt_inicio")).between(2019, 2026), F.lit("LEGISLATURA_NAO_IDENTIFICADA_REVISAR"))
        .otherwise(F.lit("CPI_HISTORICA_FORA_JANELA_56_57"))
    )
    .withColumn(
        "acp_fl_cpi_historica_fora_janela_legislatura",
        F.col("acp_tx_cobertura_legislatura") == F.lit("CPI_HISTORICA_FORA_JANELA_56_57")
    )
    .withColumn(
        "acp_fl_legislatura_pendente_revisao",
        F.col("acp_tx_cobertura_legislatura") == F.lit("LEGISLATURA_NAO_IDENTIFICADA_REVISAR")
    )
)

rank_window = Window.orderBy(F.col("acp_qt_relacoes_eventos").desc(), F.col("acp_qt_eventos_distintos").desc(), F.col("cpi_tx_sigla"))

ranked_mart_df = mart_metrics_df.withColumn("acp_nr_rank_eventos_relacionados", F.dense_rank().over(rank_window))

# COMMAND ----------

# ============================================================
# FINAL MART DATAFRAME
# ============================================================

business_key_expr = F.concat_ws("|", F.col("cpi_id_orgao"))

final_df = (
    ranked_mart_df
    .withColumn("acp_tx_business_key", business_key_expr)
    .withColumn("acp_sk_auditoria_cpi", F.sha2(F.col("acp_tx_business_key"), 256))
    .withColumn(
        "acp_fl_registro_valido_marts",
        F.col("acp_sk_auditoria_cpi").isNotNull()
        & F.col("acp_tx_business_key").isNotNull()
        & F.col("cpi_id_orgao").isNotNull()
        & F.col("cpi_tx_nome").isNotNull()
        & (F.col("acp_qt_relacoes_eventos") >= 0)
        & (F.col("acp_qt_eventos_distintos") >= 0)
    )
    .withColumn("aud_id_execucao_marts", F.lit(EXECUTION_ID))
    .withColumn("aud_dh_processamento_marts", PROCESSING_TS)
    .withColumn("aud_tx_versao_pipeline_marts", F.lit(PIPELINE_VERSION))
    .withColumn(
        "aud_tx_hash_registro_marts",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("acp_tx_business_key"), F.lit("")),
                F.coalesce(F.col("cpi_tx_sigla"), F.lit("")),
                F.coalesce(F.col("cpi_tx_status_analitico"), F.lit("")),
                F.coalesce(F.col("leg_id_legislatura"), F.lit("")),
                F.coalesce(F.col("acp_qt_relacoes_eventos").cast("string"), F.lit("0")),
                F.coalesce(F.col("acp_qt_eventos_distintos").cast("string"), F.lit("0")),
                F.coalesce(F.col("acp_qt_alta_confianca").cast("string"), F.lit("0")),
                F.coalesce(F.col("acp_vl_pct_dimensoes_completas").cast("string"), F.lit("0"))
            ),
            256
        )
    )
    .select(
        "acp_sk_auditoria_cpi",
        "acp_tx_business_key",
        "cpi_sk_cpi",
        "cpi_id_orgao",
        "cpi_tx_sigla",
        "cpi_tx_nome",
        "cpi_tx_apelido",
        "cpi_tx_tipo",
        "cpi_tx_tipo_descricao",
        "cpi_tx_tipo_orgao",
        "cpi_tx_abrangencia",
        "cpi_tx_situacao_origem",
        "cpi_tx_status_analitico",
        "cpi_dt_inicio",
        "cpi_dt_fim",
        "cpi_nr_ano_inicio",
        "leg_id_legislatura",
        "acp_tx_cobertura_legislatura",
        "acp_fl_cpi_historica_fora_janela_legislatura",
        "acp_fl_legislatura_pendente_revisao",
        "cpi_tx_uri",
        "acp_qt_dias_duracao_cpi",
        "acp_qt_relacoes_eventos",
        "acp_qt_relacoes_distintas",
        "acp_qt_eventos_distintos",
        "acp_dt_primeiro_evento",
        "acp_dt_ultimo_evento",
        "acp_qt_dias_entre_primeiro_ultimo_evento",
        "acp_qt_relacoes_diretas",
        "acp_qt_relacoes_semanticas",
        "acp_qt_alta_confianca",
        "acp_qt_eventos_realizados",
        "acp_qt_relacoes_high",
        "acp_qt_relacoes_medium",
        "acp_vl_pct_relacoes_alta_confianca",
        "acp_vl_pct_relacoes_diretas",
        "acp_vl_pct_eventos_realizados",
        "acp_vl_pct_dimensoes_completas",
        "acp_nr_rank_eventos_relacionados",
        "cpi_fl_mista",
        "cpi_fl_ativa",
        "cpi_fl_data_inicio_informada",
        "cpi_fl_data_fim_informada",
        "cpi_fl_legislatura_identificada",
        "cpi_fl_id_valido",
        "cpi_fl_nome_informado",
        "cpi_fl_tipo_cpi_valido",
        "cpi_fl_periodo_valido",
        "acp_fl_periodo_cpi_consistente",
        "acp_fl_possui_evento_relacionado",
        "acp_fl_possui_relacao_direta",
        "acp_fl_possui_relacao_semantica",
        "acp_fl_possui_alta_confianca",
        "acp_fl_possui_evento_realizado",
        "acp_fl_dimensoes_principais_completas",
        "acp_fl_registro_valido_marts",
        "acp_qt_cpi_encontrada_gold",
        "acp_qt_evento_encontrado_gold",
        "acp_qt_data_encontrada_gold",
        "acp_qt_dimensoes_completas",
        "cpi_aud_id_execucao_gold",
        "cpi_aud_dh_processamento_gold",
        "cpi_aud_tx_hash_registro_gold",
        "acp_qt_execucoes_gold_eventos",
        "acp_dh_ultimo_processamento_gold_eventos",
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

null_key_errors = final_df.filter(
    F.col("acp_sk_auditoria_cpi").isNull()
    | F.col("acp_tx_business_key").isNull()
    | F.col("cpi_id_orgao").isNull()
    | F.col("cpi_tx_nome").isNull()
).count()

if null_key_errors > 0:
    raise ValueError(f"Marts validation failed: {null_key_errors} records have null mandatory keys.")

# Legislature coverage validation:
# CPIs before the currently modeled legislature window are preserved as historical audit records.
# Only records from 2019 onward without legislature are treated as review errors.
current_window_legislature_errors = final_df.filter(
    (F.col("leg_id_legislatura").isNull())
    & (F.year(F.col("cpi_dt_inicio")) >= 2019)
).count()

if current_window_legislature_errors > 0:
    raise ValueError(
        f"Marts validation failed: {current_window_legislature_errors} current-window CPI records have missing legislature."
    )

historical_without_legislature = final_df.filter(
    (F.col("leg_id_legislatura").isNull())
    & (F.col("acp_fl_cpi_historica_fora_janela_legislatura") == F.lit(True))
).count()

print(
    f"[INFO] Historical CPIs outside the 56/57 legislature window preserved with null legislature: {historical_without_legislature}"
)

duplicate_business_key_errors = (
    final_df
    .groupBy("acp_tx_business_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicate_business_key_errors > 0:
    raise ValueError(f"Marts validation failed: {duplicate_business_key_errors} duplicated business keys found.")

invalid_record_errors = final_df.filter(F.col("acp_fl_registro_valido_marts") != F.lit(True)).count()

if invalid_record_errors > 0:
    raise ValueError(f"Marts validation failed: {invalid_record_errors} invalid records found.")

negative_metric_errors = final_df.filter(
    (F.col("acp_qt_relacoes_eventos") < 0)
    | (F.col("acp_qt_relacoes_distintas") < 0)
    | (F.col("acp_qt_eventos_distintos") < 0)
    | (F.col("acp_qt_relacoes_diretas") < 0)
    | (F.col("acp_qt_relacoes_semanticas") < 0)
    | (F.col("acp_qt_alta_confianca") < 0)
    | (F.col("acp_qt_eventos_realizados") < 0)
).count()

if negative_metric_errors > 0:
    raise ValueError(f"Marts validation failed: {negative_metric_errors} records have negative quantitative metrics.")

percentage_errors = final_df.filter(
    (F.col("acp_vl_pct_relacoes_alta_confianca") < 0)
    | (F.col("acp_vl_pct_relacoes_alta_confianca") > 100)
    | (F.col("acp_vl_pct_relacoes_diretas") < 0)
    | (F.col("acp_vl_pct_relacoes_diretas") > 100)
    | (F.col("acp_vl_pct_eventos_realizados") < 0)
    | (F.col("acp_vl_pct_eventos_realizados") > 100)
    | (F.col("acp_vl_pct_dimensoes_completas") < 0)
    | (F.col("acp_vl_pct_dimensoes_completas") > 100)
).count()

if percentage_errors > 0:
    raise ValueError(f"Marts validation failed: {percentage_errors} records have percentages outside 0-100 range.")

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
# EXPORT CSV DELIVERY EVIDENCE
# ============================================================

temp_export_path = f"{EXPORT_VOLUME_PATH}/_tmp_csv_export"

(
    final_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ",")
    .csv(temp_export_path)
)

csv_files = [f.path for f in dbutils.fs.ls(temp_export_path) if f.path.endswith(".csv")]

if len(csv_files) != 1:
    raise ValueError("CSV export failed: exactly one CSV part file was expected.")

dbutils.fs.rm(EXPORT_CSV_PATH, recurse=True)
dbutils.fs.mv(csv_files[0], EXPORT_CSV_PATH)
dbutils.fs.rm(temp_export_path, recurse=True)

# COMMAND ----------

# ============================================================
# GOVERNANCE COMMENTS
# ============================================================

table_comment = "Curated Business Mart for CPI audit analysis, including CPI status, temporal consistency, event relationships, confidence indicators, dimensional coverage and governance metadata."

spark.sql(f"COMMENT ON TABLE {TARGET_TABLE} IS '{table_comment}'")

column_comments = {
    "acp_sk_auditoria_cpi": "Marts surrogate key for the CPI audit mart record.",
    "acp_tx_business_key": "Business key identifying one CPI audit mart record.",
    "cpi_sk_cpi": "Gold surrogate key of the CPI dimension.",
    "cpi_id_orgao": "Business identifier of the parliamentary inquiry committee body.",
    "cpi_tx_sigla": "Standardized CPI acronym.",
    "cpi_tx_nome": "Parliamentary inquiry committee name.",
    "cpi_tx_apelido": "CPI nickname or short name.",
    "cpi_tx_tipo": "CPI type code or acronym.",
    "cpi_tx_tipo_descricao": "CPI type description.",
    "cpi_tx_tipo_orgao": "Source body type associated with the CPI.",
    "cpi_tx_abrangencia": "Parliamentary inquiry committee scope.",
    "cpi_tx_situacao_origem": "Original CPI status description from the source system.",
    "cpi_tx_status_analitico": "Curated analytical CPI status.",
    "cpi_dt_inicio": "CPI start date.",
    "cpi_dt_fim": "CPI end date.",
    "cpi_nr_ano_inicio": "Year when the CPI started.",
    "leg_id_legislatura": "Legislature identifier associated with the CPI when covered by the current modeled legislature window.",
    "acp_tx_cobertura_legislatura": "Legislature coverage classification for the CPI, distinguishing identified legislatures from historical records outside the modeled window.",
    "acp_fl_cpi_historica_fora_janela_legislatura": "Flag indicating whether the CPI is historical and outside the currently modeled 56/57 legislature window.",
    "acp_fl_legislatura_pendente_revisao": "Flag indicating whether a CPI inside the current modeled window has missing legislature and should be reviewed.",
    "cpi_tx_uri": "CPI URI from the source system.",
    "acp_qt_dias_duracao_cpi": "Number of days between CPI start and end dates when both dates are available.",
    "acp_qt_relacoes_eventos": "Total number of CPI-event relationship records associated with the CPI.",
    "acp_qt_relacoes_distintas": "Number of distinct CPI-event relationship business keys associated with the CPI.",
    "acp_qt_eventos_distintos": "Number of distinct legislative events associated with the CPI.",
    "acp_dt_primeiro_evento": "First event date associated with the CPI.",
    "acp_dt_ultimo_evento": "Latest event date associated with the CPI.",
    "acp_qt_dias_entre_primeiro_ultimo_evento": "Number of days between the first and latest CPI-related event.",
    "acp_qt_relacoes_diretas": "Total number of direct CPI-event relationships.",
    "acp_qt_relacoes_semanticas": "Total number of semantic CPI-event relationships.",
    "acp_qt_alta_confianca": "Total number of high-confidence CPI-event relationships.",
    "acp_qt_eventos_realizados": "Total number of CPI-related events already realized.",
    "acp_qt_relacoes_high": "Number of CPI-event relationships classified as HIGH confidence.",
    "acp_qt_relacoes_medium": "Number of CPI-event relationships classified as MEDIUM confidence.",
    "acp_vl_pct_relacoes_alta_confianca": "Percentage of CPI-event relationships classified as high confidence.",
    "acp_vl_pct_relacoes_diretas": "Percentage of CPI-event relationships classified as direct relationships.",
    "acp_vl_pct_eventos_realizados": "Percentage of CPI-event relationships linked to realized events.",
    "acp_vl_pct_dimensoes_completas": "Percentage of CPI-event relationship records with complete main Gold dimensional coverage.",
    "acp_nr_rank_eventos_relacionados": "Ranking based on the number of CPI-event relationship records.",
    "cpi_fl_mista": "Flag indicating whether the CPI is a mixed committee.",
    "cpi_fl_ativa": "Flag indicating whether the CPI is active.",
    "cpi_fl_data_inicio_informada": "Flag indicating whether CPI start date is available.",
    "cpi_fl_data_fim_informada": "Flag indicating whether CPI end date is available.",
    "cpi_fl_legislatura_identificada": "Flag indicating whether the CPI legislature was identified.",
    "cpi_fl_id_valido": "Flag indicating whether the CPI identifier is valid.",
    "cpi_fl_nome_informado": "Flag indicating whether the CPI name is informed.",
    "cpi_fl_tipo_cpi_valido": "Flag indicating whether the CPI type is valid.",
    "cpi_fl_periodo_valido": "Flag indicating whether CPI start and end dates form a valid period.",
    "acp_fl_periodo_cpi_consistente": "Flag indicating whether the CPI period is analytically consistent.",
    "acp_fl_possui_evento_relacionado": "Flag indicating whether the CPI has at least one related event.",
    "acp_fl_possui_relacao_direta": "Flag indicating whether the CPI has at least one direct event relationship.",
    "acp_fl_possui_relacao_semantica": "Flag indicating whether the CPI has at least one semantic event relationship.",
    "acp_fl_possui_alta_confianca": "Flag indicating whether the CPI has at least one high-confidence event relationship.",
    "acp_fl_possui_evento_realizado": "Flag indicating whether the CPI has at least one realized event.",
    "acp_fl_dimensoes_principais_completas": "Flag indicating whether all CPI-event relationship records have complete main Gold dimensional coverage.",
    "acp_fl_registro_valido_marts": "Flag indicating whether the mart record passed Marts validation.",
    "acp_qt_cpi_encontrada_gold": "Number of CPI-event records with CPI dimension found in Gold.",
    "acp_qt_evento_encontrado_gold": "Number of CPI-event records with event dimension found in Gold.",
    "acp_qt_data_encontrada_gold": "Number of CPI-event records with date dimension found in Gold.",
    "acp_qt_dimensoes_completas": "Number of CPI-event records with all main dimensions complete.",
    "cpi_aud_id_execucao_gold": "Gold execution identifier from the CPI dimension.",
    "cpi_aud_dh_processamento_gold": "Gold processing timestamp from the CPI dimension.",
    "cpi_aud_tx_hash_registro_gold": "Deterministic Gold hash from the CPI dimension.",
    "acp_qt_execucoes_gold_eventos": "Number of Gold executions represented in related CPI-event records.",
    "acp_dh_ultimo_processamento_gold_eventos": "Latest Gold processing timestamp represented in related CPI-event records.",
    "aud_id_execucao_marts": "Execution identifier generated during Marts processing.",
    "aud_dh_processamento_marts": "Timestamp when the record was processed in Marts.",
    "aud_tx_versao_pipeline_marts": "Pipeline version used during Marts processing.",
    "aud_tx_hash_registro_marts": "Deterministic Marts record hash."
}

for column_name, comment in column_comments.items():
    safe_comment = comment.replace("'", "''")
    spark.sql(f"ALTER TABLE {TARGET_TABLE} ALTER COLUMN {column_name} COMMENT '{safe_comment}'")
    print(f"[SUCCESS] Column comment applied: {TARGET_TABLE}.{column_name}")

# COMMAND ----------

# ============================================================
# EXECUTION SUMMARY
# ============================================================

print("=" * 80)
print("MART AUDITORIA CPIS - RESUMO EXECUCAO")
print("=" * 80)
print(f"Records read from Gold CPI dimension: {records_read_cpis}")
print(f"Records eligible from Gold CPI dimension: {records_eligible_cpis}")
print(f"Records read from Gold CPI-event fact: {records_read_events_cpis}")
print(f"Records eligible from Gold CPI-event fact: {records_eligible_events_cpis}")
print(f"Records written to Marts: {records_written}")
print(f"Historical CPIs outside legislature window preserved: {historical_without_legislature}")
print(f"Current-window CPIs with missing legislature: {current_window_legislature_errors}")
print(f"CSV export path: {EXPORT_CSV_PATH}")
print("STATUS: SUCCESS")
print("=" * 80)
