# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 03 Marts — CEAP Parliamentary Expenses
# MAGIC
# MAGIC **Notebook:** `03_am_despesas_ceap`
# MAGIC
# MAGIC Builds the curated Business Mart for Parliamentary Expense analysis based on the CEAP (Quota for Parliamentary Activity Exercise) program, supporting dashboards, audits, transparency initiatives and analytical consumption.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Parliamentary expense mart model
# MAGIC * One analytical record per expense transaction
# MAGIC * Expense aggregation indicators
# MAGIC * Supplier analysis indicators
# MAGIC * Geographic and temporal spending indicators
# MAGIC * Statistical anomaly indicators
# MAGIC * Data quality and dimensional completeness indicators
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
# MAGIC * Read validated Gold facts and dimensions
# MAGIC * Consolidate CEAP expense transactions
# MAGIC * Preserve parliamentary and supplier business identifiers
# MAGIC * Calculate spending and aggregation indicators
# MAGIC * Calculate anomaly detection indicators
# MAGIC * Calculate dimensional completeness metrics
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
# MAGIC This mart supports parliamentary expense analysis:
# MAGIC
# MAGIC 1. How much did each parliamentarian spend during a given period?
# MAGIC 2. Which suppliers received the highest expense amounts?
# MAGIC 3. What are the most common expense categories?
# MAGIC 4. Which expenses contain statistical anomalies?
# MAGIC 5. What is the geographic distribution of parliamentary expenses?
# MAGIC 6. What percentage of records contain complete dimensional information?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Mart Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per parliamentary expense transaction.
# MAGIC
# MAGIC ### Sources
# MAGIC
# MAGIC * `brazil_legislative_analytics.gold.ft_despesas_ceap`
# MAGIC * `brazil_legislative_analytics.gold.dm_deputados`
# MAGIC * `brazil_legislative_analytics.gold.dm_fornecedores`
# MAGIC * `brazil_legislative_analytics.gold.dm_partidos`
# MAGIC * `brazil_legislative_analytics.gold.dm_estados`
# MAGIC * `brazil_legislative_analytics.gold.dm_datas`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.marts.am_despesas_ceap`
# MAGIC
# MAGIC ### CSV Export
# MAGIC
# MAGIC `/Volumes/brazil_legislative_analytics/marts/exports/am_despesas_ceap/am_despesas_ceap.csv`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC * `des_id_despesa`
# MAGIC * `dep_id_deputado`
# MAGIC * `des_nr_ano`
# MAGIC * `des_nr_mes`
# MAGIC
# MAGIC ### Mart Surrogate Key
# MAGIC
# MAGIC `dce_sk_despesa_ceap_mart`
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
# MAGIC One analytical record is maintained per parliamentary expense transaction.
# MAGIC
# MAGIC Rule 3:
# MAGIC
# MAGIC Expense metrics are derived from ft_despesas_ceap.
# MAGIC
# MAGIC Rule 4:
# MAGIC
# MAGIC Supplier, deputy, party, state and date dimensions are resolved through Gold dimensions.
# MAGIC
# MAGIC Rule 5:
# MAGIC
# MAGIC Dimensional completeness indicators must be calculated.
# MAGIC
# MAGIC Rule 6:
# MAGIC
# MAGIC Anomaly indicators must be preserved from Gold processing.
# MAGIC
# MAGIC Rule 7:
# MAGIC
# MAGIC The mart must be published as Delta and exported as CSV.
# MAGIC
# MAGIC Rule 8:
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
from pyspark.sql import Window
from datetime import datetime
import uuid

# COMMAND ----------

# ============================================================
# EXECUTION CONFIGURATION
# ============================================================

CATALOG_NAME = "brazil_legislative_analytics"
GOLD_SCHEMA = f"{CATALOG_NAME}.gold"
MARTS_SCHEMA = f"{CATALOG_NAME}.marts"

SOURCE_TABLE = f"{GOLD_SCHEMA}.ft_despesas_ceap"
TARGET_TABLE = f"{MARTS_SCHEMA}.am_despesas_ceap"

EXPORT_DIR = f"/Volumes/{CATALOG_NAME}/marts/exports/am_despesas_ceap"
EXPORT_FILE = f"{EXPORT_DIR}/am_despesas_ceap.csv"
EXPORT_TMP_DIR = f"{EXPORT_DIR}/_tmp_am_despesas_ceap"

PIPELINE_VERSION = "marts_v1.0_ceap_expenses"
EXECUTION_ID = str(uuid.uuid4())
PROCESSING_TS = datetime.utcnow()

# COMMAND ----------

# ============================================================
# CREATE MARTS SCHEMA WHEN NEEDED
# ============================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")

# COMMAND ----------

# ============================================================
# READ GOLD SOURCE
# ============================================================

source_df = spark.table(SOURCE_TABLE)

source_valid_df = (
    source_df
    .filter(F.col("fdc_fl_registro_valido_gold") == F.lit(True))
)

# COMMAND ----------

# ============================================================
# STANDARDIZE SOURCE ATTRIBUTES
# ============================================================

standard_df = (
    source_valid_df
    .select(
        F.col("fdc_sk_despesa_ceap"),
        F.col("dep_sk_deputado"),
        F.col("forn_sk_fornecedor"),
        F.col("par_sk_partido"),
        F.col("est_sk_estado"),
        F.col("dat_sk_data"),
        F.col("dep_id_deputado"),
        F.col("dep_tx_nome"),
        F.col("dep_tx_sigla_partido"),
        F.col("dep_tx_sigla_uf"),
        F.col("leg_id_legislatura").cast("string").alias("leg_id_legislatura"),
        F.col("forn_tx_chave_deduplicacao"),
        F.col("forn_tx_nome"),
        F.col("forn_tx_documento_original"),
        F.col("forn_tx_documento_limpo"),
        F.col("forn_tx_tipo_documento"),
        F.col("des_nr_ano").cast("int").alias("des_nr_ano"),
        F.col("des_nr_mes").cast("int").alias("des_nr_mes"),
        F.col("des_dt_emissao"),
        F.col("des_tx_tipo_despesa"),
        F.col("des_tx_tipo_documento"),
        F.col("des_vl_documento").cast("double").alias("des_vl_documento"),
        F.col("des_vl_glosa").cast("double").alias("des_vl_glosa"),
        F.col("des_vl_liquido").cast("double").alias("des_vl_liquido"),
        F.col("des_vl_restituicao").cast("double").alias("des_vl_restituicao"),
        F.col("fdc_vl_liquido_abs").cast("double").alias("fdc_vl_liquido_abs"),
        F.col("fdc_vl_zscore_categoria_uf").cast("double").alias("fdc_vl_zscore_categoria_uf"),
        F.col("fdc_fl_anomalia_zscore"),
        F.col("fdc_fl_documento_fornecedor_valido"),
        F.col("fdc_fl_documento_fornecedor_repetido"),
        F.col("fdc_fl_deputado_encontrado_gold"),
        F.col("fdc_fl_fornecedor_encontrado_gold"),
        F.col("fdc_fl_partido_encontrado_gold"),
        F.col("fdc_fl_estado_encontrado_gold"),
        F.col("fdc_fl_data_encontrada_gold"),
        F.col("fdc_fl_dimensoes_principais_completas"),
        F.col("des_fl_documento_informado"),
        F.col("aud_id_execucao_gold"),
        F.col("aud_dh_processamento_gold"),
        F.col("aud_tx_versao_pipeline_gold"),
        F.col("aud_tx_hash_registro_gold")
    )
    .withColumn("dep_tx_nome", F.coalesce(F.col("dep_tx_nome"), F.lit("NAO_INFORMADO")))
    .withColumn("dep_tx_sigla_partido", F.coalesce(F.col("dep_tx_sigla_partido"), F.lit("NAO_INFORMADO")))
    .withColumn("dep_tx_sigla_uf", F.coalesce(F.col("dep_tx_sigla_uf"), F.lit("NAO_INFORMADO")))
    .withColumn("forn_tx_chave_deduplicacao", F.coalesce(F.col("forn_tx_chave_deduplicacao"), F.lit("NAO_INFORMADO")))
    .withColumn("forn_tx_nome", F.coalesce(F.col("forn_tx_nome"), F.lit("NAO_INFORMADO")))
    .withColumn("forn_tx_tipo_documento", F.coalesce(F.col("forn_tx_tipo_documento"), F.lit("NAO_INFORMADO")))
    .withColumn("des_tx_tipo_despesa", F.coalesce(F.col("des_tx_tipo_despesa"), F.lit("NAO_INFORMADO")))
    .withColumn("des_tx_tipo_documento", F.coalesce(F.col("des_tx_tipo_documento"), F.lit("NAO_INFORMADO")))
)

# COMMAND ----------

# ============================================================
# BUILD MART AGGREGATION
# ============================================================

business_key_cols = [
    "dep_id_deputado",
    "leg_id_legislatura",
    "des_nr_ano",
    "des_nr_mes",
    "des_tx_tipo_despesa",
    "forn_tx_chave_deduplicacao"
]

mart_agg_df = (
    standard_df
    .groupBy(*business_key_cols)
    .agg(
        F.first("dep_sk_deputado", ignorenulls=True).alias("dep_sk_deputado"),
        F.first("forn_sk_fornecedor", ignorenulls=True).alias("forn_sk_fornecedor"),
        F.first("par_sk_partido", ignorenulls=True).alias("par_sk_partido"),
        F.first("est_sk_estado", ignorenulls=True).alias("est_sk_estado"),
        F.first("dat_sk_data", ignorenulls=True).alias("dat_sk_data"),
        F.first("dep_tx_nome", ignorenulls=True).alias("dep_tx_nome"),
        F.first("dep_tx_sigla_partido", ignorenulls=True).alias("dep_tx_sigla_partido"),
        F.first("dep_tx_sigla_uf", ignorenulls=True).alias("dep_tx_sigla_uf"),
        F.first("forn_tx_nome", ignorenulls=True).alias("forn_tx_nome"),
        F.first("forn_tx_documento_original", ignorenulls=True).alias("forn_tx_documento_original"),
        F.first("forn_tx_documento_limpo", ignorenulls=True).alias("forn_tx_documento_limpo"),
        F.first("forn_tx_tipo_documento", ignorenulls=True).alias("forn_tx_tipo_documento"),
        F.count("*").alias("dce_qt_despesas"),
        F.countDistinct("fdc_sk_despesa_ceap").alias("dce_qt_despesas_distintas"),
        F.countDistinct("des_tx_tipo_documento").alias("dce_qt_tipos_documento"),
        F.min("des_dt_emissao").alias("dce_dt_primeira_emissao"),
        F.max("des_dt_emissao").alias("dce_dt_ultima_emissao"),
        F.round(F.sum(F.coalesce(F.col("des_vl_documento"), F.lit(0.0))), 2).alias("dce_vl_total_documento"),
        F.round(F.sum(F.coalesce(F.col("des_vl_glosa"), F.lit(0.0))), 2).alias("dce_vl_total_glosa"),
        F.round(F.sum(F.coalesce(F.col("des_vl_liquido"), F.lit(0.0))), 2).alias("dce_vl_total_liquido"),
        F.round(F.sum(F.coalesce(F.col("des_vl_restituicao"), F.lit(0.0))), 2).alias("dce_vl_total_restituicao"),
        F.round(F.avg(F.coalesce(F.col("des_vl_liquido"), F.lit(0.0))), 2).alias("dce_vl_medio_liquido"),
        F.round(F.max(F.coalesce(F.col("des_vl_liquido"), F.lit(0.0))), 2).alias("dce_vl_maior_liquido"),
        F.round(F.sum(F.coalesce(F.col("fdc_vl_liquido_abs"), F.lit(0.0))), 2).alias("dce_vl_total_liquido_abs"),
        F.round(F.max(F.abs(F.coalesce(F.col("fdc_vl_zscore_categoria_uf"), F.lit(0.0)))), 2).alias("dce_vl_maior_zscore_abs"),
        F.sum(F.when(F.col("fdc_fl_anomalia_zscore") == True, 1).otherwise(0)).alias("dce_qt_anomalias_zscore"),
        F.sum(F.when(F.col("fdc_fl_documento_fornecedor_valido") == True, 1).otherwise(0)).alias("dce_qt_documentos_fornecedor_validos"),
        F.sum(F.when(F.col("fdc_fl_documento_fornecedor_repetido") == True, 1).otherwise(0)).alias("dce_qt_documentos_fornecedor_repetidos"),
        F.sum(F.when(F.col("des_fl_documento_informado") == True, 1).otherwise(0)).alias("dce_qt_documentos_informados"),
        F.sum(F.when(F.col("fdc_fl_deputado_encontrado_gold") == True, 1).otherwise(0)).alias("dce_qt_deputado_encontrado_gold"),
        F.sum(F.when(F.col("fdc_fl_fornecedor_encontrado_gold") == True, 1).otherwise(0)).alias("dce_qt_fornecedor_encontrado_gold"),
        F.sum(F.when(F.col("fdc_fl_partido_encontrado_gold") == True, 1).otherwise(0)).alias("dce_qt_partido_encontrado_gold"),
        F.sum(F.when(F.col("fdc_fl_estado_encontrado_gold") == True, 1).otherwise(0)).alias("dce_qt_estado_encontrado_gold"),
        F.sum(F.when(F.col("fdc_fl_data_encontrada_gold") == True, 1).otherwise(0)).alias("dce_qt_data_encontrada_gold"),
        F.sum(F.when(F.col("fdc_fl_dimensoes_principais_completas") == True, 1).otherwise(0)).alias("dce_qt_dimensoes_completas"),
        F.countDistinct("aud_id_execucao_gold").alias("dce_qt_execucoes_gold"),
        F.max("aud_dh_processamento_gold").alias("dce_dh_ultimo_processamento_gold")
    )
)

# COMMAND ----------

# ============================================================
# DERIVE BUSINESS INDICATORS
# ============================================================

rank_deputy_period = Window.partitionBy("des_nr_ano", "des_nr_mes").orderBy(F.col("dce_vl_total_liquido").desc())
rank_supplier_period = Window.partitionBy("des_nr_ano", "des_nr_mes").orderBy(F.col("dce_vl_total_liquido").desc())

mart_metrics_df = (
    mart_agg_df
    .withColumn(
        "dce_vl_pct_glosa",
        F.when(F.col("dce_vl_total_documento") > 0, F.round((F.col("dce_vl_total_glosa") / F.col("dce_vl_total_documento")) * 100, 2)).otherwise(F.lit(0.0))
    )
    .withColumn(
        "dce_vl_pct_dimensoes_completas",
        F.when(F.col("dce_qt_despesas") > 0, F.round((F.col("dce_qt_dimensoes_completas") / F.col("dce_qt_despesas")) * 100, 2)).otherwise(F.lit(0.0))
    )
    .withColumn(
        "dce_fl_possui_glosa",
        F.col("dce_vl_total_glosa") > F.lit(0.0)
    )
    .withColumn(
        "dce_fl_possui_anomalia_zscore",
        F.col("dce_qt_anomalias_zscore") > F.lit(0)
    )
    .withColumn(
        "dce_fl_fornecedor_documento_valido",
        F.col("dce_qt_documentos_fornecedor_validos") == F.col("dce_qt_despesas")
    )
    .withColumn(
        "dce_fl_fornecedor_documento_repetido",
        F.col("dce_qt_documentos_fornecedor_repetidos") > F.lit(0)
    )
    .withColumn(
        "dce_fl_dimensoes_principais_completas",
        F.col("dce_qt_dimensoes_completas") == F.col("dce_qt_despesas")
    )
    .withColumn(
        "dce_fl_registro_valido_marts",
        (
            F.col("dep_id_deputado").isNotNull()
            & F.col("leg_id_legislatura").isNotNull()
            & F.col("des_nr_ano").isNotNull()
            & F.col("des_nr_mes").isNotNull()
            & F.col("des_tx_tipo_despesa").isNotNull()
            & F.col("forn_tx_chave_deduplicacao").isNotNull()
            & (F.col("dce_qt_despesas") > 0)
        )
    )
    .withColumn("dce_nr_rank_valor_periodo", F.dense_rank().over(rank_deputy_period))
    .withColumn("dce_nr_rank_fornecedor_periodo", F.dense_rank().over(rank_supplier_period))
)

# COMMAND ----------

# ============================================================
# FINAL MART DATAFRAME
# ============================================================

final_df = (
    mart_metrics_df
    .withColumn(
        "dce_tx_business_key",
        F.concat_ws(
            "|",
            F.coalesce(F.col("dep_id_deputado"), F.lit("NA")),
            F.coalesce(F.col("leg_id_legislatura"), F.lit("NA")),
            F.coalesce(F.col("des_nr_ano").cast("string"), F.lit("NA")),
            F.coalesce(F.col("des_nr_mes").cast("string"), F.lit("NA")),
            F.coalesce(F.col("des_tx_tipo_despesa"), F.lit("NA")),
            F.coalesce(F.col("forn_tx_chave_deduplicacao"), F.lit("NA"))
        )
    )
    .withColumn("dce_sk_despesa_ceap_mart", F.sha2(F.col("dce_tx_business_key"), 256))
    .withColumn("aud_id_execucao_marts", F.lit(EXECUTION_ID))
    .withColumn("aud_dh_processamento_marts", F.lit(PROCESSING_TS).cast("timestamp"))
    .withColumn("aud_tx_versao_pipeline_marts", F.lit(PIPELINE_VERSION))
    .withColumn(
        "aud_tx_hash_registro_marts",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("dce_tx_business_key"),
                F.coalesce(F.col("dce_qt_despesas").cast("string"), F.lit("0")),
                F.coalesce(F.col("dce_vl_total_liquido").cast("string"), F.lit("0")),
                F.coalesce(F.col("dce_vl_total_glosa").cast("string"), F.lit("0")),
                F.coalesce(F.col("dce_qt_anomalias_zscore").cast("string"), F.lit("0"))
            ),
            256
        )
    )
    .select(
        "dce_sk_despesa_ceap_mart",
        "dce_tx_business_key",
        "dep_sk_deputado",
        "forn_sk_fornecedor",
        "par_sk_partido",
        "est_sk_estado",
        "dat_sk_data",
        "dep_id_deputado",
        "dep_tx_nome",
        "dep_tx_sigla_partido",
        "dep_tx_sigla_uf",
        "leg_id_legislatura",
        "forn_tx_chave_deduplicacao",
        "forn_tx_nome",
        "forn_tx_documento_original",
        "forn_tx_documento_limpo",
        "forn_tx_tipo_documento",
        "des_nr_ano",
        "des_nr_mes",
        "des_tx_tipo_despesa",
        "dce_dt_primeira_emissao",
        "dce_dt_ultima_emissao",
        "dce_qt_despesas",
        "dce_qt_despesas_distintas",
        "dce_qt_tipos_documento",
        "dce_vl_total_documento",
        "dce_vl_total_glosa",
        "dce_vl_total_liquido",
        "dce_vl_total_restituicao",
        "dce_vl_medio_liquido",
        "dce_vl_maior_liquido",
        "dce_vl_total_liquido_abs",
        "dce_vl_pct_glosa",
        "dce_vl_maior_zscore_abs",
        "dce_qt_anomalias_zscore",
        "dce_qt_documentos_fornecedor_validos",
        "dce_qt_documentos_fornecedor_repetidos",
        "dce_qt_documentos_informados",
        "dce_vl_pct_dimensoes_completas",
        "dce_nr_rank_valor_periodo",
        "dce_nr_rank_fornecedor_periodo",
        "dce_fl_possui_glosa",
        "dce_fl_possui_anomalia_zscore",
        "dce_fl_fornecedor_documento_valido",
        "dce_fl_fornecedor_documento_repetido",
        "dce_fl_dimensoes_principais_completas",
        "dce_fl_registro_valido_marts",
        "dce_qt_deputado_encontrado_gold",
        "dce_qt_fornecedor_encontrado_gold",
        "dce_qt_partido_encontrado_gold",
        "dce_qt_estado_encontrado_gold",
        "dce_qt_data_encontrada_gold",
        "dce_qt_dimensoes_completas",
        "dce_qt_execucoes_gold",
        "dce_dh_ultimo_processamento_gold",
        "aud_id_execucao_marts",
        "aud_dh_processamento_marts",
        "aud_tx_versao_pipeline_marts",
        "aud_tx_hash_registro_marts"
    )
)

# COMMAND ----------

# ============================================================
# QUALITY GATES
# ============================================================

records_read = source_df.count()
records_eligible = source_valid_df.count()
records_written = final_df.count()

if records_written == 0:
    raise ValueError("Marts validation failed: target dataframe is empty.")

mandatory_errors = final_df.filter(
    F.col("dce_sk_despesa_ceap_mart").isNull()
    | F.col("dce_tx_business_key").isNull()
    | F.col("dep_id_deputado").isNull()
    | F.col("des_nr_ano").isNull()
    | F.col("des_nr_mes").isNull()
    | F.col("des_tx_tipo_despesa").isNull()
    | F.col("forn_tx_chave_deduplicacao").isNull()
).count()

if mandatory_errors > 0:
    raise ValueError(f"Marts validation failed: {mandatory_errors} records have mandatory key errors.")

duplicate_errors = (
    final_df
    .groupBy("dce_tx_business_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicate_errors > 0:
    raise ValueError(f"Marts validation failed: {duplicate_errors} duplicated business keys found.")

negative_metric_errors = final_df.filter(
    (F.col("dce_qt_despesas") < 0)
    | (F.col("dce_qt_despesas_distintas") < 0)
    | (F.col("dce_qt_tipos_documento") < 0)
    | (F.col("dce_qt_anomalias_zscore") < 0)
    | (F.col("dce_qt_documentos_fornecedor_validos") < 0)
    | (F.col("dce_qt_documentos_fornecedor_repetidos") < 0)
    | (F.col("dce_qt_documentos_informados") < 0)
).count()

financial_negative_records = final_df.filter(
    (F.col("dce_vl_total_documento") < 0)
    | (F.col("dce_vl_total_glosa") < 0)
    | (F.col("dce_vl_total_liquido") < 0)
    | (F.col("dce_vl_total_restituicao") < 0)
).count()

print(f"[INFO] Financial negative records detected: {financial_negative_records}")

if negative_metric_errors > 0:
    raise ValueError(f"Marts validation failed: {negative_metric_errors} records have invalid negative metrics.")

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

column_comments = {
    "dce_sk_despesa_ceap_mart": "Marts surrogate key for the CEAP expenses analytical mart record.",
    "dce_tx_business_key": "Business key identifying one CEAP mart aggregation grain.",
    "dep_sk_deputado": "Gold surrogate key of the deputy dimension.",
    "forn_sk_fornecedor": "Gold surrogate key of the supplier dimension.",
    "par_sk_partido": "Gold surrogate key of the political party dimension.",
    "est_sk_estado": "Gold surrogate key of the state dimension.",
    "dat_sk_data": "Gold surrogate key of the date dimension.",
    "dep_id_deputado": "Deputy business identifier associated with the expenses.",
    "dep_tx_nome": "Deputy name associated with the expenses.",
    "dep_tx_sigla_partido": "Deputy political party acronym associated with the expenses.",
    "dep_tx_sigla_uf": "Deputy federation unit acronym associated with the expenses.",
    "leg_id_legislatura": "Legislature identifier associated with the expenses.",
    "forn_tx_chave_deduplicacao": "Supplier business key used in the CEAP expense fact.",
    "forn_tx_nome": "Supplier standardized name.",
    "forn_tx_documento_original": "Original supplier document value.",
    "forn_tx_documento_limpo": "Clean supplier document containing only numeric characters.",
    "forn_tx_tipo_documento": "Supplier document type classification.",
    "des_nr_ano": "CEAP expense reference year.",
    "des_nr_mes": "CEAP expense reference month.",
    "des_tx_tipo_despesa": "CEAP expense category.",
    "dce_dt_primeira_emissao": "First expense issue date in the aggregation group.",
    "dce_dt_ultima_emissao": "Latest expense issue date in the aggregation group.",
    "dce_qt_despesas": "Total number of CEAP expense records in the aggregation group.",
    "dce_qt_despesas_distintas": "Number of distinct CEAP expense fact surrogate keys in the aggregation group.",
    "dce_qt_tipos_documento": "Number of distinct expense document types in the aggregation group.",
    "dce_vl_total_documento": "Total original CEAP document amount.",
    "dce_vl_total_glosa": "Total disallowed CEAP amount.",
    "dce_vl_total_liquido": "Total CEAP net reimbursed amount.",
    "dce_vl_total_restituicao": "Total CEAP refunded amount.",
    "dce_vl_medio_liquido": "Average CEAP net reimbursed amount.",
    "dce_vl_maior_liquido": "Maximum CEAP net reimbursed amount.",
    "dce_vl_total_liquido_abs": "Total absolute CEAP net amount used for anomaly analysis.",
    "dce_vl_pct_glosa": "Percentage of disallowed amount over original document amount.",
    "dce_vl_maior_zscore_abs": "Maximum absolute z-score in the aggregation group.",
    "dce_qt_anomalias_zscore": "Number of expenses classified as z-score anomalies.",
    "dce_qt_documentos_fornecedor_validos": "Number of expenses with structurally valid supplier document.",
    "dce_qt_documentos_fornecedor_repetidos": "Number of expenses with repeated-digit supplier document.",
    "dce_qt_documentos_informados": "Number of expenses with informed document.",
    "dce_vl_pct_dimensoes_completas": "Percentage of expense records with complete main Gold dimensional coverage.",
    "dce_nr_rank_valor_periodo": "Ranking by total net amount within year and month.",
    "dce_nr_rank_fornecedor_periodo": "Supplier ranking by total net amount within year and month.",
    "dce_fl_possui_glosa": "Flag indicating whether the aggregation group contains disallowed amount.",
    "dce_fl_possui_anomalia_zscore": "Flag indicating whether the aggregation group contains z-score anomalies.",
    "dce_fl_fornecedor_documento_valido": "Flag indicating whether all supplier documents in the group are structurally valid.",
    "dce_fl_fornecedor_documento_repetido": "Flag indicating whether any supplier document in the group is repeated-digit.",
    "dce_fl_dimensoes_principais_completas": "Flag indicating whether all records in the group have complete main Gold dimensions.",
    "dce_fl_registro_valido_marts": "Flag indicating whether the mart record passed Marts validation.",
    "dce_qt_deputado_encontrado_gold": "Number of records with deputy dimension found in Gold.",
    "dce_qt_fornecedor_encontrado_gold": "Number of records with supplier dimension found in Gold.",
    "dce_qt_partido_encontrado_gold": "Number of records with party dimension found in Gold.",
    "dce_qt_estado_encontrado_gold": "Number of records with state dimension found in Gold.",
    "dce_qt_data_encontrada_gold": "Number of records with date dimension found in Gold.",
    "dce_qt_dimensoes_completas": "Number of records with all main dimensions complete.",
    "dce_qt_execucoes_gold": "Number of Gold executions represented in the aggregation group.",
    "dce_dh_ultimo_processamento_gold": "Latest Gold processing timestamp represented in the aggregation group.",
    "aud_id_execucao_marts": "Execution identifier generated during Marts processing.",
    "aud_dh_processamento_marts": "Timestamp when the record was processed in Marts.",
    "aud_tx_versao_pipeline_marts": "Pipeline version used during Marts processing.",
    "aud_tx_hash_registro_marts": "Deterministic Marts record hash."
}

spark.sql(f"COMMENT ON TABLE {TARGET_TABLE} IS 'Analytical mart for CEAP parliamentary expenses aggregated by deputy, period, expense type and supplier.'")

for column_name, comment in column_comments.items():
    safe_comment = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {TARGET_TABLE} ALTER COLUMN {column_name} COMMENT '{safe_comment}'")
    print(f"[SUCCESS] Column comment applied: {TARGET_TABLE}.{column_name}")

# COMMAND ----------

# ============================================================
# CSV EXPORT
# ============================================================

try:
    dbutils.fs.rm(EXPORT_TMP_DIR, True)
except Exception:
    pass

(
    final_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ";")
    .csv(EXPORT_TMP_DIR)
)

try:
    dbutils.fs.mkdirs(EXPORT_DIR)
    for file_info in dbutils.fs.ls(EXPORT_TMP_DIR):
        if file_info.name.startswith("part-") and file_info.name.endswith(".csv"):
            dbutils.fs.rm(EXPORT_FILE, True)
            dbutils.fs.mv(file_info.path, EXPORT_FILE)
            break
    dbutils.fs.rm(EXPORT_TMP_DIR, True)
except Exception as export_error:
    print(f"[WARNING] CSV single-file normalization failed: {export_error}")
    print(f"[INFO] CSV export remains available at: {EXPORT_TMP_DIR}")

# COMMAND ----------

# ============================================================
# EXECUTION SUMMARY
# ============================================================

print("=" * 80)
print("MART DESPESAS CEAP - RESUMO EXECUCAO")
print("=" * 80)
print(f"Records read from Gold CEAP fact: {records_read}")
print(f"Records eligible from Gold CEAP fact: {records_eligible}")
print(f"Records written to Marts: {records_written}")
print(f"CSV export path: {EXPORT_FILE}")
print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)
