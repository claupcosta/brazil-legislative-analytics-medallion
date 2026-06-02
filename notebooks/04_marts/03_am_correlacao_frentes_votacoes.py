# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Marts — Parliamentary Fronts and Voting Correlation
# MAGIC
# MAGIC **Notebook:** `03_am_correlacao_frentes_votacoes`
# MAGIC
# MAGIC Builds the curated Business Mart for Parliamentary Front and voting correlation analysis used by analytical dashboards, executive reports, and business consumption.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Parliamentary Front and voting correlation mart model
# MAGIC * One analytical record per Parliamentary Front, voting event and legislature
# MAGIC * Front representativeness indicators
# MAGIC * Front voting participation indicators
# MAGIC * Vote composition indicators, including Article 17 votes
# MAGIC * Predominant vote and cohesion indicators
# MAGIC * Alignment indicators between Front predominant vote and voting result
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
# MAGIC * Correlate Parliamentary Front members with legislative voting results
# MAGIC * Keep one analytical record per Front, voting event and legislature
# MAGIC * Preserve Front and voting business identifiers
# MAGIC * Calculate participation, cohesion and vote distribution indicators
# MAGIC * Preserve Article 17 voting category as an explicit metric
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
# MAGIC This mart supports voting behavior analysis for Parliamentary Fronts:
# MAGIC
# MAGIC 1. Which Fronts participated in each legislative voting event?
# MAGIC 2. How many Front members voted in each voting event?
# MAGIC 3. What was the predominant vote of each Front?
# MAGIC 4. Which Fronts showed high voting cohesion?
# MAGIC 5. Which Fronts had high voting participation?
# MAGIC 6. Was the Front predominant vote aligned with the final voting result?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Mart Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per Parliamentary Front, voting event and legislature.
# MAGIC
# MAGIC ### Sources
# MAGIC
# MAGIC * `brazil_legislative_analytics.gold.ft_frentes_membros`
# MAGIC * `brazil_legislative_analytics.gold.ft_resultados_votacoes`
# MAGIC * `brazil_legislative_analytics.gold.dm_frentes`
# MAGIC * `brazil_legislative_analytics.gold.dm_votacoes`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.marts.am_correlacao_frentes_votacoes`
# MAGIC
# MAGIC ### CSV Export
# MAGIC
# MAGIC `/Volumes/brazil_legislative_analytics/marts/exports/am_correlacao_frentes_votacoes/am_correlacao_frentes_votacoes.csv`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC * `frn_id_frente`
# MAGIC * `vot_id_votacao`
# MAGIC * `leg_id_legislatura`
# MAGIC
# MAGIC ### Mart Surrogate Key
# MAGIC
# MAGIC `cfv_sk_correlacao_frente_votacao`
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
# MAGIC One analytical record is maintained per Front, voting event and legislature.
# MAGIC
# MAGIC Rule 3:
# MAGIC
# MAGIC Front membership metrics are derived from `ft_frentes_membros`.
# MAGIC
# MAGIC Rule 4:
# MAGIC
# MAGIC Voting metrics are derived from `ft_resultados_votacoes`.
# MAGIC
# MAGIC Rule 5:
# MAGIC
# MAGIC Vote composition explicitly preserves YES, NO, ABSTENTION, OBSTRUCTION and ARTICLE 17 categories.
# MAGIC
# MAGIC Rule 6:
# MAGIC
# MAGIC Vote consistency requires recorded votes to equal the sum of all curated vote categories.
# MAGIC
# MAGIC Rule 7:
# MAGIC
# MAGIC The mart must be published as Delta and exported as CSV.
# MAGIC
# MAGIC Rule 8:
# MAGIC
# MAGIC All Marts objects must contain governance comments.

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
from datetime import datetime
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

try:
    PROJECT_VERSION
except NameError:
    PROJECT_VERSION = "marts_v1.1_article17"

NOTEBOOK_NAME = "03_am_correlacao_frentes_votacoes"
ENTITY_NAME = "correlacao_frentes_votacoes"

SOURCE_FACT_FRENTES_MEMBROS = f"{GOLD_SCHEMA}.ft_frentes_membros"
SOURCE_FACT_RESULTADOS_VOTACOES = f"{GOLD_SCHEMA}.ft_resultados_votacoes"
SOURCE_DIM_FRENTES = f"{GOLD_SCHEMA}.dm_frentes"
SOURCE_DIM_VOTACOES = f"{GOLD_SCHEMA}.dm_votacoes"

TARGET_TABLE = f"{MARTS_SCHEMA}.am_correlacao_frentes_votacoes"
CSV_EXPORT_DIR = f"/Volumes/{CATALOG_NAME}/marts/exports/am_correlacao_frentes_votacoes"
CSV_EXPORT_FILE = f"{CSV_EXPORT_DIR}/am_correlacao_frentes_votacoes.csv"

PIPELINE_VERSION = PROJECT_VERSION
EXECUTION_ID = str(uuid.uuid4())
PIPELINE_LOG_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()
PROCESSING_TS = datetime.utcnow()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name="marts"
)

log_info(logger, f"Starting notebook {NOTEBOOK_NAME}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
dbutils.fs.mkdirs(CSV_EXPORT_DIR)

# COMMAND ----------

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def has_column(df, column_name: str) -> bool:
    return column_name in df.columns


def normalize_vote_column(df, source_col: str):
    vote_col = F.upper(F.trim(F.col(source_col)))
    return (
        F.when(vote_col.isin("SIM"), F.lit("SIM"))
         .when(vote_col.isin("NAO", "NÃO"), F.lit("NAO"))
         .when(vote_col.isin("ABSTENCAO", "ABSTENÇÃO"), F.lit("ABSTENCAO"))
         .when(vote_col.isin("OBSTRUCAO", "OBSTRUÇÃO"), F.lit("OBSTRUCAO"))
         .when(vote_col.isin("ARTIGO 17", "ART. 17", "ART17", "ARTIGO17"), F.lit("ARTIGO 17"))
         .otherwise(vote_col)
    )


def export_single_csv(dataframe, temporary_path, final_file_path):
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

    csv_files = [
        file_info.path
        for file_info in dbutils.fs.ls(temporary_path)
        if file_info.path.endswith(".csv")
    ]

    if len(csv_files) != 1:
        raise ValueError("CSV export failed: expected exactly one CSV part file.")

    dbutils.fs.rm(final_file_path, recurse=True)
    dbutils.fs.mv(csv_files[0], final_file_path)
    dbutils.fs.rm(temporary_path, recurse=True)

# COMMAND ----------

# ============================================================
# READ GOLD SOURCES
# ============================================================

front_members_df = spark.table(SOURCE_FACT_FRENTES_MEMBROS)
voting_results_df = spark.table(SOURCE_FACT_RESULTADOS_VOTACOES)
fronts_df = spark.table(SOURCE_DIM_FRENTES)
votings_df = spark.table(SOURCE_DIM_VOTACOES)

records_read_front_members = front_members_df.count()
records_read_voting_results = voting_results_df.count()
records_read = records_read_front_members + records_read_voting_results

log_info(logger, f"Records read from {SOURCE_FACT_FRENTES_MEMBROS}: {records_read_front_members}")
log_info(logger, f"Records read from {SOURCE_FACT_RESULTADOS_VOTACOES}: {records_read_voting_results}")

# COMMAND ----------

# ============================================================
# STANDARDIZE FRONT MEMBERSHIP SOURCE
# ============================================================

front_members_valid_df = front_members_df

if has_column(front_members_valid_df, "ffm_fl_registro_valido_gold"):
    front_members_valid_df = front_members_valid_df.filter(F.col("ffm_fl_registro_valido_gold") == F.lit(True))

front_members_standard_df = (
    front_members_valid_df
    .select(
        F.col("frn_sk_frente").alias("fm_frn_sk_frente"),
        F.col("frn_id_frente").cast("string").alias("frn_id_frente"),
        F.col("dep_id_deputado").cast("string").alias("dep_id_deputado"),
        F.col("dep_tx_sigla_partido").alias("fm_dep_tx_sigla_partido"),
        F.col("dep_tx_sigla_uf").alias("fm_dep_tx_sigla_uf"),
        F.col("leg_id_legislatura").cast("string").alias("leg_id_legislatura"),
        F.col("ffm_fl_frente_encontrada_gold").cast("boolean").alias("ffm_fl_frente_encontrada_gold"),
        F.col("ffm_fl_deputado_encontrado_gold").cast("boolean").alias("ffm_fl_deputado_encontrado_gold"),
        F.col("ffm_fl_dimensoes_principais_completas").cast("boolean").alias("ffm_fl_dimensoes_principais_completas")
    )
    .dropDuplicates(["frn_id_frente", "dep_id_deputado", "leg_id_legislatura"])
)

# COMMAND ----------

# ============================================================
# STANDARDIZE VOTING RESULTS SOURCE
# ============================================================

voting_results_valid_df = voting_results_df

if has_column(voting_results_valid_df, "frv_fl_registro_valido_gold"):
    voting_results_valid_df = voting_results_valid_df.filter(F.col("frv_fl_registro_valido_gold") == F.lit(True))

voting_results_standard_df = (
    voting_results_valid_df
    .withColumn("frv_tx_voto_curado_norm", normalize_vote_column(voting_results_valid_df, "frv_tx_voto_curado"))
    .select(
        F.col("vot_sk_votacao").alias("vr_vot_sk_votacao"),
        F.col("vot_id_votacao").cast("string").alias("vot_id_votacao"),
        F.col("dep_id_deputado").cast("string").alias("dep_id_deputado"),
        F.col("dep_tx_sigla_partido").alias("vr_dep_tx_sigla_partido"),
        F.col("dep_tx_sigla_uf").alias("vr_dep_tx_sigla_uf"),
        F.col("leg_id_legislatura").cast("string").alias("leg_id_legislatura"),
        F.col("frv_tx_voto_curado_norm").alias("frv_tx_voto_curado"),
        F.coalesce(F.col("frv_qt_voto"), F.lit(1)).cast("bigint").alias("frv_qt_voto"),
        F.col("frv_fl_votacao_encontrada_gold").cast("boolean").alias("frv_fl_votacao_encontrada_gold"),
        F.col("frv_fl_deputado_encontrado_gold").cast("boolean").alias("frv_fl_deputado_encontrado_gold"),
        F.col("frv_fl_partido_encontrado_gold").cast("boolean").alias("frv_fl_partido_encontrado_gold"),
        F.col("frv_fl_estado_encontrado_gold").cast("boolean").alias("frv_fl_estado_encontrado_gold"),
        F.col("frv_fl_data_encontrada_gold").cast("boolean").alias("frv_fl_data_encontrada_gold"),
        F.col("frv_fl_dimensoes_principais_completas").cast("boolean").alias("frv_fl_dimensoes_principais_completas")
    )
)

# COMMAND ----------

# ============================================================
# STANDARDIZE FRONT DIMENSION
# Avoid selecting frn_sk_frente here to prevent ambiguity.
# The Front surrogate key used by the Mart comes from the correlated Gold fact.
# ============================================================

fronts_dim_df = (
    fronts_df
    .select(
        F.col("frn_id_frente").cast("string").alias("dim_frn_id_frente"),
        F.col("frn_tx_titulo").alias("dim_frn_tx_titulo"),
        F.col("frn_tx_situacao").alias("dim_frn_tx_situacao")
    )
    .dropDuplicates(["dim_frn_id_frente"])
)

# COMMAND ----------

# ============================================================
# STANDARDIZE VOTING DIMENSION
# Avoid selecting vot_sk_votacao here to prevent ambiguity.
# The Voting surrogate key used by the Mart comes from the correlated Gold fact.
# ============================================================

votings_dim_df = (
    votings_df
    .select(
        F.col("vot_id_votacao").cast("string").alias("dim_vot_id_votacao"),
        F.col("vot_tx_descricao").alias("dim_vot_tx_descricao"),
        F.col("vot_dt_votacao").cast("date").alias("dim_vot_dt_votacao"),
        F.col("vot_dh_votacao").cast("timestamp").alias("dim_vot_dh_votacao"),
        F.col("vot_nr_ano").cast("int").alias("dim_vot_nr_ano"),
        F.col("vot_nr_mes").cast("int").alias("dim_vot_nr_mes"),
        F.col("org_tx_sigla").alias("dim_org_tx_sigla"),
        F.col("prop_id_proposicao").alias("dim_prop_id_proposicao"),
        F.col("vot_tx_resultado_curado").alias("dim_vot_tx_resultado_curado"),
        F.col("vot_fl_aprovada").cast("boolean").alias("dim_vot_fl_aprovada")
    )
    .dropDuplicates(["dim_vot_id_votacao"])
)

# COMMAND ----------

# ============================================================
# FRONT MEMBERSHIP METRICS
# ============================================================

front_metrics_df = (
    front_members_standard_df
    .groupBy("frn_id_frente", "leg_id_legislatura")
    .agg(
        F.first("fm_frn_sk_frente", ignorenulls=True).alias("fm_frn_sk_frente"),
        F.countDistinct("dep_id_deputado").alias("cfv_qt_membros_frente"),
        F.countDistinct("fm_dep_tx_sigla_partido").alias("cfv_qt_partidos_frente"),
        F.countDistinct("fm_dep_tx_sigla_uf").alias("cfv_qt_ufs_frente")
    )
)

# COMMAND ----------

# ============================================================
# CORRELATE FRONT MEMBERS WITH VOTING RESULTS
# ============================================================

correlated_votes_df = (
    front_members_standard_df.alias("fm")
    .join(
        voting_results_standard_df.alias("vr"),
        on=[
            F.col("fm.dep_id_deputado") == F.col("vr.dep_id_deputado"),
            F.col("fm.leg_id_legislatura") == F.col("vr.leg_id_legislatura")
        ],
        how="inner"
    )
    .select(
        F.col("fm.frn_id_frente").alias("frn_id_frente"),
        F.col("fm.fm_frn_sk_frente").alias("fm_frn_sk_frente"),
        F.col("vr.vot_id_votacao").alias("vot_id_votacao"),
        F.col("vr.vr_vot_sk_votacao").alias("vr_vot_sk_votacao"),
        F.col("fm.leg_id_legislatura").alias("leg_id_legislatura"),
        F.col("fm.dep_id_deputado").alias("dep_id_deputado"),
        F.coalesce(F.col("vr.vr_dep_tx_sigla_partido"), F.col("fm.fm_dep_tx_sigla_partido")).alias("dep_tx_sigla_partido"),
        F.coalesce(F.col("vr.vr_dep_tx_sigla_uf"), F.col("fm.fm_dep_tx_sigla_uf")).alias("dep_tx_sigla_uf"),
        F.col("vr.frv_tx_voto_curado").alias("frv_tx_voto_curado"),
        F.col("vr.frv_qt_voto").alias("frv_qt_voto"),
        F.col("fm.ffm_fl_frente_encontrada_gold").alias("ffm_fl_frente_encontrada_gold"),
        F.col("vr.frv_fl_votacao_encontrada_gold").alias("frv_fl_votacao_encontrada_gold"),
        F.col("vr.frv_fl_deputado_encontrado_gold").alias("frv_fl_deputado_encontrado_gold"),
        F.col("vr.frv_fl_partido_encontrado_gold").alias("frv_fl_partido_encontrado_gold"),
        F.col("vr.frv_fl_estado_encontrado_gold").alias("frv_fl_estado_encontrado_gold"),
        F.col("vr.frv_fl_data_encontrada_gold").alias("frv_fl_data_encontrada_gold"),
        F.col("vr.frv_fl_dimensoes_principais_completas").alias("frv_fl_dimensoes_principais_completas")
    )
)

# COMMAND ----------

# ============================================================
# VOTING METRICS BY FRONT AND VOTING EVENT
# ============================================================

vote_metrics_df = (
    correlated_votes_df
    .groupBy("frn_id_frente", "vot_id_votacao", "leg_id_legislatura")
    .agg(
        F.first("fm_frn_sk_frente", ignorenulls=True).alias("vm_frn_sk_frente"),
        F.first("vr_vot_sk_votacao", ignorenulls=True).alias("vm_vot_sk_votacao"),
        F.countDistinct("dep_id_deputado").alias("cfv_qt_membros_votantes"),
        F.sum("frv_qt_voto").alias("cfv_qt_votos_registrados"),
        F.sum(F.when(F.col("frv_tx_voto_curado") == "SIM", F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_votos_sim"),
        F.sum(F.when(F.col("frv_tx_voto_curado") == "NAO", F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_votos_nao"),
        F.sum(F.when(F.col("frv_tx_voto_curado") == "ABSTENCAO", F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_abstencoes"),
        F.sum(F.when(F.col("frv_tx_voto_curado") == "OBSTRUCAO", F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_obstrucoes"),
        F.sum(F.when(F.col("frv_tx_voto_curado") == "ARTIGO 17", F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_votos_artigo_17"),
        F.countDistinct("dep_tx_sigla_partido").alias("cfv_qt_partidos_votantes"),
        F.countDistinct("dep_tx_sigla_uf").alias("cfv_qt_ufs_votantes"),
        F.sum(F.when(F.col("ffm_fl_frente_encontrada_gold") == True, F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_frente_encontrada_gold"),
        F.sum(F.when(F.col("frv_fl_votacao_encontrada_gold") == True, F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_votacao_encontrada_gold"),
        F.sum(F.when(F.col("frv_fl_deputado_encontrado_gold") == True, F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_deputado_encontrado_gold"),
        F.sum(F.when(F.col("frv_fl_partido_encontrado_gold") == True, F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_partido_encontrado_gold"),
        F.sum(F.when(F.col("frv_fl_estado_encontrado_gold") == True, F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_estado_encontrado_gold"),
        F.sum(F.when(F.col("frv_fl_data_encontrada_gold") == True, F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_data_encontrada_gold"),
        F.sum(F.when(F.col("frv_fl_dimensoes_principais_completas") == True, F.col("frv_qt_voto")).otherwise(F.lit(0))).alias("cfv_qt_dimensoes_completas")
    )
)

# COMMAND ----------

# ============================================================
# PREDOMINANT VOTE METRICS
# ============================================================

vote_distribution_df = (
    correlated_votes_df
    .groupBy("frn_id_frente", "vot_id_votacao", "leg_id_legislatura", "frv_tx_voto_curado")
    .agg(F.sum("frv_qt_voto").alias("qt_membros_voto"))
)

vote_rank_window = Window.partitionBy("frn_id_frente", "vot_id_votacao", "leg_id_legislatura").orderBy(
    F.col("qt_membros_voto").desc(),
    F.col("frv_tx_voto_curado").asc()
)

predominant_vote_df = (
    vote_distribution_df
    .withColumn("rn", F.row_number().over(vote_rank_window))
    .filter(F.col("rn") == 1)
    .select(
        F.col("frn_id_frente").alias("pv_frn_id_frente"),
        F.col("vot_id_votacao").alias("pv_vot_id_votacao"),
        F.col("leg_id_legislatura").alias("pv_leg_id_legislatura"),
        F.col("frv_tx_voto_curado").alias("cfv_tx_voto_predominante"),
        F.col("qt_membros_voto").alias("cfv_qt_membros_voto_predominante")
    )
)

# COMMAND ----------

# ============================================================
# BUILD MART BASE DATAFRAME
# Explicit aliases are used to prevent duplicate column ambiguity.
# ============================================================

base_mart_df = (
    vote_metrics_df.alias("vm")
    .join(
        front_metrics_df.alias("fm"),
        on=[
            F.col("vm.frn_id_frente") == F.col("fm.frn_id_frente"),
            F.col("vm.leg_id_legislatura") == F.col("fm.leg_id_legislatura")
        ],
        how="left"
    )
    .join(
        predominant_vote_df.alias("pv"),
        on=[
            F.col("vm.frn_id_frente") == F.col("pv.pv_frn_id_frente"),
            F.col("vm.vot_id_votacao") == F.col("pv.pv_vot_id_votacao"),
            F.col("vm.leg_id_legislatura") == F.col("pv.pv_leg_id_legislatura")
        ],
        how="left"
    )
    .join(
        fronts_dim_df.alias("df"),
        F.col("vm.frn_id_frente") == F.col("df.dim_frn_id_frente"),
        how="left"
    )
    .join(
        votings_dim_df.alias("dv"),
        F.col("vm.vot_id_votacao") == F.col("dv.dim_vot_id_votacao"),
        how="left"
    )
    .select(
        F.col("vm.vm_frn_sk_frente").alias("frn_sk_frente"),
        F.col("vm.vm_vot_sk_votacao").alias("vot_sk_votacao"),
        F.col("vm.frn_id_frente").alias("frn_id_frente"),
        F.col("df.dim_frn_tx_titulo").alias("frn_tx_titulo"),
        F.col("df.dim_frn_tx_situacao").alias("frn_tx_situacao"),
        F.col("vm.vot_id_votacao").alias("vot_id_votacao"),
        F.col("dv.dim_vot_tx_descricao").alias("vot_tx_descricao"),
        F.col("dv.dim_vot_dt_votacao").alias("vot_dt_votacao"),
        F.col("dv.dim_vot_dh_votacao").alias("vot_dh_votacao"),
        F.col("dv.dim_vot_nr_ano").alias("vot_nr_ano"),
        F.col("dv.dim_vot_nr_mes").alias("vot_nr_mes"),
        F.col("vm.leg_id_legislatura").alias("leg_id_legislatura"),
        F.col("dv.dim_org_tx_sigla").alias("org_tx_sigla"),
        F.col("dv.dim_prop_id_proposicao").alias("prop_id_proposicao"),
        F.col("dv.dim_vot_tx_resultado_curado").alias("vot_tx_resultado_curado"),
        F.col("dv.dim_vot_fl_aprovada").alias("vot_fl_aprovada"),
        F.col("fm.cfv_qt_membros_frente").alias("cfv_qt_membros_frente"),
        F.col("fm.cfv_qt_partidos_frente").alias("cfv_qt_partidos_frente"),
        F.col("fm.cfv_qt_ufs_frente").alias("cfv_qt_ufs_frente"),
        F.col("vm.cfv_qt_membros_votantes").alias("cfv_qt_membros_votantes"),
        F.col("vm.cfv_qt_votos_registrados").alias("cfv_qt_votos_registrados"),
        F.col("vm.cfv_qt_votos_sim").alias("cfv_qt_votos_sim"),
        F.col("vm.cfv_qt_votos_nao").alias("cfv_qt_votos_nao"),
        F.col("vm.cfv_qt_abstencoes").alias("cfv_qt_abstencoes"),
        F.col("vm.cfv_qt_obstrucoes").alias("cfv_qt_obstrucoes"),
        F.col("vm.cfv_qt_votos_artigo_17").alias("cfv_qt_votos_artigo_17"),
        F.col("vm.cfv_qt_partidos_votantes").alias("cfv_qt_partidos_votantes"),
        F.col("vm.cfv_qt_ufs_votantes").alias("cfv_qt_ufs_votantes"),
        F.col("pv.cfv_tx_voto_predominante").alias("cfv_tx_voto_predominante"),
        F.col("pv.cfv_qt_membros_voto_predominante").alias("cfv_qt_membros_voto_predominante"),
        F.col("vm.cfv_qt_frente_encontrada_gold").alias("cfv_qt_frente_encontrada_gold"),
        F.col("vm.cfv_qt_votacao_encontrada_gold").alias("cfv_qt_votacao_encontrada_gold"),
        F.col("vm.cfv_qt_deputado_encontrado_gold").alias("cfv_qt_deputado_encontrado_gold"),
        F.col("vm.cfv_qt_partido_encontrado_gold").alias("cfv_qt_partido_encontrado_gold"),
        F.col("vm.cfv_qt_estado_encontrado_gold").alias("cfv_qt_estado_encontrado_gold"),
        F.col("vm.cfv_qt_data_encontrada_gold").alias("cfv_qt_data_encontrada_gold"),
        F.col("vm.cfv_qt_dimensoes_completas").alias("cfv_qt_dimensoes_completas")
    )
)

# COMMAND ----------

# ============================================================
# BUILD MART DATAFRAME
# ============================================================

engagement_window = Window.orderBy(
    F.col("cfv_qt_membros_votantes").desc(),
    F.col("frn_id_frente"),
    F.col("vot_id_votacao")
)

cohesion_window = Window.orderBy(
    F.col("cfv_vl_pct_voto_predominante").desc(),
    F.col("cfv_qt_membros_votantes").desc(),
    F.col("frn_id_frente"),
    F.col("vot_id_votacao")
)

mart_df = (
    base_mart_df
    .withColumn(
        "cfv_vl_pct_participacao_frente",
        F.when(
            F.col("cfv_qt_membros_frente") > 0,
            F.round((F.col("cfv_qt_membros_votantes") / F.col("cfv_qt_membros_frente")) * 100, 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "cfv_vl_pct_voto_predominante",
        F.when(
            F.col("cfv_qt_votos_registrados") > 0,
            F.round((F.col("cfv_qt_membros_voto_predominante") / F.col("cfv_qt_votos_registrados")) * 100, 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "cfv_vl_pct_dimensoes_completas",
        F.when(
            F.col("cfv_qt_votos_registrados") > 0,
            F.round((F.col("cfv_qt_dimensoes_completas") / F.col("cfv_qt_votos_registrados")) * 100, 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn("cfv_nr_rank_engajamento", F.row_number().over(engagement_window))
    .withColumn("cfv_nr_rank_coesao", F.row_number().over(cohesion_window))
    .withColumn("cfv_fl_voto_predominante_identificado", F.col("cfv_tx_voto_predominante").isNotNull())
    .withColumn(
        "cfv_fl_alinhado_resultado_votacao",
        F.when(
            F.col("vot_tx_resultado_curado").isNull() | F.col("cfv_tx_voto_predominante").isNull(),
            F.lit(None).cast("boolean")
        )
        .when(
            ((F.col("vot_tx_resultado_curado") == "MAIORIA SIM") & (F.col("cfv_tx_voto_predominante") == "SIM")) |
            ((F.col("vot_tx_resultado_curado") == "MAIORIA NAO") & (F.col("cfv_tx_voto_predominante") == "NAO")),
            F.lit(True)
        )
        .otherwise(F.lit(False))
    )
    .withColumn("cfv_fl_alta_coesao", F.col("cfv_vl_pct_voto_predominante") >= F.lit(70.0))
    .withColumn("cfv_fl_alta_participacao", F.col("cfv_vl_pct_participacao_frente") >= F.lit(50.0))
    .withColumn(
        "cfv_fl_dimensoes_principais_completas",
        (F.col("cfv_qt_frente_encontrada_gold") == F.col("cfv_qt_votos_registrados")) &
        (F.col("cfv_qt_votacao_encontrada_gold") == F.col("cfv_qt_votos_registrados")) &
        (F.col("cfv_qt_deputado_encontrado_gold") == F.col("cfv_qt_votos_registrados"))
    )
    .withColumn(
        "cfv_fl_registro_valido_marts",
        F.col("frn_id_frente").isNotNull() &
        F.col("vot_id_votacao").isNotNull() &
        F.col("leg_id_legislatura").isNotNull()
    )
    .withColumn(
        "cfv_sk_correlacao_frente_votacao",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("frn_id_frente"), F.lit("")),
                F.coalesce(F.col("vot_id_votacao"), F.lit("")),
                F.coalesce(F.col("leg_id_legislatura"), F.lit(""))
            ),
            256
        )
    )
    .withColumn("aud_id_execucao_marts", F.lit(EXECUTION_ID))
    .withColumn("aud_dh_processamento_marts", F.lit(PROCESSING_TS).cast("timestamp"))
    .withColumn("aud_tx_versao_pipeline_marts", F.lit(PIPELINE_VERSION))
)

# COMMAND ----------

# ============================================================
# FINAL SELECT AND HASH
# ============================================================

final_df = (
    mart_df
    .select(
        "cfv_sk_correlacao_frente_votacao",
        "frn_sk_frente",
        "vot_sk_votacao",
        "frn_id_frente",
        "frn_tx_titulo",
        "frn_tx_situacao",
        "vot_id_votacao",
        "vot_tx_descricao",
        "vot_dt_votacao",
        "vot_dh_votacao",
        "vot_nr_ano",
        "vot_nr_mes",
        "leg_id_legislatura",
        "org_tx_sigla",
        "prop_id_proposicao",
        "vot_tx_resultado_curado",
        "vot_fl_aprovada",
        "cfv_qt_membros_frente",
        "cfv_qt_partidos_frente",
        "cfv_qt_ufs_frente",
        "cfv_qt_membros_votantes",
        "cfv_qt_votos_registrados",
        "cfv_qt_votos_sim",
        "cfv_qt_votos_nao",
        "cfv_qt_abstencoes",
        "cfv_qt_obstrucoes",
        "cfv_qt_votos_artigo_17",
        "cfv_qt_partidos_votantes",
        "cfv_qt_ufs_votantes",
        "cfv_tx_voto_predominante",
        "cfv_qt_membros_voto_predominante",
        "cfv_vl_pct_participacao_frente",
        "cfv_vl_pct_voto_predominante",
        "cfv_vl_pct_dimensoes_completas",
        "cfv_nr_rank_engajamento",
        "cfv_nr_rank_coesao",
        "cfv_fl_voto_predominante_identificado",
        "cfv_fl_alinhado_resultado_votacao",
        "cfv_fl_alta_coesao",
        "cfv_fl_alta_participacao",
        "cfv_fl_dimensoes_principais_completas",
        "cfv_fl_registro_valido_marts",
        "cfv_qt_frente_encontrada_gold",
        "cfv_qt_votacao_encontrada_gold",
        "cfv_qt_deputado_encontrado_gold",
        "cfv_qt_partido_encontrado_gold",
        "cfv_qt_estado_encontrado_gold",
        "cfv_qt_data_encontrada_gold",
        "cfv_qt_dimensoes_completas",
        "aud_id_execucao_marts",
        "aud_dh_processamento_marts",
        "aud_tx_versao_pipeline_marts"
    )
    .withColumn(
        "aud_tx_hash_registro_marts",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("cfv_sk_correlacao_frente_votacao"), F.lit("")),
                F.coalesce(F.col("frn_id_frente"), F.lit("")),
                F.coalesce(F.col("vot_id_votacao"), F.lit("")),
                F.coalesce(F.col("leg_id_legislatura"), F.lit("")),
                F.coalesce(F.col("cfv_qt_membros_frente").cast("string"), F.lit("")),
                F.coalesce(F.col("cfv_qt_membros_votantes").cast("string"), F.lit("")),
                F.coalesce(F.col("cfv_qt_votos_registrados").cast("string"), F.lit("")),
                F.coalesce(F.col("cfv_qt_votos_sim").cast("string"), F.lit("")),
                F.coalesce(F.col("cfv_qt_votos_nao").cast("string"), F.lit("")),
                F.coalesce(F.col("cfv_qt_abstencoes").cast("string"), F.lit("")),
                F.coalesce(F.col("cfv_qt_obstrucoes").cast("string"), F.lit("")),
                F.coalesce(F.col("cfv_qt_votos_artigo_17").cast("string"), F.lit("")),
                F.coalesce(F.col("cfv_tx_voto_predominante"), F.lit(""))
            ),
            256
        )
    )
)

# COMMAND ----------

# ============================================================
# QUALITY VALIDATIONS
# ============================================================

records_eligible_front_members = front_members_valid_df.count()
records_eligible_voting_results = voting_results_valid_df.count()
records_written = final_df.count()

if records_written == 0:
    raise ValueError("Marts validation failed: target dataframe is empty.")

mandatory_invalid_count = (
    final_df
    .filter(
        F.col("cfv_sk_correlacao_frente_votacao").isNull() |
        F.col("frn_id_frente").isNull() |
        F.col("vot_id_votacao").isNull() |
        F.col("leg_id_legislatura").isNull()
    )
    .count()
)

if mandatory_invalid_count > 0:
    raise ValueError(f"Marts validation failed: {mandatory_invalid_count} records contain null mandatory keys.")

duplicate_count = (
    final_df
    .groupBy("frn_id_frente", "vot_id_votacao", "leg_id_legislatura")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicate_count > 0:
    raise ValueError(f"Marts validation failed: {duplicate_count} duplicated business keys found.")

invalid_record_count = final_df.filter(F.col("cfv_fl_registro_valido_marts") != F.lit(True)).count()

if invalid_record_count > 0:
    raise ValueError(f"Marts validation failed: {invalid_record_count} invalid mart records found.")

negative_metrics_count = (
    final_df
    .filter(
        (F.col("cfv_qt_membros_frente") < 0) |
        (F.col("cfv_qt_partidos_frente") < 0) |
        (F.col("cfv_qt_ufs_frente") < 0) |
        (F.col("cfv_qt_membros_votantes") < 0) |
        (F.col("cfv_qt_votos_registrados") < 0) |
        (F.col("cfv_qt_votos_sim") < 0) |
        (F.col("cfv_qt_votos_nao") < 0) |
        (F.col("cfv_qt_abstencoes") < 0) |
        (F.col("cfv_qt_obstrucoes") < 0) |
        (F.col("cfv_qt_votos_artigo_17") < 0)
    )
    .count()
)

if negative_metrics_count > 0:
    raise ValueError(f"Marts validation failed: {negative_metrics_count} records contain negative metrics.")

percentage_invalid_count = (
    final_df
    .filter(
        (F.col("cfv_vl_pct_participacao_frente") < 0) |
        (F.col("cfv_vl_pct_participacao_frente") > 100) |
        (F.col("cfv_vl_pct_voto_predominante") < 0) |
        (F.col("cfv_vl_pct_voto_predominante") > 100) |
        (F.col("cfv_vl_pct_dimensoes_completas") < 0) |
        (F.col("cfv_vl_pct_dimensoes_completas") > 100)
    )
    .count()
)

if percentage_invalid_count > 0:
    raise ValueError(f"Marts validation failed: {percentage_invalid_count} records contain invalid percentages.")

vote_consistency_count = (
    final_df
    .filter(
        F.col("cfv_qt_votos_registrados") != (
            F.col("cfv_qt_votos_sim") +
            F.col("cfv_qt_votos_nao") +
            F.col("cfv_qt_abstencoes") +
            F.col("cfv_qt_obstrucoes") +
            F.col("cfv_qt_votos_artigo_17")
        )
    )
    .count()
)

if vote_consistency_count > 0:
    raise ValueError(f"Marts validation failed: {vote_consistency_count} records contain inconsistent vote composition.")

participation_invalid_count = final_df.filter(F.col("cfv_qt_membros_votantes") > F.col("cfv_qt_membros_frente")).count()

if participation_invalid_count > 0:
    raise ValueError(f"Marts validation failed: {participation_invalid_count} records contain participation greater than Front members.")

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
# EXPORT CSV DELIVERY FILE
# ============================================================

_tmp_csv_dir = f"{CSV_EXPORT_DIR}/_tmp_csv_export"
export_single_csv(
    dataframe=final_df,
    temporary_path=_tmp_csv_dir,
    final_file_path=CSV_EXPORT_FILE
)

# COMMAND ----------

# ============================================================
# GOVERNANCE COMMENTS
# ============================================================

spark.sql(
    f"COMMENT ON TABLE {TARGET_TABLE} IS 'Business Mart for Parliamentary Front and voting correlation analysis, including participation, cohesion, predominant vote, Article 17 vote metric, alignment and governance indicators.'"
)

column_comments = {
    "cfv_sk_correlacao_frente_votacao": "Marts surrogate key for the Parliamentary Front and voting correlation record.",
    "frn_sk_frente": "Gold surrogate key of the Parliamentary Front dimension.",
    "vot_sk_votacao": "Gold surrogate key of the voting dimension.",
    "frn_id_frente": "Business identifier of the Parliamentary Front.",
    "frn_tx_titulo": "Standardized title of the Parliamentary Front.",
    "frn_tx_situacao": "Current situation of the Parliamentary Front when available.",
    "vot_id_votacao": "Business identifier of the legislative voting event.",
    "vot_tx_descricao": "Voting description from the Gold voting dimension or fact.",
    "vot_dt_votacao": "Voting date.",
    "vot_dh_votacao": "Voting timestamp.",
    "vot_nr_ano": "Voting year.",
    "vot_nr_mes": "Voting month number.",
    "leg_id_legislatura": "Legislature identifier used to correlate Front membership and voting behavior.",
    "org_tx_sigla": "Legislative body acronym associated with the voting event when available.",
    "prop_id_proposicao": "Proposition identifier associated with the voting event when available.",
    "vot_tx_resultado_curado": "Curated final voting result.",
    "vot_fl_aprovada": "Flag indicating whether the voting event was approved when available.",
    "cfv_qt_membros_frente": "Number of distinct members associated with the Parliamentary Front in the legislature.",
    "cfv_qt_partidos_frente": "Number of distinct parties represented in the Parliamentary Front.",
    "cfv_qt_ufs_frente": "Number of distinct federation units represented in the Parliamentary Front.",
    "cfv_qt_membros_votantes": "Number of distinct Front members with recorded votes in the voting event.",
    "cfv_qt_votos_registrados": "Total number of recorded votes from Front members in the voting event.",
    "cfv_qt_votos_sim": "Total number of YES votes from Front members.",
    "cfv_qt_votos_nao": "Total number of NO votes from Front members.",
    "cfv_qt_abstencoes": "Total number of abstentions from Front members.",
    "cfv_qt_obstrucoes": "Total number of obstruction votes from Front members.",
    "cfv_qt_votos_artigo_17": "Total number of Article 17 votes from Front members in the voting event.",
    "cfv_qt_partidos_votantes": "Number of distinct parties among Front members who voted.",
    "cfv_qt_ufs_votantes": "Number of distinct federation units among Front members who voted.",
    "cfv_tx_voto_predominante": "Predominant curated vote among Front members for the voting event.",
    "cfv_qt_membros_voto_predominante": "Number of Front members represented by the predominant vote.",
    "cfv_vl_pct_participacao_frente": "Percentage of Front members that participated in the voting event.",
    "cfv_vl_pct_voto_predominante": "Percentage of Front voters represented by the predominant vote.",
    "cfv_vl_pct_dimensoes_completas": "Percentage of correlated vote records with complete main Gold dimensional coverage.",
    "cfv_nr_rank_engajamento": "Engagement ranking based on the number of Front members who voted.",
    "cfv_nr_rank_coesao": "Cohesion ranking based on the concentration of the predominant vote.",
    "cfv_fl_voto_predominante_identificado": "Flag indicating whether a predominant vote was identified.",
    "cfv_fl_alinhado_resultado_votacao": "Flag indicating whether the Front predominant vote aligns with the final voting result when comparable.",
    "cfv_fl_alta_coesao": "Flag indicating whether the predominant vote represents at least 70 percent of Front voters.",
    "cfv_fl_alta_participacao": "Flag indicating whether at least 50 percent of Front members voted.",
    "cfv_fl_dimensoes_principais_completas": "Flag indicating whether Front, voting and deputy dimensions were found in Gold.",
    "cfv_fl_registro_valido_marts": "Flag indicating whether the mart record passed Marts validation.",
    "cfv_qt_frente_encontrada_gold": "Number of correlated records with Front dimension found in Gold.",
    "cfv_qt_votacao_encontrada_gold": "Number of correlated records with voting dimension found in Gold.",
    "cfv_qt_deputado_encontrado_gold": "Number of correlated records with deputy dimension found in Gold.",
    "cfv_qt_partido_encontrado_gold": "Number of correlated records with party dimension found in Gold.",
    "cfv_qt_estado_encontrado_gold": "Number of correlated records with state dimension found in Gold.",
    "cfv_qt_data_encontrada_gold": "Number of correlated records with date dimension found in Gold.",
    "cfv_qt_dimensoes_completas": "Number of correlated records with all main dimensions complete.",
    "aud_id_execucao_marts": "Execution identifier generated during Marts processing.",
    "aud_dh_processamento_marts": "Timestamp when the record was processed in Marts.",
    "aud_tx_versao_pipeline_marts": "Pipeline version used during Marts processing.",
    "aud_tx_hash_registro_marts": "Deterministic Marts record hash."
}

existing_columns = set(spark.table(TARGET_TABLE).columns)

for column_name, comment_text in column_comments.items():
    if column_name in existing_columns:
        safe_comment = comment_text.replace("'", "''")
        spark.sql(f"ALTER TABLE {TARGET_TABLE} ALTER COLUMN {column_name} COMMENT '{safe_comment}'")
        print(f"[SUCCESS] Column comment applied: {TARGET_TABLE}.{column_name}")

# COMMAND ----------

# ============================================================
# PIPELINE AUDIT LOG
# ============================================================

FINISHED_AT = datetime.now()
duration_seconds = (FINISHED_AT - STARTED_AT).total_seconds()

try:
    write_pipeline_log(
        log_id=PIPELINE_LOG_ID,
        execution_id=EXECUTION_ID,
        notebook_name=NOTEBOOK_NAME,
        layer_name="marts",
        entity_name=ENTITY_NAME,
        target_table=TARGET_TABLE,
        status="SUCCESS",
        message="Parliamentary Front and voting correlation mart generated successfully.",
        started_at=STARTED_AT,
        finished_at=FINISHED_AT,
        duration_seconds=duration_seconds,
        records_read=records_read,
        records_written=records_written
    )
except Exception as error:
    log_info(logger, f"Pipeline log was not written. Details: {str(error)}")

# COMMAND ----------

# ============================================================
# EXECUTION SUMMARY
# ============================================================

print("=" * 90)
print("MART CORRELACAO FRENTES VOTACOES - RESUMO EXECUCAO")
print("=" * 90)
print(f"Records read from Gold front membership fact: {records_read_front_members}")
print(f"Records read from Gold voting result fact: {records_read_voting_results}")
print(f"Records eligible from Gold front membership fact: {records_eligible_front_members}")
print(f"Records eligible from Gold voting result fact: {records_eligible_voting_results}")
print(f"Records written to Marts: {records_written}")
print(f"CSV export path: {CSV_EXPORT_FILE}")
print("STATUS: SUCCESS")
print("=" * 90)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Query — Vote Consistency Including Article 17
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     SUM(cfv_qt_votos_registrados) AS total_registrados,
# MAGIC     SUM(cfv_qt_votos_sim) AS total_sim,
# MAGIC     SUM(cfv_qt_votos_nao) AS total_nao,
# MAGIC     SUM(cfv_qt_abstencoes) AS total_abstencoes,
# MAGIC     SUM(cfv_qt_obstrucoes) AS total_obstrucoes,
# MAGIC     SUM(cfv_qt_votos_artigo_17) AS total_artigo_17,
# MAGIC     SUM(
# MAGIC         cfv_qt_votos_registrados
# MAGIC       - (
# MAGIC             cfv_qt_votos_sim
# MAGIC           + cfv_qt_votos_nao
# MAGIC           + cfv_qt_abstencoes
# MAGIC           + cfv_qt_obstrucoes
# MAGIC           + cfv_qt_votos_artigo_17
# MAGIC         )
# MAGIC     ) AS diferenca
# MAGIC FROM brazil_legislative_analytics.marts.am_correlacao_frentes_votacoes;
# MAGIC ```
