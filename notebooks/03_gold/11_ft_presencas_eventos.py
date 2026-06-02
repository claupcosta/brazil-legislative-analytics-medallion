# Databricks notebook source
# MAGIC %md
# MAGIC # 11 Gold — Event Attendance Fact
# MAGIC
# MAGIC **Notebook:** `11_ft_presencas_eventos`
# MAGIC
# MAGIC Builds the curated Gold event attendance fact used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Event attendance fact model
# MAGIC * Event attendance surrogate key generation
# MAGIC * Event, deputy, party, state and date dimensional keys
# MAGIC * Attendance analytical indicators
# MAGIC * Audit and traceability attributes
# MAGIC * Gold governance metadata
# MAGIC * Column and table comments
# MAGIC * Gold validation rules
# MAGIC * Gold execution logging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC * Read validated event attendance records from Silver
# MAGIC * Keep one analytical record per legislative event, deputy and event year
# MAGIC * Create the event attendance surrogate key
# MAGIC * Resolve Gold dimension keys for events, deputies, parties, states and dates
# MAGIC * Preserve business identifiers and descriptive attributes
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold event attendance fact
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Fact Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per legislative event, deputy and event year attendance relationship.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_presencas_eventos`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.ft_presencas_eventos`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `evt_id_evento`, `dep_id_deputado`, `pev_nr_ano_evento`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `fpe_sk_presenca_evento`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC Rule 1:
# MAGIC
# MAGIC Only Silver approved records are eligible for Gold.
# MAGIC
# MAGIC Rule 2:
# MAGIC
# MAGIC One analytical record per legislative event, deputy and event year.
# MAGIC
# MAGIC Rule 3:
# MAGIC
# MAGIC Preserve governance and lineage information.
# MAGIC
# MAGIC Rule 4:
# MAGIC
# MAGIC All Gold objects must contain governance comments.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Controls
# MAGIC
# MAGIC Validates:
# MAGIC
# MAGIC * Null surrogate keys
# MAGIC * Null event business keys
# MAGIC * Null deputy business keys
# MAGIC * Null attendance flag
# MAGIC * Duplicate event attendance relationships
# MAGIC * Invalid Gold records
# MAGIC
# MAGIC Execution is interrupted when critical validations fail.

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

NOTEBOOK_NAME = "11_ft_presencas_eventos"

ENTITY_NAME = "presencas_eventos"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_presencas_eventos"

TARGET_TABLE = f"{GOLD_SCHEMA}.ft_presencas_eventos"

DM_EVENTOS_TABLE = f"{GOLD_SCHEMA}.dm_eventos"
DM_DEPUTADOS_TABLE = f"{GOLD_SCHEMA}.dm_deputados"
DM_PARTIDOS_TABLE = f"{GOLD_SCHEMA}.dm_partidos"
DM_ESTADOS_TABLE = f"{GOLD_SCHEMA}.dm_estados"
DM_DATAS_TABLE = f"{GOLD_SCHEMA}.dm_datas"

EXECUTION_ID = str(uuid.uuid4())

STARTED_AT = datetime.now()

PIPELINE_LOG_ID = str(uuid.uuid4())

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name="gold"
)

log_info(
    logger,
    f"Starting notebook {NOTEBOOK_NAME}"
)

# COMMAND ----------

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def first_existing_column(dataframe, candidate_columns):
    """
    Returns the first existing column name from a candidate list.
    """

    for column_name in candidate_columns:
        if column_name in dataframe.columns:
            return column_name

    return None


def source_column(dataframe, candidate_columns, alias_name, target_type="string"):
    """
    Returns the first available source column or a typed null literal.
    """

    column_name = first_existing_column(
        dataframe=dataframe,
        candidate_columns=candidate_columns
    )

    if column_name:
        return F.col(column_name).cast(target_type).alias(alias_name)

    return F.lit(None).cast(target_type).alias(alias_name)


# COMMAND ----------

# ============================================================
# READ SILVER
# ============================================================

df_silver = spark.table(SOURCE_TABLE)

records_read = df_silver.count()

log_info(
    logger,
    f"Records read from Silver: {records_read}"
)

# COMMAND ----------

# ============================================================
# BUSINESS RULES
# ============================================================

df_silver_valid = (
    df_silver
    .filter(
        F.col("pev_fl_registro_valido_silver") == True
    )
)

records_valid_silver = df_silver_valid.count()

log_info(
    logger,
    f"Records eligible from Silver: {records_valid_silver}"
)

# COMMAND ----------

# ============================================================
# STANDARDIZE FACT SOURCE COLUMNS
# ============================================================

source_standardized_df = (
    df_silver_valid
    .select(
        source_column(
            df_silver_valid,
            ["pev_id_presenca_evento"],
            "pev_id_presenca_evento",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["pev_tx_chave_deduplicacao"],
            "pev_tx_chave_deduplicacao",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["evt_id_evento"],
            "evt_id_evento",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["evt_tx_uri"],
            "evt_tx_uri",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["evt_dh_inicio"],
            "evt_dh_inicio",
            "timestamp"
        ),
        source_column(
            df_silver_valid,
            ["evt_dt_inicio"],
            "evt_dt_inicio",
            "date"
        ),
        source_column(
            df_silver_valid,
            ["evt_nr_ano", "pev_nr_ano_evento"],
            "evt_nr_ano",
            "int"
        ),
        source_column(
            df_silver_valid,
            ["evt_nr_mes", "pev_nr_mes_evento"],
            "evt_nr_mes",
            "int"
        ),
        source_column(
            df_silver_valid,
            ["evt_tx_titulo"],
            "evt_tx_titulo",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["evt_tx_tipo_evento"],
            "evt_tx_tipo_evento",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["evt_tx_situacao_evento", "evt_tx_situacao"],
            "evt_tx_situacao",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["evt_tx_local_camara", "evt_tx_local"],
            "evt_tx_local",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["evt_tx_sigla_orgao"],
            "evt_tx_sigla_orgao",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["evt_tx_nome_orgao"],
            "evt_tx_nome_orgao",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["leg_id_legislatura_evento"],
            "leg_id_legislatura_evento",
            "int"
        ),
        source_column(
            df_silver_valid,
            ["dep_id_deputado"],
            "dep_id_deputado",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["dep_tx_uri"],
            "dep_tx_uri",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["dep_tx_nome"],
            "dep_tx_nome",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["dep_tx_nome_civil"],
            "dep_tx_nome_civil",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["dep_tx_sigla_partido"],
            "dep_tx_sigla_partido",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["part_tx_uri"],
            "part_tx_uri",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["dep_tx_sigla_uf"],
            "dep_tx_sigla_uf",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["leg_id_legislatura_deputado"],
            "leg_id_legislatura_deputado",
            "int"
        ),
        source_column(
            df_silver_valid,
            ["dep_tx_url_foto"],
            "dep_tx_url_foto",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["pev_fl_presenca"],
            "pev_fl_presenca",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            ["pev_fl_presenca_origem"],
            "pev_fl_presenca_origem",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["pev_nr_ano_evento"],
            "pev_nr_ano_evento",
            "int"
        ),
        source_column(
            df_silver_valid,
            ["pev_nr_mes_evento"],
            "pev_nr_mes_evento",
            "int"
        ),
        source_column(
            df_silver_valid,
            ["pev_nr_ano_arquivo"],
            "pev_nr_ano_arquivo",
            "int"
        ),
        source_column(
            df_silver_valid,
            ["pev_fl_evento_encontrado_silver"],
            "pev_fl_evento_encontrado_silver",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            ["pev_fl_deputado_encontrado_silver"],
            "pev_fl_deputado_encontrado_silver",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            ["pev_fl_evento_valido_silver"],
            "pev_fl_evento_valido_silver",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            ["pev_fl_deputado_valido_silver"],
            "pev_fl_deputado_valido_silver",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            ["pev_fl_dimensoes_completas"],
            "pev_fl_dimensoes_completas_silver",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            ["pev_fl_registro_valido_silver"],
            "pev_fl_registro_valido_silver",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            ["pev_tx_payload_json"],
            "pev_tx_payload_json",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_id_execucao_bronze"],
            "aud_id_execucao_bronze",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_dh_ingestao_bronze"],
            "aud_dh_ingestao_bronze",
            "timestamp"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_endpoint_origem_bronze"],
            "aud_tx_endpoint_origem_bronze",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_sistema_origem_bronze"],
            "aud_tx_sistema_origem_bronze",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_versao_pipeline_bronze"],
            "aud_tx_versao_pipeline_bronze",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_tipo_carga_bronze"],
            "aud_tx_tipo_carga_bronze",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_arquivo_origem_bronze"],
            "aud_tx_arquivo_origem_bronze",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_hash_registro_bronze"],
            "aud_tx_hash_registro_bronze",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_id_execucao_silver"],
            "aud_id_execucao_silver",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_dh_processamento"],
            "aud_dh_processamento",
            "timestamp"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_camada_origem"],
            "aud_tx_camada_origem",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_tabela_origem"],
            "aud_tx_tabela_origem",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_tabela_destino"],
            "aud_tx_tabela_destino",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_versao_pipeline_silver"],
            "aud_tx_versao_pipeline_silver",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_regra_extracao_presenca_evento"],
            "aud_tx_regra_extracao_presenca_evento",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_hash_registro_silver"],
            "aud_tx_hash_registro_silver",
            "string"
        ),
    )
    .withColumn(
        "dep_tx_sigla_partido",
        F.upper(F.trim(F.col("dep_tx_sigla_partido")))
    )
    .withColumn(
        "dep_tx_sigla_uf",
        F.upper(F.trim(F.col("dep_tx_sigla_uf")))
    )
    .withColumn(
        "dat_id_data",
        F.date_format(F.col("evt_dt_inicio"), "yyyyMMdd")
    )
)

# COMMAND ----------

# ============================================================
# DEDUPLICATE FACT GRAIN
# ============================================================

fact_business_key_columns = [
    "evt_id_evento",
    "dep_id_deputado",
    "pev_nr_ano_evento",
]

source_dedup_df = (
    source_standardized_df
    .withColumn(
        "fpe_tx_business_key",
        F.concat_ws(
            "||",
            F.coalesce(F.col("evt_id_evento"), F.lit("NA")),
            F.coalesce(F.col("dep_id_deputado"), F.lit("NA")),
            F.coalesce(F.col("pev_nr_ano_evento").cast("string"), F.lit("NA")),
        )
    )
    .withColumn(
        "fpe_nr_rank_deduplicacao",
        F.row_number().over(
            Window
            .partitionBy(*fact_business_key_columns)
            .orderBy(
                F.col("aud_dh_processamento").desc_nulls_last(),
                F.col("aud_dh_ingestao_bronze").desc_nulls_last(),
                F.col("aud_tx_hash_registro_bronze").asc_nulls_last(),
            )
        )
    )
    .filter(
        F.col("fpe_nr_rank_deduplicacao") == 1
    )
    .drop("fpe_nr_rank_deduplicacao")
)

# COMMAND ----------

# ============================================================
# READ GOLD DIMENSIONS
# ============================================================

dm_eventos_df = (
    spark.table(DM_EVENTOS_TABLE)
    .select(
        F.col("evt_id_evento").alias("dim_evt_id_evento"),
        F.col("evt_sk_evento"),
        F.col("evt_tx_titulo").alias("dim_evt_tx_titulo"),
        F.col("evt_tx_tipo_evento").alias("dim_evt_tx_tipo_evento"),
        F.col("evt_tx_sigla_orgao").alias("dim_evt_tx_sigla_orgao"),
        F.col("evt_tx_nome_orgao").alias("dim_evt_tx_nome_orgao"),
        F.col("leg_id_legislatura").cast("int").alias("dim_evt_leg_id_legislatura"),
    )
    .dropDuplicates([
        "dim_evt_id_evento"
    ])
)

dm_deputados_df = (
    spark.table(DM_DEPUTADOS_TABLE)
    .select(
        F.col("dep_id_deputado").alias("dim_dep_id_deputado"),
        F.col("dep_id_legislatura").cast("int").alias("dim_dep_id_legislatura"),
        F.col("dep_sk_deputado"),
        F.col("dep_tx_nome").alias("dim_dep_tx_nome"),
        F.col("dep_tx_sigla_partido").alias("dim_dep_tx_sigla_partido"),
        F.col("dep_tx_sigla_uf").alias("dim_dep_tx_sigla_uf"),
        F.col("dep_fl_legislatura_mais_recente").alias("dim_dep_fl_legislatura_mais_recente"),
    )
)

dm_deputados_exact_df = (
    dm_deputados_df
    .filter(
        F.col("dim_dep_id_legislatura").isNotNull()
    )
    .dropDuplicates([
        "dim_dep_id_deputado",
        "dim_dep_id_legislatura",
    ])
)

dm_deputados_latest_df = (
    dm_deputados_df
    .filter(
        F.col("dim_dep_fl_legislatura_mais_recente") == True
    )
    .dropDuplicates([
        "dim_dep_id_deputado"
    ])
    .select(
        F.col("dim_dep_id_deputado").alias("latest_dep_id_deputado"),
        F.col("dep_sk_deputado").alias("latest_dep_sk_deputado"),
        F.col("dim_dep_tx_nome").alias("latest_dep_tx_nome"),
        F.col("dim_dep_tx_sigla_partido").alias("latest_dep_tx_sigla_partido"),
        F.col("dim_dep_tx_sigla_uf").alias("latest_dep_tx_sigla_uf"),
    )
)

dm_partidos_df = (
    spark.table(DM_PARTIDOS_TABLE)
    .select(
        F.col("par_tx_sigla").alias("dim_par_tx_sigla"),
        F.col("par_sk_partido"),
    )
    .dropDuplicates([
        "dim_par_tx_sigla"
    ])
)

dm_estados_df = (
    spark.table(DM_ESTADOS_TABLE)
    .select(
        F.col("est_tx_sigla_uf").alias("dim_est_tx_sigla_uf"),
        F.col("est_sk_estado"),
    )
    .dropDuplicates([
        "dim_est_tx_sigla_uf"
    ])
)

dm_datas_df = (
    spark.table(DM_DATAS_TABLE)
    .select(
        F.col("dat_id_data").alias("dim_dat_id_data"),
        F.col("dat_sk_data"),
    )
    .dropDuplicates([
        "dim_dat_id_data"
    ])
)

# COMMAND ----------

# ============================================================
# ENRICH FACT WITH DIMENSION KEYS
# ============================================================

fact_enriched_df = (
    source_dedup_df
    .join(
        dm_eventos_df,
        F.trim(source_dedup_df["evt_id_evento"].cast("string")) ==
        F.trim(dm_eventos_df["dim_evt_id_evento"].cast("string")),
        "left",
    )
    .join(
        dm_deputados_exact_df,
        (
            F.trim(source_dedup_df["dep_id_deputado"].cast("string")) ==
            F.trim(dm_deputados_exact_df["dim_dep_id_deputado"].cast("string"))
        )
        & (
            F.coalesce(
                F.col("leg_id_legislatura_deputado"),
                F.col("leg_id_legislatura_evento"),
                F.col("dim_evt_leg_id_legislatura"),
            ) == dm_deputados_exact_df["dim_dep_id_legislatura"]
        ),
        "left",
    )
    .join(
        dm_deputados_latest_df,
        F.trim(source_dedup_df["dep_id_deputado"].cast("string")) ==
        F.trim(dm_deputados_latest_df["latest_dep_id_deputado"].cast("string")),
        "left",
    )
    .withColumn(
        "dep_sk_deputado_final",
        F.coalesce(
            F.col("dep_sk_deputado"),
            F.col("latest_dep_sk_deputado"),
        )
    )
    .withColumn(
        "dep_tx_nome_final",
        F.coalesce(
            F.col("dep_tx_nome"),
            F.col("dim_dep_tx_nome"),
            F.col("latest_dep_tx_nome"),
        )
    )
    .withColumn(
        "dep_tx_sigla_partido_final",
        F.coalesce(
            F.col("dep_tx_sigla_partido"),
            F.col("dim_dep_tx_sigla_partido"),
            F.col("latest_dep_tx_sigla_partido"),
        )
    )
    .withColumn(
        "dep_tx_sigla_uf_final",
        F.coalesce(
            F.col("dep_tx_sigla_uf"),
            F.col("dim_dep_tx_sigla_uf"),
            F.col("latest_dep_tx_sigla_uf"),
        )
    )
    .join(
        dm_partidos_df,
        F.col("dep_tx_sigla_partido_final") == dm_partidos_df["dim_par_tx_sigla"],
        "left",
    )
    .join(
        dm_estados_df,
        F.col("dep_tx_sigla_uf_final") == dm_estados_df["dim_est_tx_sigla_uf"],
        "left",
    )
    .join(
        dm_datas_df,
        F.col("dat_id_data") == dm_datas_df["dim_dat_id_data"],
        "left",
    )
)

# COMMAND ----------

# ============================================================
# BUILD GOLD FACT
# ============================================================

df_gold = (
    fact_enriched_df
    .withColumn(
        "leg_id_legislatura_final",
        F.coalesce(
            F.col("leg_id_legislatura_evento"),
            F.col("dim_evt_leg_id_legislatura"),
            F.col("leg_id_legislatura_deputado"),
        ).cast("int")
    )
    .withColumn(
        "evt_tx_titulo_final",
        F.coalesce(
            F.col("evt_tx_titulo"),
            F.col("dim_evt_tx_titulo"),
        )
    )
    .withColumn(
        "evt_tx_tipo_evento_final",
        F.coalesce(
            F.col("evt_tx_tipo_evento"),
            F.col("dim_evt_tx_tipo_evento"),
        )
    )
    .withColumn(
        "evt_tx_sigla_orgao_final",
        F.coalesce(
            F.col("evt_tx_sigla_orgao"),
            F.col("dim_evt_tx_sigla_orgao"),
        )
    )
    .withColumn(
        "evt_tx_nome_orgao_final",
        F.coalesce(
            F.col("evt_tx_nome_orgao"),
            F.col("dim_evt_tx_nome_orgao"),
        )
    )
    .withColumn(
        "fpe_sk_presenca_evento",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("evt_id_evento").cast("string"),
                F.col("dep_id_deputado").cast("string"),
                F.col("pev_nr_ano_evento").cast("string"),
            ),
            256
        )
    )
    .withColumn(
        "fpe_qt_registro_presenca",
        F.lit(1)
    )
    .withColumn(
        "fpe_qt_presenca",
        F.when(F.col("pev_fl_presenca") == True, F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "fpe_qt_ausencia",
        F.when(F.col("pev_fl_presenca") == False, F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "fpe_fl_evento_encontrado_gold",
        F.col("evt_sk_evento").isNotNull()
    )
    .withColumn(
        "fpe_fl_deputado_encontrado_gold",
        F.col("dep_sk_deputado_final").isNotNull()
    )
    .withColumn(
        "fpe_fl_partido_encontrado_gold",
        F.col("par_sk_partido").isNotNull()
    )
    .withColumn(
        "fpe_fl_estado_encontrado_gold",
        F.col("est_sk_estado").isNotNull()
    )
    .withColumn(
        "fpe_fl_data_encontrada_gold",
        F.col("dat_sk_data").isNotNull()
    )
    .withColumn(
        "fpe_fl_dimensoes_principais_completas",
        (
            F.col("evt_sk_evento").isNotNull()
            & F.col("dep_sk_deputado_final").isNotNull()
            & F.col("dat_sk_data").isNotNull()
        )
    )
    .withColumn(
        "fpe_fl_registro_valido_gold",
        (
            F.col("evt_id_evento").isNotNull()
            & F.col("dep_id_deputado").isNotNull()
            & F.col("pev_fl_presenca").isNotNull()
            & F.col("fpe_sk_presenca_evento").isNotNull()
        )
    )
    .withColumn(
        "aud_id_execucao_gold",
        F.lit(EXECUTION_ID)
    )
    .withColumn(
        "aud_dh_processamento_gold",
        F.current_timestamp()
    )
    .withColumn(
        "aud_tx_versao_pipeline_gold",
        F.lit(PROJECT_VERSION)
    )
    .withColumn(
        "aud_tx_hash_registro_gold",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("fpe_sk_presenca_evento").cast("string"),
                F.col("evt_sk_evento").cast("string"),
                F.col("dep_sk_deputado_final").cast("string"),
                F.col("dat_sk_data").cast("string"),
                F.col("pev_fl_presenca").cast("string"),
            ),
            256
        )
    )
    .select(
        "fpe_sk_presenca_evento",
        "evt_sk_evento",
        F.col("dep_sk_deputado_final").alias("dep_sk_deputado"),
        "par_sk_partido",
        "est_sk_estado",
        "dat_sk_data",
        "pev_id_presenca_evento",
        "pev_tx_chave_deduplicacao",
        "evt_id_evento",
        "evt_tx_uri",
        "evt_dh_inicio",
        "evt_dt_inicio",
        "evt_nr_ano",
        "evt_nr_mes",
        F.col("evt_tx_titulo_final").alias("evt_tx_titulo"),
        F.col("evt_tx_tipo_evento_final").alias("evt_tx_tipo_evento"),
        "evt_tx_situacao",
        "evt_tx_local",
        F.col("evt_tx_sigla_orgao_final").alias("evt_tx_sigla_orgao"),
        F.col("evt_tx_nome_orgao_final").alias("evt_tx_nome_orgao"),
        F.col("leg_id_legislatura_final").alias("leg_id_legislatura"),
        "dep_id_deputado",
        "dep_tx_uri",
        F.col("dep_tx_nome_final").alias("dep_tx_nome"),
        "dep_tx_nome_civil",
        F.col("dep_tx_sigla_partido_final").alias("dep_tx_sigla_partido"),
        "part_tx_uri",
        F.col("dep_tx_sigla_uf_final").alias("dep_tx_sigla_uf"),
        "leg_id_legislatura_deputado",
        "dep_tx_url_foto",
        "pev_fl_presenca",
        "pev_fl_presenca_origem",
        "pev_nr_ano_evento",
        "pev_nr_mes_evento",
        "pev_nr_ano_arquivo",
        "fpe_qt_registro_presenca",
        "fpe_qt_presenca",
        "fpe_qt_ausencia",
        "fpe_fl_evento_encontrado_gold",
        "fpe_fl_deputado_encontrado_gold",
        "fpe_fl_partido_encontrado_gold",
        "fpe_fl_estado_encontrado_gold",
        "fpe_fl_data_encontrada_gold",
        "fpe_fl_dimensoes_principais_completas",
        "pev_fl_evento_encontrado_silver",
        "pev_fl_deputado_encontrado_silver",
        "pev_fl_evento_valido_silver",
        "pev_fl_deputado_valido_silver",
        "pev_fl_dimensoes_completas_silver",
        "pev_fl_registro_valido_silver",
        "fpe_fl_registro_valido_gold",
        "pev_tx_payload_json",
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
        "aud_tx_regra_extracao_presenca_evento",
        "aud_tx_hash_registro_silver",
        "aud_id_execucao_gold",
        "aud_dh_processamento_gold",
        "aud_tx_versao_pipeline_gold",
        "aud_tx_hash_registro_gold",
    )
)

# COMMAND ----------

# ============================================================
# QUALITY VALIDATIONS
# ============================================================

required_columns_result = validate_required_columns(
    dataframe=df_gold,
    required_columns=[
        "fpe_sk_presenca_evento",
        "evt_id_evento",
        "dep_id_deputado",
        "pev_fl_presenca",
        "fpe_fl_registro_valido_gold",
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_gold,
    key_columns=[
        "evt_id_evento",
        "dep_id_deputado",
        "pev_nr_ano_evento",
    ]
)

null_results = validate_nulls(
    dataframe=df_gold,
    columns=[
        "fpe_sk_presenca_evento",
        "evt_id_evento",
        "dep_id_deputado",
        "pev_fl_presenca",
    ]
)

quality_results = [
    required_columns_result,
    duplicate_result
]

quality_results.extend(
    null_results
)

quality_df = build_quality_log(
    quality_results=quality_results,
    execution_id=EXECUTION_ID,
    notebook_name=NOTEBOOK_NAME,
    layer_name="gold",
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE
)

write_quality_log(
    quality_dataframe=quality_df
)

# COMMAND ----------

# ============================================================
# WRITE GOLD TABLE
# ============================================================

(
    df_gold
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

records_written = df_gold.count()

log_success(
    logger,
    f"Records written to Gold: {records_written}"
)

# COMMAND ----------

# ============================================================
# GOVERNANCE COMMENTS
# ============================================================

TABLE_COMMENT = """
Gold event attendance fact.

This fact contains one record per legislative event, deputy and event year attendance relationship.

Main characteristics:

* event attendance surrogate key
* event dimension key
* deputy dimension key
* party dimension key
* state dimension key
* date dimension key
* attendance and absence analytical measures
* Silver lineage
* Gold lineage
* governance metadata
"""

COLUMN_COMMENTS = {
    "fpe_sk_presenca_evento":
        "Gold surrogate key for the event attendance fact relationship.",

    "evt_sk_evento":
        "Gold surrogate key of the legislative event dimension.",

    "dep_sk_deputado":
        "Gold surrogate key of the deputy dimension.",

    "par_sk_partido":
        "Gold surrogate key of the political party dimension.",

    "est_sk_estado":
        "Gold surrogate key of the state dimension.",

    "dat_sk_data":
        "Gold surrogate key of the date dimension based on event start date.",

    "pev_id_presenca_evento":
        "Business identifier for the event attendance relationship from Silver.",

    "pev_tx_chave_deduplicacao":
        "Silver technical deduplication key for the event attendance record.",

    "evt_id_evento":
        "Legislative event business identifier.",

    "evt_tx_uri":
        "Legislative event URI from the source system.",

    "evt_dh_inicio":
        "Event start timestamp.",

    "evt_dt_inicio":
        "Event start date.",

    "evt_nr_ano":
        "Event year.",

    "evt_nr_mes":
        "Event month.",

    "evt_tx_titulo":
        "Legislative event title.",

    "evt_tx_tipo_evento":
        "Legislative event type.",

    "evt_tx_situacao":
        "Legislative event status.",

    "evt_tx_local":
        "Legislative event location.",

    "evt_tx_sigla_orgao":
        "Legislative body acronym associated with the event.",

    "evt_tx_nome_orgao":
        "Legislative body name associated with the event.",

    "leg_id_legislatura":
        "Legislature identifier associated with the event attendance relationship.",

    "dep_id_deputado":
        "Deputy business identifier.",

    "dep_tx_uri":
        "Deputy URI from the source system.",

    "dep_tx_nome":
        "Deputy name.",

    "dep_tx_nome_civil":
        "Deputy civil name.",

    "dep_tx_sigla_partido":
        "Deputy political party acronym.",

    "part_tx_uri":
        "Party URI associated with the deputy when available.",

    "dep_tx_sigla_uf":
        "Deputy federative unit acronym.",

    "leg_id_legislatura_deputado":
        "Deputy legislature identifier when available.",

    "dep_tx_url_foto":
        "Deputy photo URL.",

    "pev_fl_presenca":
        "Normalized boolean flag indicating confirmed deputy presence in the event.",

    "pev_fl_presenca_origem":
        "Original source attendance flag value.",

    "pev_nr_ano_evento":
        "Event year used in the attendance fact grain.",

    "pev_nr_mes_evento":
        "Event month used for analytical aggregation.",

    "pev_nr_ano_arquivo":
        "Reference year extracted from the source file during Bronze ingestion.",

    "fpe_qt_registro_presenca":
        "Additive measure equal to one for each event attendance relationship.",

    "fpe_qt_presenca":
        "Additive measure equal to one when the deputy was present.",

    "fpe_qt_ausencia":
        "Additive measure equal to one when the deputy was absent.",

    "fpe_fl_evento_encontrado_gold":
        "Flag indicating whether the related event was found in Gold.",

    "fpe_fl_deputado_encontrado_gold":
        "Flag indicating whether the related deputy was found in Gold.",

    "fpe_fl_partido_encontrado_gold":
        "Flag indicating whether the related party was found in Gold.",

    "fpe_fl_estado_encontrado_gold":
        "Flag indicating whether the related state was found in Gold.",

    "fpe_fl_data_encontrada_gold":
        "Flag indicating whether the related date was found in Gold.",

    "fpe_fl_dimensoes_principais_completas":
        "Flag indicating whether event, deputy and date dimensions were resolved.",

    "pev_fl_evento_encontrado_silver":
        "Flag indicating whether the event was found in Silver.",

    "pev_fl_deputado_encontrado_silver":
        "Flag indicating whether the deputy was found in Silver.",

    "pev_fl_evento_valido_silver":
        "Flag indicating whether the related event was valid in Silver.",

    "pev_fl_deputado_valido_silver":
        "Flag indicating whether the related deputy was valid in Silver.",

    "pev_fl_dimensoes_completas_silver":
        "Flag indicating whether event and deputy enrichment was complete in Silver.",

    "pev_fl_registro_valido_silver":
        "Flag indicating whether the record passed Silver validation.",

    "fpe_fl_registro_valido_gold":
        "Flag indicating whether the record passed Gold validation.",

    "pev_tx_payload_json":
        "Original source payload preserved from Silver when available.",

    "aud_id_execucao_gold":
        "Execution identifier generated during Gold processing.",

    "aud_dh_processamento_gold":
        "Timestamp when the record was processed in Gold.",

    "aud_tx_versao_pipeline_gold":
        "Pipeline version used during Gold processing.",

    "aud_tx_hash_registro_gold":
        "Deterministic Gold record hash.",

    "aud_id_execucao_bronze":
        "Execution identifier generated during Bronze ingestion.",

    "aud_dh_ingestao_bronze":
        "Timestamp when the source record was ingested into Bronze.",

    "aud_tx_endpoint_origem_bronze":
        "Source API endpoint used during Bronze ingestion.",

    "aud_tx_sistema_origem_bronze":
        "Source system used during Bronze ingestion.",

    "aud_tx_versao_pipeline_bronze":
        "Pipeline version used during Bronze processing.",

    "aud_tx_tipo_carga_bronze":
        "Bronze load type used during ingestion.",

    "aud_tx_arquivo_origem_bronze":
        "Source file associated with Bronze ingestion when applicable.",

    "aud_tx_hash_registro_bronze":
        "Deterministic Bronze record hash.",

    "aud_id_execucao_silver":
        "Execution identifier generated during Silver processing.",

    "aud_dh_processamento":
        "Timestamp when the record was processed in Silver.",

    "aud_tx_camada_origem":
        "Source data layer used during Silver processing.",

    "aud_tx_tabela_origem":
        "Source Silver table used to derive event attendance facts.",

    "aud_tx_tabela_destino":
        "Destination Gold table where event attendance facts are persisted.",

    "aud_tx_versao_pipeline_silver":
        "Silver pipeline version used to generate the source record.",

    "aud_tx_regra_extracao_presenca_evento":
        "Business extraction rule applied during Silver attendance processing.",

    "aud_tx_hash_registro_silver":
        "Deterministic Silver record hash."

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
    layer_name="gold",
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status="SUCCESS",
    message="Gold event attendance fact generated successfully.",
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

gold_df = spark.table(TARGET_TABLE)

print("=" * 80)
print("FATO PRESENÇAS EVENTOS - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records eligible from Silver: {records_valid_silver}")
print(f"Records written: {records_written}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))
