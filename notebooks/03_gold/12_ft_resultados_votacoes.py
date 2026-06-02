# Databricks notebook source
# MAGIC %md
# MAGIC  # 12 Gold — Voting Results Fact
# MAGIC
# MAGIC  **Notebook:** `12_ft_resultados_votacoes`
# MAGIC
# MAGIC  Builds the curated Gold voting results fact used by analytical models and business marts.
# MAGIC
# MAGIC  This notebook defines:
# MAGIC
# MAGIC  * Voting result fact model
# MAGIC  * Voting result surrogate key generation
# MAGIC  * Voting, deputy, party, state and date dimensional keys
# MAGIC  * Parliamentary vote analytical indicators
# MAGIC  * Audit and traceability attributes
# MAGIC  * Gold governance metadata
# MAGIC  * Column and table comments
# MAGIC  * Gold validation rules
# MAGIC  * Gold execution logging
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  Responsibilities
# MAGIC
# MAGIC  * Read validated individual voting result records from Silver
# MAGIC  * Keep one analytical record per voting event and deputy vote
# MAGIC  * Create the voting result surrogate key
# MAGIC  * Resolve Gold dimension keys for voting events, deputies, parties, states and dates
# MAGIC  * Preserve business identifiers and descriptive attributes
# MAGIC  * Preserve audit and traceability information
# MAGIC  * Generate Gold execution metadata
# MAGIC  * Apply governance comments
# MAGIC  * Execute Gold quality validations
# MAGIC  * Publish the Gold voting results fact
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  Fact Model
# MAGIC
# MAGIC # Grain
# MAGIC
# MAGIC  One record per legislative voting event and deputy vote.
# MAGIC
# MAGIC # Source
# MAGIC
# MAGIC  `brazil_legislative_analytics.silver.slv_votos`
# MAGIC
# MAGIC # Target
# MAGIC
# MAGIC  `brazil_legislative_analytics.gold.ft_resultados_votacoes`
# MAGIC
# MAGIC # Business Key
# MAGIC
# MAGIC  `vot_id_votacao`, `dep_id_deputado`
# MAGIC
# MAGIC # Surrogate Key
# MAGIC
# MAGIC  `frv_sk_resultado_votacao`
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  Business Rules
# MAGIC
# MAGIC  Rule 1:
# MAGIC
# MAGIC  Only Silver approved records are eligible for Gold when the Silver validation flag is available.
# MAGIC
# MAGIC  Rule 2:
# MAGIC
# MAGIC  One analytical record per voting event and deputy vote.
# MAGIC
# MAGIC  Rule 3:
# MAGIC
# MAGIC  Preserve governance and lineage information.
# MAGIC
# MAGIC  Rule 4:
# MAGIC
# MAGIC  All Gold objects must contain governance comments.
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  Data Quality Controls
# MAGIC
# MAGIC  Validates:
# MAGIC
# MAGIC  * Null surrogate keys
# MAGIC  * Null voting business keys
# MAGIC  * Null deputy business keys
# MAGIC  * Null vote result values
# MAGIC  * Duplicate voting result relationships
# MAGIC  * Invalid Gold records
# MAGIC
# MAGIC  Execution is interrupted when critical validations fail.

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

NOTEBOOK_NAME = "12_ft_resultados_votacoes"

ENTITY_NAME = "resultados_votacoes"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_votos"

TARGET_TABLE = f"{GOLD_SCHEMA}.ft_resultados_votacoes"

DM_VOTACOES_TABLE = f"{GOLD_SCHEMA}.dm_votacoes"
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

silver_valid_flag = first_existing_column(
    dataframe=df_silver,
    candidate_columns=[
        "vto_fl_registro_valido_silver",
        "vot_dep_fl_registro_valido_silver",
        "votd_fl_registro_valido_silver",
        "voto_fl_registro_valido_silver",
        "vot_fl_registro_valido_silver",
        "rv_fl_registro_valido_silver",
    ]
)

if silver_valid_flag:
    df_silver_valid = (
        df_silver
        .filter(
            F.col(silver_valid_flag) == True
        )
    )
else:
    df_silver_valid = df_silver

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
            [
                "vto_id_registro",
                "vto_tx_dedup_key",
                "vot_dep_id_voto",
                "votd_id_voto",
                "voto_id_voto",
                "rv_id_resultado_votacao",
                "vot_tx_chave_deduplicacao",
            ],
            "frv_id_resultado_votacao_origem",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "vot_id_votacao",
                "id_votacao",
            ],
            "vot_id_votacao",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "dep_id_deputado",
                "id_deputado",
                "vot_dep_id_deputado",
                "voto_id_deputado",
            ],
            "dep_id_deputado",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "dep_tx_nome",
                "vot_dep_tx_nome",
                "voto_tx_nome_deputado",
                "nome_deputado",
            ],
            "dep_tx_nome",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "dep_tx_sigla_partido",
                "par_tx_sigla",
                "vot_dep_tx_sigla_partido",
                "sigla_partido",
            ],
            "dep_tx_sigla_partido",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "dep_tx_sigla_uf",
                "est_tx_sigla_uf",
                "vot_dep_tx_sigla_uf",
                "sigla_uf",
            ],
            "dep_tx_sigla_uf",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "leg_id_legislatura",
                "dep_id_legislatura",
                "vot_dep_id_legislatura",
                "id_legislatura",
            ],
            "leg_id_legislatura_deputado",
            "int"
        ),
        source_column(
            df_silver_valid,
            [
                "vto_tx_voto",
                "voto_tx_voto",
                "vot_tx_voto",
                "vot_dep_tx_voto",
                "voto",
                "tipo_voto",
            ],
            "frv_tx_voto",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "vto_tx_voto_curado",
                "voto_tx_voto_curado",
                "vot_tx_voto_curado",
            ],
            "frv_tx_voto_curado_silver",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "voto_tx_orientacao",
                "vot_tx_orientacao",
                "vot_dep_tx_orientacao",
                "orientacao",
            ],
            "frv_tx_orientacao",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "vto_fl_sim",
                "voto_fl_favoravel",
                "vot_fl_favoravel",
                "vot_dep_fl_favoravel",
            ],
            "frv_fl_voto_favoravel",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "vto_fl_nao",
                "voto_fl_contrario",
                "vot_fl_contrario",
                "vot_dep_fl_contrario",
            ],
            "frv_fl_voto_contrario",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "vto_fl_abstencao",
                "voto_fl_abstencao",
                "vot_fl_abstencao",
                "vot_dep_fl_abstencao",
            ],
            "frv_fl_abstencao",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "vto_fl_obstrucao",
                "voto_fl_obstrucao",
                "vot_fl_obstrucao",
                "vot_dep_fl_obstrucao",
            ],
            "frv_fl_obstrucao",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "vot_dt_votacao",
                "voto_dt_votacao",
                "vot_dt_registro_voto",
            ],
            "vot_dt_votacao",
            "date"
        ),
        source_column(
            df_silver_valid,
            [
                "vot_dh_votacao",
                "voto_dh_votacao",
                "vot_dh_registro_voto",
            ],
            "vot_dh_votacao",
            "timestamp"
        ),
        source_column(
            df_silver_valid,
            [
                "vot_nr_ano",
                "voto_nr_ano",
                "vto_nr_ano_referencia",
            ],
            "vot_nr_ano",
            "int"
        ),
        source_column(
            df_silver_valid,
            [
                "vot_nr_mes",
                "voto_nr_mes",
            ],
            "vot_nr_mes",
            "int"
        ),
        source_column(
            df_silver_valid,
            [
                "vot_tx_resultado",
                "voto_tx_resultado",
            ],
            "vot_tx_resultado",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "vot_tx_descricao",
                "voto_tx_descricao",
            ],
            "vot_tx_descricao",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "prop_id_proposicao",
            ],
            "prop_id_proposicao",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "org_id_orgao",
            ],
            "org_id_orgao",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "org_tx_sigla",
            ],
            "org_tx_sigla",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                silver_valid_flag if silver_valid_flag else "__missing__"
            ],
            "frv_fl_registro_valido_silver",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "vto_tx_payload_json",
                "vot_dep_tx_payload_json",
                "vot_tx_payload_json",
                "voto_tx_payload_json",
                "rv_tx_payload_json",
            ],
            "frv_tx_payload_json",
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
            ["aud_dh_ingestao_bronze", "aud_dh_ingestao"],
            "aud_dh_ingestao_bronze",
            "timestamp"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_endpoint_origem_bronze", "aud_tx_endpoint_origem"],
            "aud_tx_endpoint_origem_bronze",
            "string"
        ),
        source_column(
            df_silver_valid,
            ["aud_tx_sistema_origem_bronze", "aud_tx_sistema_origem"],
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
            ["aud_tx_tipo_carga_bronze", "aud_tx_tipo_carga"],
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
            [
                "aud_tx_regra_extracao_voto",
                "aud_tx_regra_extracao_votacao",
                "aud_tx_regra_derivacao",
            ],
            "aud_tx_regra_extracao_resultado_votacao",
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
        "frv_tx_voto",
        F.upper(F.trim(F.col("frv_tx_voto")))
    )
    .withColumn(
        "frv_tx_voto_curado_silver",
        F.upper(F.trim(F.col("frv_tx_voto_curado_silver")))
    )
    .withColumn(
        "dat_id_data",
        F.date_format(F.col("vot_dt_votacao"), "yyyyMMdd")
    )
)

# COMMAND ----------

# ============================================================
# DEDUPLICATE FACT GRAIN
# ============================================================

fact_business_key_columns = [
    "vot_id_votacao",
    "dep_id_deputado",
]

source_dedup_df = (
    source_standardized_df
    .withColumn(
        "frv_tx_business_key",
        F.concat_ws(
            "||",
            F.coalesce(F.col("vot_id_votacao"), F.lit("NA")),
            F.coalesce(F.col("dep_id_deputado"), F.lit("NA")),
        )
    )
    .withColumn(
        "frv_nr_rank_deduplicacao",
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
        F.col("frv_nr_rank_deduplicacao") == 1
    )
    .drop("frv_nr_rank_deduplicacao")
)

# COMMAND ----------

# ============================================================
# READ GOLD DIMENSIONS
# ============================================================

dm_votacoes_df = (
    spark.table(DM_VOTACOES_TABLE)
    .select(
        F.col("vot_id_votacao").alias("dim_vot_id_votacao"),
        F.col("vot_sk_votacao"),
        F.col("vot_dt_votacao").alias("dim_vot_dt_votacao"),
        F.col("vot_dh_votacao").alias("dim_vot_dh_votacao"),
        F.col("vot_nr_ano").alias("dim_vot_nr_ano"),
        F.col("vot_nr_mes").alias("dim_vot_nr_mes"),
        F.col("leg_id_legislatura").cast("int").alias("dim_vot_leg_id_legislatura"),
        F.col("vot_tx_resultado").alias("dim_vot_tx_resultado"),
        F.col("vot_fl_aprovada").alias("dim_vot_fl_aprovada"),
        F.col("vot_tx_status_aprovacao").alias("dim_vot_tx_status_aprovacao"),
        F.col("vot_tx_resultado_curado").alias("dim_vot_tx_resultado_curado"),
        F.col("vot_tx_descricao").alias("dim_vot_tx_descricao"),
        F.col("prop_id_proposicao").alias("dim_prop_id_proposicao"),
        F.col("org_id_orgao").alias("dim_org_id_orgao"),
        F.col("org_tx_sigla").alias("dim_org_tx_sigla"),
    )
    .dropDuplicates([
        "dim_vot_id_votacao"
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
        dm_votacoes_df,
        F.trim(source_dedup_df["vot_id_votacao"].cast("string")) ==
        F.trim(dm_votacoes_df["dim_vot_id_votacao"].cast("string")),
        "left",
    )
    .withColumn(
        "leg_id_legislatura_final",
        F.coalesce(
            F.col("leg_id_legislatura_deputado"),
            F.col("dim_vot_leg_id_legislatura"),
        ).cast("int")
    )
    .join(
        dm_deputados_exact_df,
        (
            F.trim(source_dedup_df["dep_id_deputado"].cast("string")) ==
            F.trim(dm_deputados_exact_df["dim_dep_id_deputado"].cast("string"))
        )
        & (
            F.col("leg_id_legislatura_final") == dm_deputados_exact_df["dim_dep_id_legislatura"]
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
    .withColumn(
        "vot_dt_votacao_final",
        F.coalesce(
            F.col("vot_dt_votacao"),
            F.col("dim_vot_dt_votacao"),
        )
    )
    .withColumn(
        "dat_id_data_final",
        F.date_format(F.col("vot_dt_votacao_final"), "yyyyMMdd")
    )
    .join(
        dm_datas_df,
        F.col("dat_id_data_final") == dm_datas_df["dim_dat_id_data"],
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
        "frv_sk_resultado_votacao",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("vot_id_votacao").cast("string"),
                F.col("dep_id_deputado").cast("string"),
            ),
            256
        )
    )
    .withColumn(
        "frv_tx_voto_curado",
        F.when(F.col("frv_tx_voto_curado_silver").isNotNull(), F.col("frv_tx_voto_curado_silver"))
        .when(F.col("frv_tx_voto").isin("SIM", "YES", "S"), F.lit("SIM"))
        .when(F.col("frv_tx_voto").isin("NÃO", "NAO", "NO", "N"), F.lit("NAO"))
        .when(F.col("frv_tx_voto").contains("ABST"), F.lit("ABSTENCAO"))
        .when(F.col("frv_tx_voto").contains("OBSTR"), F.lit("OBSTRUCAO"))
        .when(F.col("frv_tx_voto").isNull(), F.lit(None).cast("string"))
        .otherwise(F.col("frv_tx_voto"))
    )
    .withColumn(
        "frv_qt_voto",
        F.lit(1)
    )
    .withColumn(
        "frv_qt_voto_sim",
        F.when((F.col("frv_tx_voto_curado") == "SIM") | (F.col("frv_fl_voto_favoravel") == True), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "frv_qt_voto_nao",
        F.when((F.col("frv_tx_voto_curado") == "NAO") | (F.col("frv_fl_voto_contrario") == True), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "frv_qt_abstencao",
        F.when((F.col("frv_tx_voto_curado") == "ABSTENCAO") | (F.col("frv_fl_abstencao") == True), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "frv_qt_obstrucao",
        F.when((F.col("frv_tx_voto_curado") == "OBSTRUCAO") | (F.col("frv_fl_obstrucao") == True), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "frv_fl_votacao_encontrada_gold",
        F.col("vot_sk_votacao").isNotNull()
    )
    .withColumn(
        "frv_fl_deputado_encontrado_gold",
        F.col("dep_sk_deputado_final").isNotNull()
    )
    .withColumn(
        "frv_fl_partido_encontrado_gold",
        F.col("par_sk_partido").isNotNull()
    )
    .withColumn(
        "frv_fl_estado_encontrado_gold",
        F.col("est_sk_estado").isNotNull()
    )
    .withColumn(
        "frv_fl_data_encontrada_gold",
        F.col("dat_sk_data").isNotNull()
    )
    .withColumn(
        "frv_fl_dimensoes_principais_completas",
        (
            F.col("vot_sk_votacao").isNotNull()
            & F.col("dep_sk_deputado_final").isNotNull()
            & F.col("dat_sk_data").isNotNull()
        )
    )
    .withColumn(
        "frv_fl_registro_valido_gold",
        (
            F.col("vot_id_votacao").isNotNull()
            & F.col("dep_id_deputado").isNotNull()
            & F.col("frv_tx_voto_curado").isNotNull()
            & F.col("frv_sk_resultado_votacao").isNotNull()
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
                F.col("frv_sk_resultado_votacao").cast("string"),
                F.col("vot_sk_votacao").cast("string"),
                F.col("dep_sk_deputado_final").cast("string"),
                F.col("dat_sk_data").cast("string"),
                F.col("frv_tx_voto_curado").cast("string"),
            ),
            256
        )
    )
    .select(
        "frv_sk_resultado_votacao",
        "vot_sk_votacao",
        F.col("dep_sk_deputado_final").alias("dep_sk_deputado"),
        "par_sk_partido",
        "est_sk_estado",
        "dat_sk_data",
        "frv_id_resultado_votacao_origem",
        "vot_id_votacao",
        "dep_id_deputado",
        F.col("dep_tx_nome_final").alias("dep_tx_nome"),
        F.col("dep_tx_sigla_partido_final").alias("dep_tx_sigla_partido"),
        F.col("dep_tx_sigla_uf_final").alias("dep_tx_sigla_uf"),
        "leg_id_legislatura_final",
        F.col("vot_dt_votacao_final").alias("vot_dt_votacao"),
        F.coalesce(F.col("vot_dh_votacao"), F.col("dim_vot_dh_votacao")).alias("vot_dh_votacao"),
        F.coalesce(F.col("vot_nr_ano"), F.col("dim_vot_nr_ano")).alias("vot_nr_ano"),
        F.coalesce(F.col("vot_nr_mes"), F.col("dim_vot_nr_mes")).alias("vot_nr_mes"),
        F.coalesce(F.col("vot_tx_resultado"), F.col("dim_vot_tx_resultado")).alias("vot_tx_resultado"),
        F.col("dim_vot_fl_aprovada").alias("vot_fl_aprovada"),
        F.col("dim_vot_tx_status_aprovacao").alias("vot_tx_status_aprovacao"),
        F.col("dim_vot_tx_resultado_curado").alias("vot_tx_resultado_curado"),
        F.coalesce(F.col("vot_tx_descricao"), F.col("dim_vot_tx_descricao")).alias("vot_tx_descricao"),
        F.coalesce(F.col("prop_id_proposicao"), F.col("dim_prop_id_proposicao")).alias("prop_id_proposicao"),
        F.coalesce(F.col("org_id_orgao"), F.col("dim_org_id_orgao")).alias("org_id_orgao"),
        F.coalesce(F.col("org_tx_sigla"), F.col("dim_org_tx_sigla")).alias("org_tx_sigla"),
        "frv_tx_voto",
        "frv_tx_voto_curado",
        "frv_tx_orientacao",
        "frv_fl_voto_favoravel",
        "frv_fl_voto_contrario",
        "frv_fl_abstencao",
        "frv_fl_obstrucao",
        "frv_qt_voto",
        "frv_qt_voto_sim",
        "frv_qt_voto_nao",
        "frv_qt_abstencao",
        "frv_qt_obstrucao",
        "frv_fl_votacao_encontrada_gold",
        "frv_fl_deputado_encontrado_gold",
        "frv_fl_partido_encontrado_gold",
        "frv_fl_estado_encontrado_gold",
        "frv_fl_data_encontrada_gold",
        "frv_fl_dimensoes_principais_completas",
        "frv_fl_registro_valido_silver",
        "frv_fl_registro_valido_gold",
        "frv_tx_payload_json",
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
        "aud_tx_regra_extracao_resultado_votacao",
        "aud_tx_hash_registro_silver",
        "aud_id_execucao_gold",
        "aud_dh_processamento_gold",
        "aud_tx_versao_pipeline_gold",
        "aud_tx_hash_registro_gold",
    )
    .withColumnRenamed(
        "leg_id_legislatura_final",
        "leg_id_legislatura"
    )
)

# COMMAND ----------

# ============================================================
# QUALITY VALIDATIONS
# ============================================================

required_columns_result = validate_required_columns(
    dataframe=df_gold,
    required_columns=[
        "frv_sk_resultado_votacao",
        "vot_id_votacao",
        "dep_id_deputado",
        "frv_tx_voto_curado",
        "frv_fl_registro_valido_gold",
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_gold,
    key_columns=[
        "vot_id_votacao",
        "dep_id_deputado",
    ]
)

null_results = validate_nulls(
    dataframe=df_gold,
    columns=[
        "frv_sk_resultado_votacao",
        "vot_id_votacao",
        "dep_id_deputado",
        "frv_tx_voto_curado",
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
Gold voting results fact.

This fact contains one record per legislative voting event and deputy vote.

Main characteristics:

* voting result surrogate key
* voting dimension key
* deputy dimension key
* party dimension key
* state dimension key
* date dimension key
* vote result analytical measures
* Silver lineage
* Gold lineage
* governance metadata
"""

COLUMN_COMMENTS = {
    "frv_sk_resultado_votacao":
        "Gold surrogate key for the voting results fact relationship.",

    "vot_sk_votacao":
        "Gold surrogate key of the voting dimension.",

    "dep_sk_deputado":
        "Gold surrogate key of the deputy dimension.",

    "par_sk_partido":
        "Gold surrogate key of the political party dimension.",

    "est_sk_estado":
        "Gold surrogate key of the state dimension.",

    "dat_sk_data":
        "Gold surrogate key of the date dimension based on voting date.",

    "frv_id_resultado_votacao_origem":
        "Original voting result relationship identifier when available in Silver.",

    "vot_id_votacao":
        "Legislative voting business identifier.",

    "dep_id_deputado":
        "Deputy business identifier.",

    "dep_tx_nome":
        "Deputy name associated with the voting result.",

    "dep_tx_sigla_partido":
        "Political party acronym associated with the deputy vote.",

    "dep_tx_sigla_uf":
        "Federative unit acronym associated with the deputy vote.",

    "leg_id_legislatura":
        "Legislature identifier associated with the voting result.",

    "vot_dt_votacao":
        "Voting date.",

    "vot_dh_votacao":
        "Voting timestamp.",

    "vot_nr_ano":
        "Voting year.",

    "vot_nr_mes":
        "Voting month.",

    "vot_tx_resultado":
        "Voting result from source or voting dimension.",

    "vot_fl_aprovada":
        "Flag indicating whether the voting event was approved.",

    "vot_tx_status_aprovacao":
        "Curated textual approval status from voting dimension.",

    "vot_tx_resultado_curado":
        "Curated voting result from voting dimension.",

    "vot_tx_descricao":
        "Voting description.",

    "prop_id_proposicao":
        "Related proposition business identifier.",

    "org_id_orgao":
        "Related legislative body identifier.",

    "org_tx_sigla":
        "Related legislative body acronym.",

    "frv_tx_voto":
        "Original or standardized deputy vote value.",

    "frv_tx_voto_curado":
        "Curated deputy vote value used for analytical aggregation.",

    "frv_tx_orientacao":
        "Party or leadership orientation when available.",

    "frv_fl_voto_favoravel":
        "Flag indicating whether the vote is favorable when available.",

    "frv_fl_voto_contrario":
        "Flag indicating whether the vote is contrary when available.",

    "frv_fl_abstencao":
        "Flag indicating whether the vote is an abstention when available.",

    "frv_fl_obstrucao":
        "Flag indicating whether the vote is obstruction when available.",

    "frv_qt_voto":
        "Additive measure equal to one for each deputy vote.",

    "frv_qt_voto_sim":
        "Additive measure equal to one when the deputy vote is yes.",

    "frv_qt_voto_nao":
        "Additive measure equal to one when the deputy vote is no.",

    "frv_qt_abstencao":
        "Additive measure equal to one when the deputy vote is abstention.",

    "frv_qt_obstrucao":
        "Additive measure equal to one when the deputy vote is obstruction.",

    "frv_fl_votacao_encontrada_gold":
        "Flag indicating whether the related voting event was found in Gold.",

    "frv_fl_deputado_encontrado_gold":
        "Flag indicating whether the related deputy was found in Gold.",

    "frv_fl_partido_encontrado_gold":
        "Flag indicating whether the related party was found in Gold.",

    "frv_fl_estado_encontrado_gold":
        "Flag indicating whether the related state was found in Gold.",

    "frv_fl_data_encontrada_gold":
        "Flag indicating whether the related date was found in Gold.",

    "frv_fl_dimensoes_principais_completas":
        "Flag indicating whether voting, deputy and date dimensions were resolved.",

    "frv_fl_registro_valido_silver":
        "Flag indicating whether the record passed Silver validation when available.",

    "frv_fl_registro_valido_gold":
        "Flag indicating whether the record passed Gold validation.",

    "frv_tx_payload_json":
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
        "Source Silver table used to derive voting result facts.",

    "aud_tx_tabela_destino":
        "Destination Gold table where voting result facts are persisted.",

    "aud_tx_versao_pipeline_silver":
        "Silver pipeline version used to generate the source record.",

    "aud_tx_regra_extracao_resultado_votacao":
        "Business extraction rule applied during Silver voting result processing.",

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
    message="Gold voting results fact generated successfully.",
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
print("FATO RESULTADOS VOTAÇÕES - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records eligible from Silver: {records_valid_silver}")
print(f"Records written: {records_written}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))


# COMMAND ----------

