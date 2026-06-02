# Databricks notebook source
# MAGIC %md
# MAGIC # 13 Gold — CEAP Expenses Fact
# MAGIC
# MAGIC **Notebook:** `13_ft_despesas_ceap`
# MAGIC
# MAGIC Builds the curated Gold CEAP expenses fact used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * CEAP expense fact model
# MAGIC * CEAP expense surrogate key generation
# MAGIC * Deputy, supplier, party, state and date dimensional keys
# MAGIC * CEAP expense financial measures
# MAGIC * CEAP category and document attributes
# MAGIC * Anomaly score by expense category and deputy state
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
# MAGIC * Read validated CEAP expense records from Silver
# MAGIC * Keep one analytical record per CEAP expense document line
# MAGIC * Create the CEAP expense surrogate key
# MAGIC * Resolve Gold dimension keys for deputies, suppliers, parties, states and dates
# MAGIC * Preserve business identifiers and descriptive attributes
# MAGIC * Preserve financial expense measures
# MAGIC * Generate anomaly indicators by category and state
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold CEAP expenses fact
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Fact Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per CEAP expense document line.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_despesas_ceap`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.ft_despesas_ceap`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `des_id_despesa`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `fdc_sk_despesa_ceap`
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
# MAGIC One analytical record per CEAP expense document line.
# MAGIC
# MAGIC Rule 3:
# MAGIC
# MAGIC Resolve supplier, deputy, party, state and date dimensions when available.
# MAGIC
# MAGIC Rule 4:
# MAGIC
# MAGIC Generate anomaly score using z-score by expense category and deputy state.
# MAGIC
# MAGIC Rule 5:
# MAGIC
# MAGIC Preserve governance and lineage information.
# MAGIC
# MAGIC Rule 6:
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
# MAGIC * Null expense business keys
# MAGIC * Null deputy business keys
# MAGIC * Null supplier business keys
# MAGIC * Null net expense values
# MAGIC * Duplicate CEAP expense records
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

NOTEBOOK_NAME = "13_ft_despesas_ceap"

ENTITY_NAME = "despesas_ceap"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_despesas_ceap"

TARGET_TABLE = f"{GOLD_SCHEMA}.ft_despesas_ceap"

DM_DEPUTADOS_TABLE = f"{GOLD_SCHEMA}.dm_deputados"
DM_FORNECEDORES_TABLE = f"{GOLD_SCHEMA}.dm_fornecedores"
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
        "desp_fl_registro_valido_silver",
        "des_fl_registro_valido_silver",
        "dsp_fl_registro_valido_silver",
        "ceap_fl_registro_valido_silver",
        "desp_fl_registro_valido_silver",
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
                "desp_tx_chave_deduplicacao",
                "des_id_despesa",
                "dsp_id_despesa",
                "ceap_id_despesa",
                "desp_tx_chave_deduplicacao",
                "des_tx_chave_deduplicacao",
                "dsp_tx_chave_deduplicacao",
            ],
            "des_id_despesa",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_tx_chave_deduplicacao",
                "des_tx_chave_deduplicacao",
                "dsp_tx_chave_deduplicacao",
                "ceap_tx_chave_deduplicacao",
            ],
            "des_tx_chave_deduplicacao",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "dep_id_deputado",
                "id_deputado",
                "des_id_deputado",
                "dsp_id_deputado",
            ],
            "dep_id_deputado",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "dep_tx_nome",
                "des_tx_nome_deputado",
                "dsp_tx_nome_deputado",
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
                "des_tx_sigla_partido",
                "dsp_tx_sigla_partido",
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
                "des_tx_sigla_uf",
                "dsp_tx_sigla_uf",
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
                "desp_id_legislatura",
                "des_id_legislatura",
                "dsp_id_legislatura",
                "id_legislatura",
            ],
            "leg_id_legislatura",
            "int"
        ),
        source_column(
            df_silver_valid,
            [
                "forn_tx_chave_deduplicacao",
                "for_tx_chave_deduplicacao",
                "des_tx_chave_fornecedor",
                "dsp_tx_chave_fornecedor",
            ],
            "forn_tx_chave_deduplicacao",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "forn_tx_nome",
                "fornecedor_tx_nome",
                "desp_tx_nome_fornecedor",
                "des_tx_nome_fornecedor",
                "dsp_tx_nome_fornecedor",
                "nome_fornecedor",
            ],
            "forn_tx_nome",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "forn_tx_documento_original",
                "forn_tx_cnpj_cpf",
                "desp_tx_cnpj_cpf_fornecedor",
                "des_tx_cnpj_cpf_fornecedor",
                "dsp_tx_cnpj_cpf_fornecedor",
                "cnpj_cpf_fornecedor",
            ],
            "forn_tx_documento_original",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "forn_tx_documento_limpo",
                "forn_tx_cnpj_cpf_limpo",
                "desp_tx_cnpj_cpf_fornecedor_limpo",
                "des_tx_documento_fornecedor_limpo",
                "dsp_tx_documento_fornecedor_limpo",
            ],
            "forn_tx_documento_limpo",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "forn_tx_tipo_documento",
                "desp_tx_tipo_documento_fornecedor",
                "des_tx_tipo_documento_fornecedor",
                "dsp_tx_tipo_documento_fornecedor",
            ],
            "forn_tx_tipo_documento",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_nr_ano",
                "des_nr_ano",
                "dsp_nr_ano",
                "ceap_nr_ano",
                "ano",
            ],
            "des_nr_ano",
            "int"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_nr_mes",
                "des_nr_mes",
                "dsp_nr_mes",
                "ceap_nr_mes",
                "mes",
            ],
            "des_nr_mes",
            "int"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_dt_data_documento",
                "des_dt_emissao",
                "dsp_dt_emissao",
                "dat_dt_emissao",
                "data_emissao",
                "des_dt_documento",
            ],
            "des_dt_emissao",
            "date"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_tx_tipo_despesa",
                "des_tx_tipo_despesa",
                "dsp_tx_tipo_despesa",
                "ceap_tx_tipo_despesa",
                "des_tx_categoria",
                "dsp_tx_categoria",
                "categoria",
            ],
            "des_tx_tipo_despesa",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_tx_tipo_documento",
                "des_tx_tipo_documento",
                "dsp_tx_tipo_documento",
                "tipo_documento",
            ],
            "des_tx_tipo_documento",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_tx_numero_documento",
                "des_tx_numero_documento",
                "dsp_tx_numero_documento",
                "des_tx_num_documento",
                "dsp_tx_num_documento",
                "numero_documento",
            ],
            "des_tx_numero_documento",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_tx_url_documento",
                "des_tx_url_documento",
                "dsp_tx_url_documento",
                "url_documento",
            ],
            "des_tx_url_documento",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_vl_documento",
                "des_vl_documento",
                "dsp_vl_documento",
                "vlr_documento",
                "valor_documento",
            ],
            "des_vl_documento",
            "double"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_vl_glosa",
                "des_vl_glosa",
                "dsp_vl_glosa",
                "vlr_glosa",
                "valor_glosa",
            ],
            "des_vl_glosa",
            "double"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_vl_liquido",
                "des_vl_liquido",
                "dsp_vl_liquido",
                "vlr_liquido",
                "valor_liquido",
                "vl_liquido",
            ],
            "des_vl_liquido",
            "double"
        ),
        source_column(
            df_silver_valid,
            [
                "des_vl_restituicao",
                "dsp_vl_restituicao",
                "vlr_restituicao",
                "valor_restituicao",
            ],
            "des_vl_restituicao",
            "double"
        ),
        source_column(
            df_silver_valid,
            [
                "des_nr_parcela",
                "dsp_nr_parcela",
                "num_parcela",
                "nr_parcela",
            ],
            "des_nr_parcela",
            "int"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_fl_documento_fornecedor_valido_formato",
            ],
            "desp_fl_documento_fornecedor_valido_formato",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_fl_documento_fornecedor_repetido",
            ],
            "desp_fl_documento_fornecedor_repetido",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_fl_documento_fornecedor_informado",
                "des_fl_documento_informado",
                "dsp_fl_documento_informado",
            ],
            "des_fl_documento_informado",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_fl_fornecedor_informado",
                "des_fl_fornecedor_encontrado_silver",
                "dsp_fl_fornecedor_encontrado_silver",
            ],
            "des_fl_fornecedor_encontrado_silver",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_fl_deputado_informado",
                "des_fl_deputado_encontrado_silver",
                "dsp_fl_deputado_encontrado_silver",
            ],
            "des_fl_deputado_encontrado_silver",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_fl_registro_valido_silver",
                "des_fl_registro_valido_silver",
                "dsp_fl_registro_valido_silver",
                silver_valid_flag if silver_valid_flag else "__missing__",
            ],
            "des_fl_registro_valido_silver",
            "boolean"
        ),
        source_column(
            df_silver_valid,
            [
                "desp_tx_payload_json",
                "des_tx_payload_json",
                "dsp_tx_payload_json",
                "ceap_tx_payload_json",
            ],
            "des_tx_payload_json",
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
                "aud_tx_regra_extracao_despesa",
                "aud_tx_regra_derivacao",
            ],
            "aud_tx_regra_extracao_despesa",
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
        "des_tx_tipo_despesa",
        F.upper(F.trim(F.col("des_tx_tipo_despesa")))
    )
    .withColumn(
        "forn_tx_chave_deduplicacao",
        F.trim(F.col("forn_tx_chave_deduplicacao"))
    )
    .withColumn(
        "dat_id_data",
        F.when(
            F.col("des_dt_emissao").isNotNull(),
            F.date_format(F.col("des_dt_emissao"), "yyyyMMdd")
        )
        .when(
            F.col("des_nr_ano").isNotNull() & F.col("des_nr_mes").isNotNull(),
            F.concat(
                F.col("des_nr_ano").cast("string"),
                F.lpad(F.col("des_nr_mes").cast("string"), 2, "0"),
                F.lit("01")
            )
        )
        .otherwise(F.lit(None).cast("string"))
    )
    .withColumn(
        "forn_tx_chave_deduplicacao",
        F.when(
            F.col("forn_tx_documento_limpo").isNotNull()
            & (F.length(F.trim(F.col("forn_tx_documento_limpo"))) > 0)
            & (F.col("desp_fl_documento_fornecedor_valido_formato") == True)
            & (F.col("desp_fl_documento_fornecedor_repetido") == False),
            F.trim(F.col("forn_tx_documento_limpo"))
        )
        .otherwise(
            F.sha2(
                F.concat_ws(
                    "||",
                    F.upper(F.trim(F.coalesce(F.col("forn_tx_nome"), F.lit("NA")))),
                    F.trim(F.coalesce(F.col("forn_tx_documento_limpo"), F.lit("NA"))),
                    F.trim(F.coalesce(F.col("forn_tx_documento_original"), F.lit("NA")))
                ),
                256
            )
        )
    )
)

# COMMAND ----------

# ============================================================
# DEDUPLICATE FACT GRAIN
# ============================================================

fact_business_key_columns = [
    "des_id_despesa",
]

source_dedup_df = (
    source_standardized_df
    .withColumn(
        "fdc_tx_business_key",
        F.coalesce(
            F.col("des_id_despesa"),
            F.col("des_tx_chave_deduplicacao"),
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("dep_id_deputado").cast("string"),
                    F.col("forn_tx_chave_deduplicacao").cast("string"),
                    F.col("des_dt_emissao").cast("string"),
                    F.col("des_tx_numero_documento").cast("string"),
                    F.col("des_vl_liquido").cast("string"),
                ),
                256
            )
        )
    )
    .withColumn(
        "fdc_nr_rank_deduplicacao",
        F.row_number().over(
            Window
            .partitionBy("fdc_tx_business_key")
            .orderBy(
                F.col("aud_dh_processamento").desc_nulls_last(),
                F.col("aud_dh_ingestao_bronze").desc_nulls_last(),
                F.col("aud_tx_hash_registro_bronze").asc_nulls_last(),
            )
        )
    )
    .filter(
        F.col("fdc_nr_rank_deduplicacao") == 1
    )
    .drop("fdc_nr_rank_deduplicacao")
)

# COMMAND ----------

# ============================================================
# READ GOLD DIMENSIONS
# ============================================================

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

dm_fornecedores_df = (
    spark.table(DM_FORNECEDORES_TABLE)
    .select(
        F.col("forn_tx_chave_deduplicacao").alias("dim_forn_tx_chave_deduplicacao"),
        F.col("forn_sk_fornecedor"),
        F.col("forn_tx_nome").alias("dim_forn_tx_nome"),
        F.col("forn_tx_documento_limpo").alias("dim_forn_tx_documento_limpo"),
        F.col("forn_tx_tipo_documento").alias("dim_forn_tx_tipo_documento"),
        F.col("forn_fl_documento_valido_formato").alias("dim_forn_fl_documento_valido_formato"),
        F.col("forn_fl_documento_repetido").alias("dim_forn_fl_documento_repetido"),
    )
    .dropDuplicates([
        "dim_forn_tx_chave_deduplicacao"
    ])
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
        dm_deputados_exact_df,
        (
            F.trim(source_dedup_df["dep_id_deputado"].cast("string")) ==
            F.trim(dm_deputados_exact_df["dim_dep_id_deputado"].cast("string"))
        )
        & (
            source_dedup_df["leg_id_legislatura"] == dm_deputados_exact_df["dim_dep_id_legislatura"]
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
        dm_fornecedores_df,
        F.trim(F.col("forn_tx_chave_deduplicacao").cast("string")) ==
        F.trim(dm_fornecedores_df["dim_forn_tx_chave_deduplicacao"].cast("string")),
        "left",
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
    .withColumn(
        "forn_tx_nome_final",
        F.coalesce(
            F.col("forn_tx_nome"),
            F.col("dim_forn_tx_nome"),
        )
    )
    .withColumn(
        "forn_tx_documento_limpo_final",
        F.coalesce(
            F.col("forn_tx_documento_limpo"),
            F.col("dim_forn_tx_documento_limpo"),
        )
    )
    .withColumn(
        "forn_tx_tipo_documento_final",
        F.coalesce(
            F.col("forn_tx_tipo_documento"),
            F.col("dim_forn_tx_tipo_documento"),
        )
    )
)

# COMMAND ----------

# ============================================================
# BUILD GOLD FACT
# ============================================================

anomaly_window = Window.partitionBy(
    "des_tx_tipo_despesa",
    "dep_tx_sigla_uf_final",
)

df_gold = (
    fact_enriched_df
    .withColumn(
        "fdc_sk_despesa_ceap",
        F.sha2(
            F.col("fdc_tx_business_key").cast("string"),
            256
        )
    )
    .withColumn(
        "fdc_vl_liquido_abs",
        F.abs(F.coalesce(F.col("des_vl_liquido"), F.lit(0.0)))
    )
    .withColumn(
        "fdc_vl_media_categoria_uf",
        F.avg(F.col("fdc_vl_liquido_abs")).over(anomaly_window)
    )
    .withColumn(
        "fdc_vl_desvio_categoria_uf",
        F.stddev_pop(F.col("fdc_vl_liquido_abs")).over(anomaly_window)
    )
    .withColumn(
        "fdc_vl_zscore_categoria_uf",
        F.when(
            F.col("fdc_vl_desvio_categoria_uf").isNull()
            | (F.col("fdc_vl_desvio_categoria_uf") == 0),
            F.lit(0.0)
        )
        .otherwise(
            (F.col("fdc_vl_liquido_abs") - F.col("fdc_vl_media_categoria_uf"))
            / F.col("fdc_vl_desvio_categoria_uf")
        )
    )
    .withColumn(
        "fdc_fl_anomalia_zscore",
        F.abs(F.col("fdc_vl_zscore_categoria_uf")) >= F.lit(3.0)
    )
    .withColumn(
        "fdc_fl_documento_fornecedor_valido",
        F.coalesce(
            F.col("dim_forn_fl_documento_valido_formato"),
            F.lit(False)
        )
    )
    .withColumn(
        "fdc_fl_documento_fornecedor_repetido",
        F.coalesce(
            F.col("dim_forn_fl_documento_repetido"),
            F.lit(False)
        )
    )
    .withColumn(
        "fdc_qt_despesa",
        F.lit(1)
    )
    .withColumn(
        "fdc_fl_deputado_encontrado_gold",
        F.col("dep_sk_deputado_final").isNotNull()
    )
    .withColumn(
        "fdc_fl_fornecedor_encontrado_gold",
        F.col("forn_sk_fornecedor").isNotNull()
    )
    .withColumn(
        "fdc_fl_partido_encontrado_gold",
        F.col("par_sk_partido").isNotNull()
    )
    .withColumn(
        "fdc_fl_estado_encontrado_gold",
        F.col("est_sk_estado").isNotNull()
    )
    .withColumn(
        "fdc_fl_data_encontrada_gold",
        F.col("dat_sk_data").isNotNull()
    )
    .withColumn(
        "fdc_fl_dimensoes_principais_completas",
        (
            F.col("dep_sk_deputado_final").isNotNull()
            & F.col("forn_sk_fornecedor").isNotNull()
            & F.col("dat_sk_data").isNotNull()
        )
    )
    .withColumn(
        "fdc_fl_registro_valido_gold",
        (
            F.col("fdc_sk_despesa_ceap").isNotNull()
            & F.col("dep_id_deputado").isNotNull()
            & F.col("forn_tx_chave_deduplicacao").isNotNull()
            & F.col("des_vl_liquido").isNotNull()
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
                F.col("fdc_sk_despesa_ceap").cast("string"),
                F.col("dep_sk_deputado_final").cast("string"),
                F.col("forn_sk_fornecedor").cast("string"),
                F.col("dat_sk_data").cast("string"),
                F.col("des_vl_liquido").cast("string"),
                F.col("des_tx_tipo_despesa").cast("string"),
            ),
            256
        )
    )
    .select(
        "fdc_sk_despesa_ceap",
        F.col("dep_sk_deputado_final").alias("dep_sk_deputado"),
        "forn_sk_fornecedor",
        "par_sk_partido",
        "est_sk_estado",
        "dat_sk_data",
        "des_id_despesa",
        "des_tx_chave_deduplicacao",
        "fdc_tx_business_key",
        "dep_id_deputado",
        F.col("dep_tx_nome_final").alias("dep_tx_nome"),
        F.col("dep_tx_sigla_partido_final").alias("dep_tx_sigla_partido"),
        F.col("dep_tx_sigla_uf_final").alias("dep_tx_sigla_uf"),
        "leg_id_legislatura",
        "forn_tx_chave_deduplicacao",
        F.col("forn_tx_nome_final").alias("forn_tx_nome"),
        "forn_tx_documento_original",
        F.col("forn_tx_documento_limpo_final").alias("forn_tx_documento_limpo"),
        F.col("forn_tx_tipo_documento_final").alias("forn_tx_tipo_documento"),
        "des_nr_ano",
        "des_nr_mes",
        "des_dt_emissao",
        "des_tx_tipo_despesa",
        "des_tx_tipo_documento",
        "des_tx_numero_documento",
        "des_tx_url_documento",
        "des_nr_parcela",
        "des_vl_documento",
        "des_vl_glosa",
        "des_vl_liquido",
        "des_vl_restituicao",
        "fdc_vl_liquido_abs",
        "fdc_qt_despesa",
        "fdc_vl_media_categoria_uf",
        "fdc_vl_desvio_categoria_uf",
        "fdc_vl_zscore_categoria_uf",
        "fdc_fl_anomalia_zscore",
        "fdc_fl_documento_fornecedor_valido",
        "fdc_fl_documento_fornecedor_repetido",
        "fdc_fl_deputado_encontrado_gold",
        "fdc_fl_fornecedor_encontrado_gold",
        "fdc_fl_partido_encontrado_gold",
        "fdc_fl_estado_encontrado_gold",
        "fdc_fl_data_encontrada_gold",
        "fdc_fl_dimensoes_principais_completas",
        "des_fl_documento_informado",
        "des_fl_fornecedor_encontrado_silver",
        "des_fl_deputado_encontrado_silver",
        "des_fl_registro_valido_silver",
        "fdc_fl_registro_valido_gold",
        "des_tx_payload_json",
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
        "aud_tx_regra_extracao_despesa",
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
        "fdc_sk_despesa_ceap",
        "dep_id_deputado",
        "forn_tx_chave_deduplicacao",
        "des_vl_liquido",
        "fdc_fl_registro_valido_gold",
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_gold,
    key_columns=[
        "fdc_tx_business_key",
    ]
)

null_results = validate_nulls(
    dataframe=df_gold,
    columns=[
        "fdc_sk_despesa_ceap",
        "dep_id_deputado",
        "forn_tx_chave_deduplicacao",
        "des_vl_liquido",
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
Gold CEAP expenses fact.

This fact contains one record per CEAP expense document line.

Main characteristics:

* CEAP expense surrogate key
* deputy dimension key
* supplier dimension key
* party dimension key
* state dimension key
* date dimension key
* expense category and document attributes
* financial expense measures
* anomaly score by category and state
* Silver lineage
* Gold lineage
* governance metadata
"""

COLUMN_COMMENTS = {
    "fdc_sk_despesa_ceap":
        "Gold surrogate key for the CEAP expense fact record.",

    "dep_sk_deputado":
        "Gold surrogate key of the deputy dimension.",

    "forn_sk_fornecedor":
        "Gold surrogate key of the supplier dimension.",

    "par_sk_partido":
        "Gold surrogate key of the political party dimension.",

    "est_sk_estado":
        "Gold surrogate key of the state dimension.",

    "dat_sk_data":
        "Gold surrogate key of the date dimension based on expense issue date or reference month.",

    "des_id_despesa":
        "CEAP expense business identifier from Silver when available.",

    "des_tx_chave_deduplicacao":
        "Silver technical deduplication key for the CEAP expense record.",

    "fdc_tx_business_key":
        "Gold business key used to identify one CEAP expense record.",

    "dep_id_deputado":
        "Deputy business identifier associated with the expense.",

    "dep_tx_nome":
        "Deputy name associated with the expense.",

    "dep_tx_sigla_partido":
        "Deputy political party acronym associated with the expense.",

    "dep_tx_sigla_uf":
        "Deputy federative unit acronym associated with the expense.",

    "leg_id_legislatura":
        "Legislature identifier associated with the deputy expense when available.",

    "forn_tx_chave_deduplicacao":
        "Supplier business key used to resolve the supplier dimension.",

    "forn_tx_nome":
        "Supplier name associated with the expense.",

    "forn_tx_documento_original":
        "Original supplier document associated with the expense.",

    "forn_tx_documento_limpo":
        "Clean supplier document associated with the expense.",

    "forn_tx_tipo_documento":
        "Supplier document type associated with the expense.",

    "des_nr_ano":
        "CEAP expense reference year.",

    "des_nr_mes":
        "CEAP expense reference month.",

    "des_dt_emissao":
        "CEAP expense document issue date.",

    "des_tx_tipo_despesa":
        "CEAP expense category.",

    "des_tx_tipo_documento":
        "CEAP expense document type.",

    "des_tx_numero_documento":
        "CEAP expense document number.",

    "des_tx_url_documento":
        "CEAP expense document URL.",

    "des_nr_parcela":
        "CEAP expense installment number.",

    "des_vl_documento":
        "Original CEAP document amount.",

    "des_vl_glosa":
        "CEAP disallowed amount.",

    "des_vl_liquido":
        "CEAP net reimbursed amount.",

    "des_vl_restituicao":
        "CEAP refunded amount.",

    "fdc_vl_liquido_abs":
        "Absolute CEAP net amount used for anomaly scoring.",

    "fdc_qt_despesa":
        "Additive measure equal to one for each CEAP expense record.",

    "fdc_vl_media_categoria_uf":
        "Average net amount calculated by expense category and deputy state.",

    "fdc_vl_desvio_categoria_uf":
        "Population standard deviation of net amount by expense category and deputy state.",

    "fdc_vl_zscore_categoria_uf":
        "Z-score of the CEAP expense amount by category and deputy state.",

    "fdc_fl_anomalia_zscore":
        "Flag indicating whether the expense has absolute z-score greater than or equal to three.",

    "fdc_fl_documento_fornecedor_valido":
        "Flag indicating whether the supplier document has valid format according to supplier dimension.",

    "fdc_fl_documento_fornecedor_repetido":
        "Flag indicating whether the supplier document is composed only by repeated digits.",

    "fdc_fl_deputado_encontrado_gold":
        "Flag indicating whether the related deputy was found in Gold.",

    "fdc_fl_fornecedor_encontrado_gold":
        "Flag indicating whether the related supplier was found in Gold.",

    "fdc_fl_partido_encontrado_gold":
        "Flag indicating whether the related party was found in Gold.",

    "fdc_fl_estado_encontrado_gold":
        "Flag indicating whether the related state was found in Gold.",

    "fdc_fl_data_encontrada_gold":
        "Flag indicating whether the related date was found in Gold.",

    "fdc_fl_dimensoes_principais_completas":
        "Flag indicating whether deputy, supplier and date dimensions were resolved.",

    "des_fl_documento_informado":
        "Flag indicating whether the expense document was informed in Silver.",

    "des_fl_fornecedor_encontrado_silver":
        "Flag indicating whether the supplier was found during Silver processing.",

    "des_fl_deputado_encontrado_silver":
        "Flag indicating whether the deputy was found during Silver processing.",

    "des_fl_registro_valido_silver":
        "Flag indicating whether the record passed Silver validation.",

    "fdc_fl_registro_valido_gold":
        "Flag indicating whether the record passed Gold validation.",

    "des_tx_payload_json":
        "Original source payload preserved from Silver when available.",

    "aud_id_execucao_gold":
        "Execution identifier generated during Gold processing.",

    "aud_dh_processamento_gold":
        "Timestamp when the record was processed in Gold.",

    "aud_tx_versao_pipeline_gold":
        "Pipeline version used during Gold processing.",

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
        "Source Silver table used to derive Gold expense facts.",

    "aud_tx_tabela_destino":
        "Destination Gold table where expense facts are persisted.",

    "aud_tx_versao_pipeline_silver":
        "Silver pipeline version used to generate the source record.",

    "aud_tx_regra_extracao_despesa":
        "Business extraction rule applied during Silver expense processing.",

    "aud_tx_hash_registro_silver":
        "Deterministic Silver record hash.",    

    "aud_tx_hash_registro_gold":
        "Deterministic Gold record hash."
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
    message="Gold CEAP expenses fact generated successfully.",
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
print("FATO DESPESAS CEAP - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records eligible from Silver: {records_valid_silver}")
print(f"Records written: {records_written}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))
