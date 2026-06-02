# Databricks notebook source
# MAGIC %md
# MAGIC # 10 Gold — Parliamentary Front Members Fact
# MAGIC
# MAGIC **Notebook:** `10_ft_frentes_membros`
# MAGIC
# MAGIC Builds the curated Gold parliamentary front members fact used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Parliamentary front membership fact model
# MAGIC * Front-member relationship surrogate key generation
# MAGIC * Front, deputy, party and state dimensional keys
# MAGIC * Membership analytical indicators
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
# MAGIC * Read validated parliamentary front member records from Silver
# MAGIC * Keep one analytical record per parliamentary front and deputy relationship
# MAGIC * Create the front-member relationship surrogate key
# MAGIC * Resolve Gold dimension keys for fronts, deputies, parties and states
# MAGIC * Preserve business identifiers and descriptive attributes
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold parliamentary front members fact
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Fact Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per parliamentary front and deputy membership relationship.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_frentes_membros`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.ft_frentes_membros`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `frn_id_frente`, `dep_id_deputado`, `leg_id_legislatura`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `ffm_sk_frente_membro`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC Rule 1:
# MAGIC
# MAGIC Only Silver approved records are eligible for Gold when the Silver validation flag is available.
# MAGIC
# MAGIC Rule 2:
# MAGIC
# MAGIC One analytical record per front, deputy and legislature relationship.
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
# MAGIC * Null front business keys
# MAGIC * Null deputy business keys
# MAGIC * Duplicate front-member relationships
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

NOTEBOOK_NAME = "10_ft_frentes_membros"

ENTITY_NAME = "frentes_membros"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_frentes_membros"

TARGET_TABLE = f"{GOLD_SCHEMA}.ft_frentes_membros"

DM_FRENTES_TABLE = f"{GOLD_SCHEMA}.dm_frentes"
DM_DEPUTADOS_TABLE = f"{GOLD_SCHEMA}.dm_deputados"
DM_PARTIDOS_TABLE = f"{GOLD_SCHEMA}.dm_partidos"
DM_ESTADOS_TABLE = f"{GOLD_SCHEMA}.dm_estados"

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


def has_column(dataframe, column_name):
    """
    Checks whether a dataframe contains a given column.
    """

    return column_name in dataframe.columns


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
        "frm_fl_registro_valido_silver",
        "mbr_fl_registro_valido_silver",
        "ffm_fl_registro_valido_silver",
        "frn_mbr_fl_registro_valido_silver",
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
                "frm_id_relacao",
                "mbr_id_relacao",
                "ffm_id_relacao",
                "frn_mbr_id_relacao",
                "frm_id_membro",
                "mbr_id_membro",
            ],
            "ffm_id_relacao_origem",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "frn_id_frente",
                "fre_id_frente",
                "id_frente",
            ],
            "frn_id_frente",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "frn_tx_titulo",
                "fre_tx_titulo",
                "titulo_frente",
            ],
            "frn_tx_titulo",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "dep_id_deputado",
                "id_deputado",
                "mbr_id_deputado",
                "frm_id_deputado",
            ],
            "dep_id_deputado",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "dep_tx_nome",
                "mbr_tx_nome",
                "frm_tx_nome",
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
                "part_tx_sigla",
                "mbr_tx_sigla_partido",
                "frm_tx_sigla_partido",
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
                "uf_tx_sigla",
                "mbr_tx_sigla_uf",
                "frm_tx_sigla_uf",
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
                "frn_id_legislatura",
                "id_legislatura",
            ],
            "leg_id_legislatura",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "frm_tx_tipo_participacao",
                "mbr_tx_tipo_participacao",
                "frm_tx_tipo_membro",
                "mbr_tx_tipo_membro",
                "frm_tx_cargo",
                "mbr_tx_cargo",
            ],
            "ffm_tx_tipo_participacao",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "frm_tx_situacao",
                "mbr_tx_situacao",
                "ffm_tx_situacao",
            ],
            "ffm_tx_situacao",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "frm_tx_uri",
                "mbr_tx_uri",
                "ffm_tx_uri",
            ],
            "ffm_tx_uri",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                "frm_tx_payload_json",
                "mbr_tx_payload_json",
                "ffm_tx_payload_json",
            ],
            "ffm_tx_payload_json",
            "string"
        ),
        source_column(
            df_silver_valid,
            [
                silver_valid_flag if silver_valid_flag else "__missing__"
            ],
            "ffm_fl_registro_valido_silver",
            "boolean"
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
        "leg_id_legislatura",
        F.trim(F.col("leg_id_legislatura"))
    )
)

# COMMAND ----------

# ============================================================
# DEDUPLICATE FACT GRAIN
# ============================================================

fact_business_key_columns = [
    "frn_id_frente",
    "dep_id_deputado",
    "leg_id_legislatura",
]

source_dedup_df = (
    source_standardized_df
    .withColumn(
        "ffm_tx_business_key",
        F.concat_ws(
            "||",
            F.coalesce(F.col("frn_id_frente"), F.lit("NA")),
            F.coalesce(F.col("dep_id_deputado"), F.lit("NA")),
            F.coalesce(F.col("leg_id_legislatura"), F.lit("NA")),
        )
    )
    .withColumn(
        "ffm_nr_rank_deduplicacao",
        F.row_number().over(
            Window
            .partitionBy(*fact_business_key_columns)
            .orderBy(
                F.col("aud_dh_processamento").desc_nulls_last(),
                F.col("aud_dh_ingestao_bronze").desc_nulls_last(),
                F.col("ffm_id_relacao_origem").asc_nulls_last(),
            )
        )
    )
    .filter(
        F.col("ffm_nr_rank_deduplicacao") == 1
    )
    .drop("ffm_nr_rank_deduplicacao")
)

# COMMAND ----------

# ============================================================
# READ GOLD DIMENSIONS
# ============================================================

dm_frentes_df = (
    spark.table(DM_FRENTES_TABLE)
    .select(
        F.col("frn_id_frente").alias("dim_frn_id_frente"),
        F.col("frn_sk_frente"),
        F.col("frn_tx_titulo").alias("dim_frn_tx_titulo"),
        F.col("leg_id_legislatura").cast("string").alias("dim_frn_leg_id_legislatura"),
    )
)

dm_deputados_df = (
    spark.table(DM_DEPUTADOS_TABLE)
    .select(
        F.col("dep_id_deputado").alias("dim_dep_id_deputado"),
        F.col("dep_id_legislatura").cast("string").alias("dim_dep_id_legislatura"),
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

# COMMAND ----------

# ============================================================
# ENRICH FACT WITH DIMENSION KEYS
# ============================================================

fact_enriched_df = (
    source_dedup_df
    .join(
        dm_frentes_df,
        F.trim(source_dedup_df["frn_id_frente"].cast("string")) ==
        F.trim(dm_frentes_df["dim_frn_id_frente"].cast("string")),
        "left",
    )
    .join(
        dm_deputados_exact_df,
        (
            (source_dedup_df["dep_id_deputado"] == dm_deputados_exact_df["dim_dep_id_deputado"])
            & (source_dedup_df["leg_id_legislatura"] == dm_deputados_exact_df["dim_dep_id_legislatura"])
        ),
        "left",
    )
    .join(
        dm_deputados_latest_df,
        source_dedup_df["dep_id_deputado"] == dm_deputados_latest_df["latest_dep_id_deputado"],
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
            F.col("leg_id_legislatura"),
            F.col("dim_frn_leg_id_legislatura"),
        )
    )
    .withColumn(
        "ffm_sk_frente_membro",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("frn_id_frente").cast("string"),
                F.col("dep_id_deputado").cast("string"),
                F.col("leg_id_legislatura_final").cast("string"),
            ),
            256
        )
    )
    .withColumn(
        "ffm_fl_frente_encontrada_gold",
        F.col("frn_sk_frente").isNotNull()
    )
    .withColumn(
        "ffm_fl_deputado_encontrado_gold",
        F.col("dep_sk_deputado_final").isNotNull()
    )
    .withColumn(
        "ffm_fl_partido_encontrado_gold",
        F.col("par_sk_partido").isNotNull()
    )
    .withColumn(
        "ffm_fl_estado_encontrado_gold",
        F.col("est_sk_estado").isNotNull()
    )
    .withColumn(
        "ffm_fl_dimensoes_principais_completas",
        (
            F.col("frn_sk_frente").isNotNull()
            & F.col("dep_sk_deputado_final").isNotNull()
        )
    )
    .withColumn(
        "ffm_qt_membro",
        F.lit(1)
    )
    .withColumn(
        "ffm_fl_registro_valido_gold",
        (
            F.col("frn_id_frente").isNotNull()
            & F.col("dep_id_deputado").isNotNull()
            & F.col("ffm_sk_frente_membro").isNotNull()
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
                F.col("ffm_sk_frente_membro").cast("string"),
                F.col("frn_sk_frente").cast("string"),
                F.col("dep_sk_deputado_final").cast("string"),
                F.col("par_sk_partido").cast("string"),
                F.col("est_sk_estado").cast("string"),
                F.col("leg_id_legislatura_final").cast("string"),
            ),
            256
        )
    )
    .select(
        "ffm_sk_frente_membro",
        F.col("frn_sk_frente"),
        F.col("dep_sk_deputado_final").alias("dep_sk_deputado"),
        "par_sk_partido",
        "est_sk_estado",
        "frn_id_frente",
        F.coalesce(
            F.col("frn_tx_titulo"),
            F.col("dim_frn_tx_titulo"),
        ).alias("frn_tx_titulo"),
        "dep_id_deputado",
        F.col("dep_tx_nome_final").alias("dep_tx_nome"),
        F.col("dep_tx_sigla_partido_final").alias("dep_tx_sigla_partido"),
        F.col("dep_tx_sigla_uf_final").alias("dep_tx_sigla_uf"),
        F.col("leg_id_legislatura_final").alias("leg_id_legislatura"),
        "ffm_id_relacao_origem",
        "ffm_tx_tipo_participacao",
        "ffm_tx_situacao",
        "ffm_tx_uri",
        "ffm_qt_membro",
        "ffm_fl_frente_encontrada_gold",
        "ffm_fl_deputado_encontrado_gold",
        "ffm_fl_partido_encontrado_gold",
        "ffm_fl_estado_encontrado_gold",
        "ffm_fl_dimensoes_principais_completas",
        "ffm_fl_registro_valido_silver",
        "ffm_fl_registro_valido_gold",
        "ffm_tx_payload_json",
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
        "ffm_sk_frente_membro",
        "frn_id_frente",
        "dep_id_deputado",
        "ffm_fl_registro_valido_gold",
    ]
)

duplicate_result = validate_duplicates(
    dataframe=df_gold,
    key_columns=[
        "frn_id_frente",
        "dep_id_deputado",
        "leg_id_legislatura",
    ]
)

null_results = validate_nulls(
    dataframe=df_gold,
    columns=[
        "ffm_sk_frente_membro",
        "frn_id_frente",
        "dep_id_deputado",
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
Gold parliamentary front members fact.

This fact contains one record per parliamentary front and deputy membership relationship.

Main characteristics:

* front-member relationship surrogate key
* front dimension key
* deputy dimension key
* party dimension key
* state dimension key
* membership analytical indicators
* Silver lineage
* Gold lineage
* governance metadata
"""

COLUMN_COMMENTS = {
    "ffm_sk_frente_membro":
        "Gold surrogate key for the parliamentary front member fact relationship.",

    "frn_sk_frente":
        "Gold surrogate key of the parliamentary front dimension.",

    "dep_sk_deputado":
        "Gold surrogate key of the deputy dimension.",

    "par_sk_partido":
        "Gold surrogate key of the political party dimension.",

    "est_sk_estado":
        "Gold surrogate key of the state dimension.",

    "frn_id_frente":
        "Business identifier of the parliamentary front from the source system.",

    "frn_tx_titulo":
        "Parliamentary front title.",

    "dep_id_deputado":
        "Deputy business identifier from the source system.",

    "dep_tx_nome":
        "Deputy name associated with the parliamentary front membership.",

    "dep_tx_sigla_partido":
        "Political party acronym associated with the deputy membership.",

    "dep_tx_sigla_uf":
        "Federative unit acronym associated with the deputy membership.",

    "leg_id_legislatura":
        "Legislature identifier associated with the parliamentary front, derived from dm_frentes when unavailable in Silver.",

    "ffm_id_relacao_origem":
        "Original relationship identifier when available in Silver.",

    "ffm_tx_tipo_participacao":
        "Membership role or participation type when available in Silver.",

    "ffm_tx_situacao":
        "Membership status when available in Silver.",

    "ffm_tx_uri":
        "Membership source URI when available in Silver.",

    "ffm_qt_membro":
        "Additive measure equal to one for each front-member relationship.",

    "ffm_fl_frente_encontrada_gold":
        "Flag indicating whether the related parliamentary front was found in Gold.",

    "ffm_fl_deputado_encontrado_gold":
        "Flag indicating whether the related deputy was found in Gold.",

    "ffm_fl_partido_encontrado_gold":
        "Flag indicating whether the related party was found in Gold.",

    "ffm_fl_estado_encontrado_gold":
        "Flag indicating whether the related state was found in Gold.",

    "ffm_fl_dimensoes_principais_completas":
        "Flag indicating whether the main mandatory dimensions were resolved.",

    "ffm_fl_registro_valido_silver":
        "Flag indicating whether the record passed Silver validation when available.",

    "ffm_fl_registro_valido_gold":
        "Flag indicating whether the record passed Gold validation.",

    "ffm_tx_payload_json":
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

    "aud_tx_hash_registro_bronze":
        "Deterministic Bronze record hash.",

    "aud_id_execucao_silver":
        "Execution identifier generated during Silver processing.",

    "aud_dh_processamento":
        "Timestamp when the record was processed in Silver.",

    "aud_tx_camada_origem":
        "Source data layer used during Silver processing.",

    "aud_tx_tabela_origem":
        "Source Silver table used to derive parliamentary front membership facts.",

    "aud_tx_tabela_destino":
        "Destination Gold table where parliamentary front membership facts are persisted.",

    "aud_tx_versao_pipeline_silver":
        "Silver pipeline version used to generate the source record.",

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
    message="Gold parliamentary front members fact generated successfully.",
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
print("FATO FRENTES MEMBROS - RESUMO EXECUÇÃO")
print("=" * 80)

print(f"Records read: {records_read}")
print(f"Records eligible from Silver: {records_valid_silver}")
print(f"Records written: {records_written}")

print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# display(gold_df.limit(20))
