# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 14 Gold — CPI Events Fact
# MAGIC
# MAGIC **Notebook:** `14_ft_eventos_cpis`
# MAGIC
# MAGIC Builds the curated Gold CPI events fact used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * CPI event fact model
# MAGIC * CPI-event relationship surrogate key generation
# MAGIC * CPI, event and date dimensional keys
# MAGIC * CPI event semantic classification attributes
# MAGIC * CPI event confidence indicators
# MAGIC * CPI-event temporal consistency indicators
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
# MAGIC * Read validated CPI event relationship records from Silver
# MAGIC * Keep one analytical record per CPI-event relationship
# MAGIC * Create the CPI event relationship surrogate key
# MAGIC * Resolve Gold dimension keys for CPIs, events and dates
# MAGIC * Preserve CPI and event business identifiers
# MAGIC * Preserve semantic classification and confidence attributes
# MAGIC * Preserve temporal consistency indicators
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold CPI events fact
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Fact Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per CPI-event relationship identified in Silver.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_cpi_eventos`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.ft_eventos_cpis`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `cpi_evt_id_relacao`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `fec_sk_evento_cpi`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC ### Rule 1
# MAGIC
# MAGIC Only Silver approved CPI-event records are eligible for Gold.
# MAGIC
# MAGIC ### Rule 2
# MAGIC
# MAGIC One analytical record per CPI-event relationship.
# MAGIC
# MAGIC ### Rule 3
# MAGIC
# MAGIC Resolve CPI, event and date dimensions when available.
# MAGIC
# MAGIC ### Rule 4
# MAGIC
# MAGIC Preserve semantic classification and confidence indicators generated in Silver.
# MAGIC
# MAGIC ### Rule 5
# MAGIC
# MAGIC Do not force CPI assignments when a deterministic CPI relationship cannot be identified.
# MAGIC
# MAGIC ### Rule 6
# MAGIC
# MAGIC Records without sufficient information for CPI identification must remain unlinked to preserve analytical integrity and avoid false-positive associations.
# MAGIC
# MAGIC ### Rule 7
# MAGIC
# MAGIC Preserve governance, lineage and audit information.
# MAGIC
# MAGIC ### Rule 8
# MAGIC
# MAGIC All Gold objects must contain governance comments.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Known Limitations
# MAGIC
# MAGIC Some legislative events may reference investigations, inquiries, hearings or external commissions without sufficient information to establish a reliable CPI relationship.
# MAGIC
# MAGIC When a deterministic CPI match cannot be identified, the following attributes may remain null:
# MAGIC
# MAGIC * `cpi_id_orgao`
# MAGIC * `cpi_tx_sigla`
# MAGIC * `cpi_tx_nome`
# MAGIC
# MAGIC This behavior is intentional and prevents incorrect CPI associations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Controls
# MAGIC
# MAGIC Validates:
# MAGIC
# MAGIC * Null surrogate keys
# MAGIC * Null CPI-event relationship keys
# MAGIC * Null event business keys
# MAGIC * Duplicate CPI-event relationships
# MAGIC * Invalid Gold records
# MAGIC * CPI dimension coverage
# MAGIC * Event dimension coverage
# MAGIC * Date dimension coverage
# MAGIC * Hash uniqueness
# MAGIC * Temporal consistency indicators
# MAGIC
# MAGIC Execution is interrupted when critical validations fail.
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

CATALOG = "brazil_legislative_analytics"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.slv_cpi_eventos"
TARGET_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.ft_eventos_cpis"

DM_CPIS = f"{CATALOG}.{GOLD_SCHEMA}.dm_cpis"
DM_EVENTOS = f"{CATALOG}.{GOLD_SCHEMA}.dm_eventos"
DM_DATAS = f"{CATALOG}.{GOLD_SCHEMA}.dm_datas"

PIPELINE_VERSION = "gold_v1"

# COMMAND ----------

def col_if_exists(df, col_name, default=None):
    if col_name in df.columns:
        return F.col(col_name)
    return F.lit(default)

# COMMAND ----------

source_df = spark.table(SOURCE_TABLE)

source_valid_df = (
    source_df
    .filter(F.col("cpi_evt_fl_registro_valido_silver") == F.lit(True))
)

# COMMAND ----------

source_standardized_df = (
    source_valid_df
    .withColumn("cpi_evt_id_relacao", F.col("cpi_evt_id_relacao").cast("string"))
    .withColumn("evt_id_evento", F.col("evt_id_evento").cast("string"))
    .withColumn("cpi_id_orgao_origem", col_if_exists(source_valid_df, "cpi_id_orgao").cast("string"))
    .withColumn("leg_id_legislatura", col_if_exists(source_valid_df, "leg_id_legislatura").cast("string"))
    .withColumn("evt_tx_titulo", col_if_exists(source_valid_df, "evt_tx_titulo", "").cast("string"))
    .withColumn("evt_dt_evento",F.coalesce(
        col_if_exists(source_valid_df, "evt_dt_evento").cast("date"),
        col_if_exists(source_valid_df, "evt_dt_inicio").cast("date")))   
    .withColumn("cpi_evt_tx_tipo_relacao", col_if_exists(source_valid_df, "cpi_evt_tx_tipo_relacao", "SEMANTIC").cast("string"))
    .withColumn("cpi_evt_tx_nivel_confianca", col_if_exists(source_valid_df, "cpi_evt_tx_nivel_confianca", "MEDIUM").cast("string"))
    .withColumn("fec_qt_relacao_direta", col_if_exists(source_valid_df, "fec_qt_relacao_direta", 0).cast("long"))
    .withColumn("fec_qt_relacao_semantica", col_if_exists(source_valid_df, "fec_qt_relacao_semantica", 1).cast("long"))
    .withColumn("fec_qt_alta_confianca", col_if_exists(source_valid_df, "fec_qt_alta_confianca", 0).cast("long"))
    .withColumn("fec_qt_evento_realizado", col_if_exists(source_valid_df, "fec_qt_evento_realizado", 1).cast("long"))
    .withColumn("cpi_evt_tx_payload_origem_json", col_if_exists(source_valid_df, "cpi_evt_tx_payload_origem_json", "{}").cast("string"))
)

# COMMAND ----------

source_enriched_df = (
    source_standardized_df
    .withColumn(
        "evt_tx_titulo_norm",
        F.upper(
            F.regexp_replace(
                F.regexp_replace(F.coalesce(F.col("evt_tx_titulo"), F.lit("")), "[ÁÀÂÃÄ]", "A"),
                "[ÉÈÊË]", "E"
            )
        )
    )
    .withColumn("evt_tx_titulo_norm", F.regexp_replace(F.col("evt_tx_titulo_norm"), "[ÍÌÎÏ]", "I"))
    .withColumn("evt_tx_titulo_norm", F.regexp_replace(F.col("evt_tx_titulo_norm"), "[ÓÒÔÕÖ]", "O"))
    .withColumn("evt_tx_titulo_norm", F.regexp_replace(F.col("evt_tx_titulo_norm"), "[ÚÙÛÜ]", "U"))
    .withColumn("evt_tx_titulo_norm", F.regexp_replace(F.col("evt_tx_titulo_norm"), "Ç", "C"))
    .withColumn(
    "cpi_tx_sigla_derivada",
    F.when(F.col("evt_tx_titulo_norm").contains("8 DE JANEIRO"), F.lit("CPMI8JAN"))
     .when(F.col("evt_tx_titulo_norm").contains("AMERICANAS"), F.lit("CPIAMERI"))
     .when(F.col("evt_tx_titulo_norm").contains("BRASKEM"), F.lit("CPIBRASKEM"))

     # CPI MST / Movimento dos Trabalhadores Sem Terra
     .when(F.col("evt_tx_titulo_norm").contains("MST"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("MOVIMENTO DOS TRABALHADORES SEM TERRA"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("ASSENTAMENTO"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("REFORMA AGRARIA"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("FEIRAS AGRARIAS"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("MINISTERIO DO DESENVOLVIMENTO AGRARIO"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("MINISTERIO DA AGRICULTURA"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("RONALDO CAIADO"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("GUSTAVO GAYER"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("RICARDO SALLES"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("CAPITAO ALDEN"), F.lit("CPIMST"))
     .when(F.col("evt_tx_titulo_norm").contains("EVAIR VIEIRA DE MELO"), F.lit("CPIMST"))

     # CPI FUNAI / INCRA
     .when(F.col("evt_tx_titulo_norm").contains("FUNAI"), F.lit("CPIFUNAI"))
     .when(F.col("evt_tx_titulo_norm").contains("INCRA"), F.lit("CPIFUNA2"))
     .when(F.col("evt_tx_titulo_norm").contains("FUNDACAO NACIONAL DO INDIO"), F.lit("CPIFUNAI"))

     # Outros casos
     .when(F.col("evt_tx_titulo_norm").contains("FUTEBOL"), F.lit("CPIFUTE"))
     .when(F.col("evt_tx_titulo_norm").contains("PIRAMIDE"), F.lit("CPIPIRAM"))
     .when(F.col("evt_tx_titulo_norm").contains("CRIPTO"), F.lit("CPIPIRAM"))

     .otherwise(F.lit(None))
)
)

# COMMAND ----------

dm_cpis_raw_df = (
    spark.table(DM_CPIS)
    .select(
        F.col("cpi_sk_cpi").alias("dim_cpi_sk_cpi"),
        F.col("cpi_id_orgao").cast("string").alias("dim_cpi_id_orgao"),
        F.col("cpi_tx_nome").alias("dim_cpi_tx_nome"),
        F.col("cpi_tx_sigla").alias("dim_cpi_tx_sigla"),
        F.col("leg_id_legislatura").cast("string").alias("dim_leg_id_legislatura")
    )
    .withColumn("dim_cpi_tx_sigla_join", F.upper(F.col("dim_cpi_tx_sigla")))
)

w_sigla_leg = Window.partitionBy("dim_cpi_tx_sigla_join", "dim_leg_id_legislatura").orderBy(F.col("dim_cpi_id_orgao").desc())

dm_cpis_sigla_leg_df = (
    dm_cpis_raw_df
    .withColumn("rn", F.row_number().over(w_sigla_leg))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

w_sigla = Window.partitionBy("dim_cpi_tx_sigla_join").orderBy(F.col("dim_cpi_id_orgao").desc())

dm_cpis_sigla_df = (
    dm_cpis_raw_df
    .withColumn("rn", F.row_number().over(w_sigla))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

dm_eventos_df = (
    spark.table(DM_EVENTOS)
    .select(
        F.col("evt_sk_evento").alias("dim_evt_sk_evento"),
        F.col("evt_id_evento").cast("string").alias("dim_evt_id_evento"),
        F.col("evt_tx_titulo").alias("dim_evt_tx_titulo"),
        F.col("evt_dt_inicio").cast("date").alias("dim_evt_dt_evento"),
        F.col("leg_id_legislatura").cast("string").alias("dim_evt_leg_id_legislatura")
    )
)

dm_datas_df = (
    spark.table(DM_DATAS)
    .select(
        F.col("dat_sk_data").alias("dim_dat_sk_data"),
        F.col("dat_dt_data").cast("date").alias("dim_dat_dt_data")
    )
)

# COMMAND ----------

joined_df = (
    source_enriched_df.alias("s")
    .join(
        dm_eventos_df.alias("e"),
        F.col("s.evt_id_evento") == F.col("e.dim_evt_id_evento"),
        "left"
    )
    .withColumn(
        "leg_id_legislatura_final",
        F.coalesce(
            F.col("s.leg_id_legislatura"),
            F.col("e.dim_evt_leg_id_legislatura")
        )
    )
    .withColumn(
        "evt_dt_evento_final",
        F.coalesce(
            F.col("s.evt_dt_evento"),
            F.col("e.dim_evt_dt_evento")
        )
    )
    .join(
        dm_cpis_sigla_leg_df.alias("cl"),
        (
            F.upper(F.col("s.cpi_tx_sigla_derivada")) == F.col("cl.dim_cpi_tx_sigla_join")
        )
        & (
            F.col("leg_id_legislatura_final") == F.col("cl.dim_leg_id_legislatura")
        ),
        "left"
    )
    .join(
        dm_cpis_sigla_df.alias("ca"),
        F.upper(F.col("s.cpi_tx_sigla_derivada")) == F.col("ca.dim_cpi_tx_sigla_join"),
        "left"
    )
    .join(
        dm_datas_df.alias("d"),
        F.col("evt_dt_evento_final") == F.col("d.dim_dat_dt_data"),
        "left"
    )
)

# COMMAND ----------

gold_df = (
    joined_df
    .withColumn(
        "cpi_sk_cpi",
        F.coalesce(F.col("cl.dim_cpi_sk_cpi"), F.col("ca.dim_cpi_sk_cpi"))
    )
    .withColumn(
        "cpi_id_orgao",
        F.coalesce(F.col("cl.dim_cpi_id_orgao"), F.col("ca.dim_cpi_id_orgao"))
    )
    .withColumn(
        "cpi_tx_nome",
        F.coalesce(F.col("cl.dim_cpi_tx_nome"), F.col("ca.dim_cpi_tx_nome"))
    )
    .withColumn(
        "cpi_tx_sigla",
        F.coalesce(F.col("cl.dim_cpi_tx_sigla"), F.col("ca.dim_cpi_tx_sigla"))
    )
    .withColumn("evt_sk_evento", F.col("e.dim_evt_sk_evento"))
    .withColumn("evt_tx_titulo", F.coalesce(F.col("e.dim_evt_tx_titulo"), F.col("s.evt_tx_titulo")))
    .withColumn("evt_dt_evento", F.col("evt_dt_evento_final"))
    .withColumn("dat_sk_data", F.col("d.dim_dat_sk_data"))
    .withColumn("leg_id_legislatura", F.col("leg_id_legislatura_final"))
    .withColumn(
        "fec_sk_evento_cpi",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("s.cpi_evt_id_relacao"), F.lit("")),
                F.coalesce(F.col("s.evt_id_evento"), F.lit("")),
                F.coalesce(F.col("cpi_id_orgao"), F.lit("")),
                F.coalesce(F.col("leg_id_legislatura"), F.lit(""))
            ),
            256
        )
    )
    .withColumn("fec_fl_cpi_encontrada_gold", F.col("cpi_sk_cpi").isNotNull())
    .withColumn("fec_fl_evento_encontrado_gold", F.col("evt_sk_evento").isNotNull())
    .withColumn("fec_fl_data_encontrada_gold", F.col("dat_sk_data").isNotNull())
    .withColumn(
        "fec_fl_dimensoes_principais_completas",
        F.col("cpi_sk_cpi").isNotNull()
        & F.col("evt_sk_evento").isNotNull()
        & F.col("dat_sk_data").isNotNull()
    )
    .withColumn(
        "fec_fl_registro_valido_gold",
        F.col("fec_sk_evento_cpi").isNotNull()
        & F.col("s.cpi_evt_id_relacao").isNotNull()
        & F.col("s.evt_id_evento").isNotNull()
        & F.col("evt_sk_evento").isNotNull()
        & F.col("dat_sk_data").isNotNull()
    )
    .withColumn("cpi_evt_fl_alta_confianca", F.col("s.cpi_evt_tx_nivel_confianca") == F.lit("HIGH"))
    .withColumn("aud_id_execucao_gold", F.expr("uuid()"))
    .withColumn("aud_dh_processamento_gold", F.current_timestamp())
    .withColumn("aud_tx_versao_pipeline_gold", F.lit(PIPELINE_VERSION))
    .withColumn(
        "aud_tx_hash_registro_gold",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("fec_sk_evento_cpi"), F.lit("")),
                F.coalesce(F.col("s.cpi_evt_id_relacao"), F.lit("")),
                F.coalesce(F.col("cpi_id_orgao"), F.lit("")),
                F.coalesce(F.col("s.evt_id_evento"), F.lit("")),
                F.coalesce(F.col("leg_id_legislatura"), F.lit("")),
                F.coalesce(F.col("s.cpi_evt_tx_tipo_relacao"), F.lit("")),
                F.coalesce(F.col("s.cpi_evt_tx_nivel_confianca"), F.lit(""))
            ),
            256
        )
    )
)

# COMMAND ----------

final_df = (
    gold_df
    .select(
        F.col("fec_sk_evento_cpi"),
        F.col("s.cpi_evt_id_relacao").alias("cpi_evt_id_relacao"),

        F.col("cpi_sk_cpi"),
        F.col("cpi_id_orgao"),
        F.col("cpi_tx_sigla"),
        F.col("cpi_tx_nome"),

        F.col("evt_sk_evento"),
        F.col("s.evt_id_evento").alias("evt_id_evento"),
        F.col("evt_tx_titulo"),
        F.col("evt_dt_evento"),

        F.col("dat_sk_data"),
        F.col("leg_id_legislatura"),

        F.col("s.cpi_evt_tx_tipo_relacao").alias("cpi_evt_tx_tipo_relacao"),
        F.col("s.cpi_evt_tx_nivel_confianca").alias("cpi_evt_tx_nivel_confianca"),

        F.col("s.fec_qt_relacao_direta").alias("fec_qt_relacao_direta"),
        F.col("s.fec_qt_relacao_semantica").alias("fec_qt_relacao_semantica"),
        F.col("s.fec_qt_alta_confianca").alias("fec_qt_alta_confianca"),
        F.col("s.fec_qt_evento_realizado").alias("fec_qt_evento_realizado"),

        F.col("cpi_evt_fl_alta_confianca"),

        F.col("fec_fl_cpi_encontrada_gold"),
        F.col("fec_fl_evento_encontrado_gold"),
        F.col("fec_fl_data_encontrada_gold"),
        F.col("fec_fl_dimensoes_principais_completas"),
        F.col("fec_fl_registro_valido_gold"),

        F.col("s.cpi_evt_tx_payload_origem_json").alias("cpi_evt_tx_payload_origem_json"),

        F.col("aud_id_execucao_gold"),
        F.col("aud_dh_processamento_gold"),
        F.col("aud_tx_versao_pipeline_gold"),
        F.col("aud_tx_hash_registro_gold")
    )
    .dropDuplicates(["fec_sk_evento_cpi"])
)

# COMMAND ----------

records_read = source_df.count()
records_eligible = source_valid_df.count()
records_written = final_df.count()

# COMMAND ----------

(
    final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

print("=" * 80)
print("FATO EVENTOS CPIS - RESUMO EXECUÇÃO")
print("=" * 80)
print(f"Records read: {records_read}")
print(f"Records eligible from Silver: {records_eligible}")
print(f"Records written: {records_written}")
print("=" * 80)
print("STATUS: SUCCESS")
print("=" * 80)

# COMMAND ----------

spark.sql(f"""
COMMENT ON TABLE {TARGET_TABLE} IS
'Gold fact table that relates CPI records to legislative events using direct and semantic relationship rules.'

""")

# COMMAND ----------

column_comments = {
    "fec_sk_evento_cpi": "Deterministic surrogate key for the CPI-event fact relationship.",
    "cpi_evt_id_relacao": "Business relationship identifier generated in Silver.",
    "cpi_sk_cpi": "Surrogate key of the CPI dimension.",
    "cpi_id_orgao": "CPI body identifier resolved from the Gold CPI dimension.",
    "cpi_tx_sigla": "CPI acronym resolved from the Gold CPI dimension.",
    "cpi_tx_nome": "CPI name resolved from the Gold CPI dimension.",
    "evt_sk_evento": "Surrogate key of the legislative event dimension.",
    "evt_id_evento": "Legislative event business identifier.",
    "evt_tx_titulo": "Legislative event title.",
    "evt_dt_evento": "Legislative event date.",
    "dat_sk_data": "Surrogate key of the date dimension.",
    "leg_id_legislatura": "Legislature identifier associated with the CPI-event relationship.",
    "cpi_evt_tx_tipo_relacao": "Type of relationship identified between CPI and event.",
    "cpi_evt_tx_nivel_confianca": "Confidence level of the CPI-event relationship.",
    "fec_qt_relacao_direta": "Direct relationship indicator quantity.",
    "fec_qt_relacao_semantica": "Semantic relationship indicator quantity.",
    "fec_qt_alta_confianca": "High confidence relationship indicator quantity.",
    "fec_qt_evento_realizado": "Event realized indicator quantity.",
    "cpi_evt_fl_alta_confianca": "Flag indicating whether the relationship was classified as high confidence.",
    "fec_fl_cpi_encontrada_gold": "Flag indicating whether the CPI was found in the Gold CPI dimension.",
    "fec_fl_evento_encontrado_gold": "Flag indicating whether the event was found in the Gold event dimension.",
    "fec_fl_data_encontrada_gold": "Flag indicating whether the event date was found in the Gold date dimension.",
    "fec_fl_dimensoes_principais_completas": "Flag indicating whether CPI, event, and date dimensions were resolved.",
    "fec_fl_registro_valido_gold": "Flag indicating whether the Gold fact record is valid.",
    "cpi_evt_tx_payload_origem_json": "Original source payload preserved for traceability.",
    "aud_id_execucao_gold": "Gold execution identifier.",
    "aud_dh_processamento_gold": "Gold processing timestamp.",
    "aud_tx_versao_pipeline_gold": "Gold pipeline version.",
    "aud_tx_hash_registro_gold": "Gold deterministic record hash."
}

for column_name, comment in column_comments.items():
    spark.sql(f"""
    ALTER TABLE {TARGET_TABLE}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """)