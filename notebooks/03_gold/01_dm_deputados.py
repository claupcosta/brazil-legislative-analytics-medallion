# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Gold — Deputados Dimension
# MAGIC
# MAGIC **Notebook:** `01_dm_deputados`
# MAGIC
# MAGIC Builds the curated Gold deputy dimension used by analytical models and business marts.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Deputy dimensional model
# MAGIC * Deputy surrogate key generation
# MAGIC * Most recent legislature selection
# MAGIC * Deputy descriptive attributes
# MAGIC * Party and state attributes
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
# MAGIC * Read validated deputy records from Silver
# MAGIC * Keep one analytical record per deputy
# MAGIC * Select the most recent legislature representation
# MAGIC * Create the deputy surrogate key
# MAGIC * Preserve business identifiers and descriptive attributes
# MAGIC * Preserve audit and traceability information
# MAGIC * Generate Gold execution metadata
# MAGIC * Apply governance comments
# MAGIC * Execute Gold quality validations
# MAGIC * Publish the Gold deputy dimension
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dimensional Model
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One record per deputy in the most recent legislature.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC `brazil_legislative_analytics.silver.slv_deputados`
# MAGIC
# MAGIC ### Target
# MAGIC
# MAGIC `brazil_legislative_analytics.gold.dm_deputados`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `dep_id_deputado`
# MAGIC
# MAGIC ### Surrogate Key
# MAGIC
# MAGIC `dep_sk_deputado`
# MAGIC
# MAGIC ### Main Analytical Attributes
# MAGIC
# MAGIC * Deputy Name
# MAGIC * Political Party
# MAGIC * State (UF)
# MAGIC * Legislature
# MAGIC * Institutional Email
# MAGIC * Deputy Photo URL
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC ### Rule 1 — Silver Valid Records
# MAGIC
# MAGIC Only records approved during Silver validation are eligible for Gold.
# MAGIC
# MAGIC ```python
# MAGIC dep_fl_registro_valido_silver = true
# MAGIC ```
# MAGIC
# MAGIC ### Rule 2 — Most Recent Legislature
# MAGIC
# MAGIC Only the most recent legislature representation for each deputy is maintained.
# MAGIC
# MAGIC ```python
# MAGIC dep_fl_legislatura_mais_recente = true
# MAGIC ```
# MAGIC
# MAGIC ### Rule 3 — One Record Per Deputy
# MAGIC
# MAGIC Duplicate deputy records are removed using deterministic ordering rules.
# MAGIC
# MAGIC ### Rule 4 — Governance Compliance
# MAGIC
# MAGIC All columns and tables must contain governance comments.
# MAGIC
# MAGIC ### Rule 5 — Traceability Preservation
# MAGIC
# MAGIC Bronze and Silver audit metadata must be preserved.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Controls
# MAGIC
# MAGIC The notebook validates:
# MAGIC
# MAGIC * Null business keys
# MAGIC * Null surrogate keys
# MAGIC * Null deputy names
# MAGIC * Duplicate deputy identifiers
# MAGIC * Invalid Gold records
# MAGIC * Governance comment coverage
# MAGIC
# MAGIC Execution is interrupted when critical validations fail.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC * Source data originates from Câmara dos Deputados open data.
# MAGIC * Gold dimensions are optimized for analytical consumption.
# MAGIC * Documentation and governance comments are written in English.
# MAGIC * Naming conventions follow project standards.
# MAGIC * Traceability fields are preserved across all Medallion layers.
# MAGIC * Gold dimensions serve as the foundation for Facts and Analytical Marts.
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC * `/docs/architecture/README.md`
# MAGIC * `/docs/decisions/silver_layer_strategy.md`
# MAGIC * `/docs/governance/data_quality.md`
# MAGIC * `/docs/operations/execution_guide.md`
# MAGIC * `/docs/changelog.md`
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

from datetime import datetime
import uuid

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# ============================================================
# COMPATIBILITY HELPERS
# ============================================================

def log_info_safe(message: str) -> None:
    """
    Uses the global logger when available.
    Falls back to print when the utility signature is different.
    """
    try:
        log_info(message)
    except Exception:
        print(message)


def apply_table_comment_safe(table_name: str, comment_text: str) -> None:
    """
    Uses global comment utility when available.
    Falls back to SQL COMMENT ON TABLE.
    """
    try:
        apply_table_comment(
            table_name=table_name,
            comment=comment_text,
        )
    except Exception:
        spark.sql(f"""
            COMMENT ON TABLE {table_name} IS '{comment_text}'
        """)


def apply_column_comments_safe(table_name: str, comments_dict: dict) -> None:
    """
    Uses global column comment utility when available.
    Falls back to ALTER COLUMN COMMENT.
    """
    try:
        apply_column_comments(
            table_name=table_name,
            column_comments=comments_dict,
        )
    except Exception:
        for column_name, comment_text in comments_dict.items():
            spark.sql(f"""
                ALTER TABLE {table_name}
                ALTER COLUMN {column_name}
                COMMENT '{comment_text}'
            """)


def sha2_concat_safe(columns: list):
    """
    Uses Spark deterministic SHA-256 hash expression.
    Kept compatible with global hash utilities.
    """
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(column_name).cast("string"), F.lit(""))
                for column_name in columns
            ]
        ),
        256
    )

# COMMAND ----------

# ============================================================
# EXECUTION CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "01_dm_deputados"
LAYER_NAME = "GOLD"
ENTITY_NAME = "deputados"

SOURCE_TABLE = f"{SILVER_SCHEMA}.slv_deputados"
TARGET_TABLE = f"{GOLD_SCHEMA}.dm_deputados"

GOLD_PIPELINE_VERSION = PROJECT_VERSION
GOLD_EXECUTION_ID = str(uuid.uuid4())
GOLD_PROCESSING_TIMESTAMP = datetime.now()

log_info_safe("=" * 80)
log_info_safe(f"{LAYER_NAME} {ENTITY_NAME.upper()} DIMENSION STARTED")
log_info_safe("=" * 80)
log_info_safe(f"Notebook: {NOTEBOOK_NAME}")
log_info_safe(f"Source Table: {SOURCE_TABLE}")
log_info_safe(f"Target Table: {TARGET_TABLE}")
log_info_safe(f"Execution ID: {GOLD_EXECUTION_ID}")
log_info_safe(f"Processing Timestamp: {GOLD_PROCESSING_TIMESTAMP}")
log_info_safe("=" * 80)

# COMMAND ----------

# ============================================================
# READ SOURCE DATA
# ============================================================

slv_deputados_df = spark.table(SOURCE_TABLE)

records_read = slv_deputados_df.count()

log_info_safe(f"Records read from Silver: {records_read}")

# COMMAND ----------

# ============================================================
# BUSINESS RULES
# ============================================================

valid_deputados_df = (
    slv_deputados_df
    .filter(F.col("dep_fl_registro_valido_silver") == F.lit(True))
    .filter(F.col("dep_fl_legislatura_mais_recente") == F.lit(True))
)

window_deputado = (
    Window
    .partitionBy("dep_id_deputado")
    .orderBy(
        F.col("aud_dh_processamento").desc_nulls_last(),
        F.col("dep_id_legislatura").desc_nulls_last()
    )
)

dedup_deputados_df = (
    valid_deputados_df
    .withColumn("dm_nr_linha_deputado", F.row_number().over(window_deputado))
    .filter(F.col("dm_nr_linha_deputado") == 1)
    .drop("dm_nr_linha_deputado")
)

records_valid = valid_deputados_df.count()
records_after_dedup = dedup_deputados_df.count()

log_info_safe(f"Records valid for Gold: {records_valid}")
log_info_safe(f"Records after deduplication: {records_after_dedup}")

# COMMAND ----------

# ============================================================
# BUILD GOLD DIMENSION
# ============================================================

dm_deputados_df = (
    dedup_deputados_df
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("dep_id_deputado").cast("string"), F.lit("")),
                F.coalesce(F.col("dep_id_legislatura").cast("string"), F.lit(""))
            ),
            256
        ).alias("dep_sk_deputado"),

        F.col("dep_id_deputado"),
        F.col("dep_tx_chave_deputado_legislatura"),
        F.col("dep_tx_uri"),
        F.col("dep_tx_nome"),
        F.col("dep_tx_sigla_partido"),
        F.col("dep_tx_uri_partido"),
        F.col("dep_tx_sigla_uf"),
        F.col("dep_id_legislatura"),
        F.col("dep_id_legislatura_referencia"),
        F.col("dep_fl_legislatura_mais_recente"),
        F.col("dep_tx_url_foto"),
        F.col("dep_tx_email"),

        F.col("dep_fl_id_valido"),
        F.col("dep_fl_legislatura_valida"),
        F.col("dep_fl_nome_valido"),
        F.col("dep_fl_partido_informado"),
        F.col("dep_fl_uf_informada"),
        F.col("dep_fl_email_informado"),

        F.lit(True).alias("dep_fl_registro_valido_gold"),

        F.col("aud_id_execucao_bronze"),
        F.col("aud_dh_ingestao_bronze"),
        F.col("aud_tx_endpoint_origem"),
        F.col("aud_tx_sistema_origem"),
        F.col("aud_tx_versao_pipeline_bronze"),
        F.col("aud_tx_tipo_carga_bronze"),
        F.col("aud_tx_hash_registro_bronze"),
        F.col("aud_id_execucao_silver"),
        F.col("aud_dh_processamento").alias("aud_dh_processamento_silver"),
        F.col("aud_tx_hash_registro_silver"),

        F.lit(GOLD_EXECUTION_ID).alias("aud_id_execucao_gold"),
        F.lit(GOLD_PROCESSING_TIMESTAMP).cast("timestamp").alias("aud_dh_processamento_gold"),
        F.lit("silver").alias("aud_tx_camada_origem"),
        F.lit("slv_deputados").alias("aud_tx_tabela_origem"),
        F.lit("dm_deputados").alias("aud_tx_tabela_destino"),
        F.lit(GOLD_PIPELINE_VERSION).alias("aud_tx_versao_pipeline_gold"),
        F.lit(
            "Gold deputy dimension derived from valid Silver deputy records, "
            "keeping one record per deputy in the most recent legislature."
        ).alias("aud_tx_regra_derivacao")
    )
    .withColumn(
        "aud_tx_hash_registro_gold",
        sha2_concat_safe(
            [
                "dep_sk_deputado",
                "dep_id_deputado",
                "dep_tx_nome",
                "dep_tx_sigla_partido",
                "dep_tx_sigla_uf",
                "dep_id_legislatura",
            ]
        )
    )
)

records_to_write = dm_deputados_df.count()

log_info_safe(f"Records prepared for Gold: {records_to_write}")

# COMMAND ----------

# ============================================================
# PRE-WRITE VALIDATIONS
# ============================================================

mandatory_invalid_count = (
    dm_deputados_df
    .filter(
        (F.col("dep_sk_deputado").isNull()) |
        (F.col("dep_sk_deputado") == "") |
        (F.col("dep_id_deputado").isNull()) |
        (F.col("dep_id_deputado") == "") |
        (F.col("dep_tx_nome").isNull()) |
        (F.col("dep_tx_nome") == "")
    )
    .count()
)

duplicate_key_count = (
    dm_deputados_df
    .groupBy("dep_id_deputado")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if mandatory_invalid_count > 0:
    raise ValueError(
        f"Gold validation failed: {mandatory_invalid_count} records have invalid mandatory fields."
    )

if duplicate_key_count > 0:
    raise ValueError(
        f"Gold validation failed: {duplicate_key_count} duplicate deputy identifiers found."
    )

log_info_safe("Gold validations passed before write.")
log_info_safe(f"Mandatory invalid records: {mandatory_invalid_count}")
log_info_safe(f"Duplicate deputy identifiers: {duplicate_key_count}")

# COMMAND ----------

# ============================================================
# CREATE GOLD SCHEMA
# ============================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# COMMAND ----------

# ============================================================
# WRITE GOLD TABLE
# ============================================================

(
    dm_deputados_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

records_written = spark.table(TARGET_TABLE).count()

log_info_safe(f"Records written to Gold: {records_written}")

# COMMAND ----------

# ============================================================
# GOVERNANCE COMMENTS
# ============================================================

table_comment = (
    "Gold deputy dimension containing one validated analytical record per deputy "
    "in the most recent legislature."
)

column_comments = {
    "dep_sk_deputado": "Surrogate deterministic key for the Gold deputy dimension.",
    "dep_id_deputado": "Deputy business identifier as provided by the Câmara dos Deputados source.",
    "dep_tx_chave_deputado_legislatura": "Natural key composed by deputy identifier and legislature identifier.",
    "dep_tx_uri": "Source URI for the deputy record.",
    "dep_tx_nome": "Standardized deputy name.",
    "dep_tx_sigla_partido": "Standardized political party acronym associated with the deputy.",
    "dep_tx_uri_partido": "Source URI for the deputy political party when available.",
    "dep_tx_sigla_uf": "Standardized Brazilian state acronym associated with the deputy.",
    "dep_id_legislatura": "Legislature identifier associated with the selected deputy record.",
    "dep_id_legislatura_referencia": "Reference legislature used during source extraction.",
    "dep_fl_legislatura_mais_recente": "Flag indicating whether the record belongs to the most recent legislature for the deputy.",
    "dep_tx_url_foto": "Deputy photo URL.",
    "dep_tx_email": "Deputy institutional email.",
    "dep_fl_id_valido": "Flag indicating whether the deputy identifier is valid.",
    "dep_fl_legislatura_valida": "Flag indicating whether the legislature identifier is valid.",
    "dep_fl_nome_valido": "Flag indicating whether the deputy name is valid.",
    "dep_fl_partido_informado": "Flag indicating whether party information is available.",
    "dep_fl_uf_informada": "Flag indicating whether UF information is available.",
    "dep_fl_email_informado": "Flag indicating whether email information is available.",
    "dep_fl_registro_valido_gold": "Flag indicating whether the record passed Gold validation rules.",
    "aud_id_execucao_bronze": "Bronze execution identifier inherited from source ingestion.",
    "aud_dh_ingestao_bronze": "Bronze ingestion timestamp inherited from source ingestion.",
    "aud_tx_endpoint_origem": "Source endpoint used during Bronze ingestion.",
    "aud_tx_sistema_origem": "Source system identified during Bronze ingestion.",
    "aud_tx_versao_pipeline_bronze": "Bronze pipeline version inherited from source ingestion.",
    "aud_tx_tipo_carga_bronze": "Bronze load type inherited from source ingestion.",
    "aud_tx_hash_registro_bronze": "Deterministic Bronze record hash inherited for traceability.",
    "aud_id_execucao_silver": "Silver execution identifier inherited from source transformation.",
    "aud_dh_processamento_silver": "Timestamp when the record was processed in Silver.",
    "aud_tx_hash_registro_silver": "Deterministic Silver record hash inherited for traceability.",
    "aud_id_execucao_gold": "Gold execution identifier for this processing run.",
    "aud_dh_processamento_gold": "Timestamp when the record was processed in Gold.",
    "aud_tx_camada_origem": "Source Medallion layer used to build the Gold dimension.",
    "aud_tx_tabela_origem": "Source Silver table used to build the Gold dimension.",
    "aud_tx_tabela_destino": "Target Gold table name.",
    "aud_tx_versao_pipeline_gold": "Pipeline version used during Gold processing.",
    "aud_tx_regra_derivacao": "Description of the Gold derivation rule applied to the record.",
    "aud_tx_hash_registro_gold": "Deterministic Gold record hash used for traceability and change detection.",
}

apply_table_comment_safe(
    table_name=TARGET_TABLE,
    comment_text=table_comment,
)

apply_column_comments_safe(
    table_name=TARGET_TABLE,
    comments_dict=column_comments,
)

log_info_safe("Governance comments applied successfully.")

# COMMAND ----------

# ============================================================
# POST-WRITE VALIDATIONS
# ============================================================

dm_deputados_gold_df = spark.table(TARGET_TABLE)

total_gold_records = dm_deputados_gold_df.count()

total_invalid_gold_records = (
    dm_deputados_gold_df
    .filter(
        (F.col("dep_sk_deputado").isNull()) |
        (F.col("dep_id_deputado").isNull()) |
        (F.col("dep_tx_nome").isNull()) |
        (F.col("dep_fl_registro_valido_gold") != F.lit(True))
    )
    .count()
)

total_duplicate_deputies = (
    dm_deputados_gold_df
    .groupBy("dep_id_deputado")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

total_without_party = (
    dm_deputados_gold_df
    .filter(
        (F.col("dep_tx_sigla_partido").isNull()) |
        (F.col("dep_tx_sigla_partido") == "")
    )
    .count()
)

total_without_uf = (
    dm_deputados_gold_df
    .filter(
        (F.col("dep_tx_sigla_uf").isNull()) |
        (F.col("dep_tx_sigla_uf") == "")
    )
    .count()
)

log_info_safe("=" * 80)
log_info_safe("POST-WRITE VALIDATION RESULTS")
log_info_safe("=" * 80)
log_info_safe(f"Total Gold records: {total_gold_records}")
log_info_safe(f"Invalid Gold records: {total_invalid_gold_records}")
log_info_safe(f"Duplicate deputy identifiers: {total_duplicate_deputies}")
log_info_safe(f"Deputies without party: {total_without_party}")
log_info_safe(f"Deputies without UF: {total_without_uf}")
log_info_safe("=" * 80)

if total_invalid_gold_records > 0:
    raise ValueError("Post-write validation failed: invalid Gold records found.")

if total_duplicate_deputies > 0:
    raise ValueError("Post-write validation failed: duplicate deputy identifiers found.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation 1: Total records
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_deputados
# MAGIC FROM brazil_legislative_analytics.gold.dm_deputados;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation 2: Mandatory fields
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_invalidos
# MAGIC FROM brazil_legislative_analytics.gold.dm_deputados
# MAGIC WHERE dep_sk_deputado IS NULL
# MAGIC    OR dep_id_deputado IS NULL
# MAGIC    OR dep_id_deputado = ''
# MAGIC    OR dep_tx_nome IS NULL
# MAGIC    OR dep_tx_nome = '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation 3: Duplicate deputy identifiers
# MAGIC SELECT
# MAGIC     dep_id_deputado,
# MAGIC     COUNT(*) AS total
# MAGIC FROM brazil_legislative_analytics.gold.dm_deputados
# MAGIC GROUP BY dep_id_deputado
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation 4: Distribution by party
# MAGIC SELECT
# MAGIC     dep_tx_sigla_partido,
# MAGIC     COUNT(*) AS total_deputados
# MAGIC FROM brazil_legislative_analytics.gold.dm_deputados
# MAGIC GROUP BY dep_tx_sigla_partido
# MAGIC ORDER BY total_deputados DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation 5: Distribution by UF
# MAGIC SELECT
# MAGIC     dep_tx_sigla_uf,
# MAGIC     COUNT(*) AS total_deputados
# MAGIC FROM brazil_legislative_analytics.gold.dm_deputados
# MAGIC GROUP BY dep_tx_sigla_uf
# MAGIC ORDER BY total_deputados DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation 6: Governance comments coverage
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     column_name,
# MAGIC     data_type,
# MAGIC     comment
# MAGIC FROM brazil_legislative_analytics.information_schema.columns
# MAGIC WHERE table_schema = 'gold'
# MAGIC   AND table_name = 'dm_deputados'
# MAGIC   AND comment IS NULL
# MAGIC ORDER BY ordinal_position;

# COMMAND ----------

# ============================================================
# FINAL EXECUTION SUMMARY
# ============================================================

log_info_safe("=" * 80)
log_info_safe("GOLD DEPUTADOS DIMENSION COMPLETED")
log_info_safe("=" * 80)
log_info_safe(f"Source Table: {SOURCE_TABLE}")
log_info_safe(f"Target Table: {TARGET_TABLE}")
log_info_safe("Grain: one record per deputy in the most recent legislature")
log_info_safe(f"Records Read: {records_read}")
log_info_safe(f"Records Valid for Gold: {records_valid}")
log_info_safe(f"Records Written: {records_written}")
log_info_safe(f"Invalid Records: {total_invalid_gold_records}")
log_info_safe(f"Duplicate Deputies: {total_duplicate_deputies}")
log_info_safe(f"Execution ID: {GOLD_EXECUTION_ID}")
log_info_safe(f"Pipeline Version: {GOLD_PIPELINE_VERSION}")
log_info_safe("=" * 80)
