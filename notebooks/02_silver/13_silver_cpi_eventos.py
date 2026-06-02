# Databricks notebook source
# MAGIC %md
# MAGIC # 13 Silver — CPI Eventos Standardization
# MAGIC
# MAGIC **Notebook:** `13_silver_cpi_eventos`
# MAGIC
# MAGIC Identifies legislative events potentially related to Parliamentary Inquiry Commissions (CPI) using direct relationship validation, governed semantic classification rules and confidence-based filtering, then persists validated, deduplicated and analytics-ready CPI event records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * CPI event derivation rules from `slv_eventos`, `slv_cpis` and `slv_orgaos`
# MAGIC * Direct CPI relationship validation when source identifiers allow explicit matching
# MAGIC * Semantic CPI detection using governed textual classification rules
# MAGIC * CPI keyword classification logic
# MAGIC * Parliamentary inquiry keyword classification logic
# MAGIC * Confidence score assignment for CPI event candidates
# MAGIC * Confidence level classification (HIGH, MEDIUM, LOW)
# MAGIC * Event date and status normalization
# MAGIC * Quality validation rules
# MAGIC * Rejected records tracking using global utilities
# MAGIC * Technical duplicate tracking
# MAGIC * Silver Delta persistence logic
# MAGIC * Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Responsibilities
# MAGIC
# MAGIC * Read CPI records from Silver layer
# MAGIC * Read legislative event records from Silver layer
# MAGIC * Read legislative body records from Silver layer
# MAGIC * Validate whether events are directly linked to CPI entities
# MAGIC * Identify CPI-related events through governed semantic rules
# MAGIC * Classify matches according to confidence levels
# MAGIC * Preserve CPI context when direct relationships exist
# MAGIC * Preserve source lineage and auditability
# MAGIC * Validate mandatory CPI event candidate fields
# MAGIC * Filter low-confidence semantic matches
# MAGIC * Remove technical duplicate records
# MAGIC * Register rejected and discarded records for traceability
# MAGIC * Persist curated Delta table
# MAGIC * Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Notes
# MAGIC
# MAGIC * CPI events are derived entities and do not require a dedicated Bronze table
# MAGIC * Source data comes from `silver.slv_eventos`, `silver.slv_cpis` and `silver.slv_orgaos`
# MAGIC * Direct CPI relationship validation is attempted whenever source identifiers support matching
# MAGIC * Current source data may not provide explicit CPI-event relationships
# MAGIC * Semantic detection is used when direct relationships are unavailable
# MAGIC * Semantic rules use controlled terms such as `CPI`, `CPMI`, `COMISSAO PARLAMENTAR DE INQUERITO`, `INQUERITO` and `INVESTIGACAO`
# MAGIC * Confidence scoring is used to reduce false-positive classifications
# MAGIC * HIGH confidence matches represent strong CPI evidence
# MAGIC * MEDIUM confidence matches represent inquiry-related evidence requiring analytical caution
# MAGIC * LOW confidence candidates are discarded and registered as rejected records
# MAGIC * Records are not forced into CPI relationships when source evidence is insufficient
# MAGIC * This design prioritizes analytical precision over record volume
# MAGIC * Rejected records remain available for governance, audit and future rule improvements
# MAGIC * Comments and documentation are written in English
# MAGIC * Naming conventions follow Portuguese mnemonic standards
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC * `/docs/decisions/silver_layer_strategy.md`
# MAGIC * `/docs/governance/data_quality.md`
# MAGIC * `/docs/governance/traceability.md`
# MAGIC * `/docs/operations/execution_guide.md`
# MAGIC * `/docs/standards/naming_conventions.md`
# MAGIC

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_comments

# COMMAND ----------

# MAGIC %run ../99_utils/utils_rejected_records

# COMMAND ----------

# ==========================================================================================
# 13 Silver — CPI Eventos Standardization
# Notebook: 13_silver_cpi_eventos
# ==========================================================================================
#
# Identifies legislative events potentially related to Parliamentary Inquiry
# Commissions using direct relationship rules and governed semantic/textual rules,
# then persists validated, deduplicated and analytics-ready CPI event records into
# the Silver layer.
#
# Responsibilities:
# - Read CPI records from Silver layer
# - Read legislative event records from Silver layer
# - Validate whether events are directly linked to CPI bodies
# - Identify CPI-related events through governed semantic rules
# - Classify matches as direct, high-confidence, medium-confidence or low-confidence
# - Identify probable semantic false positives
# - Preserve CPI context when a direct CPI relationship exists
# - Preserve event context and source lineage
# - Validate mandatory CPI event candidate fields
# - Remove technical duplicate records
# - Register rejected and discarded records for traceability
# - Persist curated Delta table
# - Apply governance comments to table and columns
#
# Notes:
# - CPI events are derived entities and do not require a dedicated Bronze table
# - Source data comes from silver.slv_eventos, silver.slv_cpis and silver.slv_orgaos
# - Direct CPI relationship is attempted using evt_id_orgao = cpi_id_orgao
# - When direct relationship is unavailable, governed semantic detection is applied
# - Probable false positives are not persisted as valid Silver CPI events
# - This design preserves analytical integrity and avoids false CPI joins
# ==========================================================================================


# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    current_timestamp,
    row_number,
    when,
    year,
    month,
    coalesce,
    concat_ws,
    sha2,
    to_json,
    struct,
    regexp_replace,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType

# COMMAND ----------

# ==========================================================================================
# Initialize Spark session explicitly for utility notebooks
# ==========================================================================================

spark = SparkSession.getActiveSession()

if spark is None:
    spark = SparkSession.builder.getOrCreate()

globals()["spark"] = spark

write_pipeline_log.__globals__["spark"] = spark

clean_rejected_records_for_entity.__globals__["spark"] = spark
persist_rejected_records.__globals__["spark"] = spark
clean_and_persist_rejected_records.__globals__["spark"] = spark
build_mandatory_rejected_records.__globals__["spark"] = spark
build_duplicate_rejected_records.__globals__["spark"] = spark
union_rejected_records.__globals__["spark"] = spark

apply_table_comment.__globals__["spark"] = spark
apply_column_comment.__globals__["spark"] = spark
apply_column_comments.__globals__["spark"] = spark
apply_governance_comments.__globals__["spark"] = spark
generate_missing_comment_report.__globals__["spark"] = spark

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("13 - SILVER CPI EVENTOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ==========================================================================================
# 1. Global Configuration
# ==========================================================================================

NOTEBOOK_NAME = "13_silver_cpi_eventos"
LAYER_NAME = "silver"
ENTITY_NAME = "cpi_eventos"

SOURCE_CPI_TABLE = get_silver_table(SILVER_TABLES["cpis"])
SOURCE_EVENTOS_TABLE = get_silver_table(SILVER_TABLES["eventos"])
SOURCE_ORGAOS_TABLE = get_silver_table(SILVER_TABLES["orgaos"])

if "cpi_eventos" in SILVER_TABLES:
    TARGET_TABLE = get_silver_table(SILVER_TABLES["cpi_eventos"])
else:
    TARGET_TABLE = get_silver_table("slv_cpi_eventos")

REJECTED_TABLE = get_silver_table(SILVER_TABLES["registros_rejeitados"])

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

APPLY_GOVERNANCE_COMMENTS = True

records_read = None
records_written = None
records_rejected = None

# COMMAND ----------

# ==========================================================================================
# 2. Start Pipeline Log
# ==========================================================================================

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver CPI events semantic derivation started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver CPI events semantic derivation.",
)

# COMMAND ----------

# ==========================================================================================
# 3. Read Source Tables
# ==========================================================================================

cpis_source_df = spark.table(SOURCE_CPI_TABLE)
eventos_source_df = spark.table(SOURCE_EVENTOS_TABLE)
orgaos_source_df = spark.table(SOURCE_ORGAOS_TABLE)

records_cpis_read = cpis_source_df.count()
records_eventos_read = eventos_source_df.count()
records_orgaos_read = orgaos_source_df.count()

records_read = records_eventos_read

log_info(
    pipeline_logger=logger,
    message=(
        f"Source tables loaded successfully "
        f"| cpis={records_cpis_read} "
        f"| eventos={records_eventos_read} "
        f"| orgaos={records_orgaos_read}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 4. Standardize CPI Source
# ==========================================================================================

cpis_df = (
    cpis_source_df
    .select(
        col("cpi_id_orgao").cast("string").alias("cpi_id_orgao"),
        col("cpi_tx_sigla").cast("string").alias("cpi_tx_sigla"),
        col("cpi_tx_nome").cast("string").alias("cpi_tx_nome"),
        col("cpi_tx_tipo").cast("string").alias("cpi_tx_tipo"),
        col("cpi_tx_tipo_descricao").cast("string").alias("cpi_tx_tipo_descricao"),
        col("cpi_tx_abrangencia").cast("string").alias("cpi_tx_abrangencia"),
        col("cpi_tx_status_analitico").cast("string").alias("cpi_tx_status_analitico"),
        col("cpi_fl_mista").alias("cpi_fl_mista"),
        col("cpi_fl_ativa").alias("cpi_fl_ativa"),
        col("cpi_dt_inicio").alias("cpi_dt_inicio"),
        col("cpi_dt_fim").alias("cpi_dt_fim"),
        col("cpi_nr_ano_inicio").cast(IntegerType()).alias("cpi_nr_ano_inicio"),
        col("leg_id_legislatura").cast(IntegerType()).alias("leg_id_legislatura_cpi"),
        col("aud_id_execucao_silver").alias("aud_id_execucao_cpis_silver"),
        col("aud_dh_processamento").alias("aud_dh_processamento_cpis_silver"),
        col("aud_tx_hash_registro_silver").alias("aud_tx_hash_registro_cpis_silver"),
    )
)

# COMMAND ----------

# ==========================================================================================
# 5. Standardize Event Source
# ==========================================================================================

eventos_df = (
    eventos_source_df
    .select(
        col("evt_id_evento").cast("string").alias("evt_id_evento"),
        col("evt_id_orgao").cast("string").alias("evt_id_orgao"),
        col("evt_tx_sigla_orgao").cast("string").alias("evt_tx_sigla_orgao"),
        col("evt_tx_nome_orgao").cast("string").alias("evt_tx_nome_orgao"),
        col("evt_tx_tipo_orgao").cast("string").alias("evt_tx_tipo_orgao"),
        col("evt_tx_uri").cast("string").alias("evt_tx_uri"),
        col("evt_tx_situacao").cast("string").alias("evt_tx_situacao"),
        col("evt_tx_titulo").cast("string").alias("evt_tx_titulo"),
        col("evt_tx_tipo_evento").cast("string").alias("evt_tx_tipo_evento"),
        col("evt_tx_local").cast("string").alias("evt_tx_local"),
        col("evt_dh_inicio_origem").cast("string").alias("evt_dh_inicio_origem"),
        col("evt_dh_fim_origem").cast("string").alias("evt_dh_fim_origem"),
        col("evt_dh_inicio").alias("evt_dh_inicio"),
        col("evt_dh_fim").alias("evt_dh_fim"),
        col("evt_dt_inicio").alias("evt_dt_inicio"),
        col("evt_dt_fim").alias("evt_dt_fim"),
        col("evt_nr_ano").cast(IntegerType()).alias("evt_nr_ano"),
        col("evt_nr_mes").cast(IntegerType()).alias("evt_nr_mes"),
        col("leg_id_legislatura").cast(IntegerType()).alias("leg_id_legislatura_evento"),
        col("evt_fl_registro_valido_silver").alias("evt_fl_registro_valido_silver"),
        col("evt_tx_payload_json").cast("string").alias("evt_tx_payload_json"),
        col("aud_id_execucao_bronze").alias("aud_id_execucao_bronze"),
        col("aud_dh_ingestao_bronze").alias("aud_dh_ingestao_bronze"),
        col("aud_tx_hash_registro_bronze").alias("aud_tx_hash_registro_bronze"),
        col("aud_id_execucao_silver").alias("aud_id_execucao_eventos_silver"),
        col("aud_dh_processamento").alias("aud_dh_processamento_eventos_silver"),
        col("aud_tx_hash_registro_silver").alias("aud_tx_hash_registro_eventos_silver"),
    )
)

# COMMAND ----------

# ==========================================================================================
# 6. Build Event Text Corpus
# ==========================================================================================

eventos_text_df = (
    eventos_df
    .withColumn(
        "cpi_evt_tx_texto_base",
        upper(
            concat_ws(
                " ",
                coalesce(col("evt_tx_titulo"), lit("")),
                coalesce(col("evt_tx_tipo_evento"), lit("")),
                coalesce(col("evt_tx_situacao"), lit("")),
                coalesce(col("evt_tx_tipo_orgao"), lit("")),
                coalesce(col("evt_tx_sigla_orgao"), lit("")),
            )
        )
    )
    .withColumn(
        "cpi_evt_tx_texto_base_normalizado",
        regexp_replace(
            col("cpi_evt_tx_texto_base"),
            r"\s+",
            " ",
        )
    )
)

# COMMAND ----------

# ==========================================================================================
# 7. Detect Direct CPI Relationships
# ==========================================================================================

direct_cpi_eventos_df = (
    eventos_text_df.alias("evt")
    .join(
        cpis_df.alias("cpi"),
        col("evt.evt_id_orgao") == col("cpi.cpi_id_orgao"),
        "inner",
    )
    .select(
        col("evt.*"),
        col("cpi.cpi_id_orgao"),
        col("cpi.cpi_tx_sigla"),
        col("cpi.cpi_tx_nome"),
        col("cpi.cpi_tx_tipo"),
        col("cpi.cpi_tx_tipo_descricao"),
        col("cpi.cpi_tx_abrangencia"),
        col("cpi.cpi_tx_status_analitico"),
        col("cpi.cpi_fl_mista"),
        col("cpi.cpi_fl_ativa"),
        col("cpi.cpi_dt_inicio"),
        col("cpi.cpi_dt_fim"),
        col("cpi.cpi_nr_ano_inicio"),
        col("cpi.leg_id_legislatura_cpi"),
        col("cpi.aud_id_execucao_cpis_silver"),
        col("cpi.aud_dh_processamento_cpis_silver"),
        col("cpi.aud_tx_hash_registro_cpis_silver"),
    )
    .withColumn("cpi_evt_tx_tipo_relacao", lit("DIRECT_ORGAO_RELATION"))
    .withColumn("cpi_evt_nr_score_confianca", lit(100))
    .withColumn("cpi_evt_tx_nivel_confianca", lit("HIGH"))
    .withColumn("cpi_evt_fl_falso_positivo_provavel", lit(False))
)

records_direct_candidates = direct_cpi_eventos_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Direct CPI event relationships identified "
        f"| direct_candidates={records_direct_candidates}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 8. Detect Semantic CPI Event Candidates
# ==========================================================================================

direct_event_ids_df = (
    direct_cpi_eventos_df
    .select("evt_id_evento")
    .distinct()
)

semantic_base_df = (
    eventos_text_df.alias("evt")
    .join(
        direct_event_ids_df.alias("direct"),
        col("evt.evt_id_evento") == col("direct.evt_id_evento"),
        "left_anti",
    )
)

semantic_eventos_df = (
    semantic_base_df
    .withColumn(
        "cpi_evt_fl_termo_cpi",
        col("cpi_evt_tx_texto_base_normalizado").rlike(r"(^|[^A-Z])CPI([^A-Z]|$)")
    )
    .withColumn(
        "cpi_evt_fl_termo_cpmi",
        col("cpi_evt_tx_texto_base_normalizado").rlike(r"(^|[^A-Z])CPMI([^A-Z]|$)")
    )
    .withColumn(
        "cpi_evt_fl_termo_comissao_inquerito",
        col("cpi_evt_tx_texto_base_normalizado").like("%COMISSÃO PARLAMENTAR DE INQUÉRITO%")
        | col("cpi_evt_tx_texto_base_normalizado").like("%COMISSAO PARLAMENTAR DE INQUERITO%")
        | col("cpi_evt_tx_texto_base_normalizado").like("%COMISSÃO PARLAMENTAR MISTA DE INQUÉRITO%")
        | col("cpi_evt_tx_texto_base_normalizado").like("%COMISSAO PARLAMENTAR MISTA DE INQUERITO%")
    )
    .withColumn(
        "cpi_evt_fl_termo_inquerito",
        col("cpi_evt_tx_texto_base_normalizado").like("%INQUÉRITO%")
        | col("cpi_evt_tx_texto_base_normalizado").like("%INQUERITO%")
    )
    .withColumn(
        "cpi_evt_fl_termo_investigacao",
        col("cpi_evt_tx_texto_base_normalizado").like("%INVESTIGAÇÃO%")
        | col("cpi_evt_tx_texto_base_normalizado").like("%INVESTIGACAO%")
    )
    .withColumn(
        "cpi_evt_fl_evento_semantico_cpi",
        (
            col("cpi_evt_fl_termo_cpi")
            | col("cpi_evt_fl_termo_cpmi")
            | col("cpi_evt_fl_termo_comissao_inquerito")
            | col("cpi_evt_fl_termo_inquerito")
            | col("cpi_evt_fl_termo_investigacao")
        )
    )
    .withColumn(
        "cpi_evt_fl_falso_positivo_provavel",
        (
            col("cpi_evt_tx_texto_base_normalizado").like("%COMISSÃO PRÓ-INDÍGENA%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%COMISSAO PRO-INDIGENA%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%CPI/AC%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%COMANDO DE POLICIAMENTO DO INTERIOR%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%CPI - 5%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%CENTRO DE INVESTIGAÇÃO%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%CENTRO DE INVESTIGACAO%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%INVESTIGAÇÃO CLÍNICA%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%INVESTIGACAO CLINICA%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%INVESTIGAÇÃO DE DUMPING%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%INVESTIGACAO DE DUMPING%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%PREVENÇÃO DE ACIDENTES%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%PREVENCAO DE ACIDENTES%")
        )
    )
    .filter(col("cpi_evt_fl_evento_semantico_cpi") == True)
)

records_semantic_candidates = semantic_eventos_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Semantic CPI event candidates identified "
        f"| semantic_candidates={records_semantic_candidates}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 9. Apply Semantic Classification Rules
# ==========================================================================================

semantic_cpi_eventos_df = (
    semantic_eventos_df
    .withColumn("cpi_id_orgao", lit(None).cast(StringType()))
    .withColumn("cpi_tx_sigla", lit(None).cast(StringType()))
    .withColumn("cpi_tx_nome", lit(None).cast(StringType()))
    .withColumn(
        "cpi_tx_tipo",
        when(
            col("cpi_evt_fl_termo_cpmi")
            | col("cpi_evt_tx_texto_base_normalizado").like("%COMISSÃO PARLAMENTAR MISTA DE INQUÉRITO%")
            | col("cpi_evt_tx_texto_base_normalizado").like("%COMISSAO PARLAMENTAR MISTA DE INQUERITO%"),
            lit("CPMI")
        )
        .when(
            col("cpi_evt_fl_termo_cpi")
            | col("cpi_evt_fl_termo_comissao_inquerito"),
            lit("CPI")
        )
        .otherwise(lit("INQUIRY_RELATED"))
    )
    .withColumn(
        "cpi_tx_tipo_descricao",
        when(col("cpi_tx_tipo") == "CPMI", lit("Comissão Parlamentar Mista de Inquérito"))
        .when(col("cpi_tx_tipo") == "CPI", lit("Comissão Parlamentar de Inquérito"))
        .otherwise(lit("Evento relacionado a inquérito ou investigação"))
    )
    .withColumn("cpi_tx_abrangencia", lit(None).cast(StringType()))
    .withColumn("cpi_tx_status_analitico", lit(None).cast(StringType()))
    .withColumn(
        "cpi_fl_mista",
        when(col("cpi_tx_tipo") == "CPMI", lit(True))
        .when(col("cpi_tx_tipo") == "CPI", lit(False))
        .otherwise(lit(None).cast("boolean"))
    )
    .withColumn("cpi_fl_ativa", lit(None).cast("boolean"))
    .withColumn("cpi_dt_inicio", lit(None).cast("date"))
    .withColumn("cpi_dt_fim", lit(None).cast("date"))
    .withColumn("cpi_nr_ano_inicio", lit(None).cast(IntegerType()))
    .withColumn("leg_id_legislatura_cpi", lit(None).cast(IntegerType()))
    .withColumn("aud_id_execucao_cpis_silver", lit(None).cast(StringType()))
    .withColumn("aud_dh_processamento_cpis_silver", lit(None).cast("timestamp"))
    .withColumn("aud_tx_hash_registro_cpis_silver", lit(None).cast(StringType()))
    .withColumn(
        "cpi_evt_tx_tipo_relacao",
        when(
            col("cpi_evt_fl_termo_cpi")
            | col("cpi_evt_fl_termo_cpmi")
            | col("cpi_evt_fl_termo_comissao_inquerito"),
            lit("SEMANTIC_CPI_EXPLICIT")
        )
        .when(col("cpi_evt_fl_termo_inquerito"), lit("SEMANTIC_INQUERITO"))
        .when(col("cpi_evt_fl_termo_investigacao"), lit("SEMANTIC_INVESTIGACAO"))
        .otherwise(lit("SEMANTIC_RELATED"))
    )
    .withColumn(
        "cpi_evt_nr_score_confianca",
        when(col("cpi_evt_fl_falso_positivo_provavel") == True, lit(10))
        .when(col("cpi_evt_tx_tipo_relacao") == "SEMANTIC_CPI_EXPLICIT", lit(90))
        .when(col("cpi_evt_tx_tipo_relacao") == "SEMANTIC_INQUERITO", lit(60))
        .when(col("cpi_evt_tx_tipo_relacao") == "SEMANTIC_INVESTIGACAO", lit(40))
        .otherwise(lit(30))
    )
    .withColumn(
        "cpi_evt_tx_nivel_confianca",
        when(col("cpi_evt_nr_score_confianca") >= 80, lit("HIGH"))
        .when(col("cpi_evt_nr_score_confianca") >= 50, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )
)

# COMMAND ----------

# ==========================================================================================
# 10. Union Direct and Semantic Candidates
# ==========================================================================================

semantic_flag_columns = [
    "cpi_evt_fl_termo_cpi",
    "cpi_evt_fl_termo_cpmi",
    "cpi_evt_fl_termo_comissao_inquerito",
    "cpi_evt_fl_termo_inquerito",
    "cpi_evt_fl_termo_investigacao",
    "cpi_evt_fl_evento_semantico_cpi",
    "cpi_evt_fl_falso_positivo_provavel",
]

for semantic_flag_column in semantic_flag_columns:
    if semantic_flag_column not in direct_cpi_eventos_df.columns:
        direct_cpi_eventos_df = direct_cpi_eventos_df.withColumn(
            semantic_flag_column,
            lit(False)
        )

for semantic_flag_column in semantic_flag_columns:
    if semantic_flag_column not in semantic_cpi_eventos_df.columns:
        semantic_cpi_eventos_df = semantic_cpi_eventos_df.withColumn(
            semantic_flag_column,
            lit(False)
        )

candidate_columns = list(
    dict.fromkeys(
        direct_cpi_eventos_df.columns
        + semantic_cpi_eventos_df.columns
    )
)

for candidate_column in candidate_columns:
    if candidate_column not in direct_cpi_eventos_df.columns:
        direct_cpi_eventos_df = direct_cpi_eventos_df.withColumn(
            candidate_column,
            lit(None)
        )

    if candidate_column not in semantic_cpi_eventos_df.columns:
        semantic_cpi_eventos_df = semantic_cpi_eventos_df.withColumn(
            candidate_column,
            lit(None)
        )

direct_aligned_df = direct_cpi_eventos_df.select(*candidate_columns)
semantic_aligned_df = semantic_cpi_eventos_df.select(*candidate_columns)

cpi_eventos_candidates_df = (
    direct_aligned_df
    .unionByName(semantic_aligned_df)
)

records_total_candidates = cpi_eventos_candidates_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Total CPI event candidates prepared "
        f"| total_candidates={records_total_candidates}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 11. Apply CPI Event Analytical Derivations
# ==========================================================================================

cpi_eventos_enriched_df = (
    cpi_eventos_candidates_df
    .withColumn(
        "cpi_evt_id_relacao",
        sha2(
            concat_ws(
                "||",
                coalesce(col("cpi_id_orgao"), lit("SEMANTIC_ONLY")),
                coalesce(col("evt_id_evento"), lit("UNKNOWN_EVENT")),
                coalesce(col("cpi_evt_tx_tipo_relacao"), lit("UNKNOWN_RELATION")),
            ),
            256,
        )
    )
    .withColumn("cpi_evt_nr_ano_evento", year(col("evt_dt_inicio")).cast(IntegerType()))
    .withColumn("cpi_evt_nr_mes_evento", month(col("evt_dt_inicio")).cast(IntegerType()))
    .withColumn(
        "cpi_evt_fl_cpi_identificada",
        when(col("cpi_id_orgao").isNotNull(), lit(True)).otherwise(lit(False))
    )
    .withColumn(
        "cpi_evt_fl_relacao_direta",
        when(col("cpi_evt_tx_tipo_relacao") == "DIRECT_ORGAO_RELATION", lit(True))
        .otherwise(lit(False))
    )
    .withColumn(
        "cpi_evt_fl_relacao_semantica",
        when(col("cpi_evt_tx_tipo_relacao").like("SEMANTIC%"), lit(True))
        .otherwise(lit(False))
    )
    .withColumn(
        "cpi_evt_fl_alta_confianca",
        when(col("cpi_evt_nr_score_confianca") >= 80, lit(True)).otherwise(lit(False))
    )
    .withColumn(
        "cpi_evt_fl_evento_com_data",
        when(col("evt_dh_inicio").isNotNull(), lit(True)).otherwise(lit(False))
    )
    .withColumn(
        "cpi_evt_fl_periodo_evento_valido",
        when(
            col("evt_dh_inicio").isNotNull()
            & col("evt_dh_fim").isNotNull()
            & (col("evt_dh_inicio") > col("evt_dh_fim")),
            lit(False)
        ).otherwise(lit(True))
    )
    .withColumn(
        "cpi_evt_fl_evento_apos_inicio_cpi",
        when(
            col("cpi_dt_inicio").isNull()
            | col("evt_dt_inicio").isNull(),
            lit(None).cast("boolean")
        )
        .when(col("evt_dt_inicio") >= col("cpi_dt_inicio"), lit(True))
        .otherwise(lit(False))
    )
    .withColumn(
        "cpi_evt_fl_evento_antes_fim_cpi",
        when(
            col("cpi_dt_fim").isNull()
            | col("evt_dt_inicio").isNull(),
            lit(None).cast("boolean")
        )
        .when(col("evt_dt_inicio") <= col("cpi_dt_fim"), lit(True))
        .otherwise(lit(False))
    )
    .withColumn(
        "cpi_evt_fl_temporalmente_consistente",
        when(col("cpi_evt_fl_evento_apos_inicio_cpi") == False, lit(False))
        .when(col("cpi_evt_fl_evento_antes_fim_cpi") == False, lit(False))
        .otherwise(lit(True))
    )
    .withColumn(
        "cpi_evt_fl_mesma_legislatura_cpi",
        when(
            col("leg_id_legislatura_cpi").isNotNull()
            & col("leg_id_legislatura_evento").isNotNull()
            & (col("leg_id_legislatura_cpi") == col("leg_id_legislatura_evento")),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "cpi_evt_tx_status_evento_analitico",
        when(upper(col("evt_tx_situacao")).like("%CANCEL%"), lit("CANCELADO"))
        .when(upper(col("evt_tx_situacao")).like("%ENCERR%"), lit("ENCERRADO"))
        .when(upper(col("evt_tx_situacao")).like("%REALIZ%"), lit("REALIZADO"))
        .when(
            upper(col("evt_tx_situacao")).like("%AGEND%")
            | upper(col("evt_tx_situacao")).like("%CONVOC%"),
            lit("AGENDADO")
        )
        .otherwise(lit("STATUS_INDEFINIDO"))
    )
    .withColumn(
        "cpi_evt_fl_evento_realizado",
        when(
            col("cpi_evt_tx_status_evento_analitico").isin(
                "REALIZADO",
                "ENCERRADO",
            ),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "cpi_evt_tx_payload_origem_json",
        to_json(
            struct(
                col("evt_id_evento"),
                col("evt_id_orgao"),
                col("evt_tx_sigla_orgao"),
                col("evt_tx_tipo_orgao"),
                col("evt_tx_titulo"),
                col("evt_tx_tipo_evento"),
                col("evt_dh_inicio_origem"),
                col("evt_dh_fim_origem"),
                col("cpi_id_orgao"),
                col("cpi_tx_tipo"),
                col("cpi_evt_tx_tipo_relacao"),
                col("cpi_evt_nr_score_confianca"),
                col("cpi_evt_tx_nivel_confianca"),
                col("cpi_evt_fl_falso_positivo_provavel"),
            )
        )
    )
)

# COMMAND ----------

# ==========================================================================================
# 12. Apply Quality Rules
# ==========================================================================================

cpi_eventos_quality_df = (
    cpi_eventos_enriched_df
    .withColumn(
        "cpi_evt_fl_id_relacao_valido",
        (
            col("cpi_evt_id_relacao").isNotNull()
            & (trim(col("cpi_evt_id_relacao")) != "")
        )
    )
    .withColumn(
        "cpi_evt_fl_evento_identificado",
        (
            col("evt_id_evento").isNotNull()
            & (trim(col("evt_id_evento")) != "")
        )
    )
    .withColumn(
        "cpi_evt_fl_orgao_evento_identificado",
        (
            col("evt_id_orgao").isNotNull()
            & (trim(col("evt_id_orgao")) != "")
        )
    )
    .withColumn(
        "cpi_evt_fl_texto_classificacao_informado",
        (
            col("cpi_evt_tx_texto_base_normalizado").isNotNull()
            & (trim(col("cpi_evt_tx_texto_base_normalizado")) != "")
        )
    )
    .withColumn(
        "cpi_evt_fl_regra_semantica_valida",
        (
            col("cpi_evt_fl_relacao_direta")
            | col("cpi_evt_fl_relacao_semantica")
        )
    )
    .withColumn(
        "cpi_evt_fl_registro_valido_silver",
        (
            col("cpi_evt_fl_id_relacao_valido")
            & col("cpi_evt_fl_evento_identificado")
            & col("cpi_evt_fl_texto_classificacao_informado")
            & col("cpi_evt_fl_regra_semantica_valida")
            & col("cpi_evt_fl_periodo_evento_valido")
            & (col("cpi_evt_nr_score_confianca") >= 50)
            & (col("cpi_evt_fl_falso_positivo_provavel") == False)
        )
    )
    .withColumn(
        "cpi_evt_tx_motivo_rejeicao",
        when(
            ~col("cpi_evt_fl_id_relacao_valido"),
            lit("CPI_EVENTO_ID_RELACAO_INVALIDO")
        )
        .when(
            ~col("cpi_evt_fl_evento_identificado"),
            lit("CPI_EVENTO_EVENTO_NAO_IDENTIFICADO")
        )
        .when(
            ~col("cpi_evt_fl_texto_classificacao_informado"),
            lit("CPI_EVENTO_TEXTO_CLASSIFICACAO_NAO_INFORMADO")
        )
        .when(
            ~col("cpi_evt_fl_regra_semantica_valida"),
            lit("CPI_EVENTO_REGRA_SEMANTICA_INVALIDA")
        )
        .when(
            ~col("cpi_evt_fl_periodo_evento_valido"),
            lit("CPI_EVENTO_PERIODO_INVALIDO")
        )
        .when(
            col("cpi_evt_fl_falso_positivo_provavel") == True,
            lit("CPI_EVENTO_FALSO_POSITIVO_PROVAVEL")
        )
        .when(
            col("cpi_evt_nr_score_confianca") < 50,
            lit("CPI_EVENTO_BAIXA_CONFIANCA_SEMANTICA")
        )
        .otherwise(lit(None).cast(StringType()))
    )
)

# COMMAND ----------

# ==========================================================================================
# 13. Build Rejected Records
# ==========================================================================================

mandatory_rejected_source_df = (
    cpi_eventos_quality_df
    .filter(col("cpi_evt_fl_registro_valido_silver") == False)
)

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=mandatory_rejected_source_df,
    execution_id=execution_id,
    source_table=SOURCE_EVENTOS_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="cpi_evt_id_relacao",
    validation_rule_column="cpi_evt_tx_motivo_rejeicao",
    payload_column="cpi_evt_tx_payload_origem_json",
    valid_flag_column="cpi_evt_fl_registro_valido_silver",
)

# COMMAND ----------

# ==========================================================================================
# 14. Keep Valid CPI Event Candidate Records
# ==========================================================================================

valid_df = (
    cpi_eventos_quality_df
    .filter(col("cpi_evt_fl_registro_valido_silver") == True)
)

# COMMAND ----------

# ==========================================================================================
# 15. Deduplicate CPI Event Candidate Records
# ==========================================================================================

dedup_window = (
    Window
    .partitionBy("cpi_evt_id_relacao")
    .orderBy(
        col("cpi_evt_nr_score_confianca").desc_nulls_last(),
        col("aud_dh_processamento_eventos_silver").desc_nulls_last(),
    )
)

dedup_df = (
    valid_df
    .withColumn(
        "rn_deduplicacao",
        row_number().over(dedup_window)
    )
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=dedup_df,
    execution_id=execution_id,
    source_table=SOURCE_EVENTOS_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="cpi_evt_id_relacao",
    payload_column="cpi_evt_tx_payload_origem_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="CPI_EVENTO_REGISTRO_DUPLICADO",
    observation=(
        "Duplicate CPI event candidate removed keeping highest confidence classification."
    ),
)

silver_df = (
    dedup_df
    .filter(col("rn_deduplicacao") == 1)
    .drop("rn_deduplicacao")
    .drop("cpi_evt_tx_motivo_rejeicao")
)

# COMMAND ----------

# ==========================================================================================
# 16. Persist Rejected Records
# ==========================================================================================

rejected_df = union_rejected_records(
    mandatory_rejected_dataframe=mandatory_rejected_df,
    duplicate_rejected_dataframe=duplicate_rejected_df,
)

records_rejected = rejected_df.count()

clean_and_persist_rejected_records(
    rejected_dataframe=rejected_df,
    rejected_table=REJECTED_TABLE,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    mode="append",
)

log_info(
    pipeline_logger=logger,
    message=(
        f"Rejected and discarded CPI event records persisted "
        f"| records_rejected={records_rejected}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 17. Add Silver Traceability Columns
# ==========================================================================================

silver_df = (
    silver_df
    .withColumn("aud_id_execucao_silver", lit(execution_id))
    .withColumn("aud_dh_processamento", current_timestamp())
    .withColumn("aud_tx_camada_origem", lit("silver"))
    .withColumn("aud_tx_tabela_origem_cpis", lit(SOURCE_CPI_TABLE))
    .withColumn("aud_tx_tabela_origem_eventos", lit(SOURCE_EVENTOS_TABLE))
    .withColumn("aud_tx_tabela_origem_orgaos", lit(SOURCE_ORGAOS_TABLE))
    .withColumn("aud_tx_tabela_destino", lit(TARGET_TABLE))
    .withColumn("aud_tx_versao_pipeline_silver", lit(PROJECT_VERSION))
    .withColumn(
        "aud_tx_regra_derivacao",
        lit(
            "CPI event candidates derived by direct CPI body relationship when available, "
            "and by governed semantic rules over event text when direct relationship is unavailable. "
            "Probable false positives and low-confidence generic investigation terms are not persisted as valid Silver CPI events."
        )
    )
)

# COMMAND ----------

# ==========================================================================================
# 18. Add Silver Hash
# ==========================================================================================

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "cpi_evt_id_relacao",
        "evt_id_evento",
        "evt_id_orgao",
        "cpi_id_orgao",
        "cpi_evt_tx_tipo_relacao",
        "cpi_evt_nr_score_confianca",
        "evt_dh_inicio",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# ==========================================================================================
# 19. Select Final Columns
# ==========================================================================================

final_columns = [
    "cpi_evt_id_relacao",

    "cpi_id_orgao",
    "cpi_tx_sigla",
    "cpi_tx_nome",
    "cpi_tx_tipo",
    "cpi_tx_tipo_descricao",
    "cpi_tx_abrangencia",
    "cpi_tx_status_analitico",
    "cpi_fl_mista",
    "cpi_fl_ativa",
    "cpi_dt_inicio",
    "cpi_dt_fim",
    "cpi_nr_ano_inicio",
    "leg_id_legislatura_cpi",

    "evt_id_evento",
    "evt_id_orgao",
    "evt_tx_sigla_orgao",
    "evt_tx_nome_orgao",
    "evt_tx_tipo_orgao",
    "evt_tx_uri",
    "evt_tx_situacao",
    "evt_tx_titulo",
    "evt_tx_tipo_evento",
    "evt_tx_local",
    "evt_dh_inicio_origem",
    "evt_dh_fim_origem",
    "evt_dh_inicio",
    "evt_dh_fim",
    "evt_dt_inicio",
    "evt_dt_fim",
    "evt_nr_ano",
    "evt_nr_mes",
    "leg_id_legislatura_evento",

    "cpi_evt_tx_tipo_relacao",
    "cpi_evt_nr_score_confianca",
    "cpi_evt_tx_nivel_confianca",
    "cpi_evt_fl_alta_confianca",
    "cpi_evt_fl_falso_positivo_provavel",
    "cpi_evt_tx_status_evento_analitico",
    "cpi_evt_tx_texto_base_normalizado",

    "cpi_evt_fl_termo_cpi",
    "cpi_evt_fl_termo_cpmi",
    "cpi_evt_fl_termo_comissao_inquerito",
    "cpi_evt_fl_termo_inquerito",
    "cpi_evt_fl_termo_investigacao",
    "cpi_evt_fl_evento_semantico_cpi",

    "cpi_evt_fl_cpi_identificada",
    "cpi_evt_fl_relacao_direta",
    "cpi_evt_fl_relacao_semantica",
    "cpi_evt_fl_evento_com_data",
    "cpi_evt_fl_evento_realizado",
    "cpi_evt_fl_mesma_legislatura_cpi",
    "cpi_evt_fl_evento_apos_inicio_cpi",
    "cpi_evt_fl_evento_antes_fim_cpi",
    "cpi_evt_fl_temporalmente_consistente",
    "cpi_evt_fl_periodo_evento_valido",
    "cpi_evt_fl_id_relacao_valido",
    "cpi_evt_fl_evento_identificado",
    "cpi_evt_fl_orgao_evento_identificado",
    "cpi_evt_fl_texto_classificacao_informado",
    "cpi_evt_fl_regra_semantica_valida",
    "cpi_evt_fl_registro_valido_silver",

    "cpi_evt_tx_payload_origem_json",

    "aud_id_execucao_bronze",
    "aud_dh_ingestao_bronze",
    "aud_tx_hash_registro_bronze",

    "aud_id_execucao_cpis_silver",
    "aud_dh_processamento_cpis_silver",
    "aud_tx_hash_registro_cpis_silver",

    "aud_id_execucao_eventos_silver",
    "aud_dh_processamento_eventos_silver",
    "aud_tx_hash_registro_eventos_silver",

    "aud_id_execucao_silver",
    "aud_dh_processamento",
    "aud_tx_camada_origem",
    "aud_tx_tabela_origem_cpis",
    "aud_tx_tabela_origem_eventos",
    "aud_tx_tabela_origem_orgaos",
    "aud_tx_tabela_destino",
    "aud_tx_versao_pipeline_silver",
    "aud_tx_regra_derivacao",
    "aud_tx_hash_registro_silver",
]

silver_df = silver_df.select(*final_columns)

# COMMAND ----------

# ==========================================================================================
# 20. Persist Silver Table
# ==========================================================================================

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

records_written = spark.table(TARGET_TABLE).count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Silver CPI events table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# ==========================================================================================
# 21. Apply Governance Comments
# ==========================================================================================

table_comment = """
Standardized CPI event candidate table in the Silver layer.

This table contains validated and deduplicated legislative events potentially
related to Parliamentary Inquiry Commissions.

The table supports CPI audit analytics by combining direct CPI relationship
rules with governed semantic detection rules over event textual attributes.

Probable false positives and low-confidence generic investigation terms are
tracked as rejected records and are not persisted as valid CPI event records.
"""

column_comments = {
    "cpi_evt_id_relacao":
        "Deterministic CPI event candidate relationship identifier.",

    "cpi_id_orgao":
        "CPI legislative body identifier when direct CPI relationship is available.",

    "cpi_tx_sigla":
        "CPI acronym when direct CPI relationship is available.",

    "cpi_tx_nome":
        "CPI name when direct CPI relationship is available.",

    "cpi_tx_tipo":
        "Analytical CPI type inferred from direct CPI context or semantic classification.",

    "evt_id_evento":
        "Legislative event identifier inherited from slv_eventos.",

    "evt_id_orgao":
        "Legislative body identifier associated with the event.",

    "evt_tx_titulo":
        "Standardized event title used for semantic CPI detection.",

    "evt_tx_tipo_evento":
        "Standardized event type.",

    "cpi_evt_tx_tipo_relacao":
        "Relationship type used to classify the CPI event candidate.",

    "cpi_evt_nr_score_confianca":
        "Numeric confidence score assigned to the CPI event candidate.",

    "cpi_evt_tx_nivel_confianca":
        "Confidence level assigned to the CPI event candidate.",

    "cpi_evt_fl_alta_confianca":
        "Flag indicating whether the CPI event candidate has high confidence score.",

    "cpi_evt_fl_falso_positivo_provavel":
        "Flag indicating probable semantic false positive for CPI event detection.",

    "cpi_evt_fl_relacao_direta":
        "Flag indicating whether event is directly linked to a CPI body.",

    "cpi_evt_fl_relacao_semantica":
        "Flag indicating whether event was detected through semantic rules.",

    "cpi_evt_fl_termo_cpi":
        "Flag indicating whether text contains explicit CPI keyword.",

    "cpi_evt_fl_termo_cpmi":
        "Flag indicating whether text contains explicit CPMI keyword.",

    "cpi_evt_fl_termo_inquerito":
        "Flag indicating whether text contains inquiry-related keyword.",

    "cpi_evt_fl_termo_investigacao":
        "Flag indicating whether text contains investigation-related keyword.",

    "cpi_evt_fl_registro_valido_silver":
        "Flag indicating whether CPI event candidate passed Silver validation.",

    "cpi_evt_tx_payload_origem_json":
        "JSON payload preserving source fields used for CPI event candidate derivation.",

    "aud_tx_regra_derivacao":
        "Textual description of CPI event candidate derivation rule.",

    "aud_tx_hash_registro_silver":
        "Deterministic Silver hash used for CPI event traceability.",
    "cpi_tx_tipo_descricao":
    "Detailed description of the CPI type provided by the source system.",

    "cpi_tx_abrangencia":
        "Scope or jurisdiction associated with the CPI investigation.",

    "cpi_tx_status_analitico":
        "Standardized analytical status assigned to the CPI during Silver processing.",

    "cpi_nr_ano_inicio":
        "Year when the CPI investigation started.",

    "leg_id_legislatura_cpi":
        "Legislature identifier associated with the CPI.",   
    "evt_tx_sigla_orgao":
    "Acronym of the legislative body associated with the event.",

    "evt_tx_nome_orgao":
        "Name of the legislative body associated with the event.",

    "evt_tx_tipo_orgao":
        "Type of legislative body associated with the event.",

    "evt_tx_uri":
        "Source URI identifying the legislative event.",

    "evt_tx_situacao":
        "Current status of the legislative event from the source system.",

    "evt_tx_local":
        "Physical or virtual location where the event occurred.",

    "evt_dh_inicio_origem":
        "Original event start datetime received from the source system.",

    "evt_dh_fim_origem":
        "Original event end datetime received from the source system.",

    "evt_nr_ano":
        "Event year derived from the event date.",

    "evt_nr_mes":
        "Event month derived from the event date.",

    "leg_id_legislatura_evento":
        "Legislature identifier associated with the event.",     
    "cpi_evt_tx_status_evento_analitico":
    "Analytical classification of the event execution status.",

    "cpi_evt_tx_texto_base_normalizado":
        "Normalized text used for CPI semantic classification.",

    "cpi_evt_fl_termo_comissao_inquerito":
        "Flag indicating detection of Parliamentary Inquiry Commission terminology.",

    "cpi_evt_fl_evento_semantico_cpi":
        "Flag indicating semantic classification of the event as CPI related.",

    "cpi_evt_fl_cpi_identificada":
        "Flag indicating whether a CPI entity was successfully identified.",

    "cpi_evt_fl_evento_com_data":
        "Flag indicating whether the event contains valid date information.",

    "cpi_evt_fl_evento_realizado":
        "Flag indicating whether the event was effectively carried out.",

    "cpi_evt_fl_mesma_legislatura_cpi":
        "Flag indicating whether CPI and event belong to the same legislature.",

    "cpi_evt_fl_evento_apos_inicio_cpi":
        "Flag indicating whether the event occurred after CPI start date.",

    "cpi_evt_fl_evento_antes_fim_cpi":
        "Flag indicating whether the event occurred before CPI end date.",

    "cpi_evt_fl_temporalmente_consistente":
        "Flag indicating temporal consistency between CPI and event dates.",

    "cpi_evt_fl_periodo_evento_valido":
        "Flag indicating whether the event period is valid.",

    "cpi_evt_fl_id_relacao_valido":
        "Flag indicating whether the CPI-event relationship identifier is valid.",

    "cpi_evt_fl_evento_identificado":
        "Flag indicating whether the event identifier was successfully detected.",

    "cpi_evt_fl_orgao_evento_identificado":
        "Flag indicating whether the event organization identifier was detected.",

    "cpi_evt_fl_texto_classificacao_informado":
        "Flag indicating whether classification text was available.",

    "cpi_evt_fl_regra_semantica_valida":
        "Flag indicating whether semantic classification rules were satisfied.",
    "aud_id_execucao_silver":
    "Execution identifier generated during Silver processing.",

    "aud_dh_processamento":
        "Timestamp when the Silver record was processed.",

    "aud_tx_camada_origem":
        "Source layer used during Silver processing.",

    "aud_tx_tabela_origem_cpis":
        "Source CPI table used during processing.",

    "aud_tx_tabela_origem_eventos":
        "Source event table used during processing.",

    "aud_tx_tabela_origem_orgaos":
        "Source organization table used during processing.",

    "aud_tx_tabela_destino":
        "Destination Silver table name.",

    "aud_tx_versao_pipeline_silver":
        "Pipeline version used during Silver processing."    

}

if APPLY_GOVERNANCE_COMMENTS:
    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# ==========================================================================================
# 22. Final Pipeline Log
# ==========================================================================================

finished_at = datetime.now()
duration_seconds = (finished_at - started_at).total_seconds()

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_SUCCESS,
    message=(
        f"Silver CPI events semantic derivation completed successfully "
        f"| events_read={records_eventos_read} "
        f"| cpis_read={records_cpis_read} "
        f"| direct_candidates={records_direct_candidates} "
        f"| semantic_candidates={records_semantic_candidates} "
        f"| total_candidates={records_total_candidates} "
        f"| records_written={records_written} "
        f"| records_rejected={records_rejected}"
    ),
    started_at=started_at,
    finished_at=finished_at,
    duration_seconds=duration_seconds,
    records_read=records_read,
    records_written=records_written,
)

log_success(
    pipeline_logger=logger,
    message=(
        f"Silver CPI events semantic derivation completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER CPI EVENTOS COMPLETED")
print("=" * 90)
print(f"CPI Source Table: {SOURCE_CPI_TABLE}")
print(f"Event Source Table: {SOURCE_EVENTOS_TABLE}")
print(f"Orgaos Source Table: {SOURCE_ORGAOS_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"CPI Records Read: {records_cpis_read}")
print(f"Event Records Read: {records_eventos_read}")
print(f"Orgaos Records Read: {records_orgaos_read}")
print(f"Direct CPI Event Candidates: {records_direct_candidates}")
print(f"Semantic CPI Event Candidates: {records_semantic_candidates}")
print(f"Total Candidates Prepared: {records_total_candidates}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)

# COMMAND ----------

# MAGIC %md
# MAGIC # ==========================================================================================
# MAGIC # Initialize Spark session explicitly for utility notebooks
# MAGIC # ==========================================================================================
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark = SparkSession.getActiveSession()
# MAGIC
# MAGIC if spark is None:
# MAGIC     spark = SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC globals()["spark"] = spark
# MAGIC
# MAGIC write_pipeline_log.__globals__["spark"] = spark
# MAGIC
# MAGIC clean_rejected_records_for_entity.__globals__["spark"] = spark
# MAGIC persist_rejected_records.__globals__["spark"] = spark
# MAGIC clean_and_persist_rejected_records.__globals__["spark"] = spark
# MAGIC build_mandatory_rejected_records.__globals__["spark"] = spark
# MAGIC build_duplicate_rejected_records.__globals__["spark"] = spark
# MAGIC union_rejected_records.__globals__["spark"] = spark
# MAGIC
# MAGIC apply_table_comment.__globals__["spark"] = spark
# MAGIC apply_column_comment.__globals__["spark"] = spark
# MAGIC apply_column_comments.__globals__["spark"] = spark
# MAGIC apply_governance_comments.__globals__["spark"] = spark
# MAGIC generate_missing_comment_report.__globals__["spark"] = spark

# COMMAND ----------

# MAGIC %md
# MAGIC from datetime import datetime
# MAGIC import uuid
# MAGIC
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC from pyspark.sql.functions import (
# MAGIC     col,
# MAGIC     lit,
# MAGIC     trim,
# MAGIC     upper,
# MAGIC     current_timestamp,
# MAGIC     row_number,
# MAGIC     when,
# MAGIC     year,
# MAGIC     month,
# MAGIC     coalesce,
# MAGIC     concat_ws,
# MAGIC     sha2,
# MAGIC     to_json,
# MAGIC     struct,
# MAGIC     regexp_replace,
# MAGIC )
# MAGIC
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.types import StringType, IntegerType
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # Initialize Spark session explicitly for utility notebooks
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC spark = SparkSession.getActiveSession()
# MAGIC
# MAGIC if spark is None:
# MAGIC     spark = SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC globals()["spark"] = spark
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC print("=" * 90)
# MAGIC print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
# MAGIC print("13 - SILVER CPI EVENTOS")
# MAGIC print("=" * 90)
# MAGIC print(f"Execution Timestamp: {datetime.now()}")
# MAGIC print("=" * 90)
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 1. Global Configuration
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC NOTEBOOK_NAME = "13_silver_cpi_eventos"
# MAGIC LAYER_NAME = "silver"
# MAGIC ENTITY_NAME = "cpi_eventos"
# MAGIC
# MAGIC SOURCE_CPI_TABLE = get_silver_table(
# MAGIC     SILVER_TABLES["cpis"]
# MAGIC )
# MAGIC
# MAGIC SOURCE_EVENTOS_TABLE = get_silver_table(
# MAGIC     SILVER_TABLES["eventos"]
# MAGIC )
# MAGIC
# MAGIC SOURCE_ORGAOS_TABLE = get_silver_table(
# MAGIC     SILVER_TABLES["orgaos"]
# MAGIC )
# MAGIC
# MAGIC if "cpi_eventos" in SILVER_TABLES:
# MAGIC     TARGET_TABLE = get_silver_table(
# MAGIC         SILVER_TABLES["cpi_eventos"]
# MAGIC     )
# MAGIC else:
# MAGIC     TARGET_TABLE = get_silver_table(
# MAGIC         "slv_cpi_eventos"
# MAGIC     )
# MAGIC
# MAGIC REJECTED_TABLE = get_silver_table(
# MAGIC     SILVER_TABLES["registros_rejeitados"]
# MAGIC )
# MAGIC
# MAGIC execution_id = str(uuid.uuid4())
# MAGIC started_at = datetime.now()
# MAGIC
# MAGIC logger = get_logger(
# MAGIC     logger_name=NOTEBOOK_NAME,
# MAGIC     layer_name=LAYER_NAME,
# MAGIC )
# MAGIC
# MAGIC APPLY_GOVERNANCE_COMMENTS = True
# MAGIC
# MAGIC records_read = None
# MAGIC records_written = None
# MAGIC records_rejected = None
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 2. Start Pipeline Log
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC write_pipeline_log(
# MAGIC     log_id=str(uuid.uuid4()),
# MAGIC     execution_id=execution_id,
# MAGIC     notebook_name=NOTEBOOK_NAME,
# MAGIC     layer_name=LAYER_NAME,
# MAGIC     entity_name=ENTITY_NAME,
# MAGIC     target_table=TARGET_TABLE,
# MAGIC     status=EXECUTION_STATUS_STARTED,
# MAGIC     message="Silver CPI events semantic derivation started.",
# MAGIC     started_at=started_at,
# MAGIC     finished_at=None,
# MAGIC     duration_seconds=None,
# MAGIC     records_read=None,
# MAGIC     records_written=None,
# MAGIC )
# MAGIC
# MAGIC log_info(
# MAGIC     pipeline_logger=logger,
# MAGIC     message="Starting Silver CPI events semantic derivation.",
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 3. Read Source Tables
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC cpis_source_df = spark.table(
# MAGIC     SOURCE_CPI_TABLE
# MAGIC )
# MAGIC
# MAGIC eventos_source_df = spark.table(
# MAGIC     SOURCE_EVENTOS_TABLE
# MAGIC )
# MAGIC
# MAGIC orgaos_source_df = spark.table(
# MAGIC     SOURCE_ORGAOS_TABLE
# MAGIC )
# MAGIC
# MAGIC records_cpis_read = cpis_source_df.count()
# MAGIC records_eventos_read = eventos_source_df.count()
# MAGIC records_orgaos_read = orgaos_source_df.count()
# MAGIC
# MAGIC records_read = records_eventos_read
# MAGIC
# MAGIC log_info(
# MAGIC     pipeline_logger=logger,
# MAGIC     message=(
# MAGIC         f"Source tables loaded successfully "
# MAGIC         f"| cpis={records_cpis_read} "
# MAGIC         f"| eventos={records_eventos_read} "
# MAGIC         f"| orgaos={records_orgaos_read}"
# MAGIC     ),
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 4. Standardize CPI Source
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC cpis_df = (
# MAGIC     cpis_source_df
# MAGIC     .select(
# MAGIC         col("cpi_id_orgao").cast("string").alias("cpi_id_orgao"),
# MAGIC         col("cpi_tx_sigla").cast("string").alias("cpi_tx_sigla"),
# MAGIC         col("cpi_tx_nome").cast("string").alias("cpi_tx_nome"),
# MAGIC         col("cpi_tx_tipo").cast("string").alias("cpi_tx_tipo"),
# MAGIC         col("cpi_tx_tipo_descricao").cast("string").alias("cpi_tx_tipo_descricao"),
# MAGIC         col("cpi_tx_abrangencia").cast("string").alias("cpi_tx_abrangencia"),
# MAGIC         col("cpi_tx_status_analitico").cast("string").alias("cpi_tx_status_analitico"),
# MAGIC         col("cpi_fl_mista").alias("cpi_fl_mista"),
# MAGIC         col("cpi_fl_ativa").alias("cpi_fl_ativa"),
# MAGIC         col("cpi_dt_inicio").alias("cpi_dt_inicio"),
# MAGIC         col("cpi_dt_fim").alias("cpi_dt_fim"),
# MAGIC         col("cpi_nr_ano_inicio").cast(IntegerType()).alias("cpi_nr_ano_inicio"),
# MAGIC         col("leg_id_legislatura").cast(IntegerType()).alias("leg_id_legislatura_cpi"),
# MAGIC
# MAGIC         col("aud_id_execucao_silver").alias("aud_id_execucao_cpis_silver"),
# MAGIC         col("aud_dh_processamento").alias("aud_dh_processamento_cpis_silver"),
# MAGIC         col("aud_tx_hash_registro_silver").alias("aud_tx_hash_registro_cpis_silver"),
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 5. Standardize Event Source
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC eventos_df = (
# MAGIC     eventos_source_df
# MAGIC     .select(
# MAGIC         col("evt_id_evento").cast("string").alias("evt_id_evento"),
# MAGIC         col("evt_id_orgao").cast("string").alias("evt_id_orgao"),
# MAGIC         col("evt_tx_sigla_orgao").cast("string").alias("evt_tx_sigla_orgao"),
# MAGIC         col("evt_tx_nome_orgao").cast("string").alias("evt_tx_nome_orgao"),
# MAGIC         col("evt_tx_tipo_orgao").cast("string").alias("evt_tx_tipo_orgao"),
# MAGIC
# MAGIC         col("evt_tx_uri").cast("string").alias("evt_tx_uri"),
# MAGIC         col("evt_tx_situacao").cast("string").alias("evt_tx_situacao"),
# MAGIC         col("evt_tx_titulo").cast("string").alias("evt_tx_titulo"),
# MAGIC         col("evt_tx_tipo_evento").cast("string").alias("evt_tx_tipo_evento"),
# MAGIC         col("evt_tx_local").cast("string").alias("evt_tx_local"),
# MAGIC
# MAGIC         col("evt_dh_inicio_origem").cast("string").alias("evt_dh_inicio_origem"),
# MAGIC         col("evt_dh_fim_origem").cast("string").alias("evt_dh_fim_origem"),
# MAGIC         col("evt_dh_inicio").alias("evt_dh_inicio"),
# MAGIC         col("evt_dh_fim").alias("evt_dh_fim"),
# MAGIC         col("evt_dt_inicio").alias("evt_dt_inicio"),
# MAGIC         col("evt_dt_fim").alias("evt_dt_fim"),
# MAGIC         col("evt_nr_ano").cast(IntegerType()).alias("evt_nr_ano"),
# MAGIC         col("evt_nr_mes").cast(IntegerType()).alias("evt_nr_mes"),
# MAGIC         col("leg_id_legislatura").cast(IntegerType()).alias("leg_id_legislatura_evento"),
# MAGIC
# MAGIC         col("evt_fl_registro_valido_silver").alias("evt_fl_registro_valido_silver"),
# MAGIC         col("evt_tx_payload_json").cast("string").alias("evt_tx_payload_json"),
# MAGIC
# MAGIC         col("aud_id_execucao_bronze").alias("aud_id_execucao_bronze"),
# MAGIC         col("aud_dh_ingestao_bronze").alias("aud_dh_ingestao_bronze"),
# MAGIC         col("aud_tx_hash_registro_bronze").alias("aud_tx_hash_registro_bronze"),
# MAGIC
# MAGIC         col("aud_id_execucao_silver").alias("aud_id_execucao_eventos_silver"),
# MAGIC         col("aud_dh_processamento").alias("aud_dh_processamento_eventos_silver"),
# MAGIC         col("aud_tx_hash_registro_silver").alias("aud_tx_hash_registro_eventos_silver"),
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 6. Build Event Text Corpus
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC eventos_text_df = (
# MAGIC     eventos_df
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_tx_texto_base",
# MAGIC         upper(
# MAGIC             concat_ws(
# MAGIC                 " ",
# MAGIC                 coalesce(col("evt_tx_titulo"), lit("")),
# MAGIC                 coalesce(col("evt_tx_tipo_evento"), lit("")),
# MAGIC                 coalesce(col("evt_tx_situacao"), lit("")),
# MAGIC                 coalesce(col("evt_tx_tipo_orgao"), lit("")),
# MAGIC                 coalesce(col("evt_tx_sigla_orgao"), lit("")),
# MAGIC             )
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_tx_texto_base_normalizado",
# MAGIC         regexp_replace(
# MAGIC             col("cpi_evt_tx_texto_base"),
# MAGIC             r"\s+",
# MAGIC             " ",
# MAGIC         )
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 7. Detect Direct CPI Relationships
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC direct_cpi_eventos_df = (
# MAGIC     eventos_text_df.alias("evt")
# MAGIC     .join(
# MAGIC         cpis_df.alias("cpi"),
# MAGIC         col("evt.evt_id_orgao") == col("cpi.cpi_id_orgao"),
# MAGIC         "inner",
# MAGIC     )
# MAGIC     .select(
# MAGIC         col("evt.*"),
# MAGIC         col("cpi.cpi_id_orgao"),
# MAGIC         col("cpi.cpi_tx_sigla"),
# MAGIC         col("cpi.cpi_tx_nome"),
# MAGIC         col("cpi.cpi_tx_tipo"),
# MAGIC         col("cpi.cpi_tx_tipo_descricao"),
# MAGIC         col("cpi.cpi_tx_abrangencia"),
# MAGIC         col("cpi.cpi_tx_status_analitico"),
# MAGIC         col("cpi.cpi_fl_mista"),
# MAGIC         col("cpi.cpi_fl_ativa"),
# MAGIC         col("cpi.cpi_dt_inicio"),
# MAGIC         col("cpi.cpi_dt_fim"),
# MAGIC         col("cpi.cpi_nr_ano_inicio"),
# MAGIC         col("cpi.leg_id_legislatura_cpi"),
# MAGIC         col("cpi.aud_id_execucao_cpis_silver"),
# MAGIC         col("cpi.aud_dh_processamento_cpis_silver"),
# MAGIC         col("cpi.aud_tx_hash_registro_cpis_silver"),
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_tx_tipo_relacao",
# MAGIC         lit("DIRECT_ORGAO_RELATION")
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_nr_score_confianca",
# MAGIC         lit(100)
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_tx_nivel_confianca",
# MAGIC         lit("HIGH")
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC records_direct_candidates = direct_cpi_eventos_df.count()
# MAGIC
# MAGIC log_info(
# MAGIC     pipeline_logger=logger,
# MAGIC     message=(
# MAGIC         f"Direct CPI event relationships identified "
# MAGIC         f"| direct_candidates={records_direct_candidates}"
# MAGIC     ),
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 8. Detect Semantic CPI Event Candidates
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC direct_event_ids_df = (
# MAGIC     direct_cpi_eventos_df
# MAGIC     .select("evt_id_evento")
# MAGIC     .distinct()
# MAGIC )
# MAGIC
# MAGIC semantic_base_df = (
# MAGIC     eventos_text_df.alias("evt")
# MAGIC     .join(
# MAGIC         direct_event_ids_df.alias("direct"),
# MAGIC         col("evt.evt_id_evento") == col("direct.evt_id_evento"),
# MAGIC         "left_anti",
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC semantic_eventos_df = (
# MAGIC     semantic_base_df
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_termo_cpi",
# MAGIC         col("cpi_evt_tx_texto_base_normalizado").rlike(
# MAGIC             r"(^|[^A-Z])CPI([^A-Z]|$)"
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_termo_cpmi",
# MAGIC         col("cpi_evt_tx_texto_base_normalizado").rlike(
# MAGIC             r"(^|[^A-Z])CPMI([^A-Z]|$)"
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_termo_comissao_inquerito",
# MAGIC         col("cpi_evt_tx_texto_base_normalizado").like(
# MAGIC             "%COMISSÃO PARLAMENTAR DE INQUÉRITO%"
# MAGIC         )
# MAGIC         | col("cpi_evt_tx_texto_base_normalizado").like(
# MAGIC             "%COMISSAO PARLAMENTAR DE INQUERITO%"
# MAGIC         )
# MAGIC         | col("cpi_evt_tx_texto_base_normalizado").like(
# MAGIC             "%COMISSÃO PARLAMENTAR MISTA DE INQUÉRITO%"
# MAGIC         )
# MAGIC         | col("cpi_evt_tx_texto_base_normalizado").like(
# MAGIC             "%COMISSAO PARLAMENTAR MISTA DE INQUERITO%"
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_termo_inquerito",
# MAGIC         col("cpi_evt_tx_texto_base_normalizado").like("%INQUÉRITO%")
# MAGIC         | col("cpi_evt_tx_texto_base_normalizado").like("%INQUERITO%")
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_termo_investigacao",
# MAGIC         col("cpi_evt_tx_texto_base_normalizado").like("%INVESTIGAÇÃO%")
# MAGIC         | col("cpi_evt_tx_texto_base_normalizado").like("%INVESTIGACAO%")
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_evento_semantico_cpi",
# MAGIC         (
# MAGIC             col("cpi_evt_fl_termo_cpi")
# MAGIC             | col("cpi_evt_fl_termo_cpmi")
# MAGIC             | col("cpi_evt_fl_termo_comissao_inquerito")
# MAGIC             | col("cpi_evt_fl_termo_inquerito")
# MAGIC             | col("cpi_evt_fl_termo_investigacao")
# MAGIC         )
# MAGIC     )
# MAGIC     .filter(
# MAGIC         col("cpi_evt_fl_evento_semantico_cpi") == True
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC records_semantic_candidates = semantic_eventos_df.count()
# MAGIC
# MAGIC log_info(
# MAGIC     pipeline_logger=logger,
# MAGIC     message=(
# MAGIC         f"Semantic CPI event candidates identified "
# MAGIC         f"| semantic_candidates={records_semantic_candidates}"
# MAGIC     ),
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 9. Apply Semantic Classification Rules
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC semantic_cpi_eventos_df = (
# MAGIC     semantic_eventos_df
# MAGIC     .withColumn("cpi_id_orgao", lit(None).cast(StringType()))
# MAGIC     .withColumn("cpi_tx_sigla", lit(None).cast(StringType()))
# MAGIC     .withColumn("cpi_tx_nome", lit(None).cast(StringType()))
# MAGIC     .withColumn(
# MAGIC         "cpi_tx_tipo",
# MAGIC         when(
# MAGIC             col("cpi_evt_fl_termo_cpmi")
# MAGIC             | col("cpi_evt_tx_texto_base_normalizado").like(
# MAGIC                 "%COMISSÃO PARLAMENTAR MISTA DE INQUÉRITO%"
# MAGIC             )
# MAGIC             | col("cpi_evt_tx_texto_base_normalizado").like(
# MAGIC                 "%COMISSAO PARLAMENTAR MISTA DE INQUERITO%"
# MAGIC             ),
# MAGIC             lit("CPMI")
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("cpi_evt_fl_termo_cpi")
# MAGIC             | col("cpi_evt_fl_termo_comissao_inquerito"),
# MAGIC             lit("CPI")
# MAGIC         )
# MAGIC         .otherwise(lit("INQUIRY_RELATED"))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_tx_tipo_descricao",
# MAGIC         when(
# MAGIC             col("cpi_tx_tipo") == "CPMI",
# MAGIC             lit("Comissão Parlamentar Mista de Inquérito")
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("cpi_tx_tipo") == "CPI",
# MAGIC             lit("Comissão Parlamentar de Inquérito")
# MAGIC         )
# MAGIC         .otherwise(
# MAGIC             lit("Evento relacionado a inquérito ou investigação")
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn("cpi_tx_abrangencia", lit(None).cast(StringType()))
# MAGIC     .withColumn("cpi_tx_status_analitico", lit(None).cast(StringType()))
# MAGIC     .withColumn(
# MAGIC         "cpi_fl_mista",
# MAGIC         when(
# MAGIC             col("cpi_tx_tipo") == "CPMI",
# MAGIC             lit(True)
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("cpi_tx_tipo") == "CPI",
# MAGIC             lit(False)
# MAGIC         )
# MAGIC         .otherwise(lit(None).cast("boolean"))
# MAGIC     )
# MAGIC     .withColumn("cpi_fl_ativa", lit(None).cast("boolean"))
# MAGIC     .withColumn("cpi_dt_inicio", lit(None).cast("date"))
# MAGIC     .withColumn("cpi_dt_fim", lit(None).cast("date"))
# MAGIC     .withColumn("cpi_nr_ano_inicio", lit(None).cast(IntegerType()))
# MAGIC     .withColumn("leg_id_legislatura_cpi", lit(None).cast(IntegerType()))
# MAGIC     .withColumn("aud_id_execucao_cpis_silver", lit(None).cast(StringType()))
# MAGIC     .withColumn("aud_dh_processamento_cpis_silver", lit(None).cast("timestamp"))
# MAGIC     .withColumn("aud_tx_hash_registro_cpis_silver", lit(None).cast(StringType()))
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_tx_tipo_relacao",
# MAGIC         when(
# MAGIC             col("cpi_evt_fl_termo_cpi")
# MAGIC             | col("cpi_evt_fl_termo_cpmi")
# MAGIC             | col("cpi_evt_fl_termo_comissao_inquerito"),
# MAGIC             lit("SEMANTIC_CPI_EXPLICIT")
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("cpi_evt_fl_termo_inquerito"),
# MAGIC             lit("SEMANTIC_INQUERITO")
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("cpi_evt_fl_termo_investigacao"),
# MAGIC             lit("SEMANTIC_INVESTIGACAO")
# MAGIC         )
# MAGIC         .otherwise(lit("SEMANTIC_RELATED"))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_nr_score_confianca",
# MAGIC         when(
# MAGIC             col("cpi_evt_tx_tipo_relacao") == "SEMANTIC_CPI_EXPLICIT",
# MAGIC             lit(90)
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("cpi_evt_tx_tipo_relacao") == "SEMANTIC_INQUERITO",
# MAGIC             lit(60)
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("cpi_evt_tx_tipo_relacao") == "SEMANTIC_INVESTIGACAO",
# MAGIC             lit(50)
# MAGIC         )
# MAGIC         .otherwise(lit(40))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_tx_nivel_confianca",
# MAGIC         when(
# MAGIC             col("cpi_evt_nr_score_confianca") >= 80,
# MAGIC             lit("HIGH")
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("cpi_evt_nr_score_confianca") >= 50,
# MAGIC             lit("MEDIUM")
# MAGIC         )
# MAGIC         .otherwise(lit("LOW"))
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 10. Union Direct and Semantic Candidates
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC semantic_flag_columns = [
# MAGIC     "cpi_evt_fl_termo_cpi",
# MAGIC     "cpi_evt_fl_termo_cpmi",
# MAGIC     "cpi_evt_fl_termo_comissao_inquerito",
# MAGIC     "cpi_evt_fl_termo_inquerito",
# MAGIC     "cpi_evt_fl_termo_investigacao",
# MAGIC     "cpi_evt_fl_evento_semantico_cpi",
# MAGIC ]
# MAGIC
# MAGIC for semantic_flag_column in semantic_flag_columns:
# MAGIC     if semantic_flag_column not in direct_cpi_eventos_df.columns:
# MAGIC         direct_cpi_eventos_df = direct_cpi_eventos_df.withColumn(
# MAGIC             semantic_flag_column,
# MAGIC             lit(False)
# MAGIC         )
# MAGIC
# MAGIC for semantic_flag_column in semantic_flag_columns:
# MAGIC     if semantic_flag_column not in semantic_cpi_eventos_df.columns:
# MAGIC         semantic_cpi_eventos_df = semantic_cpi_eventos_df.withColumn(
# MAGIC             semantic_flag_column,
# MAGIC             lit(False)
# MAGIC         )
# MAGIC
# MAGIC candidate_columns = list(
# MAGIC     dict.fromkeys(
# MAGIC         direct_cpi_eventos_df.columns
# MAGIC         + semantic_cpi_eventos_df.columns
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC for candidate_column in candidate_columns:
# MAGIC     if candidate_column not in direct_cpi_eventos_df.columns:
# MAGIC         direct_cpi_eventos_df = direct_cpi_eventos_df.withColumn(
# MAGIC             candidate_column,
# MAGIC             lit(None)
# MAGIC         )
# MAGIC
# MAGIC     if candidate_column not in semantic_cpi_eventos_df.columns:
# MAGIC         semantic_cpi_eventos_df = semantic_cpi_eventos_df.withColumn(
# MAGIC             candidate_column,
# MAGIC             lit(None)
# MAGIC         )
# MAGIC
# MAGIC direct_aligned_df = direct_cpi_eventos_df.select(*candidate_columns)
# MAGIC semantic_aligned_df = semantic_cpi_eventos_df.select(*candidate_columns)
# MAGIC
# MAGIC cpi_eventos_candidates_df = (
# MAGIC     direct_aligned_df
# MAGIC     .unionByName(semantic_aligned_df)
# MAGIC )
# MAGIC
# MAGIC records_total_candidates = cpi_eventos_candidates_df.count()
# MAGIC
# MAGIC log_info(
# MAGIC     pipeline_logger=logger,
# MAGIC     message=(
# MAGIC         f"Total CPI event candidates prepared "
# MAGIC         f"| total_candidates={records_total_candidates}"
# MAGIC     ),
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 11. Apply CPI Event Analytical Derivations
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC cpi_eventos_enriched_df = (
# MAGIC     cpi_eventos_candidates_df
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_id_relacao",
# MAGIC         sha2(
# MAGIC             concat_ws(
# MAGIC                 "||",
# MAGIC                 coalesce(col("cpi_id_orgao"), lit("SEMANTIC_ONLY")),
# MAGIC                 coalesce(col("evt_id_evento"), lit("UNKNOWN_EVENT")),
# MAGIC                 coalesce(col("cpi_evt_tx_tipo_relacao"), lit("UNKNOWN_RELATION")),
# MAGIC             ),
# MAGIC             256,
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_nr_ano_evento",
# MAGIC         year(col("evt_dt_inicio")).cast(IntegerType())
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_nr_mes_evento",
# MAGIC         month(col("evt_dt_inicio")).cast(IntegerType())
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_cpi_identificada",
# MAGIC         when(col("cpi_id_orgao").isNotNull(), lit(True)).otherwise(lit(False))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_relacao_direta",
# MAGIC         when(
# MAGIC             col("cpi_evt_tx_tipo_relacao") == "DIRECT_ORGAO_RELATION",
# MAGIC             lit(True)
# MAGIC         ).otherwise(lit(False))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_relacao_semantica",
# MAGIC         when(
# MAGIC             col("cpi_evt_tx_tipo_relacao").like("SEMANTIC%"),
# MAGIC             lit(True)
# MAGIC         ).otherwise(lit(False))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_alta_confianca",
# MAGIC         when(
# MAGIC             col("cpi_evt_nr_score_confianca") >= 80,
# MAGIC             lit(True)
# MAGIC         ).otherwise(lit(False))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_evento_com_data",
# MAGIC         when(col("evt_dh_inicio").isNotNull(), lit(True)).otherwise(lit(False))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_periodo_evento_valido",
# MAGIC         when(
# MAGIC             col("evt_dh_inicio").isNotNull()
# MAGIC             & col("evt_dh_fim").isNotNull()
# MAGIC             & (col("evt_dh_inicio") > col("evt_dh_fim")),
# MAGIC             lit(False)
# MAGIC         ).otherwise(lit(True))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_evento_apos_inicio_cpi",
# MAGIC         when(
# MAGIC             col("cpi_dt_inicio").isNull()
# MAGIC             | col("evt_dt_inicio").isNull(),
# MAGIC             lit(None).cast("boolean")
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("evt_dt_inicio") >= col("cpi_dt_inicio"),
# MAGIC             lit(True)
# MAGIC         )
# MAGIC         .otherwise(lit(False))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_evento_antes_fim_cpi",
# MAGIC         when(
# MAGIC             col("cpi_dt_fim").isNull()
# MAGIC             | col("evt_dt_inicio").isNull(),
# MAGIC             lit(None).cast("boolean")
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("evt_dt_inicio") <= col("cpi_dt_fim"),
# MAGIC             lit(True)
# MAGIC         )
# MAGIC         .otherwise(lit(False))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_temporalmente_consistente",
# MAGIC         when(
# MAGIC             col("cpi_evt_fl_evento_apos_inicio_cpi") == False,
# MAGIC             lit(False)
# MAGIC         )
# MAGIC         .when(
# MAGIC             col("cpi_evt_fl_evento_antes_fim_cpi") == False,
# MAGIC             lit(False)
# MAGIC         )
# MAGIC         .otherwise(lit(True))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_mesma_legislatura_cpi",
# MAGIC         when(
# MAGIC             col("leg_id_legislatura_cpi").isNotNull()
# MAGIC             & col("leg_id_legislatura_evento").isNotNull()
# MAGIC             & (col("leg_id_legislatura_cpi") == col("leg_id_legislatura_evento")),
# MAGIC             lit(True)
# MAGIC         ).otherwise(lit(False))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_tx_status_evento_analitico",
# MAGIC         when(
# MAGIC             upper(col("evt_tx_situacao")).like("%CANCEL%"),
# MAGIC             lit("CANCELADO")
# MAGIC         )
# MAGIC         .when(
# MAGIC             upper(col("evt_tx_situacao")).like("%ENCERR%"),
# MAGIC             lit("ENCERRADO")
# MAGIC         )
# MAGIC         .when(
# MAGIC             upper(col("evt_tx_situacao")).like("%REALIZ%"),
# MAGIC             lit("REALIZADO")
# MAGIC         )
# MAGIC         .when(
# MAGIC             upper(col("evt_tx_situacao")).like("%AGEND%")
# MAGIC             | upper(col("evt_tx_situacao")).like("%CONVOC%"),
# MAGIC             lit("AGENDADO")
# MAGIC         )
# MAGIC         .otherwise(lit("STATUS_INDEFINIDO"))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_evento_realizado",
# MAGIC         when(
# MAGIC             col("cpi_evt_tx_status_evento_analitico").isin(
# MAGIC                 "REALIZADO",
# MAGIC                 "ENCERRADO",
# MAGIC             ),
# MAGIC             lit(True)
# MAGIC         ).otherwise(lit(False))
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_tx_payload_origem_json",
# MAGIC         to_json(
# MAGIC             struct(
# MAGIC                 col("evt_id_evento"),
# MAGIC                 col("evt_id_orgao"),
# MAGIC                 col("evt_tx_sigla_orgao"),
# MAGIC                 col("evt_tx_tipo_orgao"),
# MAGIC                 col("evt_tx_titulo"),
# MAGIC                 col("evt_tx_tipo_evento"),
# MAGIC                 col("evt_dh_inicio_origem"),
# MAGIC                 col("evt_dh_fim_origem"),
# MAGIC                 col("cpi_id_orgao"),
# MAGIC                 col("cpi_tx_tipo"),
# MAGIC                 col("cpi_evt_tx_tipo_relacao"),
# MAGIC                 col("cpi_evt_nr_score_confianca"),
# MAGIC                 col("cpi_evt_tx_nivel_confianca"),
# MAGIC             )
# MAGIC         )
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 12. Apply Quality Rules
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC cpi_eventos_quality_df = (
# MAGIC     cpi_eventos_enriched_df
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_id_relacao_valido",
# MAGIC         (
# MAGIC             col("cpi_evt_id_relacao").isNotNull()
# MAGIC             & (trim(col("cpi_evt_id_relacao")) != "")
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_evento_identificado",
# MAGIC         (
# MAGIC             col("evt_id_evento").isNotNull()
# MAGIC             & (trim(col("evt_id_evento")) != "")
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_orgao_evento_identificado",
# MAGIC         (
# MAGIC             col("evt_id_orgao").isNotNull()
# MAGIC             & (trim(col("evt_id_orgao")) != "")
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_texto_classificacao_informado",
# MAGIC         (
# MAGIC             col("cpi_evt_tx_texto_base_normalizado").isNotNull()
# MAGIC             & (trim(col("cpi_evt_tx_texto_base_normalizado")) != "")
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_regra_semantica_valida",
# MAGIC         (
# MAGIC             col("cpi_evt_fl_relacao_direta")
# MAGIC             | col("cpi_evt_fl_relacao_semantica")
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_fl_registro_valido_silver",
# MAGIC         (
# MAGIC             col("cpi_evt_fl_id_relacao_valido")
# MAGIC             & col("cpi_evt_fl_evento_identificado")
# MAGIC             & col("cpi_evt_fl_texto_classificacao_informado")
# MAGIC             & col("cpi_evt_fl_regra_semantica_valida")
# MAGIC             & col("cpi_evt_fl_periodo_evento_valido")
# MAGIC         )
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "cpi_evt_tx_motivo_rejeicao",
# MAGIC         when(
# MAGIC             ~col("cpi_evt_fl_id_relacao_valido"),
# MAGIC             lit("CPI_EVENTO_ID_RELACAO_INVALIDO")
# MAGIC         )
# MAGIC         .when(
# MAGIC             ~col("cpi_evt_fl_evento_identificado"),
# MAGIC             lit("CPI_EVENTO_EVENTO_NAO_IDENTIFICADO")
# MAGIC         )
# MAGIC         .when(
# MAGIC             ~col("cpi_evt_fl_texto_classificacao_informado"),
# MAGIC             lit("CPI_EVENTO_TEXTO_CLASSIFICACAO_NAO_INFORMADO")
# MAGIC         )
# MAGIC         .when(
# MAGIC             ~col("cpi_evt_fl_regra_semantica_valida"),
# MAGIC             lit("CPI_EVENTO_REGRA_SEMANTICA_INVALIDA")
# MAGIC         )
# MAGIC         .when(
# MAGIC             ~col("cpi_evt_fl_periodo_evento_valido"),
# MAGIC             lit("CPI_EVENTO_PERIODO_INVALIDO")
# MAGIC         )
# MAGIC         .otherwise(lit(None).cast(StringType()))
# MAGIC     )
# MAGIC )
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 13. Build Rejected Records
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC mandatory_rejected_source_df = (
# MAGIC     cpi_eventos_quality_df
# MAGIC     .filter(col("cpi_evt_fl_registro_valido_silver") == False)
# MAGIC )
# MAGIC
# MAGIC mandatory_rejected_df = build_mandatory_rejected_records(
# MAGIC     dataframe=mandatory_rejected_source_df,
# MAGIC     execution_id=execution_id,
# MAGIC     source_table=SOURCE_EVENTOS_TABLE,
# MAGIC     target_table=TARGET_TABLE,
# MAGIC     project_version=PROJECT_VERSION,
# MAGIC     entity_name=ENTITY_NAME,
# MAGIC     record_id_column="cpi_evt_id_relacao",
# MAGIC     validation_rule_column="cpi_evt_tx_motivo_rejeicao",
# MAGIC     payload_column="cpi_evt_tx_payload_origem_json",
# MAGIC     valid_flag_column="cpi_evt_fl_registro_valido_silver",
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 14. Keep Valid CPI Event Candidate Records
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC valid_df = (
# MAGIC     cpi_eventos_quality_df
# MAGIC     .filter(col("cpi_evt_fl_registro_valido_silver") == True)
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 15. Deduplicate CPI Event Candidate Records
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC dedup_window = (
# MAGIC     Window
# MAGIC     .partitionBy("cpi_evt_id_relacao")
# MAGIC     .orderBy(
# MAGIC         col("cpi_evt_nr_score_confianca").desc_nulls_last(),
# MAGIC         col("aud_dh_processamento_eventos_silver").desc_nulls_last(),
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC dedup_df = (
# MAGIC     valid_df
# MAGIC     .withColumn(
# MAGIC         "rn_deduplicacao",
# MAGIC         row_number().over(dedup_window)
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC duplicate_rejected_df = build_duplicate_rejected_records(
# MAGIC     dataframe=dedup_df,
# MAGIC     execution_id=execution_id,
# MAGIC     source_table=SOURCE_EVENTOS_TABLE,
# MAGIC     target_table=TARGET_TABLE,
# MAGIC     project_version=PROJECT_VERSION,
# MAGIC     entity_name=ENTITY_NAME,
# MAGIC     record_id_column="cpi_evt_id_relacao",
# MAGIC     payload_column="cpi_evt_tx_payload_origem_json",
# MAGIC     dedup_rank_column="rn_deduplicacao",
# MAGIC     duplicate_rule_code="CPI_EVENTO_REGISTRO_DUPLICADO",
# MAGIC     observation=(
# MAGIC         "Duplicate CPI event candidate removed keeping highest confidence classification."
# MAGIC     ),
# MAGIC )
# MAGIC
# MAGIC silver_df = (
# MAGIC     dedup_df
# MAGIC     .filter(col("rn_deduplicacao") == 1)
# MAGIC     .drop("rn_deduplicacao")
# MAGIC     .drop("cpi_evt_tx_motivo_rejeicao")
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 16. Persist Rejected Records
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC rejected_df = union_rejected_records(
# MAGIC     mandatory_rejected_dataframe=mandatory_rejected_df,
# MAGIC     duplicate_rejected_dataframe=duplicate_rejected_df,
# MAGIC )
# MAGIC
# MAGIC records_rejected = rejected_df.count()
# MAGIC
# MAGIC clean_and_persist_rejected_records(
# MAGIC     rejected_dataframe=rejected_df,
# MAGIC     rejected_table=REJECTED_TABLE,
# MAGIC     entity_name=ENTITY_NAME,
# MAGIC     target_table=TARGET_TABLE,
# MAGIC     mode="append",
# MAGIC )
# MAGIC
# MAGIC log_info(
# MAGIC     pipeline_logger=logger,
# MAGIC     message=(
# MAGIC         f"Rejected and discarded CPI event records persisted "
# MAGIC         f"| records_rejected={records_rejected}"
# MAGIC     ),
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 17. Add Silver Traceability Columns
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC silver_df = (
# MAGIC     silver_df
# MAGIC     .withColumn("aud_id_execucao_silver", lit(execution_id))
# MAGIC     .withColumn("aud_dh_processamento", current_timestamp())
# MAGIC     .withColumn("aud_tx_camada_origem", lit("silver"))
# MAGIC     .withColumn("aud_tx_tabela_origem_cpis", lit(SOURCE_CPI_TABLE))
# MAGIC     .withColumn("aud_tx_tabela_origem_eventos", lit(SOURCE_EVENTOS_TABLE))
# MAGIC     .withColumn("aud_tx_tabela_origem_orgaos", lit(SOURCE_ORGAOS_TABLE))
# MAGIC     .withColumn("aud_tx_tabela_destino", lit(TARGET_TABLE))
# MAGIC     .withColumn("aud_tx_versao_pipeline_silver", lit(PROJECT_VERSION))
# MAGIC     .withColumn(
# MAGIC         "aud_tx_regra_derivacao",
# MAGIC         lit(
# MAGIC             "CPI event candidates derived by direct CPI body relationship when available and by governed semantic rules over event text when direct relationship is unavailable."
# MAGIC         )
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 18. Add Silver Hash
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC silver_df = add_hash(
# MAGIC     dataframe=silver_df,
# MAGIC     columns=[
# MAGIC         "cpi_evt_id_relacao",
# MAGIC         "evt_id_evento",
# MAGIC         "evt_id_orgao",
# MAGIC         "cpi_id_orgao",
# MAGIC         "cpi_evt_tx_tipo_relacao",
# MAGIC         "cpi_evt_nr_score_confianca",
# MAGIC         "evt_dh_inicio",
# MAGIC     ],
# MAGIC     hash_column="aud_tx_hash_registro_silver",
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 19. Select Final Columns
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC final_columns = [
# MAGIC     "cpi_evt_id_relacao",
# MAGIC
# MAGIC     "cpi_id_orgao",
# MAGIC     "cpi_tx_sigla",
# MAGIC     "cpi_tx_nome",
# MAGIC     "cpi_tx_tipo",
# MAGIC     "cpi_tx_tipo_descricao",
# MAGIC     "cpi_tx_abrangencia",
# MAGIC     "cpi_tx_status_analitico",
# MAGIC     "cpi_fl_mista",
# MAGIC     "cpi_fl_ativa",
# MAGIC     "cpi_dt_inicio",
# MAGIC     "cpi_dt_fim",
# MAGIC     "cpi_nr_ano_inicio",
# MAGIC     "leg_id_legislatura_cpi",
# MAGIC
# MAGIC     "evt_id_evento",
# MAGIC     "evt_id_orgao",
# MAGIC     "evt_tx_sigla_orgao",
# MAGIC     "evt_tx_nome_orgao",
# MAGIC     "evt_tx_tipo_orgao",
# MAGIC     "evt_tx_uri",
# MAGIC     "evt_tx_situacao",
# MAGIC     "evt_tx_titulo",
# MAGIC     "evt_tx_tipo_evento",
# MAGIC     "evt_tx_local",
# MAGIC     "evt_dh_inicio_origem",
# MAGIC     "evt_dh_fim_origem",
# MAGIC     "evt_dh_inicio",
# MAGIC     "evt_dh_fim",
# MAGIC     "evt_dt_inicio",
# MAGIC     "evt_dt_fim",
# MAGIC     "evt_nr_ano",
# MAGIC     "evt_nr_mes",
# MAGIC     "leg_id_legislatura_evento",
# MAGIC
# MAGIC     "cpi_evt_tx_tipo_relacao",
# MAGIC     "cpi_evt_nr_score_confianca",
# MAGIC     "cpi_evt_tx_nivel_confianca",
# MAGIC     "cpi_evt_fl_alta_confianca",
# MAGIC     "cpi_evt_tx_status_evento_analitico",
# MAGIC     "cpi_evt_tx_texto_base_normalizado",
# MAGIC
# MAGIC     "cpi_evt_fl_termo_cpi",
# MAGIC     "cpi_evt_fl_termo_cpmi",
# MAGIC     "cpi_evt_fl_termo_comissao_inquerito",
# MAGIC     "cpi_evt_fl_termo_inquerito",
# MAGIC     "cpi_evt_fl_termo_investigacao",
# MAGIC     "cpi_evt_fl_evento_semantico_cpi",
# MAGIC
# MAGIC     "cpi_evt_fl_cpi_identificada",
# MAGIC     "cpi_evt_fl_relacao_direta",
# MAGIC     "cpi_evt_fl_relacao_semantica",
# MAGIC     "cpi_evt_fl_evento_com_data",
# MAGIC     "cpi_evt_fl_evento_realizado",
# MAGIC     "cpi_evt_fl_mesma_legislatura_cpi",
# MAGIC     "cpi_evt_fl_evento_apos_inicio_cpi",
# MAGIC     "cpi_evt_fl_evento_antes_fim_cpi",
# MAGIC     "cpi_evt_fl_temporalmente_consistente",
# MAGIC     "cpi_evt_fl_periodo_evento_valido",
# MAGIC     "cpi_evt_fl_id_relacao_valido",
# MAGIC     "cpi_evt_fl_evento_identificado",
# MAGIC     "cpi_evt_fl_orgao_evento_identificado",
# MAGIC     "cpi_evt_fl_texto_classificacao_informado",
# MAGIC     "cpi_evt_fl_regra_semantica_valida",
# MAGIC     "cpi_evt_fl_registro_valido_silver",
# MAGIC
# MAGIC     "cpi_evt_tx_payload_origem_json",
# MAGIC
# MAGIC     "aud_id_execucao_bronze",
# MAGIC     "aud_dh_ingestao_bronze",
# MAGIC     "aud_tx_hash_registro_bronze",
# MAGIC
# MAGIC     "aud_id_execucao_cpis_silver",
# MAGIC     "aud_dh_processamento_cpis_silver",
# MAGIC     "aud_tx_hash_registro_cpis_silver",
# MAGIC
# MAGIC     "aud_id_execucao_eventos_silver",
# MAGIC     "aud_dh_processamento_eventos_silver",
# MAGIC     "aud_tx_hash_registro_eventos_silver",
# MAGIC
# MAGIC     "aud_id_execucao_silver",
# MAGIC     "aud_dh_processamento",
# MAGIC     "aud_tx_camada_origem",
# MAGIC     "aud_tx_tabela_origem_cpis",
# MAGIC     "aud_tx_tabela_origem_eventos",
# MAGIC     "aud_tx_tabela_origem_orgaos",
# MAGIC     "aud_tx_tabela_destino",
# MAGIC     "aud_tx_versao_pipeline_silver",
# MAGIC     "aud_tx_regra_derivacao",
# MAGIC     "aud_tx_hash_registro_silver",
# MAGIC ]
# MAGIC
# MAGIC silver_df = silver_df.select(*final_columns)
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 20. Persist Silver Table
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC (
# MAGIC     silver_df.write
# MAGIC     .format("delta")
# MAGIC     .mode("overwrite")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .saveAsTable(TARGET_TABLE)
# MAGIC )
# MAGIC
# MAGIC records_written = spark.table(TARGET_TABLE).count()
# MAGIC
# MAGIC log_info(
# MAGIC     pipeline_logger=logger,
# MAGIC     message=(
# MAGIC         f"Silver CPI events table persisted successfully "
# MAGIC         f"| records_written={records_written}"
# MAGIC     ),
# MAGIC )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 21. Apply Governance Comments
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC table_comment = """
# MAGIC Standardized CPI event candidate table in the Silver layer.
# MAGIC
# MAGIC This table contains validated and deduplicated legislative events potentially
# MAGIC related to Parliamentary Inquiry Commissions.
# MAGIC
# MAGIC The table supports CPI audit analytics by combining direct CPI relationship
# MAGIC rules with governed semantic detection rules over event textual attributes.
# MAGIC
# MAGIC Current Câmara event data may not expose a direct CPI body relationship through
# MAGIC event legislative body identifiers. Therefore, this table preserves direct
# MAGIC relationships when available and otherwise identifies semantically related
# MAGIC events with confidence levels, avoiding false CPI joins.
# MAGIC """
# MAGIC
# MAGIC column_comments = {
# MAGIC     "cpi_evt_id_relacao":
# MAGIC         "Deterministic CPI event candidate relationship identifier.",
# MAGIC
# MAGIC     "cpi_id_orgao":
# MAGIC         "CPI legislative body identifier when direct CPI relationship is available.",
# MAGIC
# MAGIC     "cpi_tx_sigla":
# MAGIC         "CPI acronym when direct CPI relationship is available.",
# MAGIC
# MAGIC     "cpi_tx_nome":
# MAGIC         "CPI name when direct CPI relationship is available.",
# MAGIC
# MAGIC     "cpi_tx_tipo":
# MAGIC         "Analytical CPI type inferred from direct CPI context or semantic classification.",
# MAGIC
# MAGIC     "evt_id_evento":
# MAGIC         "Legislative event identifier inherited from slv_eventos.",
# MAGIC
# MAGIC     "evt_id_orgao":
# MAGIC         "Legislative body identifier associated with the event.",
# MAGIC
# MAGIC     "evt_tx_titulo":
# MAGIC         "Standardized event title used for semantic CPI detection.",
# MAGIC
# MAGIC     "evt_tx_tipo_evento":
# MAGIC         "Standardized event type.",
# MAGIC
# MAGIC     "cpi_evt_tx_tipo_relacao":
# MAGIC         "Relationship type used to classify the CPI event candidate.",
# MAGIC
# MAGIC     "cpi_evt_nr_score_confianca":
# MAGIC         "Numeric confidence score assigned to the CPI event candidate.",
# MAGIC
# MAGIC     "cpi_evt_tx_nivel_confianca":
# MAGIC         "Confidence level assigned to the CPI event candidate.",
# MAGIC
# MAGIC     "cpi_evt_fl_alta_confianca":
# MAGIC         "Flag indicating whether the CPI event candidate has high confidence score.",
# MAGIC
# MAGIC     "cpi_evt_fl_relacao_direta":
# MAGIC         "Flag indicating whether event is directly linked to a CPI body.",
# MAGIC
# MAGIC     "cpi_evt_fl_relacao_semantica":
# MAGIC         "Flag indicating whether event was detected through semantic rules.",
# MAGIC
# MAGIC     "cpi_evt_fl_termo_cpi":
# MAGIC         "Flag indicating whether text contains explicit CPI keyword.",
# MAGIC
# MAGIC     "cpi_evt_fl_termo_cpmi":
# MAGIC         "Flag indicating whether text contains explicit CPMI keyword.",
# MAGIC
# MAGIC     "cpi_evt_fl_termo_inquerito":
# MAGIC         "Flag indicating whether text contains inquiry-related keyword.",
# MAGIC
# MAGIC     "cpi_evt_fl_termo_investigacao":
# MAGIC         "Flag indicating whether text contains investigation-related keyword.",
# MAGIC
# MAGIC     "cpi_evt_fl_registro_valido_silver":
# MAGIC         "Flag indicating whether CPI event candidate passed Silver validation.",
# MAGIC
# MAGIC     "cpi_evt_tx_payload_origem_json":
# MAGIC         "JSON payload preserving source fields used for CPI event candidate derivation.",
# MAGIC
# MAGIC     "aud_tx_regra_derivacao":
# MAGIC         "Textual description of CPI event candidate derivation rule.",
# MAGIC
# MAGIC     "aud_tx_hash_registro_silver":
# MAGIC         "Deterministic Silver hash used for CPI event traceability.",
# MAGIC }
# MAGIC
# MAGIC if APPLY_GOVERNANCE_COMMENTS:
# MAGIC     apply_governance_comments(
# MAGIC         table_name=TARGET_TABLE,
# MAGIC         table_comment=table_comment,
# MAGIC         column_comments=column_comments,
# MAGIC     )
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # ==========================================================================================
# MAGIC # 22. Final Pipeline Log
# MAGIC # ==========================================================================================
# MAGIC
# MAGIC finished_at = datetime.now()
# MAGIC
# MAGIC duration_seconds = (
# MAGIC     finished_at - started_at
# MAGIC ).total_seconds()
# MAGIC
# MAGIC write_pipeline_log(
# MAGIC     log_id=str(uuid.uuid4()),
# MAGIC     execution_id=execution_id,
# MAGIC     notebook_name=NOTEBOOK_NAME,
# MAGIC     layer_name=LAYER_NAME,
# MAGIC     entity_name=ENTITY_NAME,
# MAGIC     target_table=TARGET_TABLE,
# MAGIC     status=EXECUTION_STATUS_SUCCESS,
# MAGIC     message=(
# MAGIC         f"Silver CPI events semantic derivation completed successfully "
# MAGIC         f"| events_read={records_eventos_read} "
# MAGIC         f"| cpis_read={records_cpis_read} "
# MAGIC         f"| direct_candidates={records_direct_candidates} "
# MAGIC         f"| semantic_candidates={records_semantic_candidates} "
# MAGIC         f"| total_candidates={records_total_candidates} "
# MAGIC         f"| records_written={records_written} "
# MAGIC         f"| records_rejected={records_rejected}"
# MAGIC     ),
# MAGIC     started_at=started_at,
# MAGIC     finished_at=finished_at,
# MAGIC     duration_seconds=duration_seconds,
# MAGIC     records_read=records_read,
# MAGIC     records_written=records_written,
# MAGIC )
# MAGIC
# MAGIC log_success(
# MAGIC     pipeline_logger=logger,
# MAGIC     message=(
# MAGIC         f"Silver CPI events semantic derivation completed "
# MAGIC         f"| duration_seconds={duration_seconds}"
# MAGIC     ),
# MAGIC )
# MAGIC
# MAGIC print("=" * 90)
# MAGIC print("SILVER CPI EVENTOS COMPLETED")
# MAGIC print("=" * 90)
# MAGIC print(f"CPI Source Table: {SOURCE_CPI_TABLE}")
# MAGIC print(f"Event Source Table: {SOURCE_EVENTOS_TABLE}")
# MAGIC print(f"Orgaos Source Table: {SOURCE_ORGAOS_TABLE}")
# MAGIC print(f"Target Table: {TARGET_TABLE}")
# MAGIC print(f"Rejected Table: {REJECTED_TABLE}")
# MAGIC print(f"CPI Records Read: {records_cpis_read}")
# MAGIC print(f"Event Records Read: {records_eventos_read}")
# MAGIC print(f"Orgaos Records Read: {records_orgaos_read}")
# MAGIC print(f"Direct CPI Event Candidates: {records_direct_candidates}")
# MAGIC print(f"Semantic CPI Event Candidates: {records_semantic_candidates}")
# MAGIC print(f"Total Candidates Prepared: {records_total_candidates}")
# MAGIC print(f"Records Written: {records_written}")
# MAGIC print(f"Records Rejected: {records_rejected}")
# MAGIC print(f"Execution Duration: {duration_seconds}")
# MAGIC print("=" * 90)