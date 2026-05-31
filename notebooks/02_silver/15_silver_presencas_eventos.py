# Databricks notebook source
# MAGIC %md
# MAGIC # 15 Silver — Presenças Eventos Standardization
# MAGIC
# MAGIC **Notebook:** `15_silver_presencas_eventos`
# MAGIC
# MAGIC Standardizes deputy attendance records for legislative events from the Bronze layer and persists validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Legislative event attendance schema normalization rules
# MAGIC - Event and deputy identifier standardization
# MAGIC - Attendance flag normalization
# MAGIC - Integration with `slv_eventos`
# MAGIC - Integration with `slv_deputados`
# MAGIC - Quality validation rules
# MAGIC - Technical duplicate detection and removal
# MAGIC - Rejected records tracking using global utilities
# MAGIC - Silver Delta persistence logic
# MAGIC - Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read event attendance records from Bronze layer
# MAGIC - Standardize event, deputy and attendance attributes
# MAGIC - Validate mandatory attendance relationship fields
# MAGIC - Enrich event information from `slv_eventos`
# MAGIC - Enrich deputy information from `slv_deputados`
# MAGIC - Preserve historical attendance records even when dimensions are incomplete
# MAGIC - Remove technical duplicate records
# MAGIC - Register rejected records for traceability
# MAGIC - Persist curated Silver table
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - The grain of this table is one relationship between event and deputy attendance
# MAGIC - The source is CSV fallback from Câmara event attendance files
# MAGIC - The source does not provide deputy legislature directly
# MAGIC - Deputy enrichment uses `slv_deputados` when available
# MAGIC - Event enrichment uses `slv_eventos` when available
# MAGIC - Missing historical deputy attributes should be handled in Gold when required
# MAGIC - Invalid records are redirected to `slv_registros_rejeitados`
# MAGIC - Technical duplicates are also registered as discarded records
# MAGIC - Comments and documentation are written in English
# MAGIC - Naming conventions follow Portuguese mnemonic standards
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/decisions/silver_layer_strategy.md`
# MAGIC - `/docs/governance/data_quality.md`
# MAGIC - `/docs/governance/traceability.md`
# MAGIC - `/docs/operations/execution_guide.md`
# MAGIC - `/docs/standards/naming_conventions.md`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_legislature

# COMMAND ----------

# MAGIC  %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_text

# COMMAND ----------

# MAGIC %run ../99_utils/utils_comments

# COMMAND ----------

# MAGIC %run ../99_utils/utils_rejected_records

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------


from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    lower,
    when,
    coalesce,
    concat_ws,
    current_timestamp,
    to_timestamp,
    to_date,
    year,
    month,
    row_number,
    regexp_replace,
    sha2,
)
from pyspark.sql.types import StringType, BooleanType, IntegerType, TimestampType
from pyspark.sql.window import Window

# COMMAND ----------

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

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("15 - SILVER PRESENCAS EVENTOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "15_silver_presencas_eventos"
LAYER_NAME = "silver"
ENTITY_NAME = "presencas_eventos"

SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES["presencas_eventos"]
)

EVENTOS_TABLE = get_silver_table(
    SILVER_TABLES["eventos"]
)

DEPUTADOS_TABLE = get_silver_table(
    SILVER_TABLES["deputados"]
)

TARGET_TABLE = get_silver_table(
    "slv_presencas_eventos"
)

REJECTED_TABLE = get_silver_table(
    SILVER_TABLES["registros_rejeitados"]
)

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

# ============================================================
# Helper Functions
# ============================================================

def column_exists(dataframe, column_name: str) -> bool:
    return column_name in dataframe.columns


def safe_col(dataframe, column_name: str, default_value=None):
    if column_exists(dataframe, column_name):
        return col(column_name)

    return lit(default_value)


def clean_string(column_expression):
    return trim(
        regexp_replace(
            column_expression.cast("string"),
            r"\s+",
            " ",
        )
    )


def clean_upper(column_expression):
    return upper(clean_string(column_expression))


def clean_lower(column_expression):
    return lower(clean_string(column_expression))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start Pipeline Log

# COMMAND ----------

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver event attendance standardization started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver event attendance standardization.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Bronze and Lookup Tables

# COMMAND ----------

bronze_df = spark.table(SOURCE_TABLE)
eventos_df = spark.table(EVENTOS_TABLE)
deputados_df = spark.table(DEPUTADOS_TABLE)

records_read = bronze_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze event attendance table loaded successfully "
        f"| records_read={records_read}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Silver Lookup Tables

# COMMAND ----------

eventos_lookup_df = (
    eventos_df
    .select(
        col("evt_id_evento").cast(StringType()).alias("evt_id_evento_lookup"),
        safe_col(eventos_df, "evt_tx_titulo").cast(StringType()).alias("evt_tx_titulo"),
        safe_col(eventos_df, "evt_tx_tipo_evento").cast(StringType()).alias("evt_tx_tipo_evento"),
        safe_col(eventos_df, "evt_tx_situacao_evento").cast(StringType()).alias("evt_tx_situacao_evento"),
        safe_col(eventos_df, "evt_tx_local_camara").cast(StringType()).alias("evt_tx_local_camara"),
        safe_col(eventos_df, "evt_tx_sigla_orgao").cast(StringType()).alias("evt_tx_sigla_orgao"),
        safe_col(eventos_df, "evt_tx_nome_orgao").cast(StringType()).alias("evt_tx_nome_orgao"),
        safe_col(eventos_df, "evt_nr_ano").cast(IntegerType()).alias("evt_nr_ano"),
        safe_col(eventos_df, "evt_nr_mes").cast(IntegerType()).alias("evt_nr_mes"),
        safe_col(eventos_df, "leg_id_legislatura_evento").cast(IntegerType()).alias("leg_id_legislatura_evento"),
        safe_col(eventos_df, "evt_fl_registro_valido_silver", True).cast(BooleanType()).alias("evt_fl_registro_valido_silver"),
    )
    .dropDuplicates(["evt_id_evento_lookup"])
)

deputados_lookup_df = (
    deputados_df
    .filter(
        coalesce(
            safe_col(deputados_df, "dep_fl_legislatura_mais_recente").cast(BooleanType()),
            lit(True),
        ) == True
    )
    .select(
        col("dep_id_deputado").cast(StringType()).alias("dep_id_deputado_lookup"),
        safe_col(deputados_df, "dep_tx_nome").cast(StringType()).alias("dep_tx_nome"),
        safe_col(deputados_df, "dep_tx_nome_civil").cast(StringType()).alias("dep_tx_nome_civil"),
        safe_col(deputados_df, "dep_tx_sigla_partido").cast(StringType()).alias("dep_tx_sigla_partido"),
        safe_col(deputados_df, "part_tx_uri").cast(StringType()).alias("part_tx_uri"),
        safe_col(deputados_df, "dep_tx_sigla_uf").cast(StringType()).alias("dep_tx_sigla_uf"),
        safe_col(deputados_df, "leg_id_legislatura").cast(IntegerType()).alias("leg_id_legislatura_deputado"),
        safe_col(deputados_df, "dep_tx_url_foto").cast(StringType()).alias("dep_tx_url_foto"),
        safe_col(deputados_df, "dep_fl_registro_valido_silver", True).cast(BooleanType()).alias("dep_fl_registro_valido_silver"),
    )
    .dropDuplicates(["dep_id_deputado_lookup"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize Bronze Attendance Records

# COMMAND ----------

standardized_df = (
    bronze_df
    .select(
        clean_string(safe_col(bronze_df, "evt_id_evento")).alias("evt_id_evento"),
        clean_string(safe_col(bronze_df, "evt_tx_uri")).alias("evt_tx_uri"),
        to_timestamp(safe_col(bronze_df, "evt_dh_inicio").cast(StringType())).alias("evt_dh_inicio"),
        clean_string(safe_col(bronze_df, "dep_id_deputado")).alias("dep_id_deputado"),
        clean_string(safe_col(bronze_df, "dep_tx_uri")).alias("dep_tx_uri"),
        safe_col(bronze_df, "pev_nr_ano_arquivo").cast(IntegerType()).alias("pev_nr_ano_arquivo"),
        safe_col(bronze_df, "pev_fl_presenca").cast(StringType()).alias("pev_fl_presenca_origem"),
        safe_col(bronze_df, "pev_tx_payload_json").cast(StringType()).alias("pev_tx_payload_json"),
        safe_col(bronze_df, "aud_tx_arquivo_origem").cast(StringType()).alias("aud_tx_arquivo_origem_bronze"),
        safe_col(bronze_df, "aud_id_execucao").cast(StringType()).alias("aud_id_execucao_bronze"),
        safe_col(bronze_df, "aud_dh_ingestao").cast(TimestampType()).alias("aud_dh_ingestao_bronze"),
        safe_col(bronze_df, "aud_tx_endpoint_origem").cast(StringType()).alias("aud_tx_endpoint_origem_bronze"),
        safe_col(bronze_df, "aud_tx_sistema_origem").cast(StringType()).alias("aud_tx_sistema_origem_bronze"),
        safe_col(bronze_df, "aud_tx_versao_pipeline").cast(StringType()).alias("aud_tx_versao_pipeline_bronze"),
        safe_col(bronze_df, "aud_tx_tipo_carga").cast(StringType()).alias("aud_tx_tipo_carga_bronze"),
        safe_col(bronze_df, "aud_tx_hash_registro").cast(StringType()).alias("aud_tx_hash_registro_bronze"),
    )
    .withColumn(
        "pev_tx_presenca_origem_normalizada",
        lower(trim(col("pev_fl_presenca_origem").cast(StringType()))),
    )
    .withColumn(
        "pev_fl_presenca",
        when(
            col("pev_tx_presenca_origem_normalizada").isin(
                "1", "true", "t", "sim", "s", "yes", "y", "presente", "presenca", "presença"
            ),
            lit(True),
        )
        .when(
            col("pev_tx_presenca_origem_normalizada").isin(
                "0", "false", "f", "nao", "não", "n", "no", "ausente", "ausencia", "ausência"
            ),
            lit(False),
        )
        .otherwise(lit(None).cast(BooleanType())),
    )
    .withColumn("evt_dt_inicio", to_date(col("evt_dh_inicio")))
    .withColumn(
        "pev_nr_ano_evento",
        coalesce(
            year(col("evt_dh_inicio")),
            col("pev_nr_ano_arquivo"),
        ).cast(IntegerType()),
    )
    .withColumn(
        "pev_nr_mes_evento",
        month(col("evt_dh_inicio")).cast(IntegerType()),
    )
    .withColumn(
        "leg_id_legislatura_evento_calculada",
        when(col("pev_nr_ano_evento").between(2019, 2022), lit(56))
        .when(col("pev_nr_ano_evento").between(2023, 2026), lit(57))
        .otherwise(lit(None).cast(IntegerType())),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Enrich with Event and Deputy Dimensions

# COMMAND ----------

enriched_df = (
    standardized_df
    .join(
        eventos_lookup_df,
        standardized_df["evt_id_evento"] == eventos_lookup_df["evt_id_evento_lookup"],
        "left",
    )
    .join(
        deputados_lookup_df,
        standardized_df["dep_id_deputado"] == deputados_lookup_df["dep_id_deputado_lookup"],
        "left",
    )
    .drop("evt_id_evento_lookup", "dep_id_deputado_lookup")
    .withColumn(
        "leg_id_legislatura_evento",
        coalesce(
            col("leg_id_legislatura_evento"),
            col("leg_id_legislatura_evento_calculada"),
        ).cast(IntegerType()),
    )
    .withColumn(
        "leg_id_legislatura_deputado",
        col("leg_id_legislatura_deputado").cast(IntegerType()),
    )
    .withColumn(
        "evt_nr_ano",
        coalesce(col("evt_nr_ano"), col("pev_nr_ano_evento")).cast(IntegerType()),
    )
    .withColumn(
        "evt_nr_mes",
        coalesce(col("evt_nr_mes"), col("pev_nr_mes_evento")).cast(IntegerType()),
    )
    .withColumn(
        "pev_fl_evento_encontrado_silver",
        col("evt_tx_titulo").isNotNull() | col("evt_tx_tipo_evento").isNotNull() | col("evt_fl_registro_valido_silver").isNotNull(),
    )
    .withColumn(
        "pev_fl_deputado_encontrado_silver",
        col("dep_tx_nome").isNotNull() | col("dep_fl_registro_valido_silver").isNotNull(),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply Silver Quality Rules

# COMMAND ----------

silver_quality_df = (
    enriched_df
    .withColumn(
        "pev_tx_motivo_rejeicao",
        when(
            col("evt_id_evento").isNull() | (col("evt_id_evento") == ""),
            lit("PEV_EVT_ID_EVENTO_OBRIGATORIO"),
        )
        .when(
            col("dep_id_deputado").isNull() | (col("dep_id_deputado") == ""),
            lit("PEV_DEP_ID_DEPUTADO_OBRIGATORIO"),
        )
        .when(
            col("pev_fl_presenca").isNull(),
            lit("PEV_FLAG_PRESENCA_INVALIDO"),
        )
        .when(
            col("pev_nr_ano_evento").isNull(),
            lit("PEV_ANO_EVENTO_OBRIGATORIO"),
        )
        .otherwise(lit(None).cast(StringType())),
    )
    .withColumn(
        "pev_fl_registro_valido_silver",
        col("pev_tx_motivo_rejeicao").isNull(),
    )
    .withColumn(
        "pev_fl_evento_valido_silver",
        coalesce(col("evt_fl_registro_valido_silver"), lit(False)),
    )
    .withColumn(
        "pev_fl_deputado_valido_silver",
        coalesce(col("dep_fl_registro_valido_silver"), lit(False)),
    )
    .withColumn(
        "pev_fl_dimensoes_completas",
        col("pev_fl_evento_encontrado_silver") & col("pev_fl_deputado_encontrado_silver"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Build Deduplication Key

# COMMAND ----------

silver_quality_df = (
    silver_quality_df
    .withColumn(
        "pev_tx_chave_deduplicacao",
        sha2(
            concat_ws(
                "||",
                coalesce(col("evt_id_evento"), lit("__SEM_EVENTO__")),
                coalesce(col("dep_id_deputado"), lit("__SEM_DEP__")),
                coalesce(col("evt_dh_inicio").cast("string"), lit("__SEM_DATA__")),
                coalesce(col("pev_nr_ano_evento").cast("string"), lit("__SEM_ANO__")),
                coalesce(col("aud_tx_hash_registro_bronze"), lit("__SEM_HASH_BRONZE__")),
            ),
            256,
        ),
    )
    .withColumn(
        "pev_id_presenca_evento",
        concat_ws(
            "_",
            coalesce(col("evt_id_evento"), lit("SEM_EVENTO")),
            coalesce(col("dep_id_deputado"), lit("SEM_DEP")),
            coalesce(col("pev_nr_ano_evento").cast(StringType()), lit("SEM_ANO")),
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Build Mandatory Rejected Records

# COMMAND ----------

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=silver_quality_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="pev_id_presenca_evento",
    validation_rule_column="pev_tx_motivo_rejeicao",
    payload_column="pev_tx_payload_json",
    valid_flag_column="pev_fl_registro_valido_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Keep Valid Records and Deduplicate

# COMMAND ----------

valid_df = (
    silver_quality_df
    .filter(col("pev_fl_registro_valido_silver") == True)
    .drop("pev_tx_motivo_rejeicao")
)

dedup_window = (
    Window
    .partitionBy("evt_id_evento", "dep_id_deputado", "pev_nr_ano_evento")
    .orderBy(
        col("aud_dh_ingestao_bronze").desc_nulls_last(),
        col("aud_tx_hash_registro_bronze").desc_nulls_last(),
    )
)

valid_ranked_df = (
    valid_df
    .withColumn(
        "rn_deduplicacao",
        row_number().over(dedup_window),
    )
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=valid_ranked_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="pev_id_presenca_evento",
    payload_column="pev_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="PEV_REGISTRO_DUPLICADO_TECNICO",
    observation=(
        "Event attendance record kept only once by event, deputy and event year. "
        "Deduplication order uses latest Bronze ingestion timestamp and Bronze hash."
    ),
)

silver_dedup_df = (
    valid_ranked_df
    .filter(col("rn_deduplicacao") == 1)
    .drop("rn_deduplicacao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Persist Rejected Records

# COMMAND ----------

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
        f"Rejected and discarded presencas eventos records persisted "
        f"| records_rejected={records_rejected}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Add Silver Traceability Columns

# COMMAND ----------

silver_df = (
    silver_dedup_df
    .withColumn("aud_id_execucao_silver", lit(execution_id))
    .withColumn("aud_dh_processamento", current_timestamp())
    .withColumn("aud_tx_camada_origem", lit("bronze"))
    .withColumn("aud_tx_tabela_origem", lit(SOURCE_TABLE))
    .withColumn("aud_tx_tabela_destino", lit(TARGET_TABLE))
    .withColumn("aud_tx_versao_pipeline_silver", lit(PROJECT_VERSION))
    .withColumn(
        "aud_tx_regra_extracao_presenca_evento",
        lit(
            "Deputy event attendance standardized from Bronze CSV fallback and enriched with Silver events and deputies when available."
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Add Silver Record Hash

# COMMAND ----------

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "evt_id_evento",
        "dep_id_deputado",
        "evt_dh_inicio",
        "pev_fl_presenca",
        "pev_nr_ano_evento",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Select Final Silver Columns

# COMMAND ----------

final_columns = [
    "pev_id_presenca_evento",
    "pev_tx_chave_deduplicacao",

    "evt_id_evento",
    "evt_tx_uri",
    "evt_dh_inicio",
    "evt_dt_inicio",
    "evt_nr_ano",
    "evt_nr_mes",
    "evt_tx_titulo",
    "evt_tx_tipo_evento",
    "evt_tx_situacao_evento",
    "evt_tx_local_camara",
    "evt_tx_sigla_orgao",
    "evt_tx_nome_orgao",
    "leg_id_legislatura_evento",

    "dep_id_deputado",
    "dep_tx_uri",
    "dep_tx_nome",
    "dep_tx_nome_civil",
    "dep_tx_sigla_partido",
    "part_tx_uri",
    "dep_tx_sigla_uf",
    "leg_id_legislatura_deputado",
    "dep_tx_url_foto",

    "pev_fl_presenca",
    "pev_fl_presenca_origem",
    "pev_tx_presenca_origem_normalizada",
    "pev_nr_ano_evento",
    "pev_nr_mes_evento",
    "pev_nr_ano_arquivo",

    "pev_fl_evento_encontrado_silver",
    "pev_fl_deputado_encontrado_silver",
    "pev_fl_evento_valido_silver",
    "pev_fl_deputado_valido_silver",
    "pev_fl_dimensoes_completas",
    "pev_fl_registro_valido_silver",

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
]

final_df = silver_df.select(*final_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Persist Silver Table

# COMMAND ----------

(
    final_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

records_written = final_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Silver presencas eventos table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Curated Silver table for deputy attendance in legislative events.

This table standardizes records loaded from the Bronze event attendance CSV fallback,
preserving one relationship between a legislative event and a deputy presence record.

Main characteristics:
- one row per event and deputy attendance relationship
- event identifiers and timestamps standardized
- deputy identifiers standardized
- presence flag normalized as boolean
- enrichment with Silver events and Silver deputies when available
- mandatory validation for event, deputy, presence flag and event year
- technical deduplication by event, deputy and event year
- rejected and duplicate records traced in slv_registros_rejeitados
- Bronze and Silver audit metadata preserved

Architecture note:
- Missing event or deputy dimension enrichment does not reject the attendance relationship.
- Dimension completeness is exposed through quality flags and can be handled in Gold.
- This table supports the mandatory Attendance and Absenteeism Monitor deliverable.
"""

column_comments = {
    "pev_id_presenca_evento": "Business identifier for the event attendance relationship generated from event, deputy and event year.",
    "pev_tx_chave_deduplicacao": "Deterministic technical deduplication key for the event attendance record.",
    "evt_id_evento": "Legislative event identifier associated with the attendance record.",
    "evt_tx_uri": "Legislative event URI from the Bronze source.",
    "evt_dh_inicio": "Event start timestamp standardized in Silver.",
    "evt_dt_inicio": "Event start date derived from the event start timestamp.",
    "evt_nr_ano": "Event year used for analytical aggregation.",
    "evt_nr_mes": "Event month used for analytical aggregation.",
    "evt_tx_titulo": "Event title enriched from the Silver events table when available.",
    "evt_tx_tipo_evento": "Event type enriched from the Silver events table when available.",
    "evt_tx_situacao_evento": "Event status enriched from the Silver events table when available.",
    "evt_tx_local_camara": "Event location in Câmara facilities enriched from the Silver events table when available.",
    "evt_tx_sigla_orgao": "Legislative body acronym enriched from the Silver events table when available.",
    "evt_tx_nome_orgao": "Legislative body name enriched from the Silver events table when available.",
    "leg_id_legislatura_evento": "Event legislature identifier derived from event data or calculated from event year.",
    "dep_id_deputado": "Deputy identifier associated with the event attendance record.",
    "dep_tx_uri": "Deputy URI from the Bronze source.",
    "dep_tx_nome": "Deputy electoral name enriched from the Silver deputies table when available.",
    "dep_tx_nome_civil": "Deputy civil name enriched from the Silver deputies table when available.",
    "dep_tx_sigla_partido": "Deputy party acronym enriched from the Silver deputies table when available.",
    "part_tx_uri": "Party URI enriched from the Silver deputies table when available.",
    "dep_tx_sigla_uf": "Deputy state acronym enriched from the Silver deputies table when available.",
    "leg_id_legislatura_deputado": "Deputy legislature identifier from the Silver deputies table when available.",
    "dep_tx_url_foto": "Deputy photo URL enriched from the Silver deputies table when available.",
    "pev_fl_presenca": "Normalized boolean flag indicating confirmed deputy presence in the event.",
    "pev_fl_presenca_origem": "Original presence flag value from Bronze.",
    "pev_tx_presenca_origem_normalizada": "Normalized string representation of original presence flag used to derive boolean presence.",
    "pev_nr_ano_evento": "Event year derived from the event timestamp or source file year.",
    "pev_nr_mes_evento": "Event month derived from the event timestamp.",
    "pev_nr_ano_arquivo": "Reference year extracted from the source CSV file name during Bronze ingestion.",
    "pev_fl_evento_encontrado_silver": "Flag indicating whether the event was found in the Silver events lookup.",
    "pev_fl_deputado_encontrado_silver": "Flag indicating whether the deputy was found in the Silver deputies lookup.",
    "pev_fl_evento_valido_silver": "Flag indicating whether the matched Silver event is valid according to its own Silver quality rule.",
    "pev_fl_deputado_valido_silver": "Flag indicating whether the matched Silver deputy is valid according to its own Silver quality rule.",
    "pev_fl_dimensoes_completas": "Flag indicating whether both event and deputy enrichments are available.",
    "pev_fl_registro_valido_silver": "Flag indicating whether the attendance record passed mandatory Silver validation.",
    "pev_tx_payload_json": "Original Bronze JSON payload preserved for traceability.",
    "aud_id_execucao_bronze": "Execution identifier from Bronze ingestion.",
    "aud_dh_ingestao_bronze": "Bronze ingestion timestamp.",
    "aud_tx_endpoint_origem_bronze": "Source endpoint or file path used during Bronze ingestion.",
    "aud_tx_sistema_origem_bronze": "Source system identified during Bronze ingestion.",
    "aud_tx_versao_pipeline_bronze": "Pipeline version used during Bronze ingestion.",
    "aud_tx_tipo_carga_bronze": "Load type applied during Bronze ingestion.",
    "aud_tx_arquivo_origem_bronze": "Original source file path captured during Bronze ingestion.",
    "aud_tx_hash_registro_bronze": "Deterministic Bronze record hash.",
    "aud_id_execucao_silver": "Execution identifier for Silver transformation.",
    "aud_dh_processamento": "Timestamp when record was processed in Silver.",
    "aud_tx_camada_origem": "Source Medallion layer used during processing.",
    "aud_tx_tabela_origem": "Source table used during processing.",
    "aud_tx_tabela_destino": "Target Silver table.",
    "aud_tx_versao_pipeline_silver": "Pipeline version used during Silver transformation.",
    "aud_tx_regra_extracao_presenca_evento": "Textual description of event attendance extraction and enrichment rule.",
    "aud_tx_hash_registro_silver": "Deterministic Silver record hash.",
}

if APPLY_GOVERNANCE_COMMENTS:

    existing_final_columns = set(final_df.columns)
    column_comments = {
        column_name: column_comment
        for column_name, column_comment in column_comments.items()
        if column_name in existing_final_columns
    }

    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Final Pipeline Log

# COMMAND ----------

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
        f"Silver presencas eventos transformation completed successfully "
        f"| records_read={records_read} "
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
        f"Silver presencas eventos transformation completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER PRESENCAS EVENTOS COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print("Grain: one deputy attendance relationship per legislative event")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)
