# Databricks notebook source
# MAGIC %md
# MAGIC # 11 Silver — Eventos Legislativos Standardization
# MAGIC
# MAGIC **Notebook:** `11_silver_eventos`
# MAGIC
# MAGIC Standardizes legislative event records from the Bronze layer and persists
# MAGIC validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Legislative event schema normalization rules
# MAGIC - Event identifier standardization logic
# MAGIC - Event date and time normalization
# MAGIC - Event legislative body extraction from JSON payload
# MAGIC - Event title and type extraction from JSON payload
# MAGIC - Event location normalization
# MAGIC - Quality validation rules
# MAGIC - Rejected records tracking using global utilities
# MAGIC - Technical duplicate tracking
# MAGIC - Silver Delta persistence logic
# MAGIC - Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read legislative event data from Bronze layer
# MAGIC - Standardize event identifiers, URI, dates, status and location
# MAGIC - Extract event date fields from `evt_tx_payload_json`
# MAGIC - Extract event title from `evt_tx_payload_json`
# MAGIC - Extract event type from `evt_tx_payload_json`
# MAGIC - Extract legislative body identifier from `evt_tx_payload_json`
# MAGIC - Extract legislative body acronym and type from `evt_tx_payload_json`
# MAGIC - Normalize textual fields
# MAGIC - Validate mandatory event fields
# MAGIC - Remove technical duplicate records
# MAGIC - Preserve Bronze ingestion lineage
# MAGIC - Register rejected and discarded records for traceability
# MAGIC - Persist curated Delta table
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves raw source values
# MAGIC - Silver standardizes, validates and deduplicates records
# MAGIC - Event identifier and start date are mandatory for analytical use
# MAGIC - Event start date is extracted from JSON field `dataHoraInicio`
# MAGIC - Event end date is extracted from JSON field `dataHoraFim`
# MAGIC - Event legislative body is extracted from JSON field `orgaos[0]`
# MAGIC - Event title is extracted from JSON field `descricao`
# MAGIC - Event type is extracted from JSON field `descricaoTipo`
# MAGIC - Extracting event body fields enables analytical joins with `slv_orgaos` and `slv_cpis`
# MAGIC - Records with unavailable optional fields are preserved with quality flags
# MAGIC - Technical duplicates are registered as discarded records
# MAGIC - Global utility notebooks are used to reduce duplicated logic
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

# MAGIC %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_datetime

# COMMAND ----------

# MAGIC %run ../99_utils/utils_text

# COMMAND ----------

# MAGIC %run ../99_utils/utils_comments

# COMMAND ----------

# MAGIC %run ../99_utils/utils_rejected_records

# COMMAND ----------

# MAGIC
# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# Databricks notebook source

# MAGIC %md
# MAGIC # 11 Silver — Eventos Legislativos Standardization
# MAGIC
# MAGIC **Notebook:** `11_silver_eventos`  
# MAGIC **Layer:** `Silver`  
# MAGIC **Source:** `brazil_legislative_analytics.bronze.br_eventos`  
# MAGIC **Target:** `brazil_legislative_analytics.silver.slv_eventos`
# MAGIC
# MAGIC Standardizes legislative event records from the Bronze layer and persists
# MAGIC validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook extracts and standardizes event dates, event type, title,
# MAGIC situation, location and legislative body attributes, preserving full
# MAGIC Bronze-to-Silver traceability.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read legislative event data from Bronze layer
# MAGIC - Standardize event identifiers, URI, dates, status and location
# MAGIC - Extract event analytical attributes from JSON payload
# MAGIC - Normalize event timestamps using tolerant parsing
# MAGIC - Derive year, month and legislature from event start date
# MAGIC - Validate mandatory event fields
# MAGIC - Register rejected and duplicate records
# MAGIC - Preserve Bronze ingestion lineage
# MAGIC - Generate Silver traceability metadata
# MAGIC - Generate deterministic Silver record hashes
# MAGIC - Persist Silver Delta table
# MAGIC - Apply governance comments to table and columns

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_datetime

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

from pyspark.sql import functions as F

from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    current_timestamp,
    row_number,
    when,
    to_date,
    year,
    month,
    get_json_object,
    coalesce,
    expr,
)

from pyspark.sql.window import Window

from pyspark.sql.types import (
    StringType,
    IntegerType,
)

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("11 - SILVER EVENTOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "11_silver_eventos"
LAYER_NAME = "silver"
ENTITY_NAME = "eventos"

SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES["eventos"]
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES["eventos"]
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
    message="Silver eventos standardization started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver eventos standardization.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze Table

# COMMAND ----------

source_df = spark.table(SOURCE_TABLE)

records_read = source_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze eventos table loaded successfully "
        f"| records_read={records_read}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper Functions

# COMMAND ----------

def first_existing_column(
    dataframe,
    candidate_columns,
):
    """
    Returns the first existing column from a list of candidate column names.
    """

    for candidate_column in candidate_columns:

        if candidate_column in dataframe.columns:

            return col(candidate_column).cast("string")

    return lit(None).cast(StringType())


def clean_text_from_column(
    column_expression,
):
    """
    Cleans textual column expressions by trimming and normalizing spaces.
    """

    return (
        when(
            column_expression.isNull(),
            lit(None).cast(StringType()),
        )
        .otherwise(
            trim(
                F.regexp_replace(
                    column_expression.cast("string"),
                    r"\s+",
                    " ",
                )
            )
        )
    )


def clean_upper_from_column(
    column_expression,
):
    """
    Cleans textual column expressions and converts values to uppercase.
    """

    return upper(
        clean_text_from_column(column_expression)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize Base Event Attributes

# COMMAND ----------

eventos_base_df = (
    source_df
    .select(

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "evt_id_evento",
                    "id",
                    "id_evento",
                    "idEvento",
                ],
            )
        ).alias("evt_id_evento"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "evt_tx_uri",
                    "uri",
                ],
            )
        ).alias("evt_tx_uri"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "evt_dt_data_hora_inicio",
                    "evt_dh_inicio",
                    "dataHoraInicio",
                    "data_hora_inicio",
                ],
            )
        ).alias("evt_dh_inicio_origem_coluna"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "evt_dt_data_hora_fim",
                    "evt_dh_fim",
                    "dataHoraFim",
                    "data_hora_fim",
                ],
            )
        ).alias("evt_dh_fim_origem_coluna"),

        clean_upper_from_column(
            first_existing_column(
                source_df,
                [
                    "evt_tx_situacao",
                    "situacao",
                ],
            )
        ).alias("evt_tx_situacao_coluna"),

        clean_upper_from_column(
            first_existing_column(
                source_df,
                [
                    "evt_tx_local",
                    "local",
                    "localCamara",
                ],
            )
        ).alias("evt_tx_local_coluna"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "evt_tx_tipo_evento",
                    "descricaoTipo",
                    "tipoEvento",
                ],
            )
        ).alias("evt_tx_tipo_evento_coluna"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "evt_tx_descricao",
                    "descricao",
                ],
            )
        ).alias("evt_tx_titulo_coluna"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "evt_tx_payload_json",
                    "payload_json",
                    "raw_payload_json",
                ],
            )
        ).alias("evt_tx_payload_json"),

        col("aud_id_execucao")
            .alias("aud_id_execucao_bronze"),

        col("aud_dh_ingestao")
            .alias("aud_dh_ingestao_bronze"),

        col("aud_tx_endpoint_origem")
            .alias("aud_tx_endpoint_origem_bronze"),

        col("aud_tx_sistema_origem")
            .alias("aud_tx_sistema_origem_bronze"),

        col("aud_tx_versao_pipeline")
            .alias("aud_tx_versao_pipeline_bronze"),

        col("aud_tx_tipo_carga")
            .alias("aud_tx_tipo_carga_bronze"),

        col("aud_tx_hash_registro")
            .alias("aud_tx_hash_registro_bronze"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extract Event Analytical Attributes from JSON Payload

# COMMAND ----------

eventos_enriched_df = (
    eventos_base_df

    .withColumn(
        "evt_dh_inicio_origem",
        coalesce(
            col("evt_dh_inicio_origem_coluna"),
            get_json_object(
                col("evt_tx_payload_json"),
                "$.dataHoraInicio",
            )
        )
    )

    .withColumn(
        "evt_dh_fim_origem",
        coalesce(
            col("evt_dh_fim_origem_coluna"),
            get_json_object(
                col("evt_tx_payload_json"),
                "$.dataHoraFim",
            )
        )
    )

    .withColumn(
        "evt_tx_situacao",
        coalesce(
            col("evt_tx_situacao_coluna"),
            clean_upper_from_column(
                get_json_object(
                    col("evt_tx_payload_json"),
                    "$.situacao",
                )
            )
        )
    )

    .withColumn(
        "evt_tx_titulo",
        coalesce(
            clean_upper_from_column(col("evt_tx_titulo_coluna")),
            clean_upper_from_column(
                get_json_object(
                    col("evt_tx_payload_json"),
                    "$.descricao",
                )
            )
        )
    )

    .withColumn(
        "evt_tx_tipo_evento",
        coalesce(
            clean_upper_from_column(col("evt_tx_tipo_evento_coluna")),
            clean_upper_from_column(
                get_json_object(
                    col("evt_tx_payload_json"),
                    "$.descricaoTipo",
                )
            )
        )
    )

    .withColumn(
        "evt_id_orgao",
        clean_text_from_column(
            get_json_object(
                col("evt_tx_payload_json"),
                "$.orgaos[0].id",
            )
        )
    )

    .withColumn(
        "evt_tx_sigla_orgao",
        clean_upper_from_column(
            get_json_object(
                col("evt_tx_payload_json"),
                "$.orgaos[0].sigla",
            )
        )
    )

    .withColumn(
        "evt_tx_nome_orgao",
        clean_upper_from_column(
            get_json_object(
                col("evt_tx_payload_json"),
                "$.orgaos[0].nome",
            )
        )
    )

    .withColumn(
        "evt_tx_tipo_orgao",
        clean_upper_from_column(
            get_json_object(
                col("evt_tx_payload_json"),
                "$.orgaos[0].tipoOrgao",
            )
        )
    )

    .withColumn(
        "evt_tx_local_json",
        clean_upper_from_column(
            get_json_object(
                col("evt_tx_payload_json"),
                "$.localCamara.nome",
            )
        )
    )

    .withColumn(
        "evt_tx_local",
        coalesce(
            col("evt_tx_local_coluna"),
            col("evt_tx_local_json")
        )
    )

    .drop(
        "evt_dh_inicio_origem_coluna",
        "evt_dh_fim_origem_coluna",
        "evt_tx_situacao_coluna",
        "evt_tx_local_coluna",
        "evt_tx_local_json",
        "evt_tx_tipo_evento_coluna",
        "evt_tx_titulo_coluna",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Normalize Event Dates and Legislature

# COMMAND ----------

eventos_date_df = (
    eventos_enriched_df

    .withColumn(
        "evt_dh_inicio",
        coalesce(
            expr("try_to_timestamp(evt_dh_inicio_origem, \"yyyy-MM-dd'T'HH:mm:ss\")"),
            expr("try_to_timestamp(evt_dh_inicio_origem, \"yyyy-MM-dd'T'HH:mm\")"),
            expr("try_to_timestamp(evt_dh_inicio_origem)")
        )
    )

    .withColumn(
        "evt_dh_fim",
        coalesce(
            expr("try_to_timestamp(evt_dh_fim_origem, \"yyyy-MM-dd'T'HH:mm:ss\")"),
            expr("try_to_timestamp(evt_dh_fim_origem, \"yyyy-MM-dd'T'HH:mm\")"),
            expr("try_to_timestamp(evt_dh_fim_origem)")
        )
    )

    .withColumn(
        "evt_dt_inicio",
        to_date(col("evt_dh_inicio"))
    )

    .withColumn(
        "evt_dt_fim",
        to_date(col("evt_dh_fim"))
    )

    .withColumn(
        "evt_nr_ano",
        year(col("evt_dt_inicio")).cast(IntegerType())
    )

    .withColumn(
        "evt_nr_mes",
        month(col("evt_dt_inicio")).cast(IntegerType())
    )

    .withColumn(
        "leg_id_legislatura",
        when(
            col("evt_nr_ano").between(2019, 2022),
            lit(56)
        )
        .when(
            col("evt_nr_ano").between(2023, 2026),
            lit(57)
        )
        .otherwise(lit(None).cast(IntegerType()))
    )

    .withColumn(
        "evt_fl_legislatura_identificada",
        when(
            col("leg_id_legislatura").isNotNull(),
            lit(True)
        )
        .otherwise(lit(False))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Apply Quality Rules

# COMMAND ----------

eventos_quality_df = (
    eventos_date_df

    .withColumn(
        "evt_fl_id_valido",
        (
            col("evt_id_evento").isNotNull()
            & (trim(col("evt_id_evento")) != "")
        )
    )

    .withColumn(
        "evt_fl_data_inicio_informada",
        col("evt_dh_inicio").isNotNull()
    )

    .withColumn(
        "evt_fl_orgao_informado",
        (
            col("evt_id_orgao").isNotNull()
            & (trim(col("evt_id_orgao")) != "")
        )
    )

    .withColumn(
        "evt_fl_tipo_evento_informado",
        (
            col("evt_tx_tipo_evento").isNotNull()
            & (trim(col("evt_tx_tipo_evento")) != "")
        )
    )

    .withColumn(
        "evt_fl_titulo_informado",
        (
            col("evt_tx_titulo").isNotNull()
            & (trim(col("evt_tx_titulo")) != "")
        )
    )

    .withColumn(
        "evt_fl_periodo_valido",
        when(
            col("evt_dh_inicio").isNotNull()
            & col("evt_dh_fim").isNotNull()
            & (col("evt_dh_inicio") > col("evt_dh_fim")),
            lit(False)
        )
        .otherwise(lit(True))
    )

    .withColumn(
        "evt_fl_registro_valido_silver",
        (
            col("evt_fl_id_valido")
            & col("evt_fl_data_inicio_informada")
            & col("evt_fl_periodo_valido")
        )
    )

    .withColumn(
        "evt_tx_motivo_rejeicao",
        when(
            ~col("evt_fl_id_valido"),
            lit("EVT_ID_NULO_OU_VAZIO")
        )
        .when(
            ~col("evt_fl_data_inicio_informada"),
            lit("EVT_DATA_INICIO_NULA")
        )
        .when(
            ~col("evt_fl_periodo_valido"),
            lit("EVT_PERIODO_INVALIDO")
        )
        .otherwise(lit(None).cast(StringType()))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Build Mandatory Rejected Records

# COMMAND ----------

mandatory_rejected_source_df = (
    eventos_quality_df
    .filter(
        col("evt_fl_registro_valido_silver") == False
    )
)

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=mandatory_rejected_source_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="evt_id_evento",
    validation_rule_column="evt_tx_motivo_rejeicao",
    payload_column="evt_tx_payload_json",
    valid_flag_column="evt_fl_registro_valido_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Keep Valid Event Records

# COMMAND ----------

valid_df = (
    eventos_quality_df
    .filter(
        col("evt_fl_registro_valido_silver") == True
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Deduplicate Event Records

# COMMAND ----------

dedup_window = (
    Window
    .partitionBy("evt_id_evento")
    .orderBy(
        col("aud_dh_ingestao_bronze").desc_nulls_last()
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
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="evt_id_evento",
    payload_column="evt_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="EVT_REGISTRO_DUPLICADO",
    observation=(
        "Duplicate event records removed keeping latest Bronze ingestion."
    ),
)

silver_df = (
    dedup_df
    .filter(
        col("rn_deduplicacao") == 1
    )
    .drop("rn_deduplicacao")
    .drop("evt_tx_motivo_rejeicao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Persist Rejected Records

# COMMAND ----------

rejected_df = union_rejected_records(
    mandatory_rejected_dataframe=mandatory_rejected_df,
    duplicate_rejected_dataframe=duplicate_rejected_df,
)

clean_and_persist_rejected_records(
    rejected_dataframe=rejected_df,
    rejected_table=REJECTED_TABLE,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    mode="append",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Add Silver Traceability Columns

# COMMAND ----------

silver_df = (
    silver_df

    .withColumn(
        "aud_id_execucao_silver",
        lit(execution_id)
    )

    .withColumn(
        "aud_dh_processamento",
        current_timestamp()
    )

    .withColumn(
        "aud_tx_camada_origem",
        lit("bronze")
    )

    .withColumn(
        "aud_tx_tabela_origem",
        lit(SOURCE_TABLE)
    )

    .withColumn(
        "aud_tx_tabela_destino",
        lit(TARGET_TABLE)
    )

    .withColumn(
        "aud_tx_versao_pipeline_silver",
        lit(PROJECT_VERSION)
    )

    .withColumn(
        "aud_tx_regra_extracao_evento",
        lit(
            "Event date, type, title and legislative body extracted from source columns and evt_tx_payload_json."
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Add Silver Hash

# COMMAND ----------

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "evt_id_evento",
        "evt_id_orgao",
        "evt_dh_inicio",
        "evt_tx_tipo_evento",
        "evt_tx_situacao",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Select Final Columns

# COMMAND ----------

final_columns = [

    "evt_id_evento",
    "evt_tx_uri",

    "evt_dh_inicio_origem",
    "evt_dh_fim_origem",
    "evt_dh_inicio",
    "evt_dh_fim",
    "evt_dt_inicio",
    "evt_dt_fim",
    "evt_nr_ano",
    "evt_nr_mes",
    "leg_id_legislatura",

    "evt_tx_situacao",
    "evt_tx_titulo",
    "evt_tx_tipo_evento",
    "evt_tx_local",

    "evt_id_orgao",
    "evt_tx_sigla_orgao",
    "evt_tx_nome_orgao",
    "evt_tx_tipo_orgao",

    "evt_fl_id_valido",
    "evt_fl_data_inicio_informada",
    "evt_fl_orgao_informado",
    "evt_fl_tipo_evento_informado",
    "evt_fl_titulo_informado",
    "evt_fl_periodo_valido",
    "evt_fl_legislatura_identificada",
    "evt_fl_registro_valido_silver",

    "evt_tx_payload_json",

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
    "aud_tx_regra_extracao_evento",
    "aud_tx_hash_registro_silver",
]

silver_df = silver_df.select(
    *final_columns
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Persist Silver Table

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS {TARGET_TABLE}
""")

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

records_written = spark.table(
    TARGET_TABLE
).count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Silver eventos table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Apply Governance Comments

# COMMAND ----------

table_comment = """
Standardized legislative events table in the Silver layer.

This table contains validated, standardized and deduplicated legislative event
records prepared for analytical use.

The table extracts event date, title, event type and legislative body attributes
from source columns and the original JSON payload, enabling joins with legislative
bodies, CPIs and future Gold fact tables.
"""

column_comments = {

    "evt_id_evento": "Legislative event identifier.",
    "evt_tx_uri": "Legislative event API URI.",
    "evt_dh_inicio_origem": "Original event start datetime value extracted from source column or JSON payload.",
    "evt_dh_fim_origem": "Original event end datetime value extracted from source column or JSON payload.",
    "evt_dh_inicio": "Standardized event start timestamp.",
    "evt_dh_fim": "Standardized event end timestamp.",
    "evt_dt_inicio": "Standardized event start date.",
    "evt_dt_fim": "Standardized event end date.",
    "evt_nr_ano": "Year extracted from event start date.",
    "evt_nr_mes": "Month extracted from event start date.",
    "leg_id_legislatura": "Derived legislature identifier based on event year.",
    "evt_tx_situacao": "Standardized event status.",
    "evt_tx_titulo": "Event title or description extracted from source column or JSON payload.",
    "evt_tx_tipo_evento": "Event type extracted from source column or JSON payload.",
    "evt_tx_local": "Standardized event location.",
    "evt_id_orgao": "Legislative body identifier extracted from JSON payload path $.orgaos[0].id.",
    "evt_tx_sigla_orgao": "Legislative body acronym extracted from JSON payload path $.orgaos[0].sigla.",
    "evt_tx_nome_orgao": "Legislative body name extracted from JSON payload path $.orgaos[0].nome.",
    "evt_tx_tipo_orgao": "Legislative body type extracted from JSON payload path $.orgaos[0].tipoOrgao.",
    "evt_fl_id_valido": "Flag indicating whether event identifier is valid.",
    "evt_fl_data_inicio_informada": "Flag indicating whether event start datetime is available.",
    "evt_fl_orgao_informado": "Flag indicating whether event legislative body identifier is available.",
    "evt_fl_tipo_evento_informado": "Flag indicating whether event type is available.",
    "evt_fl_titulo_informado": "Flag indicating whether event title is available.",
    "evt_fl_periodo_valido": "Flag indicating whether event start and end timestamps form a valid period.",
    "evt_fl_legislatura_identificada": "Flag indicating whether event legislature was derived from event year.",
    "evt_fl_registro_valido_silver": "Flag indicating whether event record passed Silver validation.",
    "evt_tx_payload_json": "Original Bronze JSON payload preserved for traceability.",
    "aud_id_execucao_bronze": "Bronze execution identifier.",
    "aud_dh_ingestao_bronze": "Bronze ingestion timestamp.",
    "aud_tx_endpoint_origem_bronze": "Bronze source endpoint or CSV fallback source.",
    "aud_tx_sistema_origem_bronze": "Bronze source system.",
    "aud_tx_versao_pipeline_bronze": "Bronze pipeline version.",
    "aud_tx_tipo_carga_bronze": "Bronze load type.",
    "aud_tx_hash_registro_bronze": "Bronze deterministic record hash.",
    "aud_id_execucao_silver": "Silver execution identifier.",
    "aud_dh_processamento": "Silver processing timestamp.",
    "aud_tx_camada_origem": "Source Medallion layer used during processing.",
    "aud_tx_tabela_origem": "Source table used during processing.",
    "aud_tx_tabela_destino": "Target Silver table.",
    "aud_tx_versao_pipeline_silver": "Silver pipeline version.",
    "aud_tx_regra_extracao_evento": "Textual description of the event extraction rule.",
    "aud_tx_hash_registro_silver": "Deterministic Silver hash used for event traceability.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Final Pipeline Log

# COMMAND ----------

finished_at = datetime.now()

duration_seconds = (
    finished_at - started_at
).total_seconds()

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_SUCCESS,
    message=(
        f"Silver eventos standardization completed successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written}"
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
        f"Silver eventos standardization completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER EVENTOS COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)