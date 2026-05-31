# Databricks notebook source
# MAGIC %md
# MAGIC # 14 Silver — Proposições Legislativas Standardization
# MAGIC
# MAGIC **Notebook:** `14_silver_proposicoes`
# MAGIC
# MAGIC Standardizes legislative proposition records from the Bronze layer and persists
# MAGIC validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Legislative proposition schema normalization rules
# MAGIC - Proposition identifier standardization logic
# MAGIC - Proposition year preservation and analytical year treatment
# MAGIC - Invalid year treatment using `prop_nr_ano_original` and `prop_nr_ano_tratado`
# MAGIC - Legislature derivation from treated proposition year
# MAGIC - Project legislature scope derivation for legislatures 56 and 57
# MAGIC - Text normalization using global utilities
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
# MAGIC - Read legislative proposition data from Bronze layer
# MAGIC - Standardize proposition identifiers, type, year, URI and summary attributes
# MAGIC - Preserve the original proposition year in `prop_nr_ano_original`
# MAGIC - Convert invalid analytical year values from `0` to `NULL` in `prop_nr_ano_tratado`
# MAGIC - Derive legislature identifier from `prop_nr_ano_tratado`
# MAGIC - Assign legislature `56` for years between 2019 and 2022
# MAGIC - Assign legislature `57` for years between 2023 and 2026
# MAGIC - Keep legislature as `NULL` when analytical year is unavailable or outside scope
# MAGIC - Create year and legislature quality flags
# MAGIC - Normalize textual fields
# MAGIC - Validate mandatory proposition fields
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
# MAGIC - Proposition identifier is mandatory for analytical use
# MAGIC - Original year values are preserved for auditability
# MAGIC - Analytical year treatment converts `0` to `NULL`
# MAGIC - `prop_fl_ano_extraido` is `TRUE` only when a non-zero year is available
# MAGIC - `prop_fl_ano_valido` is `TRUE` only when the treated year is within the analytical scope
# MAGIC - Legislature is derived from `prop_nr_ano_tratado`
# MAGIC - Legislatures 56 and 57 are derived from the configured project period
# MAGIC - Records with unavailable analytical year are preserved, not rejected
# MAGIC - Records without identified legislature remain in Silver with quality flags
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

# ==========================================================================================
# Initialize Spark session explicitly for utility notebooks
# ==========================================================================================

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

if spark is None:
    spark = SparkSession.builder.getOrCreate()

globals()["spark"] = spark

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
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("14 - SILVER PROPOSICOES")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

NOTEBOOK_NAME = "14_silver_proposicoes"
LAYER_NAME = "silver"
ENTITY_NAME = "proposicoes"

SOURCE_TABLE = get_bronze_table(BRONZE_TABLES["proposicoes"])
TARGET_TABLE = get_silver_table(SILVER_TABLES["proposicoes"])
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

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver proposicoes standardization started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver proposicoes standardization.",
)

# ==========================================================================================
# 1. Read Bronze Table
# ==========================================================================================

source_df = spark.table(SOURCE_TABLE)

records_read = source_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze proposicoes table loaded successfully "
        f"| records_read={records_read}"
    ),
)

# ==========================================================================================
# 2. Helper Functions
# ==========================================================================================

def first_existing_column(dataframe, candidate_columns):
    """
    Returns the first existing column from a list of candidate column names.
    """

    for candidate_column in candidate_columns:
        if candidate_column in dataframe.columns:
            return col(candidate_column).cast("string")

    return lit(None).cast(StringType())


def clean_text_from_column(column_expression):
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


def clean_upper_from_column(column_expression):
    """
    Cleans textual column expressions and converts values to uppercase.
    """

    return upper(
        clean_text_from_column(column_expression)
    )


def safe_date_from_column(column_expression):
    """
    Safely converts date-like string values into DATE.
    Supports values such as yyyy-MM-dd and yyyy-MM-ddTHH:mm:ss.
    """

    return F.to_date(
        F.substring(
            clean_text_from_column(column_expression),
            1,
            10,
        ),
        "yyyy-MM-dd",
    )

# ==========================================================================================
# 3. Standardize Base Attributes
# ==========================================================================================

proposicoes_base_df = (
    source_df
    .select(
        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_id_proposicao",
                    "id",
                    "idProposicao",
                    "id_proposicao",
                ],
            )
        ).alias("prop_id_proposicao"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_uri",
                    "uri",
                ],
            )
        ).alias("prop_tx_uri"),

        clean_upper_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_sigla_tipo",
                    "siglaTipo",
                    "sigla_tipo",
                ],
            )
        ).alias("prop_tx_sigla_tipo"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_cd_tipo",
                    "codTipo",
                    "cod_tipo",
                ],
            )
        ).alias("prop_cd_tipo"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_nr_numero",
                    "numero",
                    "nr_numero",
                ],
            )
        ).alias("prop_nr_numero"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_nr_ano",
                    "ano",
                    "nr_ano",
                ],
            )
        ).alias("prop_nr_ano"),

        clean_upper_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_descricao_tipo",
                    "descricaoTipo",
                    "descricao_tipo",
                ],
            )
        ).alias("prop_tx_descricao_tipo"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_dt_apresentacao",
                    "dataApresentacao",
                    "data_apresentacao",
                ],
            )
        ).alias("prop_dt_apresentacao_original"),

        clean_upper_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_ementa",
                    "ementa",
                ],
            )
        ).alias("prop_tx_ementa"),

        clean_upper_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_ementa_detalhada",
                    "ementaDetalhada",
                    "ementa_detalhada",
                ],
            )
        ).alias("prop_tx_ementa_detalhada"),

        clean_upper_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_keywords",
                    "keywords",
                    "palavrasChave",
                ],
            )
        ).alias("prop_tx_keywords"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_uri_orgao_numerador",
                    "uriOrgaoNumerador",
                    "uri_orgao_numerador",
                ],
            )
        ).alias("prop_tx_uri_orgao_numerador"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_uri_prop_principal",
                    "uriPropPrincipal",
                    "uri_prop_principal",
                ],
            )
        ).alias("prop_tx_uri_prop_principal"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_uri_prop_anterior",
                    "uriPropAnterior",
                    "uri_prop_anterior",
                ],
            )
        ).alias("prop_tx_uri_prop_anterior"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_uri_prop_posterior",
                    "uriPropPosterior",
                    "uri_prop_posterior",
                ],
            )
        ).alias("prop_tx_uri_prop_posterior"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_url_inteiro_teor",
                    "urlInteiroTeor",
                    "url_inteiro_teor",
                ],
            )
        ).alias("prop_tx_url_inteiro_teor"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_urn_final",
                    "urnFinal",
                    "urn_final",
                ],
            )
        ).alias("prop_tx_urn_final"),

        clean_text_from_column(
            first_existing_column(
                source_df,
                [
                    "prop_tx_payload_json",
                    "payload_json",
                    "raw_payload_json",
                ],
            )
        ).alias("prop_tx_payload_json"),

        col("aud_id_execucao").alias("aud_id_execucao_bronze"),
        col("aud_dh_ingestao").alias("aud_dh_ingestao_bronze"),
        col("aud_tx_endpoint_origem").alias("aud_tx_endpoint_origem_bronze"),
        col("aud_tx_sistema_origem").alias("aud_tx_sistema_origem_bronze"),
        col("aud_tx_versao_pipeline").alias("aud_tx_versao_pipeline_bronze"),
        col("aud_tx_tipo_carga").alias("aud_tx_tipo_carga_bronze"),
        col("aud_tx_hash_registro").alias("aud_tx_hash_registro_bronze"),
    )
)

# ==========================================================================================
# 4. Apply Date, Year and Legislature Rules
# ==========================================================================================

proposicoes_enriched_df = (
    proposicoes_base_df
    .withColumn(
        "prop_dt_apresentacao",
        safe_date_from_column(col("prop_dt_apresentacao_original")),
    )
    .withColumn(
        "prop_nr_ano_original",
        col("prop_nr_ano").cast(IntegerType()),
    )
    .withColumn(
        "prop_nr_ano_tratado",
        when(
            col("prop_nr_ano_original") == 0,
            lit(None).cast(IntegerType()),
        )
        .otherwise(
            col("prop_nr_ano_original").cast(IntegerType())
        ),
    )
    .withColumn(
        "prop_fl_ano_extraido",
        when(
            col("prop_nr_ano_original").isNotNull()
            & (col("prop_nr_ano_original") != 0),
            lit(True),
        )
        .otherwise(lit(False)),
    )
    .withColumn(
        "prop_fl_ano_valido",
        when(
            col("prop_nr_ano_tratado").between(2019, 2026),
            lit(True),
        )
        .otherwise(lit(False)),
    )
    .withColumn(
        "leg_id_legislatura",
        when(
            col("prop_nr_ano_tratado").between(2019, 2022),
            lit(56),
        )
        .when(
            col("prop_nr_ano_tratado").between(2023, 2026),
            lit(57),
        )
        .otherwise(lit(None).cast(IntegerType())),
    )
    .withColumn(
        "prop_fl_legislatura_identificada",
        when(
            col("leg_id_legislatura").isNotNull(),
            lit(True),
        )
        .otherwise(lit(False)),
    )
    .withColumn(
        "prop_fl_data_apresentacao_informada",
        when(
            col("prop_dt_apresentacao_original").isNotNull()
            & (trim(col("prop_dt_apresentacao_original")) != ""),
            lit(True),
        )
        .otherwise(lit(False)),
    )
    .withColumn(
        "prop_fl_data_apresentacao_valida",
        when(
            col("prop_dt_apresentacao").isNotNull(),
            lit(True),
        )
        .otherwise(lit(False)),
    )
)

# ==========================================================================================
# 5. Apply Quality Rules
# ==========================================================================================

proposicoes_quality_df = (
    proposicoes_enriched_df
    .withColumn(
        "prop_fl_id_valido",
        (
            col("prop_id_proposicao").isNotNull()
            & (trim(col("prop_id_proposicao")) != "")
        ),
    )
    .withColumn(
        "prop_fl_tipo_informado",
        (
            col("prop_tx_sigla_tipo").isNotNull()
            & (trim(col("prop_tx_sigla_tipo")) != "")
        ),
    )
    .withColumn(
        "prop_fl_numero_informado",
        (
            col("prop_nr_numero").isNotNull()
            & (trim(col("prop_nr_numero")) != "")
        ),
    )
    .withColumn(
        "prop_fl_ementa_informada",
        (
            col("prop_tx_ementa").isNotNull()
            & (trim(col("prop_tx_ementa")) != "")
        ),
    )
    .withColumn(
        "prop_fl_uri_informada",
        (
            col("prop_tx_uri").isNotNull()
            & (trim(col("prop_tx_uri")) != "")
        ),
    )
    .withColumn(
        "prop_fl_registro_valido_silver",
        col("prop_fl_id_valido"),
    )
    .withColumn(
        "prop_tx_motivo_rejeicao",
        when(
            ~col("prop_fl_id_valido"),
            lit("PROP_ID_NULO_OU_VAZIO"),
        )
        .otherwise(lit(None).cast(StringType())),
    )
)

# ==========================================================================================
# 6. Build Mandatory Rejected Records
# ==========================================================================================

mandatory_rejected_source_df = (
    proposicoes_quality_df
    .filter(
        col("prop_fl_registro_valido_silver") == False
    )
)

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=mandatory_rejected_source_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="prop_id_proposicao",
    validation_rule_column="prop_tx_motivo_rejeicao",
    payload_column="prop_tx_payload_json",
    valid_flag_column="prop_fl_registro_valido_silver",
)

# ==========================================================================================
# 7. Keep Valid Records
# ==========================================================================================

valid_df = (
    proposicoes_quality_df
    .filter(
        col("prop_fl_registro_valido_silver") == True
    )
)

# ==========================================================================================
# 8. Deduplicate Records
# ==========================================================================================

dedup_window = (
    Window
    .partitionBy("prop_id_proposicao")
    .orderBy(
        col("aud_dh_ingestao_bronze").desc_nulls_last()
    )
)

dedup_df = (
    valid_df
    .withColumn(
        "rn_deduplicacao",
        row_number().over(dedup_window),
    )
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=dedup_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="prop_id_proposicao",
    payload_column="prop_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="PROP_REGISTRO_DUPLICADO",
    observation=(
        "Duplicate proposition records removed keeping latest Bronze ingestion."
    ),
)

silver_df = (
    dedup_df
    .filter(
        col("rn_deduplicacao") == 1
    )
    .drop("rn_deduplicacao")
    .drop("prop_tx_motivo_rejeicao")
)

# ==========================================================================================
# 9. Persist Rejected Records
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
        f"Rejected and discarded proposicoes records persisted "
        f"| records_rejected={records_rejected}"
    ),
)

# ==========================================================================================
# 10. Add Silver Traceability Columns
# ==========================================================================================

silver_df = (
    silver_df
    .withColumn(
        "aud_id_execucao_silver",
        lit(execution_id),
    )
    .withColumn(
        "aud_dh_processamento",
        current_timestamp(),
    )
    .withColumn(
        "aud_tx_camada_origem",
        lit("bronze"),
    )
    .withColumn(
        "aud_tx_tabela_origem",
        lit(SOURCE_TABLE),
    )
    .withColumn(
        "aud_tx_tabela_destino",
        lit(TARGET_TABLE),
    )
    .withColumn(
        "aud_tx_versao_pipeline_silver",
        lit(PROJECT_VERSION),
    )
)

# ==========================================================================================
# 11. Add Silver Hash
# ==========================================================================================

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "prop_id_proposicao",
        "prop_tx_sigla_tipo",
        "prop_cd_tipo",
        "prop_nr_numero",
        "prop_nr_ano_tratado",
        "leg_id_legislatura",
        "prop_dt_apresentacao",
        "prop_tx_ementa",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# ==========================================================================================
# 12. Select Final Columns
# ==========================================================================================

final_columns = [
    "prop_id_proposicao",
    "prop_tx_uri",
    "prop_tx_sigla_tipo",
    "prop_cd_tipo",
    "prop_nr_numero",

    "prop_nr_ano",
    "prop_nr_ano_original",
    "prop_nr_ano_tratado",
    "leg_id_legislatura",

    "prop_tx_descricao_tipo",
    "prop_dt_apresentacao_original",
    "prop_dt_apresentacao",
    "prop_tx_ementa",
    "prop_tx_ementa_detalhada",
    "prop_tx_keywords",
    "prop_tx_uri_orgao_numerador",
    "prop_tx_uri_prop_principal",
    "prop_tx_uri_prop_anterior",
    "prop_tx_uri_prop_posterior",
    "prop_tx_url_inteiro_teor",
    "prop_tx_urn_final",

    "prop_fl_id_valido",
    "prop_fl_tipo_informado",
    "prop_fl_numero_informado",
    "prop_fl_ementa_informada",
    "prop_fl_uri_informada",
    "prop_fl_data_apresentacao_informada",
    "prop_fl_data_apresentacao_valida",
    "prop_fl_ano_extraido",
    "prop_fl_ano_valido",
    "prop_fl_legislatura_identificada",
    "prop_fl_registro_valido_silver",

    "prop_tx_payload_json",

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
]

silver_df = silver_df.select(*final_columns)

# ==========================================================================================
# 13. Persist Silver Table
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
        f"Silver proposicoes table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# ==========================================================================================
# 14. Apply Governance Comments
# ==========================================================================================

table_comment = """
Standardized legislative propositions table in the Silver layer.

This table contains validated, standardized and deduplicated proposition records
used to support Gold dimensions, voting analysis, CPI analysis and legislative
agenda marts.

The table preserves original proposition year values while also creating
treated analytical year and legislature fields for downstream analysis.
"""

column_comments = {
    "prop_id_proposicao":
        "Proposition identifier.",

    "prop_tx_uri":
        "Proposition API URI.",

    "prop_tx_sigla_tipo":
        "Standardized proposition type acronym.",

    "prop_cd_tipo":
        "Proposition type code.",

    "prop_nr_numero":
        "Proposition number.",

    "prop_nr_ano":
        "Original standardized proposition year field before analytical treatment.",

    "prop_nr_ano_original":
        "Original proposition year value preserved for auditability.",

    "prop_nr_ano_tratado":
        "Analytical proposition year. Value 0 is converted to NULL.",

    "leg_id_legislatura":
        "Derived legislature identifier based on treated proposition year.",

    "prop_tx_descricao_tipo":
        "Proposition type description.",

    "prop_dt_apresentacao_original":
        "Original proposition presentation date value from Bronze.",

    "prop_dt_apresentacao":
        "Parsed proposition presentation date.",

    "prop_tx_ementa":
        "Standardized proposition summary.",

    "prop_tx_ementa_detalhada":
        "Detailed proposition summary.",

    "prop_tx_keywords":
        "Proposition keywords.",

    "prop_tx_uri_orgao_numerador":
        "URI of the numbering legislative body.",

    "prop_tx_uri_prop_principal":
        "URI of the main proposition.",

    "prop_tx_uri_prop_anterior":
        "URI of the previous proposition.",

    "prop_tx_uri_prop_posterior":
        "URI of the next proposition.",

    "prop_tx_url_inteiro_teor":
        "Full text URL.",

    "prop_tx_urn_final":
        "Final URN reference.",

    "prop_fl_id_valido":
        "Flag indicating whether proposition identifier is valid.",

    "prop_fl_tipo_informado":
        "Flag indicating whether proposition type acronym is informed.",

    "prop_fl_numero_informado":
        "Flag indicating whether proposition number is informed.",

    "prop_fl_ementa_informada":
        "Flag indicating whether proposition summary is informed.",

    "prop_fl_uri_informada":
        "Flag indicating whether proposition URI is informed.",

    "prop_fl_data_apresentacao_informada":
        "Flag indicating whether proposition presentation date was informed.",

    "prop_fl_data_apresentacao_valida":
        "Flag indicating whether proposition presentation date was successfully parsed.",

    "prop_fl_ano_extraido":
        "Flag indicating whether a non-zero proposition year was available.",

    "prop_fl_ano_valido":
        "Flag indicating whether the treated proposition year is within the analytical scope.",

    "prop_fl_legislatura_identificada":
        "Flag indicating whether legislature was derived from the treated year.",

    "prop_fl_registro_valido_silver":
        "Flag indicating whether the record is valid in Silver.",

    "prop_tx_payload_json":
        "Original Bronze JSON payload preserved for traceability.",

    "aud_id_execucao_bronze":
        "Bronze execution identifier.",

    "aud_dh_ingestao_bronze":
        "Bronze ingestion timestamp.",

    "aud_tx_endpoint_origem_bronze":
        "Source endpoint used during Bronze ingestion.",

    "aud_tx_sistema_origem_bronze":
        "Source system identified during Bronze ingestion.",

    "aud_tx_versao_pipeline_bronze":
        "Pipeline version used during Bronze ingestion.",

    "aud_tx_tipo_carga_bronze":
        "Load type applied during Bronze ingestion.",

    "aud_tx_hash_registro_bronze":
        "Deterministic Bronze record hash.",

    "aud_id_execucao_silver":
        "Execution identifier for Silver transformation.",

    "aud_dh_processamento":
        "Timestamp when record was processed in Silver.",

    "aud_tx_camada_origem":
        "Source Medallion layer used during processing.",

    "aud_tx_tabela_origem":
        "Source table used during processing.",

    "aud_tx_tabela_destino":
        "Target Silver table.",

    "aud_tx_versao_pipeline_silver":
        "Pipeline version used during Silver transformation.",

    "aud_tx_hash_registro_silver":
        "Deterministic Silver hash used for traceability.",
}

if APPLY_GOVERNANCE_COMMENTS:
    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# ==========================================================================================
# 15. Final Pipeline Log
# ==========================================================================================

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
        f"Silver proposicoes standardization completed successfully "
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
        f"Silver proposicoes standardization completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER PROPOSICOES COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)