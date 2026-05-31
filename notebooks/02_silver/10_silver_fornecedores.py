# Databricks notebook source
# MAGIC %md
# MAGIC # 10 Silver — Fornecedores Standardization
# MAGIC
# MAGIC **Notebook:** `10_silver_fornecedores`
# MAGIC
# MAGIC Consolidates CEAP suppliers from the standardized expenses table and persists
# MAGIC deduplicated, classified and analytics-ready supplier records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Supplier consolidation rules
# MAGIC - Supplier document normalization logic
# MAGIC - CNPJ/CPF structural classification
# MAGIC - Text normalization using global utilities
# MAGIC - Supplier usage metrics based on CEAP expenses
# MAGIC - Supplier deduplication strategy
# MAGIC - Silver Delta persistence logic
# MAGIC - Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read standardized CEAP expenses from the Silver layer
# MAGIC - Extract supplier names and supplier documents
# MAGIC - Normalize supplier names and document values
# MAGIC - Remove punctuation from CNPJ/CPF fields
# MAGIC - Classify supplier document type as CNPJ, CPF, OUTRO or NAO_INFORMADO
# MAGIC - Identify repeated or malformed supplier documents
# MAGIC - Consolidate supplier-level CEAP usage metrics
# MAGIC - Deduplicate suppliers using document-based and fallback keys
# MAGIC - Preserve lineage from the standardized CEAP expenses table
# MAGIC - Persist standardized supplier records into the Silver layer
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - This notebook does not call external APIs
# MAGIC - Public CNPJ API enrichment is handled in a separated enrichment notebook
# MAGIC - Supplier records are consolidated from `silver.slv_despesas_ceap`
# MAGIC - CNPJ/CPF values are standardized as numeric-only strings
# MAGIC - Supplier document classification is structural and does not depend on external services
# MAGIC - Missing or malformed documents are preserved with quality flags
# MAGIC - Supplier CNPJ/CPF is informative and should not be used alone as a rejection criterion
# MAGIC - Supplier usage metrics are derived from CEAP expense records
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

# MAGIC %run ../99_utils/utils_cnpj

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import functions as F

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    row_number,
    when,
)

from pyspark.sql.window import Window

from pyspark.sql.types import (
    StringType,
)

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("10 - SILVER FORNECEDORES")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Global Configuration

# COMMAND ----------

NOTEBOOK_NAME = "10_silver_fornecedores"
LAYER_NAME = "silver"
ENTITY_NAME = "fornecedores"

SOURCE_TABLE = get_silver_table(
    SILVER_TABLES["despesas_ceap"]
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES["fornecedores"]
)

REJECTED_TABLE = get_silver_table(
    SILVER_TABLES["registros_rejeitados"]
)

LOAD_TYPE = LOAD_TYPE_FULL

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

# MAGIC %md
# MAGIC ## 2. Start Pipeline Log

# COMMAND ----------

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver fornecedores standardization started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver fornecedores standardization.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Standardized CEAP Expenses

# COMMAND ----------

try:

    source_df = spark.table(
        SOURCE_TABLE
    )

    log_info(
        pipeline_logger=logger,
        message=(
            "Silver despesas CEAP table loaded "
            "| records_read=None"
        ),
    )

except Exception as error:

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
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed reading Silver despesas CEAP table "
            f"| error={str(error)}"
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=None,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed reading Silver despesas CEAP table.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extract Supplier Attributes

# COMMAND ----------

fornecedores_base_df = (
    source_df
    .select(
        normalize_upper_text("desp_tx_nome_fornecedor").alias("forn_tx_nome"),
        normalize_text("desp_tx_cnpj_cpf_fornecedor").alias("forn_tx_documento_original"),
        F.regexp_replace(
            col("desp_tx_cnpj_cpf_fornecedor").cast("string"),
            "[^0-9]",
            "",
        ).alias("forn_tx_documento_limpo"),
        col("desp_tx_tipo_despesa"),
        col("desp_vl_liquido"),
        col("aud_id_execucao_bronze"),
        col("aud_dh_ingestao_bronze"),
        col("aud_tx_hash_registro_bronze"),
        col("aud_id_execucao_silver").alias("aud_id_execucao_despesa_silver"),
        col("aud_dh_processamento").alias("aud_dh_processamento_despesa_silver"),
    )
    .withColumn(
        "forn_tx_payload_json",
        lit(None).cast(StringType()),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Apply Supplier Quality Rules

# COMMAND ----------

fornecedores_quality_df = (
    fornecedores_base_df
    .withColumn(
        "forn_fl_nome_informado",
        (
            col("forn_tx_nome").isNotNull()
            & (col("forn_tx_nome") != "")
        ).cast("boolean"),
    )
    .withColumn(
        "forn_fl_documento_informado",
        (
            col("forn_tx_documento_limpo").isNotNull()
            & (col("forn_tx_documento_limpo") != "")
        ).cast("boolean"),
    )
    .withColumn(
        "forn_fl_documento_repetido",
        (
            col("forn_tx_documento_limpo").rlike(r"^([0-9])\1+$")
        ).cast("boolean"),
    )
    .withColumn(
        "forn_tx_tipo_documento",
        when(
            (F.length(col("forn_tx_documento_limpo")) == 14)
            & (~col("forn_fl_documento_repetido")),
            lit("CNPJ"),
        )
        .when(
            (F.length(col("forn_tx_documento_limpo")) == 11)
            & (~col("forn_fl_documento_repetido")),
            lit("CPF"),
        )
        .when(
            col("forn_fl_documento_informado") == False,
            lit("NAO_INFORMADO"),
        )
        .otherwise(
            lit("OUTRO")
        ),
    )
    .withColumn(
        "forn_fl_documento_valido_formato",
        col("forn_tx_tipo_documento").isin(
            "CNPJ",
            "CPF",
        ).cast("boolean"),
    )
    .withColumn(
        "forn_tx_chave_deduplicacao",
        when(
            col("forn_fl_documento_valido_formato"),
            col("forn_tx_documento_limpo"),
        )
        .otherwise(
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(
                        col("forn_tx_nome"),
                        lit("__SEM_NOME__"),
                    ),
                    F.coalesce(
                        col("forn_tx_documento_limpo"),
                        lit("__SEM_DOC__"),
                    ),
                ),
                256,
            )
        ),
    )
    .withColumn(
        "forn_fl_registro_valido_silver",
        col("forn_fl_nome_informado").cast("boolean"),
    )
    .withColumn(
        "forn_tx_motivo_rejeicao",
        when(
            ~col("forn_fl_nome_informado"),
            lit("FORN_NOME_NULO_OU_VAZIO"),
        )
        .otherwise(
            lit(None).cast(StringType())
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Build Mandatory Rejected Records

# COMMAND ----------

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=fornecedores_quality_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="forn_tx_chave_deduplicacao",
    validation_rule_column="forn_tx_motivo_rejeicao",
    payload_column="forn_tx_payload_json",
    valid_flag_column="forn_fl_registro_valido_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Keep Valid Supplier Records

# COMMAND ----------

valid_df = (
    fornecedores_quality_df
    .filter(
        col("forn_fl_registro_valido_silver") == True
    )
    .drop("forn_tx_motivo_rejeicao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Aggregate Supplier Usage Metrics

# COMMAND ----------

fornecedores_agg_df = (
    valid_df
    .groupBy(
       "forn_tx_chave_deduplicacao",
        "forn_tx_nome",
        "forn_tx_documento_original",
        "forn_tx_documento_limpo",
        "forn_tx_tipo_documento",
        "forn_fl_nome_informado",
        "forn_fl_documento_informado",
        "forn_fl_documento_repetido",
        "forn_fl_documento_valido_formato",
        "forn_fl_registro_valido_silver",
        "forn_tx_payload_json",
    )
    .agg(
        F.count("*").alias("forn_qt_despesas"),
        F.round(
            F.sum("desp_vl_liquido"),
            2,
        ).alias("forn_vl_total_liquido"),
        F.round(
            F.avg("desp_vl_liquido"),
            2,
        ).alias("forn_vl_medio_liquido"),
        F.max("aud_dh_ingestao_bronze").alias("aud_dh_ultima_ingestao_bronze"),
        F.max("aud_dh_processamento_despesa_silver").alias("aud_dh_ultimo_processamento_despesa_silver"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Identify Technical Duplicates

# COMMAND ----------

dedup_window = (
    Window
    .partitionBy(
        "forn_tx_chave_deduplicacao"
    )
    .orderBy(
        col("forn_vl_total_liquido").desc_nulls_last(),
        col("forn_qt_despesas").desc_nulls_last(),
        col("forn_tx_nome").asc_nulls_last(),
    )
)

fornecedores_ranked_df = (
    fornecedores_agg_df
    .withColumn(
        "rn_deduplicacao",
        row_number().over(dedup_window),
    )
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=fornecedores_ranked_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="forn_tx_chave_deduplicacao",
    payload_column="forn_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="FORN_REGISTRO_DUPLICADO_TECNICO",
    observation=(
        "Supplier record kept only once by supplier deduplication key. "
        "Deduplication order uses total CEAP net value and expense count."
    ),
)

fornecedores_dedup_df = (
    fornecedores_ranked_df
    .filter(
        col("rn_deduplicacao") == 1
    )
    .drop("rn_deduplicacao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Persist Rejected and Discarded Records

# COMMAND ----------

rejected_df = union_rejected_records(
    mandatory_rejected_dataframe=mandatory_rejected_df,
    duplicate_rejected_dataframe=duplicate_rejected_df,
)

try:

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
            "Rejected and discarded fornecedores records persisted "
            f"| rejected_table={REJECTED_TABLE} "
            "| records_rejected=None"
        ),
    )

except Exception as error:

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
        target_table=REJECTED_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed writing rejected fornecedores records "
            f"| error={str(error)}"
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=None,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed writing rejected fornecedores records.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Add Silver Traceability Columns

# COMMAND ----------

silver_df = (
    fornecedores_dedup_df
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
        lit("silver"),
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Add Silver Record Hash

# COMMAND ----------

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "forn_tx_chave_deduplicacao",
        "forn_tx_nome",
        "forn_tx_documento_limpo",
        "forn_tx_tipo_documento",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Select Final Silver Columns

# COMMAND ----------

final_columns = [
    "forn_tx_chave_deduplicacao",
    "forn_tx_nome",
    "forn_tx_documento_original",
    "forn_tx_documento_limpo",
    "forn_tx_tipo_documento",
    "forn_fl_nome_informado",
    "forn_fl_documento_informado",
    "forn_fl_documento_repetido",
    "forn_fl_documento_valido_formato",
    "forn_qt_despesas",
    "forn_vl_total_liquido",
    "forn_vl_medio_liquido",
    "forn_fl_registro_valido_silver",
    "aud_dh_ultima_ingestao_bronze",
    "aud_dh_ultimo_processamento_despesa_silver",
    "aud_id_execucao_silver",
    "aud_dh_processamento",
    "aud_tx_camada_origem",
    "aud_tx_tabela_origem",
    "aud_tx_tabela_destino",
    "aud_tx_versao_pipeline_silver",
    "aud_tx_hash_registro_silver",
]

silver_df = silver_df.select(
    *final_columns
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Persist Silver Table

# COMMAND ----------

try:

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    log_info(
        pipeline_logger=logger,
        message=(
            "Silver fornecedores table persisted successfully "
            "| records_written=None"
        ),
    )

except Exception as error:

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
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed writing Silver fornecedores table "
            f"| error={str(error)}"
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=records_read,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed writing Silver fornecedores table.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Apply Governance Comments

# COMMAND ----------

table_comment = """
Standardized suppliers table in the Silver layer.

This table consolidates suppliers identified in CEAP expenses without calling
external APIs.

Main characteristics:
- one row per supplier deduplication key
- supplier name standardization
- CNPJ/CPF cleaning
- document type classification
- supplier usage metrics from CEAP expenses
- missing or malformed documents preserved with quality flags
- mandatory rejected records persisted separately when supplier name is missing
- technical duplicate tracking
- preserved analytical traceability
- deterministic Silver record hash

Silver layer note:
- Supplier CNPJ/CPF is informative and should not be used alone as a rejection criterion.
- Missing, placeholder or malformed supplier documents are preserved and flagged.
- External CNPJ API enrichment is handled in a separated enrichment notebook.
"""

column_comments = {
    "forn_tx_chave_deduplicacao": "Supplier deduplication key based on document when valid or fallback hash when document is unavailable or malformed.",
    "forn_tx_nome": "Standardized supplier name.",
    "forn_tx_documento_original": "Original supplier CNPJ or CPF value from CEAP expenses.",
    "forn_tx_documento_limpo": "Supplier document containing only numeric characters.",
    "forn_tx_tipo_documento": "Supplier document type classification: CNPJ, CPF, OUTRO or NAO_INFORMADO.",
    "forn_fl_nome_informado": "Flag indicating whether supplier name is informed.",
    "forn_fl_documento_informado": "Flag indicating whether supplier document is informed.",
    "forn_fl_documento_repetido": "Flag indicating whether supplier document is composed only by repeated digits.",
    "forn_fl_documento_valido_formato": "Flag indicating whether supplier document has valid structural format.",
    "forn_qt_despesas": "Number of CEAP expense records associated with the supplier.",
    "forn_vl_total_liquido": "Total CEAP net value associated with the supplier.",
    "forn_vl_medio_liquido": "Average CEAP net value associated with the supplier.",
    "forn_fl_registro_valido_silver": "Flag indicating whether supplier record is valid in Silver.",
    "aud_dh_ultima_ingestao_bronze": "Latest Bronze ingestion timestamp associated with supplier expenses.",
    "aud_dh_ultimo_processamento_despesa_silver": "Latest Silver expense processing timestamp associated with the supplier.",
    "aud_id_execucao_silver": "Execution identifier for Silver supplier processing.",
    "aud_dh_processamento": "Timestamp when supplier record was processed.",
    "aud_tx_camada_origem": "Source Medallion layer used during processing.",
    "aud_tx_tabela_origem": "Source table used during supplier consolidation.",
    "aud_tx_tabela_destino": "Target Silver supplier table.",
    "aud_tx_versao_pipeline_silver": "Pipeline version used during Silver supplier processing.",
    "aud_tx_hash_registro_silver": "Deterministic Silver supplier record hash.",
}

if APPLY_GOVERNANCE_COMMENTS:

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
        "Silver fornecedores standardization completed successfully "
        "| grain=one supplier per deduplication key"
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
        f"Silver fornecedores standardization completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("SILVER FORNECEDORES COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print("Grain: one supplier per deduplication key")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)