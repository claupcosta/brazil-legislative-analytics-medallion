# Databricks notebook source
# MAGIC %md
# MAGIC # 11 Silver — Fornecedores CNPJ API Enrichment
# MAGIC
# MAGIC **Notebook:** `11_silver_fornecedores_enrichment`
# MAGIC
# MAGIC Enriches standardized supplier records using public CNPJ data from BrasilAPI and
# MAGIC persists API responses and enriched supplier attributes into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Supplier CNPJ selection rules
# MAGIC - Controlled public API enrichment logic
# MAGIC - API status tracking
# MAGIC - CNPJ registration status enrichment
# MAGIC - Supplier analytical risk flags
# MAGIC - API response Delta persistence logic
# MAGIC - Enriched supplier Delta persistence logic
# MAGIC - Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read standardized suppliers from the Silver layer
# MAGIC - Select valid CNPJ suppliers for API enrichment
# MAGIC - Query BrasilAPI using controlled request limits
# MAGIC - Persist raw structured API responses
# MAGIC - Preserve suppliers that were not queried
# MAGIC - Preserve suppliers when API returns errors
# MAGIC - Create analytical flags for CNPJ status
# MAGIC - Persist enriched supplier records into the Silver layer
# MAGIC - Apply governance comments to API and enriched tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - This notebook does not reject suppliers because of API failures
# MAGIC - API enrichment is analytical and non-destructive
# MAGIC - Supplier records must remain available even when API validation fails
# MAGIC - API failures are persisted as status values, not rejected records
# MAGIC - `slv_registros_rejeitados` is not used for API failures
# MAGIC - External enrichment is separated from supplier standardization
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

# MAGIC
# MAGIC %run ../99_utils/utils_comments

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_cnpj

# COMMAND ----------

from datetime import datetime
import uuid
import time

from pyspark.sql import functions as F

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    when,
)

from pyspark.sql import types as T

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("11 - SILVER FORNECEDORES CNPJ API ENRICHMENT")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Global Configuration

# COMMAND ----------

NOTEBOOK_NAME = "11_silver_fornecedores_enrichment"
LAYER_NAME = "silver"
ENTITY_NAME = "fornecedores_cnpj_api"

SOURCE_TABLE = get_silver_table(
    SILVER_TABLES["fornecedores"]
)

TARGET_API_TABLE = get_silver_table(
    SILVER_TABLES["fornecedores_cnpj_api"]
)

TARGET_ENRICHED_TABLE = get_silver_table(
    SILVER_TABLES["fornecedores_enriched"]
)

LOAD_TYPE = LOAD_TYPE_FULL

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

APPLY_GOVERNANCE_COMMENTS = True

MAX_CNPJS_TO_VALIDATE = 50
REQUEST_SLEEP_SECONDS = 1.2

records_read = None
records_written = None

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
    target_table=TARGET_API_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver fornecedores CNPJ API enrichment started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver fornecedores CNPJ API enrichment.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Standardized Suppliers

# COMMAND ----------

try:

    source_df = spark.table(
        SOURCE_TABLE
    )

    log_info(
        pipeline_logger=logger,
        message=(
            "Silver fornecedores table loaded "
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
        target_table=TARGET_API_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed reading Silver fornecedores table "
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
        message="Failed reading Silver fornecedores table.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Select CNPJs for Controlled API Enrichment

# COMMAND ----------

cnpj_df = (
    source_df
    .filter(
        col("forn_tx_tipo_documento") == "CNPJ"
    )
    .filter(
        col("forn_fl_documento_valido_formato") == True
    )
    .filter(
        col("forn_tx_documento_limpo").isNotNull()
    )
    .orderBy(
        col("forn_vl_total_liquido").desc_nulls_last(),
        col("forn_qt_despesas").desc_nulls_last(),
    )
    .select(
        col("forn_tx_documento_limpo")
    )
    .distinct()
    .limit(MAX_CNPJS_TO_VALIDATE)
)

cnpj_list = [
    row["forn_tx_documento_limpo"]
    for row in cnpj_df.collect()
]

records_read = len(cnpj_list)

print(f"CNPJs selected for API enrichment: {records_read}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Query BrasilAPI CNPJ Service

# COMMAND ----------

api_results = []

for index, cnpj in enumerate(cnpj_list, start=1):

    result = fetch_cnpj_data(
        cnpj=cnpj
    )

    result["aud_id_execucao_silver"] = execution_id
    result["aud_dh_consulta_api"] = datetime.now()
    result["aud_tx_api_origem"] = "BrasilAPI CNPJ"
    result["aud_tx_versao_pipeline_silver"] = PROJECT_VERSION

    api_results.append(result)

    if index % 10 == 0:
        print(f"Processed {index}/{len(cnpj_list)} CNPJs")

    time.sleep(REQUEST_SLEEP_SECONDS)

records_written = len(api_results)

print(f"CNPJ API enrichment finished: {records_written} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create API Response DataFrame

# COMMAND ----------

api_schema = T.StructType(
    [
        T.StructField("forn_tx_documento_limpo", T.StringType(), True),
        T.StructField("api_tx_status_consulta_cnpj", T.StringType(), True),
        T.StructField("api_tx_situacao_cadastral", T.StringType(), True),
        T.StructField("api_tx_razao_social", T.StringType(), True),
        T.StructField("api_tx_nome_fantasia", T.StringType(), True),
        T.StructField("api_tx_cnae_principal", T.StringType(), True),
        T.StructField("api_tx_uf", T.StringType(), True),
        T.StructField("api_tx_municipio", T.StringType(), True),
        T.StructField("api_tx_porte", T.StringType(), True),
        T.StructField("api_vl_capital_social", T.StringType(), True),
        T.StructField("api_cd_http_status", T.IntegerType(), True),
        T.StructField("api_tx_erro", T.StringType(), True),
        T.StructField("aud_id_execucao_silver", T.StringType(), True),
        T.StructField("aud_dh_consulta_api", T.TimestampType(), True),
        T.StructField("aud_tx_api_origem", T.StringType(), True),
        T.StructField("aud_tx_versao_pipeline_silver", T.StringType(), True),
    ]
)

api_df = spark.createDataFrame(
    api_results,
    schema=api_schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Persist API Response Table

# COMMAND ----------

try:

    (
        api_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_API_TABLE)
    )

    log_info(
        pipeline_logger=logger,
        message=(
            "Silver fornecedores CNPJ API response table persisted successfully "
            f"| records_written={records_written}"
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
        target_table=TARGET_API_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed writing Silver fornecedores CNPJ API table "
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
        message="Failed writing Silver fornecedores CNPJ API table.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Build Enriched Supplier Table

# COMMAND ----------

enriched_df = (
    source_df.alias("forn")
    .join(
        api_df.alias("api"),
        col("forn.forn_tx_documento_limpo") == col("api.forn_tx_documento_limpo"),
        "left",
    )
    .select(
        col("forn.*"),
        F.coalesce(
            col("api.api_tx_status_consulta_cnpj"),
            when(
                col("forn.forn_tx_tipo_documento") == "CNPJ",
                lit("NOT_VALIDATED"),
            ).otherwise(
                lit("NOT_APPLICABLE")
            ),
        ).alias("forn_tx_status_consulta_cnpj"),
        col("api.api_cd_http_status").alias("forn_cd_http_status_cnpj"),
        col("api.api_tx_erro").alias("forn_tx_erro_consulta_cnpj"),
        col("api.api_tx_situacao_cadastral").alias("forn_tx_situacao_cadastral"),
        col("api.api_tx_razao_social").alias("forn_tx_razao_social_receita"),
        col("api.api_tx_nome_fantasia").alias("forn_tx_nome_fantasia_receita"),
        col("api.api_tx_cnae_principal").alias("forn_tx_cnae_principal"),
        col("api.api_tx_uf").alias("forn_tx_uf_receita"),
        col("api.api_tx_municipio").alias("forn_tx_municipio_receita"),
        col("api.api_tx_porte").alias("forn_tx_porte_empresa"),
        col("api.api_vl_capital_social").cast("double").alias("forn_vl_capital_social"),
        col("api.aud_dh_consulta_api").alias("forn_dh_consulta_api"),
    )
    .withColumn(
        "forn_fl_cnpj_encontrado",
        (
            col("forn_tx_status_consulta_cnpj") == "FOUND"
        ).cast("boolean"),
    )
    .withColumn(
        "forn_fl_cnpj_ativo",
        (
            F.upper(
                F.coalesce(
                    col("forn_tx_situacao_cadastral"),
                    lit("")
                )
            ) == "ATIVA"
        ).cast("boolean"),
    )
    .withColumn(
        "forn_fl_cnpj_suspeito",
        when(
            col("forn_tx_tipo_documento") != "CNPJ",
            lit(False),
        )
        .when(
            col("forn_tx_status_consulta_cnpj") == "NOT_VALIDATED",
            lit(False),
        )
        .when(
            col("forn_tx_status_consulta_cnpj").isin(
                "INVALID_FORMAT",
                "NOT_FOUND",
                "ERROR",
            ),
            lit(True),
        )
        .when(
            F.upper(
                F.coalesce(
                    col("forn_tx_situacao_cadastral"),
                    lit("")
                )
            ).isin(
                "BAIXADA",
                "INAPTA",
                "SUSPENSA",
                "NULA",
            ),
            lit(True),
        )
        .otherwise(
            lit(False)
        ),
    )
    .withColumn(
        "forn_tx_motivo_cnpj_suspeito",
        when(
            col("forn_tx_tipo_documento") != "CNPJ",
            lit("Document is not CNPJ and was not queried."),
        )
        .when(
            col("forn_tx_status_consulta_cnpj") == "NOT_VALIDATED",
            lit("CNPJ was not selected for API validation."),
        )
        .when(
            col("forn_tx_status_consulta_cnpj") == "INVALID_FORMAT",
            lit("CNPJ has invalid format."),
        )
        .when(
            col("forn_tx_status_consulta_cnpj") == "NOT_FOUND",
            lit("CNPJ was not found in public API."),
        )
        .when(
            col("forn_tx_status_consulta_cnpj") == "ERROR",
            lit("API query returned error."),
        )
        .when(
            F.upper(
                F.coalesce(
                    col("forn_tx_situacao_cadastral"),
                    lit("")
                )
            ).isin(
                "BAIXADA",
                "INAPTA",
                "SUSPENSA",
                "NULA",
            ),
            F.concat(
                lit("Non-active registration status: "),
                col("forn_tx_situacao_cadastral"),
            ),
        )
        .otherwise(
            lit("No critical registration issue identified.")
        ),
    )
    .withColumn(
        "aud_id_execucao_enrichment",
        lit(execution_id),
    )
    .withColumn(
        "aud_dh_processamento_enrichment",
        current_timestamp(),
    )
    .withColumn(
        "aud_tx_tabela_api_origem",
        lit(TARGET_API_TABLE),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Persist Enriched Supplier Table

# COMMAND ----------

try:

    (
        enriched_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_ENRICHED_TABLE)
    )

    log_info(
        pipeline_logger=logger,
        message=(
            "Silver fornecedores enriched table persisted successfully "
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
        target_table=TARGET_ENRICHED_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed writing Silver fornecedores enriched table "
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
        message="Failed writing Silver fornecedores enriched table.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Apply Governance Comments

# COMMAND ----------

api_table_comment = """
CNPJ API enrichment response table for suppliers.

This table stores structured public CNPJ API responses for selected suppliers.
It is used for analytical enrichment and governance purposes only.

Important:
- Supplier records are not rejected due to API failures.
- API errors are persisted as status values.
- API enrichment is non-destructive and separated from supplier standardization.
"""

api_column_comments = {
    "forn_tx_documento_limpo": "Supplier CNPJ containing only numeric characters.",
    "api_tx_status_consulta_cnpj": "API query status for the CNPJ.",
    "api_tx_situacao_cadastral": "Company registration status returned by the public API.",
    "api_tx_razao_social": "Company legal name returned by the public API.",
    "api_tx_nome_fantasia": "Company trade name returned by the public API.",
    "api_tx_cnae_principal": "Main economic activity returned by the public API.",
    "api_tx_uf": "Company state returned by the public API.",
    "api_tx_municipio": "Company city returned by the public API.",
    "api_tx_porte": "Company size returned by the public API.",
    "api_vl_capital_social": "Company capital value returned by the public API.",
    "api_cd_http_status": "HTTP status code returned by the API.",
    "api_tx_erro": "API error message, when applicable.",
    "aud_id_execucao_silver": "Execution identifier for API enrichment.",
    "aud_dh_consulta_api": "Timestamp when the API query was performed.",
    "aud_tx_api_origem": "Public API source used for enrichment.",
    "aud_tx_versao_pipeline_silver": "Pipeline version used during enrichment.",
}

enriched_table_comment = """
Enriched suppliers table in the Silver layer.

This table combines standardized supplier records with optional public CNPJ API
enrichment data.

Main characteristics:
- preserves all standardized suppliers
- adds CNPJ API status when queried
- adds company registration attributes when available
- adds analytical CNPJ status flags
- does not reject suppliers due to API failures
- supports future Gold dimensions and supplier risk marts
"""

enriched_column_comments = {
    "forn_tx_chave_deduplicacao": "Supplier deduplication key inherited from the standardized suppliers table.",
    "forn_tx_nome": "Standardized supplier name.",
    "forn_tx_documento_original": "Original supplier CNPJ or CPF value from CEAP expenses.",
    "forn_tx_documento_limpo": "Supplier document containing only numeric characters.",
    "forn_tx_tipo_documento": "Supplier document type classification.",
    "forn_fl_nome_informado": "Flag indicating whether supplier name is informed.",
    "forn_fl_documento_informado": "Flag indicating whether supplier document is informed.",
    "forn_fl_documento_repetido": "Flag indicating whether supplier document is composed only by repeated digits.",
    "forn_fl_documento_valido_formato": "Flag indicating whether supplier document has valid structural format.",
    "forn_qt_despesas": "Number of CEAP expense records associated with the supplier.",
    "forn_vl_total_liquido": "Total CEAP net value associated with the supplier.",
    "forn_vl_medio_liquido": "Average CEAP net value associated with the supplier.",
    "forn_fl_registro_valido_silver": "Flag indicating whether supplier record is valid in Silver.",
    "aud_dh_ultima_ingestao_bronze": "Latest Bronze ingestion timestamp associated with supplier expenses.",
    "aud_dh_ultimo_processamento_despesa_silver": "Latest Silver expense processing timestamp associated with supplier expenses.",
    "aud_id_execucao_silver": "Execution identifier for Silver supplier standardization.",
    "aud_dh_processamento": "Timestamp when supplier standardization was processed.",
    "aud_tx_camada_origem": "Source Medallion layer used during supplier standardization.",
    "aud_tx_tabela_origem": "Source table used during supplier standardization.",
    "aud_tx_tabela_destino": "Target table used during supplier standardization.",
    "aud_tx_versao_pipeline_silver": "Pipeline version used during supplier standardization.",
    "aud_tx_hash_registro_silver": "Deterministic Silver supplier record hash.",
    "forn_tx_status_consulta_cnpj": "CNPJ API query status for the supplier.",
    "forn_cd_http_status_cnpj": "HTTP status returned by the CNPJ API.",
    "forn_tx_erro_consulta_cnpj": "Error message returned by the CNPJ API when applicable.",
    "forn_tx_situacao_cadastral": "Supplier registration status returned by the CNPJ API.",
    "forn_tx_razao_social_receita": "Company legal name returned by the CNPJ API.",
    "forn_tx_nome_fantasia_receita": "Company trade name returned by the CNPJ API.",
    "forn_tx_cnae_principal": "Main economic activity returned by the CNPJ API.",
    "forn_tx_uf_receita": "Company state returned by the CNPJ API.",
    "forn_tx_municipio_receita": "Company city returned by the CNPJ API.",
    "forn_tx_porte_empresa": "Company size returned by the CNPJ API.",
    "forn_vl_capital_social": "Company capital value returned by the CNPJ API.",
    "forn_dh_consulta_api": "Timestamp when the CNPJ API query was performed.",
    "forn_fl_cnpj_encontrado": "Flag indicating whether the CNPJ was found in the public API.",
    "forn_fl_cnpj_ativo": "Flag indicating whether the CNPJ registration status is active.",
    "forn_fl_cnpj_suspeito": "Analytical flag indicating possible CNPJ registration risk.",
    "forn_tx_motivo_cnpj_suspeito": "Reason describing the CNPJ suspicion flag.",
    "aud_id_execucao_enrichment": "Execution identifier for supplier enrichment.",
    "aud_dh_processamento_enrichment": "Timestamp when supplier enrichment was processed.",
    "aud_tx_tabela_api_origem": "API response table used as enrichment source.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_governance_comments(
        table_name=TARGET_API_TABLE,
        table_comment=api_table_comment,
        column_comments=api_column_comments,
    )

    apply_governance_comments(
        table_name=TARGET_ENRICHED_TABLE,
        table_comment=enriched_table_comment,
        column_comments=enriched_column_comments,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Final Pipeline Log

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
    target_table=TARGET_ENRICHED_TABLE,
    status=EXECUTION_STATUS_SUCCESS,
    message=(
        "Silver fornecedores CNPJ API enrichment completed successfully "
        "| enrichment=BrasilAPI CNPJ"
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
        f"Silver fornecedores CNPJ enrichment completed "
        f"| cnpjs_processed={records_written} "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("SILVER FORNECEDORES CNPJ API ENRICHMENT COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"API Table: {TARGET_API_TABLE}")
print(f"Enriched Table: {TARGET_ENRICHED_TABLE}")
print(f"CNPJs Processed: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)