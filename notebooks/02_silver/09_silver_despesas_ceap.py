# Databricks notebook source
# MAGIC %md
# MAGIC # 09 Silver — Despesas CEAP Standardization
# MAGIC
# MAGIC **Notebook:** `09_silver_despesas_ceap`
# MAGIC
# MAGIC Standardizes CEAP parliamentary expense records from the Bronze layer and persists validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Schema normalization rules
# MAGIC - CEAP expense standardization logic
# MAGIC - Text normalization using global utilities
# MAGIC - Safe date conversion
# MAGIC - Safe monetary conversion
# MAGIC - Supplier CPF/CNPJ cleansing and classification
# MAGIC - Supplier document quality validation
# MAGIC - Mandatory business field validation
# MAGIC - Technical duplicate detection and removal
# MAGIC - Rejected records tracking using global utilities
# MAGIC - Bronze-to-Silver lineage preservation
# MAGIC - Silver Delta persistence logic
# MAGIC - Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Responsibilities
# MAGIC
# MAGIC - Read CEAP expense data from Bronze layer
# MAGIC - Standardize deputy, supplier and expense attributes
# MAGIC - Normalize textual fields
# MAGIC - Safely convert expense document dates
# MAGIC - Safely convert monetary values
# MAGIC - Clean supplier CPF/CNPJ identifiers
# MAGIC - Classify supplier document type
# MAGIC - Generate supplier document quality indicators
# MAGIC - Validate mandatory CEAP expense fields
# MAGIC - Reject records with null or empty deputy identifier
# MAGIC - Remove technical duplicate records
# MAGIC - Preserve Bronze ingestion lineage
# MAGIC - Register rejected and discarded records for traceability
# MAGIC - Persist curated Delta table
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Notes
# MAGIC
# MAGIC - Bronze preserves raw source values
# MAGIC - Silver standardizes, validates and deduplicates records
# MAGIC - Monetary fields are converted from source string format into numeric values|
# MAGIC - Supplier CPF/CNPJ is treated as informative, not mandatory
# MAGIC - Invalid supplier documents are retained with quality flags
# MAGIC - Atypical supplier documents are retained for analytical completeness
# MAGIC - Negative monetary values are preserved because they may represent reimbursements, reversals or accounting adjustments
# MAGIC - Records with null or empty `dep_id_deputado` are rejected
# MAGIC - Invalid records are redirected to `slv_registros_rejeitados`
# MAGIC - Technical duplicates are also registered as discarded records
# MAGIC - Bronze-to-Silver traceability is preserved through audit metadata
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
    when,
    coalesce,
    current_timestamp,
    row_number,
    concat_ws,
    sha2,
    regexp_replace,
    length,
    to_date,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, TimestampType, DoubleType, IntegerType

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("09 - SILVER DESPESAS CEAP")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# 1. Global Configuration
# ============================================================

NOTEBOOK_NAME = "09_silver_despesas_ceap"
LAYER_NAME = "silver"
ENTITY_NAME = "despesas_ceap"

SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES["despesas_ceap"]
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES["despesas_ceap"]
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

records_read = 0
records_written = 0
records_rejected = 0

# COMMAND ----------

# ============================================================
# 2. Helper Functions
# ============================================================

def clean_text(column_expression):
    """
    Trims textual values and normalizes repeated spaces.
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


def clean_upper_text(column_expression):
    """
    Trims textual values, normalizes repeated spaces and converts to uppercase.
    """

    return upper(
        clean_text(column_expression)
    )


def clean_numeric_document(column_expression):
    """
    Removes non-numeric characters from CPF/CNPJ fields.
    """

    return regexp_replace(
        clean_text(column_expression),
        "[^0-9]",
        "",
    )

# COMMAND ----------

# ============================================================
# 3. Start Pipeline Log
# ============================================================

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver despesas CEAP transformation started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver despesas CEAP transformation.",
)

# COMMAND ----------

# ============================================================
# 4. Read Bronze Data
# ============================================================

try:

    bronze_df = spark.table(SOURCE_TABLE)

    records_read = bronze_df.count()

    log_info(
        pipeline_logger=logger,
        message=(
            f"Bronze despesas CEAP table loaded successfully "
            f"| records_read={records_read}"
        ),
    )

except Exception as error:

    finished_at = datetime.now()
    duration_seconds = (finished_at - started_at).total_seconds()

    write_pipeline_log(
        log_id=str(uuid.uuid4()),
        execution_id=execution_id,
        notebook_name=NOTEBOOK_NAME,
        layer_name=LAYER_NAME,
        entity_name=ENTITY_NAME,
        target_table=TARGET_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=f"Failed reading Bronze despesas CEAP table | error={str(error)}",
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        records_read=None,
        records_written=None,
    )

    log_error(
        pipeline_logger=logger,
        message="Failed reading Bronze despesas CEAP table.",
        error=error,
    )

    raise error

# COMMAND ----------

# ============================================================
# 5. Standardize Bronze Columns
# ============================================================

silver_base_df = (
    bronze_df
    .select(
        clean_text(col("dep_id_deputado")).alias("dep_id_deputado"),
        clean_upper_text(col("dep_tx_nome_parlamentar")).alias("dep_tx_nome"),
        clean_upper_text(col("dep_tx_sigla_partido")).alias("dep_tx_sigla_partido"),
        clean_upper_text(col("dep_tx_sigla_uf")).alias("dep_tx_sigla_uf"),
        clean_text(col("dep_nr_legislatura")).alias("leg_id_legislatura"),

        col("desp_nr_ano").cast(IntegerType()).alias("desp_nr_ano"),
        col("desp_nr_ano_referencia").cast(IntegerType()).alias("desp_nr_ano_referencia"),
        col("desp_nr_mes").cast(IntegerType()).alias("desp_nr_mes"),

        clean_upper_text(col("desp_tx_tipo_despesa")).alias("desp_tx_tipo_despesa"),
        clean_text(col("desp_tx_tipo_documento")).alias("desp_tx_tipo_documento"),
        clean_text(col("desp_tx_numero_documento")).alias("desp_tx_numero_documento"),

        clean_upper_text(col("desp_tx_nome_fornecedor")).alias("desp_tx_nome_fornecedor"),
        clean_text(col("desp_tx_cnpj_cpf_fornecedor")).alias("desp_tx_cnpj_cpf_fornecedor"),

        clean_text(col("desp_dt_data_documento")).alias("desp_dt_data_documento_original"),
        clean_text(col("desp_tx_url_documento")).alias("desp_tx_url_documento"),

        col("desp_vl_documento").cast(DoubleType()).alias("desp_vl_documento"),
        coalesce(col("desp_vl_glosa").cast(DoubleType()), lit(0.0)).alias("desp_vl_glosa"),
        col("desp_vl_liquido").cast(DoubleType()).alias("desp_vl_liquido"),

        clean_text(col("desp_tx_payload_json")).alias("desp_tx_payload_json"),

        col("aud_id_execucao").cast(StringType()).alias("aud_id_execucao_bronze"),
        col("aud_dh_ingestao").cast(TimestampType()).alias("aud_dh_ingestao_bronze"),
        col("aud_tx_endpoint_origem").cast(StringType()).alias("aud_tx_endpoint_origem_bronze"),
        col("aud_tx_sistema_origem").cast(StringType()).alias("aud_tx_sistema_origem_bronze"),
        col("aud_tx_versao_pipeline").cast(StringType()).alias("aud_tx_versao_pipeline_bronze"),
        col("aud_tx_tipo_carga").cast(StringType()).alias("aud_tx_tipo_carga_bronze"),
        col("aud_tx_arquivo_origem").cast(StringType()).alias("aud_tx_arquivo_origem_bronze"),
        col("aud_tx_hash_registro").cast(StringType()).alias("aud_tx_hash_registro_bronze"),
    )
)

# COMMAND ----------

# ============================================================
# 6. Normalize Date and Supplier Document
# ============================================================

silver_document_df = (
    silver_base_df
    .withColumn(
        "desp_dt_data_documento",
    F.to_date(
        F.substring(
            col("desp_dt_data_documento_original"),
            1,
            10,
        ),
        "yyyy-MM-dd",
    ),
    )
    .withColumn(
        "desp_tx_cnpj_cpf_fornecedor_limpo",
        clean_numeric_document(col("desp_tx_cnpj_cpf_fornecedor")),
    )
    .withColumn(
        "desp_fl_documento_fornecedor_informado",
        (
            col("desp_tx_cnpj_cpf_fornecedor_limpo").isNotNull()
            & (col("desp_tx_cnpj_cpf_fornecedor_limpo") != "")
        ).cast("boolean"),
    )
    .withColumn(
        "desp_fl_documento_fornecedor_repetido",
        (
            col("desp_tx_cnpj_cpf_fornecedor_limpo").rlike(r"^([0-9])\1+$")
        ).cast("boolean"),
    )
    .withColumn(
        "desp_tx_tipo_documento_fornecedor",
        when(
            (length(col("desp_tx_cnpj_cpf_fornecedor_limpo")) == 14)
            & (~col("desp_fl_documento_fornecedor_repetido")),
            lit("CNPJ"),
        )
        .when(
            (length(col("desp_tx_cnpj_cpf_fornecedor_limpo")) == 11)
            & (~col("desp_fl_documento_fornecedor_repetido")),
            lit("CPF"),
        )
        .when(
            col("desp_fl_documento_fornecedor_informado") == False,
            lit("NAO_INFORMADO"),
        )
        .otherwise(lit("OUTRO")),
    )
    .withColumn(
        "desp_fl_documento_fornecedor_valido_formato",
        col("desp_tx_tipo_documento_fornecedor").isin("CNPJ", "CPF").cast("boolean"),
    )
    .withColumn(
        "desp_fl_documento_fornecedor_atipico",
        (
            col("desp_tx_tipo_documento_fornecedor").isin("OUTRO")
            | col("desp_fl_documento_fornecedor_repetido")
        ).cast("boolean"),
    )
)

# COMMAND ----------

# ============================================================
# 7. Create Analytical and Quality Flags
# ============================================================

silver_flags_df = (
    silver_document_df
    .withColumn(
        "desp_fl_possui_documento_url",
        (
            col("desp_tx_url_documento").isNotNull()
            & (col("desp_tx_url_documento") != "")
        ).cast("boolean"),
    )
    .withColumn(
        "desp_fl_possui_glosa",
        (
            coalesce(col("desp_vl_glosa"), lit(0.0)) > lit(0.0)
        ).cast("boolean"),
    )
    .withColumn(
        "desp_fl_valor_negativo",
        (
            coalesce(col("desp_vl_liquido"), lit(0.0)) < lit(0.0)
        ).cast("boolean"),
    )
    .withColumn(
        "desp_fl_deputado_informado",
        (
            col("dep_id_deputado").isNotNull()
            & (col("dep_id_deputado") != "")
        ).cast("boolean"),
    )
    .withColumn(
        "desp_fl_periodo_valido",
        (
            col("desp_nr_ano").isNotNull()
            & (col("desp_nr_ano") >= 1900)
            & col("desp_nr_mes").between(1, 12)
        ).cast("boolean"),
    )
    .withColumn(
        "desp_fl_tipo_despesa_informado",
        (
            col("desp_tx_tipo_despesa").isNotNull()
            & (col("desp_tx_tipo_despesa") != "")
        ).cast("boolean"),
    )
    .withColumn(
        "desp_fl_fornecedor_informado",
        (
            col("desp_tx_nome_fornecedor").isNotNull()
            & (col("desp_tx_nome_fornecedor") != "")
        ).cast("boolean"),
    )
    .withColumn(
        "desp_fl_valor_liquido_informado",
        col("desp_vl_liquido").isNotNull().cast("boolean"),
    )
    .withColumn(
        "desp_fl_registro_valido_silver",
        (
            col("desp_fl_deputado_informado")
            & col("desp_fl_periodo_valido")
            & col("desp_fl_tipo_despesa_informado")
            & col("desp_fl_fornecedor_informado")
            & col("desp_fl_valor_liquido_informado")
        ).cast("boolean"),
    )
    .withColumn(
        "desp_tx_motivo_rejeicao",
        when(~col("desp_fl_deputado_informado"), lit("DESP_ID_DEPUTADO_NULO_OU_VAZIO"))
        .when(~col("desp_fl_periodo_valido"), lit("DESP_PERIODO_INVALIDO"))
        .when(~col("desp_fl_tipo_despesa_informado"), lit("DESP_TIPO_DESPESA_NULO_OU_VAZIO"))
        .when(~col("desp_fl_fornecedor_informado"), lit("DESP_FORNECEDOR_NULO_OU_VAZIO"))
        .when(~col("desp_fl_valor_liquido_informado"), lit("DESP_VALOR_LIQUIDO_NULO"))
        .otherwise(lit(None).cast(StringType())),
    )
)

# COMMAND ----------

# ============================================================
# 8. Add Deduplication Key
# ============================================================

silver_quality_df = (
    silver_flags_df
    .withColumn(
        "desp_tx_chave_deduplicacao",
        sha2(
            concat_ws(
                "||",
                coalesce(col("dep_id_deputado"), lit("__SEM_DEP__")),
                coalesce(col("desp_nr_ano").cast("string"), lit("__SEM_ANO__")),
                coalesce(col("desp_nr_mes").cast("string"), lit("__SEM_MES__")),
                coalesce(col("desp_tx_tipo_despesa"), lit("__SEM_TIPO__")),
                coalesce(col("desp_tx_nome_fornecedor"), lit("__SEM_FORN__")),
                coalesce(col("desp_tx_cnpj_cpf_fornecedor_limpo"), lit("__SEM_DOC_FORN__")),
                coalesce(col("desp_tx_numero_documento"), lit("__SEM_DOC_DESP__")),
                coalesce(col("desp_vl_liquido").cast("string"), lit("__SEM_VALOR__")),
                coalesce(col("aud_tx_hash_registro_bronze"), lit("__SEM_HASH_BRONZE__")),
            ),
            256,
        ),
    )
)

# COMMAND ----------

# ============================================================
# 9. Build Mandatory Rejected Records
# ============================================================

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=silver_quality_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="desp_tx_chave_deduplicacao",
    validation_rule_column="desp_tx_motivo_rejeicao",
    payload_column="desp_tx_payload_json",
    valid_flag_column="desp_fl_registro_valido_silver",
)

# COMMAND ----------

# ============================================================
# 10. Keep Valid Records and Deduplicate
# ============================================================

valid_df = (
    silver_quality_df
    .filter(col("desp_fl_registro_valido_silver") == True)
    .drop("desp_tx_motivo_rejeicao")
)

dedup_window = (
    Window
    .partitionBy("desp_tx_chave_deduplicacao")
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
    record_id_column="desp_tx_chave_deduplicacao",
    payload_column="desp_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="DESP_REGISTRO_DUPLICADO_TECNICO",
    observation=(
        "Expense record kept only once by technical deduplication key. "
        "Deduplication order uses latest Bronze ingestion timestamp and Bronze hash."
    ),
)

silver_dedup_df = (
    valid_ranked_df
    .filter(col("rn_deduplicacao") == 1)
    .drop("rn_deduplicacao")
)

# COMMAND ----------

# ============================================================
# 11. Persist Rejected Records
# ============================================================

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
        f"Rejected and discarded despesas CEAP records persisted "
        f"| records_rejected={records_rejected}"
    ),
)

# COMMAND ----------

# ============================================================
# 12. Add Silver Traceability Columns
# ============================================================

silver_df = (
    silver_dedup_df
    .withColumn("aud_id_execucao_silver", lit(execution_id))
    .withColumn("aud_dh_processamento", current_timestamp())
    .withColumn("aud_tx_camada_origem", lit("bronze"))
    .withColumn("aud_tx_tabela_origem", lit(SOURCE_TABLE))
    .withColumn("aud_tx_tabela_destino", lit(TARGET_TABLE))
    .withColumn("aud_tx_versao_pipeline_silver", lit(PROJECT_VERSION))
    .withColumn(
        "aud_tx_regra_extracao_despesa",
        lit(
            "CEAP expense standardized from Bronze fields. Supplier document quality is flagged but does not reject records."
        ),
    )
)

# COMMAND ----------

# ============================================================
# 13. Add Silver Record Hash
# ============================================================

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "dep_id_deputado",
        "desp_nr_ano",
        "desp_nr_mes",
        "desp_tx_tipo_despesa",
        "desp_tx_nome_fornecedor",
        "desp_tx_cnpj_cpf_fornecedor_limpo",
        "desp_tx_numero_documento",
        "desp_vl_liquido",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# ============================================================
# 14. Select Final Silver Columns
# ============================================================

final_columns = [
    "desp_tx_chave_deduplicacao",

    "dep_id_deputado",
    "dep_tx_nome",
    "dep_tx_sigla_partido",
    "dep_tx_sigla_uf",
    "leg_id_legislatura",

    "desp_nr_ano",
    "desp_nr_ano_referencia",
    "desp_nr_mes",

    "desp_tx_tipo_despesa",
    "desp_tx_tipo_documento",
    "desp_tx_numero_documento",
    "desp_dt_data_documento_original",
    "desp_dt_data_documento",
    "desp_tx_url_documento",

    "desp_tx_nome_fornecedor",
    "desp_tx_cnpj_cpf_fornecedor",
    "desp_tx_cnpj_cpf_fornecedor_limpo",
    "desp_tx_tipo_documento_fornecedor",

    "desp_vl_documento",
    "desp_vl_glosa",
    "desp_vl_liquido",

    "desp_fl_documento_fornecedor_informado",
    "desp_fl_documento_fornecedor_repetido",
    "desp_fl_documento_fornecedor_valido_formato",
    "desp_fl_documento_fornecedor_atipico",

    "desp_fl_possui_documento_url",
    "desp_fl_possui_glosa",
    "desp_fl_valor_negativo",

    "desp_fl_deputado_informado",
    "desp_fl_periodo_valido",
    "desp_fl_tipo_despesa_informado",
    "desp_fl_fornecedor_informado",
    "desp_fl_valor_liquido_informado",
    "desp_fl_registro_valido_silver",

    "desp_tx_payload_json",

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
]

silver_df = silver_df.select(*final_columns)

# COMMAND ----------

# ============================================================
# 15. Persist Silver Table
# ============================================================

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
        f"Silver despesas CEAP table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# ============================================================
# 16. Apply Governance Comments
# ============================================================

table_comment = """
Standardized CEAP expenses table in the Silver layer.

This table contains cleaned, validated, deduplicated and analytics-ready CEAP
expense records derived from the Bronze ingestion layer.

The table supports analysis of expenses by deputy, party, state, legislature,
supplier, expense type, period, document URL coverage, glosa and negative values.

Supplier CNPJ/CPF fields are standardized and classified. Supplier document
format issues are preserved as quality flags and do not reject expense records.
"""

column_comments = {
    "desp_tx_chave_deduplicacao": "Technical deterministic deduplication key for CEAP expense records.",
    "dep_id_deputado": "Deputy identifier associated with the expense.",
    "dep_tx_nome": "Standardized parliamentary name derived from Bronze field dep_tx_nome_parlamentar.",
    "dep_tx_sigla_partido": "Deputy party acronym.",
    "dep_tx_sigla_uf": "Deputy state acronym.",
    "leg_id_legislatura": "Legislature identifier derived from Bronze field dep_nr_legislatura..",
    "desp_nr_ano": "Expense reference year.",
    "desp_nr_ano_referencia": "Reference year associated with source extraction.",
    "desp_nr_mes": "Expense reference month.",
    "desp_tx_tipo_despesa": "Standardized expense type description.",
    "desp_tx_tipo_documento": "Expense document type from source.",
    "desp_tx_numero_documento": "Expense document number from source.",
    "desp_dt_data_documento_original": "Original document date value from Bronze.",
    "desp_dt_data_documento": "Parsed document date.",
    "desp_tx_url_documento": "Expense document URL.",
    "desp_tx_nome_fornecedor": "Standardized supplier name.",
    "desp_tx_cnpj_cpf_fornecedor": "Original supplier CPF/CNPJ value.",
    "desp_tx_cnpj_cpf_fornecedor_limpo": "Supplier CPF/CNPJ containing only numeric characters.",
    "desp_tx_tipo_documento_fornecedor": "Supplier document type classification: CNPJ, CPF, OUTRO or NAO_INFORMADO.",
    "desp_vl_documento": "Document amount in BRL as received from Bronze.",
    "desp_vl_glosa": "Glosa amount in BRL as received from Bronze.",
    "desp_vl_liquido": "Net reimbursed amount in BRL as received from Bronze.",
    "desp_fl_documento_fornecedor_informado": "Flag indicating whether supplier document is informed.",
    "desp_fl_documento_fornecedor_repetido": "Flag indicating whether supplier document is composed only by repeated digits.",
    "desp_fl_documento_fornecedor_valido_formato": "Flag indicating whether supplier document has a valid CPF or CNPJ structural length and non-repeated pattern.",
    "desp_fl_documento_fornecedor_atipico": "Flag indicating whether supplier document is atypical but preserved.",
    "desp_fl_possui_documento_url": "Flag indicating whether expense has document URL.",
    "desp_fl_possui_glosa": "Flag indicating whether expense has glosa amount.",
    "desp_fl_valor_negativo": "Flag indicating whether net expense value is negative.",
    "desp_fl_deputado_informado": "Flag indicating whether deputy identifier is available.",
    "desp_fl_periodo_valido": "Flag indicating whether year and month are valid.",
    "desp_fl_tipo_despesa_informado": "Flag indicating whether expense type is informed.",
    "desp_fl_fornecedor_informado": "Flag indicating whether supplier name is informed.",
    "desp_fl_valor_liquido_informado": "Flag indicating whether net amount is informed.",
    "desp_fl_registro_valido_silver": "Flag indicating whether record passed mandatory Silver validation.",
    "desp_tx_payload_json": "Original Bronze JSON payload preserved for traceability.",
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
    "aud_tx_regra_extracao_despesa": "Textual description of CEAP expense extraction and quality rule.",
    "aud_tx_hash_registro_silver": "Deterministic Silver record hash.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# ============================================================
# 17. Final Pipeline Log
# ============================================================

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
        f"Silver despesas CEAP transformation completed successfully "
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
        f"Silver despesas CEAP transformation completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER DESPESAS CEAP COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print("Grain: one CEAP expense record")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)