# Databricks notebook source
# MAGIC %md
# MAGIC # 12 Silver — CPIs Standardization
# MAGIC
# MAGIC **Notebook:** `12_silver_cpis`
# MAGIC
# MAGIC Derives Parliamentary Inquiry Commission records from standardized legislative
# MAGIC bodies and persists validated, deduplicated and analytics-ready CPI records into
# MAGIC the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - CPI semantic derivation rules from `slv_orgaos`
# MAGIC - CPI and CPMI classification logic
# MAGIC - CPI identifier standardization logic
# MAGIC - CPI analytical status derivation
# MAGIC - CPI active/inactive flag derivation
# MAGIC - Legislature derivation from CPI start year
# MAGIC - Date normalization rules
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
# MAGIC - Read standardized legislative body data from Silver layer
# MAGIC - Identify CPI and CPMI records from legislative body type
# MAGIC - Standardize CPI identifiers, names, acronym, dates and type attributes
# MAGIC - Derive CPI analytical type as `CPI` or `CPMI`
# MAGIC - Derive mixed commission flag from CPI type
# MAGIC - Derive analytical status and active flag
# MAGIC - Derive legislature identifier from CPI start date
# MAGIC - Normalize textual fields
# MAGIC - Validate mandatory CPI fields
# MAGIC - Remove technical duplicate records
# MAGIC - Preserve Silver legislative body lineage
# MAGIC - Preserve Bronze ingestion lineage inherited from `slv_orgaos`
# MAGIC - Register rejected and discarded records for traceability
# MAGIC - Persist curated Delta table
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - CPIs are derived entities and do not require a dedicated Bronze table
# MAGIC - Source data comes from `silver.slv_orgaos`, already standardized and validated
# MAGIC - CPI records are identified by legislative body type containing inquiry commission semantics
# MAGIC - `COMISSÃO PARLAMENTAR DE INQUÉRITO` is classified as `CPI`
# MAGIC - `COMISSÃO PARLAMENTAR MISTA DE INQUÉRITO` is classified as `CPMI`
# MAGIC - Legislature is derived from the CPI start year when available
# MAGIC - Records without identified legislature are preserved with quality flags
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

# MAGIC %run ../99_utils/utils_text

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
    coalesce,
    to_json,
    struct,
)

from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("12 - SILVER CPIS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "12_silver_cpis"

LAYER_NAME = "silver"

ENTITY_NAME = "cpis"

SOURCE_TABLE = get_silver_table(
    SILVER_TABLES["orgaos"]
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES["cpis"]
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

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver CPIs derivation started from standardized legislative bodies.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver CPIs derivation from slv_orgaos.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Silver Legislative Bodies Table

# COMMAND ----------

source_df = spark.table(
    SOURCE_TABLE
)

records_read = source_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Silver orgaos table loaded successfully "
        f"| records_read={records_read}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helper Functions

# COMMAND ----------

def clean_text(column_name):
    """
    Cleans textual columns by trimming and normalizing repeated spaces.
    """

    return (
        when(
            col(column_name).isNull(),
            lit(None).cast(StringType()),
        )
        .otherwise(
            trim(
                F.regexp_replace(
                    col(column_name).cast("string"),
                    r"\s+",
                    " ",
                )
            )
        )
    )


# COMMAND ----------

def clean_upper_text(column_name):
    """
    Cleans textual columns and converts values to uppercase.
    """

    return upper(
        clean_text(
            column_name
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Filter CPI and CPMI Legislative Bodies

# COMMAND ----------

cpi_source_df = (
    source_df
    .filter(
        upper(col("org_tx_tipo_orgao")).like("%INQUÉRITO%")
    )
)

records_cpi_candidates = cpi_source_df.count()

log_info(
    pipeline_logger=logger,
    message=(
        f"CPI candidate records identified "
        f"| records_cpi_candidates={records_cpi_candidates}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize CPI Base Attributes

# COMMAND ----------

cpis_base_df = (
    cpi_source_df
    .select(

        clean_text("org_id_orgao")
            .alias("cpi_id_orgao"),

        clean_upper_text("org_tx_sigla")
            .alias("cpi_tx_sigla"),

        clean_upper_text("org_tx_nome")
            .alias("cpi_tx_nome"),

        clean_upper_text("org_tx_apelido")
            .alias("cpi_tx_apelido"),

        clean_upper_text("org_tx_tipo_orgao")
            .alias("cpi_tx_tipo_orgao"),

        clean_upper_text("org_tx_situacao")
            .alias("cpi_tx_situacao_origem"),

        clean_text("org_dt_inicio")
            .alias("cpi_dt_inicio_origem"),

        clean_text("org_dt_fim")
            .alias("cpi_dt_fim_origem"),

        clean_text("org_tx_uri")
            .alias("cpi_tx_uri"),

        col("org_fl_registro_valido_silver")
            .alias("cpi_fl_orgao_valido_origem"),

        col("aud_id_execucao_bronze")
            .alias("aud_id_execucao_bronze"),

        col("aud_dh_ingestao_bronze")
            .alias("aud_dh_ingestao_bronze"),

        col("aud_tx_endpoint_origem_bronze")
            .alias("aud_tx_endpoint_origem_bronze"),

        col("aud_tx_sistema_origem_bronze")
            .alias("aud_tx_sistema_origem_bronze"),

        col("aud_tx_versao_pipeline_bronze")
            .alias("aud_tx_versao_pipeline_bronze"),

        col("aud_tx_tipo_carga_bronze")
            .alias("aud_tx_tipo_carga_bronze"),

        col("aud_tx_hash_registro_bronze")
            .alias("aud_tx_hash_registro_bronze"),

        col("aud_id_execucao_silver")
            .alias("aud_id_execucao_orgaos_silver"),

        col("aud_dh_processamento")
            .alias("aud_dh_processamento_orgaos_silver"),

        col("aud_tx_hash_registro_silver")
            .alias("aud_tx_hash_registro_orgaos_silver"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Apply CPI Semantic Derivation Rules

# COMMAND ----------

cpis_semantic_df = (
    cpis_base_df

    .withColumn(
        "cpi_dt_inicio",
        to_date(
            col("cpi_dt_inicio_origem")
        )
    )

    .withColumn(
        "cpi_dt_fim",
        to_date(
            col("cpi_dt_fim_origem")
        )
    )

    .withColumn(
        "cpi_nr_ano_inicio",
        year(
            col("cpi_dt_inicio")
        ).cast(IntegerType())
    )

    .withColumn(
        "cpi_tx_tipo",
        when(
            col("cpi_tx_tipo_orgao").like("%MISTA%"),
            lit("CPMI")
        )
        .otherwise(lit("CPI"))
    )

    .withColumn(
        "cpi_tx_tipo_descricao",
        when(
            col("cpi_tx_tipo") == "CPMI",
            lit("Comissão Parlamentar Mista de Inquérito")
        )
        .otherwise(
            lit("Comissão Parlamentar de Inquérito")
        )
    )

    .withColumn(
        "cpi_fl_mista",
        when(
            col("cpi_tx_tipo") == "CPMI",
            lit(True)
        )
        .otherwise(lit(False))
    )

    .withColumn(
        "cpi_tx_abrangencia",
        when(
            col("cpi_fl_mista") == True,
            lit("CONGRESSO NACIONAL")
        )
        .otherwise(
            lit("CÂMARA DOS DEPUTADOS")
        )
    )

    .withColumn(
        "cpi_fl_data_inicio_informada",
        when(
            col("cpi_dt_inicio").isNotNull(),
            lit(True)
        )
        .otherwise(lit(False))
    )

    .withColumn(
        "cpi_fl_data_fim_informada",
        when(
            col("cpi_dt_fim").isNotNull(),
            lit(True)
        )
        .otherwise(lit(False))
    )

    .withColumn(
        "cpi_tx_status_analitico",
        when(
            col("cpi_dt_fim").isNotNull(),
            lit("ENCERRADA_COM_DATA_FIM")
        )
        .when(
            col("cpi_tx_situacao_origem").isin(
                "EXTINTA",
                "DESCONSTITUIDA",
                "DESCONSTITUÍDA",
            ),
            lit("ENCERRADA_POR_STATUS")
        )
        .when(
            col("cpi_tx_situacao_origem").isin(
                "CRIADA",
                "CONSTITUÍDA",
                "CONSTITUIDA",
                "INSTALADA",
                "PRONTA PARA CRIAÇÃO",
            )
            & col("cpi_dt_fim").isNull(),
            lit("ATIVA_OU_EM_INSTALACAO")
        )
        .otherwise(
            lit("STATUS_INDEFINIDO")
        )
    )

    .withColumn(
        "cpi_fl_ativa",
        when(
            col("cpi_tx_status_analitico") == "ATIVA_OU_EM_INSTALACAO",
            lit(True)
        )
        .otherwise(lit(False))
    )

    .withColumn(
        "leg_id_legislatura",
        when(
            col("cpi_nr_ano_inicio").between(2019, 2022),
            lit(56)
        )
        .when(
            col("cpi_nr_ano_inicio").between(2023, 2026),
            lit(57)
        )
        .otherwise(
            lit(None).cast(IntegerType())
        )
    )

    .withColumn(
        "cpi_fl_legislatura_identificada",
        when(
            col("leg_id_legislatura").isNotNull(),
            lit(True)
        )
        .otherwise(lit(False))
    )

    .withColumn(
        "cpi_tx_payload_origem_json",
        to_json(
            struct(
                col("cpi_id_orgao"),
                col("cpi_tx_sigla"),
                col("cpi_tx_nome"),
                col("cpi_tx_apelido"),
                col("cpi_tx_tipo_orgao"),
                col("cpi_tx_situacao_origem"),
                col("cpi_dt_inicio_origem"),
                col("cpi_dt_fim_origem"),
                col("cpi_tx_uri"),
                col("aud_id_execucao_bronze"),
                col("aud_id_execucao_orgaos_silver"),
            )
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply Quality Rules

# COMMAND ----------

cpis_quality_df = (
    cpis_semantic_df

    .withColumn(
        "cpi_fl_id_valido",
        (
            col("cpi_id_orgao").isNotNull()
            & (trim(col("cpi_id_orgao")) != "")
        )
    )

    .withColumn(
        "cpi_fl_nome_informado",
        (
            col("cpi_tx_nome").isNotNull()
            & (trim(col("cpi_tx_nome")) != "")
        )
    )

    .withColumn(
        "cpi_fl_tipo_cpi_valido",
        col("cpi_tx_tipo_orgao").isin(
            "COMISSÃO PARLAMENTAR DE INQUÉRITO",
            "COMISSÃO PARLAMENTAR MISTA DE INQUÉRITO",
        )
    )

    .withColumn(
        "cpi_fl_periodo_valido",
        when(
            col("cpi_dt_inicio").isNotNull()
            & col("cpi_dt_fim").isNotNull()
            & (col("cpi_dt_inicio") > col("cpi_dt_fim")),
            lit(False)
        )
        .otherwise(lit(True))
    )

    .withColumn(
        "cpi_fl_registro_valido_silver",
        (
            col("cpi_fl_id_valido")
            & col("cpi_fl_nome_informado")
            & col("cpi_fl_tipo_cpi_valido")
            & col("cpi_fl_periodo_valido")
        )
    )

    .withColumn(
        "cpi_tx_motivo_rejeicao",
        when(
            ~col("cpi_fl_id_valido"),
            lit("CPI_ID_NULO_OU_VAZIO")
        )
        .when(
            ~col("cpi_fl_nome_informado"),
            lit("CPI_NOME_NULO_OU_VAZIO")
        )
        .when(
            ~col("cpi_fl_tipo_cpi_valido"),
            lit("CPI_TIPO_FORA_ESCOPO")
        )
        .when(
            ~col("cpi_fl_periodo_valido"),
            lit("CPI_PERIODO_INVALIDO")
        )
        .otherwise(
            lit(None).cast(StringType())
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Build Mandatory Rejected Records

# COMMAND ----------

mandatory_rejected_source_df = (
    cpis_quality_df
    .filter(
        col("cpi_fl_registro_valido_silver") == False
    )
)

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=mandatory_rejected_source_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="cpi_id_orgao",
    validation_rule_column="cpi_tx_motivo_rejeicao",
    payload_column="cpi_tx_payload_origem_json",
    valid_flag_column="cpi_fl_registro_valido_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Keep Valid CPI Records

# COMMAND ----------

valid_df = (
    cpis_quality_df
    .filter(
        col("cpi_fl_registro_valido_silver") == True
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Deduplicate CPI Records

# COMMAND ----------

dedup_window = (
    Window
    .partitionBy("cpi_id_orgao")
    .orderBy(
        col("aud_dh_processamento_orgaos_silver").desc_nulls_last(),
        col("aud_dh_ingestao_bronze").desc_nulls_last(),
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
    record_id_column="cpi_id_orgao",
    payload_column="cpi_tx_payload_origem_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="CPI_REGISTRO_DUPLICADO",
    observation=(
        "Duplicate CPI records removed keeping latest standardized legislative body record."
    ),
)

silver_df = (
    dedup_df
    .filter(
        col("rn_deduplicacao") == 1
    )
    .drop("rn_deduplicacao")
    .drop("cpi_tx_motivo_rejeicao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Persist Rejected Records

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
# MAGIC ## 11. Add Silver Traceability Columns

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
        lit("silver")
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
        "aud_tx_regra_derivacao",
        lit("Derived from slv_orgaos where legislative body type represents CPI or CPMI.")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Add Silver Hash

# COMMAND ----------

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "cpi_id_orgao",
        "cpi_tx_sigla",
        "cpi_tx_nome",
        "cpi_tx_tipo",
        "cpi_dt_inicio",
        "cpi_dt_fim",
        "cpi_tx_status_analitico",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Select Final Columns

# COMMAND ----------

final_columns = [

    "cpi_id_orgao",
    "cpi_tx_sigla",
    "cpi_tx_nome",
    "cpi_tx_apelido",
    "cpi_tx_tipo",
    "cpi_tx_tipo_descricao",
    "cpi_tx_tipo_orgao",
    "cpi_tx_abrangencia",
    "cpi_tx_situacao_origem",
    "cpi_tx_status_analitico",
    "cpi_dt_inicio_origem",
    "cpi_dt_fim_origem",
    "cpi_dt_inicio",
    "cpi_dt_fim",
    "cpi_nr_ano_inicio",
    "leg_id_legislatura",
    "cpi_tx_uri",
    "cpi_tx_payload_origem_json",

    "cpi_fl_mista",
    "cpi_fl_ativa",
    "cpi_fl_data_inicio_informada",
    "cpi_fl_data_fim_informada",
    "cpi_fl_legislatura_identificada",
    "cpi_fl_id_valido",
    "cpi_fl_nome_informado",
    "cpi_fl_tipo_cpi_valido",
    "cpi_fl_periodo_valido",
    "cpi_fl_registro_valido_silver",
    "cpi_fl_orgao_valido_origem",

    "aud_id_execucao_bronze",
    "aud_dh_ingestao_bronze",
    "aud_tx_endpoint_origem_bronze",
    "aud_tx_sistema_origem_bronze",
    "aud_tx_versao_pipeline_bronze",
    "aud_tx_tipo_carga_bronze",
    "aud_tx_hash_registro_bronze",

    "aud_id_execucao_orgaos_silver",
    "aud_dh_processamento_orgaos_silver",
    "aud_tx_hash_registro_orgaos_silver",

    "aud_id_execucao_silver",
    "aud_dh_processamento",
    "aud_tx_camada_origem",
    "aud_tx_tabela_origem",
    "aud_tx_tabela_destino",
    "aud_tx_versao_pipeline_silver",
    "aud_tx_regra_derivacao",
    "aud_tx_hash_registro_silver",
]

silver_df = silver_df.select(
    *final_columns
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Persist Silver Table

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS {TARGET_TABLE}
""")

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

records_written = spark.table(
    TARGET_TABLE
).count()

log_info(
    pipeline_logger=logger,
    message=(
        f"Silver CPIs table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Apply Governance Comments

# COMMAND ----------

table_comment = """
Standardized Parliamentary Inquiry Commissions table in the Silver layer.

This table contains validated, standardized and deduplicated CPI and CPMI records
derived from standardized legislative body records.

The table supports CPI audit analytics, Gold dimensions, CPI event facts and
analytical marts related to parliamentary inquiry commissions.

This is a derived Silver entity. CPIs are identified from legislative body type
semantics in slv_orgaos, preserving both Bronze lineage and Silver source lineage.
"""

column_comments = {

    "cpi_id_orgao":
        "CPI legislative body identifier inherited from slv_orgaos.",

    "cpi_tx_sigla":
        "Standardized CPI acronym.",

    "cpi_tx_nome":
        "Standardized CPI name.",

    "cpi_tx_apelido":
        "Standardized CPI nickname.",

    "cpi_tx_tipo":
        "Analytical CPI type. Values: CPI or CPMI.",

    "cpi_tx_tipo_descricao":
        "Analytical CPI type description.",

    "cpi_tx_tipo_orgao":
        "Original standardized legislative body type used to identify CPI records.",

    "cpi_tx_abrangencia":
        "Analytical scope of the inquiry commission.",

    "cpi_tx_situacao_origem":
        "Original legislative body status inherited from slv_orgaos.",

    "cpi_tx_status_analitico":
        "Derived analytical CPI status.",

    "cpi_dt_inicio_origem":
        "Original CPI start date value inherited from slv_orgaos.",

    "cpi_dt_fim_origem":
        "Original CPI end date value inherited from slv_orgaos.",

    "cpi_dt_inicio":
        "Standardized CPI start date.",

    "cpi_dt_fim":
        "Standardized CPI end date.",

    "cpi_nr_ano_inicio":
        "Year extracted from standardized CPI start date.",

    "leg_id_legislatura":
        "Derived legislature identifier based on CPI start year.",

    "cpi_tx_uri":
        "CPI source URI inherited from slv_orgaos.",

    "cpi_tx_payload_origem_json":
        "JSON payload preserving source fields used for CPI derivation.",

    "cpi_fl_mista":
        "Flag indicating whether the CPI is a mixed parliamentary inquiry commission.",

    "cpi_fl_ativa":
        "Flag indicating whether the CPI is analytically active or in installation.",

    "cpi_fl_data_inicio_informada":
        "Flag indicating whether CPI start date is available.",

    "cpi_fl_data_fim_informada":
        "Flag indicating whether CPI end date is available.",

    "cpi_fl_legislatura_identificada":
        "Flag indicating whether legislature was derived from CPI start year.",

    "cpi_fl_id_valido":
        "Flag indicating whether CPI identifier is valid.",

    "cpi_fl_nome_informado":
        "Flag indicating whether CPI name is informed.",

    "cpi_fl_tipo_cpi_valido":
        "Flag indicating whether legislative body type is valid for CPI derivation.",

    "cpi_fl_periodo_valido":
        "Flag indicating whether CPI start and end dates form a valid period.",

    "cpi_fl_registro_valido_silver":
        "Flag indicating whether CPI record passed Silver validation.",

    "cpi_fl_orgao_valido_origem":
        "Flag inherited from source legislative body Silver validation.",

    "aud_id_execucao_silver":
        "Unique execution identifier for this CPI Silver run.",

    "aud_dh_processamento":
        "Timestamp when CPI record was processed in this Silver notebook.",

    "aud_tx_regra_derivacao":
        "Textual description of the CPI derivation rule.",

    "aud_tx_hash_registro_silver":
        "Deterministic Silver hash used for CPI traceability.",
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
        f"Silver CPIs derivation completed successfully "
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
        f"Silver CPIs derivation completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("SILVER CPIS COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"Records Read: {records_read}")
print(f"CPI Candidates: {records_cpi_candidates}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)