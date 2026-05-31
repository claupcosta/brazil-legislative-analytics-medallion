# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Silver — Votações Standardization
# MAGIC
# MAGIC **Notebook:** `07_silver_votacoes`  
# MAGIC **Layer:** `Silver`  
# MAGIC **Source:** `brazil_legislative_analytics.bronze.br_votacoes`  
# MAGIC **Target:** `brazil_legislative_analytics.silver.slv_votacoes`
# MAGIC
# MAGIC Standardizes legislative voting session records from the Bronze layer and persists
# MAGIC validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook preserves voting metadata, derives temporal and legislature
# MAGIC attributes, extracts analytical relationships from the JSON payload when
# MAGIC available, applies data quality rules and keeps Bronze-to-Silver traceability.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read voting session records from Bronze
# MAGIC - Standardize voting identifiers, description, result and organization acronym
# MAGIC - Validate voting identifier format
# MAGIC - Convert voting registration datetime safely
# MAGIC - Derive voting date, year, month and legislature
# MAGIC - Extract event, organization, proposition and vote-count attributes from JSON payload when available
# MAGIC - Derive voting result analytical fields
# MAGIC - Validate mandatory Silver fields
# MAGIC - Register rejected and duplicate records
# MAGIC - Preserve Bronze ingestion lineage
# MAGIC - Generate Silver traceability metadata
# MAGIC - Generate deterministic Silver record hashes
# MAGIC - Persist Silver Delta table
# MAGIC - Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves raw source values
# MAGIC - Silver standardizes, validates and deduplicates records
# MAGIC - Invalid records are redirected to `slv_registros_rejeitados`
# MAGIC - Technical duplicates are also registered as rejected records
# MAGIC - JSON extraction is tolerant: unavailable fields remain null
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
    coalesce,
    get_json_object,
    to_date,
    year,
    month,
    expr,
)

from pyspark.sql.window import Window

from pyspark.sql.types import (
    StringType,
    TimestampType,
    IntegerType,
)

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("07 - SILVER VOTACOES")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "07_silver_votacoes"
LAYER_NAME = "silver"
ENTITY_NAME = "votacoes"

SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES["votacoes"]
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES["votacoes"]
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

records_read = 0
records_written = 0
records_rejected = 0

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
    message="Silver votacoes transformation started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver votacoes transformation.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze Data

# COMMAND ----------

try:

    bronze_df = spark.table(
        SOURCE_TABLE
    )

    records_read = bronze_df.count()

    log_info(
        pipeline_logger=logger,
        message=(
            f"Bronze votacoes table loaded successfully "
            f"| records_read={records_read}"
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
            f"Failed reading Bronze votacoes table "
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
        message="Failed reading Bronze votacoes table.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper Functions

# COMMAND ----------

def get_source_column(dataframe, column_name: str):
    """
    Returns a source column when it exists.
    Otherwise returns a null literal to keep the Silver schema stable.
    """

    if column_name in dataframe.columns:
        return col(column_name)

    return lit(None)


def clean_text(column_expression):
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


def clean_upper_text(column_expression):
    """
    Cleans textual column expressions and converts values to uppercase.
    """

    return upper(
        clean_text(column_expression)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize Bronze Columns

# COMMAND ----------

silver_base_df = (
    bronze_df
    .select(
        clean_text(
            get_source_column(bronze_df, "vot_id_votacao")
        ).alias("vot_id_votacao"),

        clean_text(
            get_source_column(bronze_df, "vot_tx_uri")
        ).alias("vot_tx_uri"),

        clean_text(
            get_source_column(bronze_df, "vot_tx_descricao")
        ).alias("vot_tx_descricao"),

        clean_text(
            get_source_column(bronze_df, "vot_dt_data_hora_registro")
        ).alias("vot_dt_data_hora_registro"),

        clean_upper_text(
            get_source_column(bronze_df, "vot_tx_sigla_orgao")
        ).alias("org_tx_sigla"),

        clean_upper_text(
            get_source_column(bronze_df, "vot_tx_aprovacao")
        ).alias("vot_tx_resultado"),

        clean_text(
            get_source_column(bronze_df, "vot_nr_ano_referencia")
        ).alias("vot_nr_ano_referencia_origem"),

        clean_text(
            get_source_column(bronze_df, "vot_tx_payload_json")
        ).alias("vot_tx_payload_json"),

        get_source_column(bronze_df, "aud_id_execucao")
            .cast(StringType())
            .alias("aud_id_execucao_bronze"),

        get_source_column(bronze_df, "aud_dh_ingestao")
            .cast(TimestampType())
            .alias("aud_dh_ingestao_bronze"),

        get_source_column(bronze_df, "aud_tx_endpoint_origem")
            .cast(StringType())
            .alias("aud_tx_endpoint_origem_bronze"),

        get_source_column(bronze_df, "aud_tx_sistema_origem")
            .cast(StringType())
            .alias("aud_tx_sistema_origem_bronze"),

        get_source_column(bronze_df, "aud_tx_versao_pipeline")
            .cast(StringType())
            .alias("aud_tx_versao_pipeline_bronze"),

        get_source_column(bronze_df, "aud_tx_tipo_carga")
            .cast(StringType())
            .alias("aud_tx_tipo_carga_bronze"),

        get_source_column(bronze_df, "aud_tx_arquivo_origem")
            .cast(StringType())
            .alias("aud_tx_arquivo_origem_bronze"),

        get_source_column(bronze_df, "aud_tx_hash_registro")
            .cast(StringType())
            .alias("aud_tx_hash_registro_bronze"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Normalize Datetime and Derive Temporal Fields

# COMMAND ----------

silver_datetime_df = (
    silver_base_df

    .withColumn(
        "vot_dh_votacao",
        coalesce(
            expr("try_to_timestamp(vot_dt_data_hora_registro, \"yyyy-MM-dd'T'HH:mm:ss\")"),
            expr("try_to_timestamp(vot_dt_data_hora_registro, \"yyyy-MM-dd'T'HH:mm\")"),
            expr("try_to_timestamp(vot_dt_data_hora_registro)")
        )
    )

    .withColumn(
        "vot_dt_votacao",
        to_date(col("vot_dh_votacao"))
    )

    .withColumn(
        "vot_nr_ano_referencia",
        expr("try_cast(vot_nr_ano_referencia_origem as int)")
    )

    .withColumn(
        "vot_nr_ano",
        coalesce(
            year(col("vot_dt_votacao")),
            col("vot_nr_ano_referencia")
        ).cast(IntegerType())
    )

    .withColumn(
        "vot_nr_mes",
        month(col("vot_dt_votacao")).cast(IntegerType())
    )

    .withColumn(
        "leg_id_legislatura",
        when(
            col("vot_nr_ano").between(2019, 2022),
            lit(56)
        )
        .when(
            col("vot_nr_ano").between(2023, 2026),
            lit(57)
        )
        .otherwise(lit(None).cast(IntegerType()))
    )

    .withColumn(
        "vot_fl_legislatura_identificada",
        when(
            col("leg_id_legislatura").isNotNull(),
            lit(True)
        ).otherwise(lit(False))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Extract Analytical Attributes from JSON Payload

# COMMAND ----------

silver_enriched_df = (
    silver_datetime_df

    .withColumn(
        "evt_id_evento",
        clean_text(
            get_json_object(
                col("vot_tx_payload_json"),
                "$.idEvento",
            )
        )
    )

    .withColumn(
        "evt_tx_uri",
        clean_text(
            get_json_object(
                col("vot_tx_payload_json"),
                "$.uriEvento",
            )
        )
    )

    .withColumn(
        "org_id_orgao",
        clean_text(
            get_json_object(
                col("vot_tx_payload_json"),
                "$.idOrgao",
            )
        )
    )

    .withColumn(
        "org_tx_uri",
        clean_text(
            get_json_object(
                col("vot_tx_payload_json"),
                "$.uriOrgao",
            )
        )
    )

    .withColumn(
        "prop_id_proposicao",
        clean_text(
            get_json_object(
                col("vot_tx_payload_json"),
                "$.ultimaApresentacaoProposicao_idProposicao",
            )
        )
    )

    .withColumn(
        "prop_tx_uri",
        clean_text(
            get_json_object(
                col("vot_tx_payload_json"),
                "$.ultimaApresentacaoProposicao_uriProposicao",
            )
        )
    )

    .withColumn(
        "prop_tx_descricao",
        clean_text(
            get_json_object(
                col("vot_tx_payload_json"),
                "$.ultimaApresentacaoProposicao_descricao",
            )
        )
    )

    .withColumn(
        "vot_qt_sim",
        expr("try_cast(get_json_object(vot_tx_payload_json, '$.votosSim') as int)")
    )

    .withColumn(
        "vot_qt_nao",
        expr("try_cast(get_json_object(vot_tx_payload_json, '$.votosNao') as int)")
    )

    .withColumn(
        "vot_qt_outros",
        expr("try_cast(get_json_object(vot_tx_payload_json, '$.votosOutros') as int)")
    )

    .withColumn(
        "vot_qt_total",
        (
            coalesce(col("vot_qt_sim"), lit(0)) +
            coalesce(col("vot_qt_nao"), lit(0)) +
            coalesce(col("vot_qt_outros"), lit(0))
        ).cast(IntegerType())
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Derive Voting Result Fields

# COMMAND ----------

silver_result_df = (
    silver_enriched_df

    .withColumn(
        "vot_fl_aprovada",
        when(
            col("vot_tx_resultado").isin(
                "1",
                "SIM",
                "S",
                "APROVADA",
                "APROVADO",
            ),
            lit(True),
        )
        .when(
            col("vot_tx_resultado").isin(
                "0",
                "NAO",
                "NÃO",
                "N",
                "REJEITADA",
                "REJEITADO",
            ),
            lit(False),
        )
        .otherwise(lit(None))
    )

    .withColumn(
        "vot_tx_status_aprovacao",
        when(
            col("vot_fl_aprovada") == True,
            lit("APROVADA")
        )
        .when(
            col("vot_fl_aprovada") == False,
            lit("NAO APROVADA")
        )
        .otherwise(lit("NAO INFORMADO"))
    )

    .withColumn(
        "vot_tx_resultado_curado",
        when(
            coalesce(col("vot_qt_sim"), lit(0)) >
            coalesce(col("vot_qt_nao"), lit(0)),
            lit("MAIORIA SIM")
        )
        .when(
            coalesce(col("vot_qt_nao"), lit(0)) >
            coalesce(col("vot_qt_sim"), lit(0)),
            lit("MAIORIA NAO")
        )
        .when(
            col("vot_qt_total") == 0,
            lit("SEM VOTOS CONTABILIZADOS")
        )
        .otherwise(lit("EMPATE OU INDETERMINADO"))
    )

    .withColumn(
        "vot_fl_possui_evento",
        when(
            col("evt_id_evento").isNotNull() &
            (col("evt_id_evento") != ""),
            lit(1)
        ).otherwise(lit(0))
    )

    .withColumn(
        "vot_fl_possui_orgao",
        when(
            col("org_id_orgao").isNotNull() &
            (col("org_id_orgao") != ""),
            lit(1)
        ).otherwise(lit(0))
    )

    .withColumn(
        "vot_fl_possui_proposicao",
        when(
            col("prop_id_proposicao").isNotNull() &
            (col("prop_id_proposicao") != ""),
            lit(1)
        ).otherwise(lit(0))
    )

    .withColumn(
        "vot_fl_possui_votos_contabilizados",
        when(
            col("vot_qt_total") > 0,
            lit(1)
        ).otherwise(lit(0))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Apply Silver Quality Flags

# COMMAND ----------

silver_quality_df = (
    silver_result_df

    .withColumn(
        "vot_fl_id_valido",
        (
            col("vot_id_votacao").isNotNull()
            & (col("vot_id_votacao") != "")
        ).cast("boolean"),
    )

    .withColumn(
        "vot_fl_id_formato_valido",
        (
            col("vot_id_votacao").rlike("^[0-9]+-[0-9]+$")
        ).cast("boolean"),
    )

    .withColumn(
        "vot_fl_data_valida",
        (
            col("vot_dh_votacao").isNotNull()
        ).cast("boolean"),
    )

    .withColumn(
        "vot_fl_descricao_valida",
        (
            col("vot_tx_descricao").isNotNull()
            & (col("vot_tx_descricao") != "")
        ).cast("boolean"),
    )

    .withColumn(
        "vot_fl_resultado_informado",
        (
            col("vot_tx_resultado").isNotNull()
            & (col("vot_tx_resultado") != "")
        ).cast("boolean"),
    )

    .withColumn(
        "vot_fl_orgao_sigla_informada",
        (
            col("org_tx_sigla").isNotNull()
            & (col("org_tx_sigla") != "")
        ).cast("boolean"),
    )

    .withColumn(
        "vot_fl_registro_valido_silver",
        (
            col("vot_fl_id_valido")
            & col("vot_fl_id_formato_valido")
            & col("vot_fl_data_valida")
            & col("vot_fl_descricao_valida")
        ).cast("boolean"),
    )

    .withColumn(
        "vot_tx_motivo_rejeicao",
        when(
            ~col("vot_fl_id_valido"),
            lit("VOT_ID_NULO_OU_VAZIO"),
        )
        .when(
            ~col("vot_fl_id_formato_valido"),
            lit("VOT_ID_FORMATO_INVALIDO"),
        )
        .when(
            ~col("vot_fl_data_valida"),
            lit("VOT_DATA_NULA_OU_INVALIDA"),
        )
        .when(
            ~col("vot_fl_descricao_valida"),
            lit("VOT_DESCRICAO_NULA_OU_VAZIA"),
        )
        .otherwise(
            lit(None).cast(StringType())
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Build Mandatory Rejected Records

# COMMAND ----------

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=silver_quality_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="vot_id_votacao",
    validation_rule_column="vot_tx_motivo_rejeicao",
    payload_column="vot_tx_payload_json",
    valid_flag_column="vot_fl_registro_valido_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Identify Technical Duplicates

# COMMAND ----------

valid_df = (
    silver_quality_df
    .filter(
        col("vot_fl_registro_valido_silver") == True
    )
    .drop("vot_tx_motivo_rejeicao")
)

dedup_window = (
    Window
    .partitionBy(
        "vot_id_votacao",
    )
    .orderBy(
        col("aud_dh_ingestao_bronze").desc_nulls_last()
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
    record_id_column="vot_id_votacao",
    payload_column="vot_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="VOT_REGISTRO_DUPLICADO_TECNICO",
    observation=(
        "Record kept only once by vot_id_votacao. "
        "Deduplication order uses latest Bronze ingestion timestamp."
    ),
)

silver_dedup_df = (
    valid_ranked_df
    .filter(
        col("rn_deduplicacao") == 1
    )
    .drop("rn_deduplicacao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Persist Rejected and Discarded Records

# COMMAND ----------

rejected_df = union_rejected_records(
    mandatory_rejected_dataframe=mandatory_rejected_df,
    duplicate_rejected_dataframe=duplicate_rejected_df,
)

records_rejected = rejected_df.count()

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
            "Rejected and discarded votacoes records persisted "
            f"| rejected_table={REJECTED_TABLE} "
            f"| records_rejected={records_rejected}"
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
            f"Failed writing rejected votacoes records "
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
        message="Failed writing rejected votacoes records.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Add Silver Traceability Columns

# COMMAND ----------

silver_df = (
    silver_dedup_df

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

    .withColumn(
        "aud_tx_regra_extracao_votacao",
        lit(
            "Voting attributes standardized from Bronze columns and enriched from vot_tx_payload_json when available."
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Add Silver Record Hash

# COMMAND ----------

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "vot_id_votacao",
        "vot_dh_votacao",
        "vot_tx_descricao",
        "vot_tx_resultado",
        "org_tx_sigla",
        "vot_nr_ano_referencia",
        "evt_id_evento",
        "org_id_orgao",
        "prop_id_proposicao",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Select Final Silver Columns

# COMMAND ----------

final_columns = [

    "vot_id_votacao",
    "vot_tx_uri",
    "vot_tx_descricao",

    "vot_dt_data_hora_registro",
    "vot_dh_votacao",
    "vot_dt_votacao",
    "vot_nr_ano",
    "vot_nr_mes",
    "vot_nr_ano_referencia",
    "leg_id_legislatura",

    "org_tx_sigla",
    "vot_tx_resultado",
    "vot_fl_aprovada",
    "vot_tx_status_aprovacao",
    "vot_tx_resultado_curado",

    "vot_qt_sim",
    "vot_qt_nao",
    "vot_qt_outros",
    "vot_qt_total",

    "evt_id_evento",
    "evt_tx_uri",
    "org_id_orgao",
    "org_tx_uri",
    "prop_id_proposicao",
    "prop_tx_uri",
    "prop_tx_descricao",

    "vot_fl_possui_evento",
    "vot_fl_possui_orgao",
    "vot_fl_possui_proposicao",
    "vot_fl_possui_votos_contabilizados",

    "vot_fl_id_valido",
    "vot_fl_id_formato_valido",
    "vot_fl_data_valida",
    "vot_fl_descricao_valida",
    "vot_fl_resultado_informado",
    "vot_fl_orgao_sigla_informada",
    "vot_fl_legislatura_identificada",
    "vot_fl_registro_valido_silver",

    "vot_tx_payload_json",

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
    "aud_tx_regra_extracao_votacao",
    "aud_tx_hash_registro_silver",
]

silver_df = silver_df.select(
    *final_columns
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Persist Silver Table

# COMMAND ----------

try:

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
            f"Silver votacoes table persisted successfully "
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
        target_table=TARGET_TABLE,
        status=EXECUTION_STATUS_FAILED,
        message=(
            f"Failed writing Silver votacoes table "
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
        message="Failed writing Silver votacoes table.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Apply Governance Comments

# COMMAND ----------

table_comment = """
Standardized legislative voting table in the Silver layer.

This table contains cleaned, typed, deduplicated and analytics-ready voting records
derived from the Bronze voting ingestion table.

The table preserves voting metadata, derives temporal and legislature attributes,
extracts event, organization, proposition and vote-count attributes from the JSON
payload when available, and preserves Bronze-to-Silver traceability.
"""

column_comments = {
    "vot_id_votacao": "Voting session identifier as provided by the source system.",
    "vot_tx_uri": "Voting session source URI.",
    "vot_tx_descricao": "Standardized voting session description.",

    "vot_dt_data_hora_registro": "Original voting registration datetime string from the Bronze layer.",
    "vot_dh_votacao": "Voting timestamp safely converted from the Bronze registration datetime field.",
    "vot_dt_votacao": "Voting date derived from the voting timestamp.",
    "vot_nr_ano": "Voting year derived from voting date or reference year.",
    "vot_nr_mes": "Voting month derived from voting date.",
    "vot_nr_ano_referencia": "Reference year extracted from the source file or Bronze metadata.",
    "leg_id_legislatura": "Derived legislature identifier based on voting year.",

    "org_tx_sigla": "Legislative body acronym related to the voting session.",
    "vot_tx_resultado": "Original voting approval or result information standardized as text.",
    "vot_fl_aprovada": "Flag indicating whether the voting session was approved based on the source result value.",
    "vot_tx_status_aprovacao": "Curated textual approval status.",
    "vot_tx_resultado_curado": "Curated voting result based on vote counts when available.",

    "vot_qt_sim": "Number of yes votes extracted from JSON payload when available.",
    "vot_qt_nao": "Number of no votes extracted from JSON payload when available.",
    "vot_qt_outros": "Number of other votes extracted from JSON payload when available.",
    "vot_qt_total": "Total number of votes calculated from yes, no and other votes.",

    "evt_id_evento": "Event identifier associated with the voting session when available in the JSON payload.",
    "evt_tx_uri": "Event URI associated with the voting session when available in the JSON payload.",
    "org_id_orgao": "Legislative body identifier associated with the voting session when available in the JSON payload.",
    "org_tx_uri": "Legislative body URI associated with the voting session when available in the JSON payload.",
    "prop_id_proposicao": "Proposition identifier associated with the voting session when available in the JSON payload.",
    "prop_tx_uri": "Proposition URI associated with the voting session when available in the JSON payload.",
    "prop_tx_descricao": "Proposition description associated with the voting session when available in the JSON payload.",

    "vot_fl_possui_evento": "Flag indicating whether the voting session has an associated event.",
    "vot_fl_possui_orgao": "Flag indicating whether the voting session has an associated organization identifier.",
    "vot_fl_possui_proposicao": "Flag indicating whether the voting session has an associated proposition.",
    "vot_fl_possui_votos_contabilizados": "Flag indicating whether the voting session has vote counts available.",

    "vot_fl_id_valido": "Flag indicating whether the voting identifier is present.",
    "vot_fl_id_formato_valido": "Flag indicating whether the voting identifier follows the expected numeric pattern.",
    "vot_fl_data_valida": "Flag indicating whether the voting timestamp is available and valid.",
    "vot_fl_descricao_valida": "Flag indicating whether voting descriptive information is available.",
    "vot_fl_resultado_informado": "Flag indicating whether the voting result is available.",
    "vot_fl_orgao_sigla_informada": "Flag indicating whether the voting legislative body acronym is available.",
    "vot_fl_legislatura_identificada": "Flag indicating whether the voting legislature was derived from the voting year.",
    "vot_fl_registro_valido_silver": "Flag indicating whether the record passed mandatory Silver validation rules.",

    "vot_tx_payload_json": "Original Bronze JSON payload preserved for traceability.",

    "aud_id_execucao_bronze": "Execution identifier from the Bronze ingestion process.",
    "aud_dh_ingestao_bronze": "Bronze ingestion timestamp.",
    "aud_tx_endpoint_origem_bronze": "Source endpoint or volume path used during Bronze ingestion.",
    "aud_tx_sistema_origem_bronze": "Original source system identified in Bronze ingestion.",
    "aud_tx_versao_pipeline_bronze": "Pipeline version used during Bronze ingestion.",
    "aud_tx_tipo_carga_bronze": "Load type applied during Bronze ingestion.",
    "aud_tx_arquivo_origem_bronze": "Original source file path captured during Bronze CSV ingestion.",
    "aud_tx_hash_registro_bronze": "Deterministic record hash generated in the Bronze layer.",

    "aud_id_execucao_silver": "Execution identifier for the Silver transformation process.",
    "aud_dh_processamento": "Timestamp when the record was processed in the Silver layer.",
    "aud_tx_camada_origem": "Source Medallion layer used as input for this Silver table.",
    "aud_tx_tabela_origem": "Fully qualified source table used as input for this Silver table.",
    "aud_tx_tabela_destino": "Fully qualified target Silver table.",
    "aud_tx_versao_pipeline_silver": "Pipeline version used during Silver transformation.",
    "aud_tx_regra_extracao_votacao": "Textual description of the voting extraction and enrichment rule.",
    "aud_tx_hash_registro_silver": "Deterministic record hash generated in the Silver layer.",
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
        f"Silver votacoes transformation completed successfully "
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
        f"Silver votacoes transformation completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER VOTACOES COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)