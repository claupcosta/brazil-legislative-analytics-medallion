# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Bronze Layer — Parliamentary Front Members CSV Fallback Ingestion
# MAGIC
# MAGIC **Notebook:** `02b_bronze_frentes_membros_csv_fallback`  
# MAGIC **Layer:** `Bronze`  
# MAGIC **Source/File:** `frentesDeputados.csv`  
# MAGIC **Target:** `brazil_legislative_analytics.bronze.br_frentes_membros`
# MAGIC
# MAGIC Loads parliamentary front member records from official Câmara dos Deputados CSV fallback files
# MAGIC and persists them into the Bronze layer.
# MAGIC
# MAGIC This notebook creates the Bronze relationship table between parliamentary fronts and deputies,
# MAGIC supporting the analytical construction of the Parliamentary Fronts Atlas.
# MAGIC
# MAGIC This notebook preserves raw source extraction fidelity,
# MAGIC including ingestion metadata, original payloads and traceability fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read parliamentary front member CSV fallback files
# MAGIC - Preserve the relationship between parliamentary fronts and deputies
# MAGIC - Preserve deputy role within each parliamentary front
# MAGIC - Preserve legislature and membership period information
# MAGIC - Generate ingestion traceability metadata
# MAGIC - Generate deterministic record hashes
# MAGIC - Persist Bronze Delta tables
# MAGIC - Register operational execution logs
# MAGIC - Apply governance comments to tables and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze preserves source-system extraction fidelity
# MAGIC - Technical duplicates and business validation are handled in Silver
# MAGIC - CSV fallback replaces direct API ingestion when `/frentes/{id}/membros` is unstable
# MAGIC - This table supports the mandatory Parliamentary Fronts Atlas deliverable
# MAGIC - Original source payloads are preserved for auditability
# MAGIC - Governance comments are applied to tables and columns
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/decisions/api_limitations.md`
# MAGIC - `/docs/governance/data_lineage.md`
# MAGIC - `/docs/governance/data_quality_rules.md`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_hash

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_legislature

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    to_json,
    struct,
)

from pyspark.sql.types import StringType

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("02B - BRONZE FRENTES MEMBROS CSV FALLBACK")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

NOTEBOOK_NAME = "02b_bronze_frentes_membros_csv_fallback"
LAYER_NAME = "bronze"
ENTITY_NAME = "frentes_membros"

SOURCE_FILE_PATH = f"{VOLUME_RAW_FILES}/frentes_membros"

TARGET_TABLE = get_bronze_table(
    BRONZE_TABLES["frentes_membros"]
)

LOAD_TYPE = LOAD_TYPE_FALLBACK

execution_id = str(uuid.uuid4())
started_at = datetime.now()

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

# COMMAND ----------

CSV_SEPARATOR = ";"
CSV_ENCODING = "UTF-8"

APPLY_GOVERNANCE_COMMENTS = True

# COMMAND ----------

def get_source_column(dataframe, column_name: str):
    """
    Returns a source column when it exists.
    Handles CSV column names containing dots by escaping them with backticks.
    Otherwise returns a null literal to keep the target schema stable.
    """

    if column_name in dataframe.columns:
        return col(f"`{column_name}`")

    return lit(None)

def apply_table_and_column_comments(
    target_table: str,
    table_comment: str,
    column_comments: dict,
) -> None:
    """
    Applies table and column comments for Unity Catalog governance documentation.
    """

    escaped_table_comment = table_comment.replace("'", "''")

    spark.sql(f"""
    COMMENT ON TABLE {target_table}
    IS '{escaped_table_comment}'
    """)

    for column_name, column_comment in column_comments.items():

        escaped_column_comment = column_comment.replace("'", "''")

        spark.sql(f"""
        ALTER TABLE {target_table}
        ALTER COLUMN {column_name}
        COMMENT '{escaped_column_comment}'
        """)

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
    message=(
        f"Bronze frentes membros CSV fallback ingestion started "
        f"| source_path={SOURCE_FILE_PATH}"
    ),
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message=(
        f"Starting frentes membros CSV fallback ingestion "
        f"| source_path={SOURCE_FILE_PATH}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read CSV Files

# COMMAND ----------

try:

    source_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", CSV_SEPARATOR)
        .option("encoding", CSV_ENCODING)
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .load(f"{SOURCE_FILE_PATH}/*.csv")
        .withColumn(
            "aud_tx_arquivo_origem",
            col("_metadata.file_path")
        )
    )

    log_info(
        pipeline_logger=logger,
        message="Frentes membros CSV file loaded successfully.",
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
            f"Failed reading frentes membros CSV files "
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
        message="Failed reading frentes membros CSV files.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Bronze Columns

# COMMAND ----------

source_with_payload_df = (
    source_df
    .withColumn(
        "frm_tx_payload_json",
        to_json(struct("*"))
    )
)

bronze_df = (
    source_with_payload_df
    .select(
        get_source_column(source_with_payload_df, "id")
            .cast(StringType())
            .alias("frm_id_frente"),

        get_source_column(source_with_payload_df, "uri")
            .cast(StringType())
            .alias("frm_tx_uri_frente"),

        get_source_column(source_with_payload_df, "titulo")
            .cast(StringType())
            .alias("frm_tx_titulo_frente"),

        get_source_column(source_with_payload_df, "deputado_.id")
            .cast(StringType())
            .alias("dep_id_deputado"),

        get_source_column(source_with_payload_df, "deputado_.uri")
            .cast(StringType())
            .alias("dep_tx_uri"),

        get_source_column(source_with_payload_df, "deputado_.nome")
            .cast(StringType())
            .alias("dep_tx_nome"),

        get_source_column(source_with_payload_df, "deputado_.siglaPartido")
            .cast(StringType())
            .alias("dep_tx_sigla_partido"),

        get_source_column(source_with_payload_df, "deputado_.uriPartido")
            .cast(StringType())
            .alias("dep_tx_uri_partido"),

        get_source_column(source_with_payload_df, "deputado_.siglaUf")
            .cast(StringType())
            .alias("dep_tx_sigla_uf"),

        get_source_column(source_with_payload_df, "deputado_.idLegislatura")
            .cast(StringType())
            .alias("dep_id_legislatura"),

        get_source_column(source_with_payload_df, "deputado_.urlFoto")
            .cast(StringType())
            .alias("dep_tx_url_foto"),

        get_source_column(source_with_payload_df, "deputado_.codTitulo")
            .cast(StringType())
            .alias("frm_cd_titulo_membro"),

        get_source_column(source_with_payload_df, "deputado_.titulo")
            .cast(StringType())
            .alias("frm_tx_titulo_membro"),

        get_source_column(source_with_payload_df, "dataInicio")
            .cast(StringType())
            .alias("frm_dt_inicio"),

        get_source_column(source_with_payload_df, "dataFim")
            .cast(StringType())
            .alias("frm_dt_fim"),

        col("frm_tx_payload_json")
            .cast(StringType())
            .alias("frm_tx_payload_json"),

        col("aud_tx_arquivo_origem")
            .cast(StringType())
            .alias("aud_tx_arquivo_origem"),
    )
    .withColumn("aud_id_execucao", lit(execution_id))
    .withColumn("aud_dh_ingestao", current_timestamp())
    .withColumn("aud_tx_endpoint_origem", lit("csv_fallback/frentesDeputados.csv"))
    .withColumn("aud_tx_sistema_origem", lit("camara_csv"))
    .withColumn("aud_tx_versao_pipeline", lit(PROJECT_VERSION))
    .withColumn("aud_tx_tipo_carga", lit(LOAD_TYPE))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Add Record Hash

# COMMAND ----------

bronze_df = add_hash(
    dataframe=bronze_df,
    columns=[
        "frm_id_frente",
        "dep_id_deputado",
        "dep_tx_sigla_partido",
        "dep_tx_sigla_uf",
        "dep_id_legislatura",
        "frm_tx_titulo_membro",
        "frm_tx_payload_json",
    ],
    hash_column="aud_tx_hash_registro",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persist Bronze Table

# COMMAND ----------

records_read = bronze_df.count()

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

records_written = records_read

log_info(
    pipeline_logger=logger,
    message=(
        f"Bronze frentes membros CSV fallback table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply Table and Column Comments

# COMMAND ----------

table_comment = """
Raw parliamentary front members ingestion table from official Câmara dos Deputados CSV fallback files.

This Bronze table preserves the relationship between parliamentary fronts and deputies loaded from frentesDeputados.csv.

Main characteristics:
- raw ingestion fidelity
- front-to-deputy relationship preservation
- party, UF and legislature context preservation
- member role preservation
- original payload preservation
- ingestion metadata
- record hash support
- auditability
- CSV fallback resilience

Architecture note:
- This table supports the mandatory Parliamentary Fronts Atlas deliverable.
- Technical duplicates and business validation are intentionally handled in the Silver layer.

Source:
- csv_fallback/frentesDeputados.csv
"""

column_comments = {
    "frm_id_frente": "Parliamentary front identifier as provided by the official Câmara source.",
    "frm_tx_uri_frente": "Parliamentary front URI as provided by the official Câmara source.",
    "frm_tx_titulo_frente": "Parliamentary front title as provided by the official Câmara source.",

    "dep_id_deputado": "Deputy identifier associated with the parliamentary front.",
    "dep_tx_uri": "Deputy URI as provided by the official Câmara source.",
    "dep_tx_nome": "Deputy name as provided by the official Câmara source.",
    "dep_tx_sigla_partido": "Deputy political party acronym associated with the parliamentary front membership.",
    "dep_tx_uri_partido": "Deputy political party URI as provided by the source.",
    "dep_tx_sigla_uf": "Deputy federation unit acronym.",
    "dep_id_legislatura": "Deputy legislature identifier related to the parliamentary front membership.",
    "dep_tx_url_foto": "Deputy photo URL.",

    "frm_cd_titulo_membro": "Membership role code within the parliamentary front.",
    "frm_tx_titulo_membro": "Membership role description within the parliamentary front, such as Coordinator or Member.",
    "frm_dt_inicio": "Membership start date as provided by the source file.",
    "frm_dt_fim": "Membership end date as provided by the source file.",

    "frm_tx_payload_json": "Original raw payload preserved from the CSV source as JSON.",
    "aud_tx_arquivo_origem": "Source CSV file path.",
    "aud_id_execucao": "Unique execution identifier for the ingestion run.",
    "aud_dh_ingestao": "Timestamp when the record was ingested into the Bronze layer.",
    "aud_tx_endpoint_origem": "Logical source endpoint or CSV fallback source used to extract the record.",
    "aud_tx_sistema_origem": "Source system name.",
    "aud_tx_versao_pipeline": "Pipeline version responsible for the ingestion.",
    "aud_tx_tipo_carga": "Load type applied during ingestion.",
    "aud_tx_hash_registro": "Deterministic hash used for traceability and deduplication.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_table_and_column_comments(
        target_table=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Final Pipeline Log

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
        f"Bronze frentes membros CSV fallback ingestion completed successfully "
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
        f"Bronze frentes membros CSV fallback ingestion completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

# COMMAND ----------

print("=" * 90)
print("BRONZE FRENTES MEMBROS CSV FALLBACK COMPLETED")
print("=" * 90)
print(f"Source Path: {SOURCE_FILE_PATH}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)