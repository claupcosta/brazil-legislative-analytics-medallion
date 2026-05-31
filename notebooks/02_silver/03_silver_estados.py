# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver — Estados Standardization
# MAGIC
# MAGIC **Notebook:** `03_silver_estados`
# MAGIC
# MAGIC Standardizes Brazilian federative units and persists a curated state dimension into the Silver layer.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC * Brazilian federative unit reference standardization
# MAGIC * Official UF acronym normalization
# MAGIC * State and region dimension creation
# MAGIC * Federative unit quality validation rules
# MAGIC * Technical duplicate prevention
# MAGIC * Traceability and lineage preservation
# MAGIC * Rejected record registration using global utilities
# MAGIC * Silver Delta persistence logic
# MAGIC * Governance comments using global utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Responsibilities
# MAGIC
# MAGIC * Build the official Brazilian federative unit dimension
# MAGIC * Standardize state identifiers
# MAGIC * Standardize UF acronyms
# MAGIC * Standardize state names
# MAGIC * Standardize macro-region names
# MAGIC * Validate mandatory dimension attributes
# MAGIC * Eliminate technical duplicates
# MAGIC * Preserve execution lineage metadata
# MAGIC * Register rejected records for traceability
# MAGIC * Persist curated Silver dimension table
# MAGIC * Apply governance comments to table and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Notes
# MAGIC
# MAGIC * This notebook is a reference dimension builder
# MAGIC * Records are derived from official Brazilian federative unit definitions
# MAGIC * One record is generated for each UF
# MAGIC * The resulting grain is one row per federative unit
# MAGIC * State identifiers are deterministic
# MAGIC * Region classification follows official Brazilian geography
# MAGIC * Silver validation rules guarantee dimension consistency
# MAGIC * Rejected records are redirected to `slv_registros_rejeitados`
# MAGIC * Global utility notebooks are used to reduce duplicated logic
# MAGIC * Documentation and governance comments are written in English
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
    when,
    coalesce,
    concat_ws,
    current_timestamp,
    regexp_replace,
    row_number,
    collect_set,
)
from pyspark.sql.types import (
    StringType,
    BooleanType,
    StructType,
    StructField,
)
from pyspark.sql.window import Window

# COMMAND ----------

# ==========================================================================================
# Initialize Spark Session
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

# COMMAND ----------

# ==========================================================================================
# Execution Header
# ==========================================================================================

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("03 - SILVER ESTADOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ==========================================================================================
# 1. Global Configuration
# ==========================================================================================

NOTEBOOK_NAME = "03_silver_estados"
LAYER_NAME = "silver"
ENTITY_NAME = "estados"

SOURCE_TABLE = "derived_static_brazilian_federative_units"

TARGET_TABLE = get_silver_table(
    SILVER_TABLES.get("estados", "slv_estados")
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

# ==========================================================================================
# 2. Helper Functions
# ==========================================================================================

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


def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        try:
            spark.table(table_name).limit(1).count()
            return True
        except Exception:
            return False


def get_table_if_exists(table_name: str):
    if table_exists(table_name):
        return spark.table(table_name)

    return None


def extract_uf_reference(table_name: str, column_name: str, source_label: str):
    dataframe = get_table_if_exists(table_name)

    if dataframe is None:
        return None

    if column_name not in dataframe.columns:
        return None

    return (
        dataframe
        .select(
            clean_upper(col(column_name)).alias("est_tx_sigla_uf"),
            lit(source_label).alias("est_tx_fonte_referencia"),
        )
        .filter(
            col("est_tx_sigla_uf").isNotNull()
            & (col("est_tx_sigla_uf") != "")
        )
    )

# COMMAND ----------

# ==========================================================================================
# 3. Start Pipeline Log
# ==========================================================================================

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=execution_id,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table=TARGET_TABLE,
    status=EXECUTION_STATUS_STARTED,
    message="Silver estados derivation started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver estados derivation.",
)

# COMMAND ----------

# ==========================================================================================
# 4. Build Official Federative Unit Reference
# ==========================================================================================

estado_reference_rows = [
    ("AC", "Acre", "Norte"),
    ("AL", "Alagoas", "Nordeste"),
    ("AP", "Amapá", "Norte"),
    ("AM", "Amazonas", "Norte"),
    ("BA", "Bahia", "Nordeste"),
    ("CE", "Ceará", "Nordeste"),
    ("DF", "Distrito Federal", "Centro-Oeste"),
    ("ES", "Espírito Santo", "Sudeste"),
    ("GO", "Goiás", "Centro-Oeste"),
    ("MA", "Maranhão", "Nordeste"),
    ("MT", "Mato Grosso", "Centro-Oeste"),
    ("MS", "Mato Grosso do Sul", "Centro-Oeste"),
    ("MG", "Minas Gerais", "Sudeste"),
    ("PA", "Pará", "Norte"),
    ("PB", "Paraíba", "Nordeste"),
    ("PR", "Paraná", "Sul"),
    ("PE", "Pernambuco", "Nordeste"),
    ("PI", "Piauí", "Nordeste"),
    ("RJ", "Rio de Janeiro", "Sudeste"),
    ("RN", "Rio Grande do Norte", "Nordeste"),
    ("RS", "Rio Grande do Sul", "Sul"),
    ("RO", "Rondônia", "Norte"),
    ("RR", "Roraima", "Norte"),
    ("SC", "Santa Catarina", "Sul"),
    ("SP", "São Paulo", "Sudeste"),
    ("SE", "Sergipe", "Nordeste"),
    ("TO", "Tocantins", "Norte"),
]

estado_reference_schema = StructType(
    [
        StructField("est_tx_sigla_uf", StringType(), False),
        StructField("est_tx_nome", StringType(), False),
        StructField("est_tx_regiao", StringType(), False),
    ]
)

estado_reference_df = spark.createDataFrame(
    estado_reference_rows,
    estado_reference_schema,
)

records_read = estado_reference_df.count()

# COMMAND ----------

# ==========================================================================================
# 5. Derive Observed UF References From Available Sources
# ==========================================================================================

reference_dataframes = []

bronze_deputados_table = get_bronze_table(
    BRONZE_TABLES.get("deputados", "br_deputados")
)

bronze_despesas_table = get_bronze_table(
    BRONZE_TABLES.get("despesas_ceap", "br_despesas_ceap")
)

bronze_frentes_membros_table = get_bronze_table(
    BRONZE_TABLES.get("frentes_membros", "br_frentes_membros")
)

silver_deputados_table = get_silver_table(
    SILVER_TABLES.get("deputados", "slv_deputados")
)

silver_frentes_membros_table = get_silver_table(
    SILVER_TABLES.get("frentes_membros", "slv_frentes_membros")
)

silver_presencas_eventos_table = get_silver_table(
    SILVER_TABLES.get("presencas_eventos", "slv_presencas_eventos")
)

source_candidates = [
    (bronze_deputados_table, "dep_tx_sigla_uf", "bronze.br_deputados"),
    (bronze_despesas_table, "dep_tx_sigla_uf", "bronze.br_despesas_ceap"),
    (bronze_frentes_membros_table, "dep_tx_sigla_uf", "bronze.br_frentes_membros"),
    (silver_deputados_table, "dep_tx_sigla_uf", "silver.slv_deputados"),
    (silver_frentes_membros_table, "dep_tx_sigla_uf", "silver.slv_frentes_membros"),
    (silver_presencas_eventos_table, "dep_tx_sigla_uf", "silver.slv_presencas_eventos"),
]

for table_name, column_name, source_label in source_candidates:
    reference_df = extract_uf_reference(
        table_name=table_name,
        column_name=column_name,
        source_label=source_label,
    )

    if reference_df is not None:
        reference_dataframes.append(reference_df)

if len(reference_dataframes) > 0:
    observed_ufs_df = reference_dataframes[0]

    for reference_df in reference_dataframes[1:]:
        observed_ufs_df = observed_ufs_df.unionByName(reference_df)

    observed_ufs_df = (
        observed_ufs_df
        .groupBy("est_tx_sigla_uf")
        .agg(
            concat_ws(
                ", ",
                collect_set("est_tx_fonte_referencia")
            ).alias("est_tx_fontes_observadas")
        )
    )

else:
    observed_ufs_schema = StructType(
        [
            StructField("est_tx_sigla_uf", StringType(), True),
            StructField("est_tx_fontes_observadas", StringType(), True),
        ]
    )

    observed_ufs_df = spark.createDataFrame(
        [],
        observed_ufs_schema,
    )

# COMMAND ----------

# ==========================================================================================
# 6. Build Estados Base
# ==========================================================================================

estados_base_df = (
    estado_reference_df
    .join(
        observed_ufs_df,
        on="est_tx_sigla_uf",
        how="left",
    )
    .withColumn(
        "est_id_estado",
        concat_ws("_", lit("EST"), col("est_tx_sigla_uf")),
    )
    .withColumn(
        "est_tx_pais",
        lit("Brasil"),
    )
    .withColumn(
        "est_tx_codigo_pais",
        lit("BR"),
    )
    .withColumn(
        "est_tx_origem_registro",
        lit("static_brazilian_federative_units_reference"),
    )
    .withColumn(
        "est_fl_registro_derivado",
        lit(True).cast(BooleanType()),
    )
    .withColumn(
        "est_fl_uf_observada_nas_fontes",
        when(
            col("est_tx_fontes_observadas").isNotNull()
            & (col("est_tx_fontes_observadas") != ""),
            lit(True),
        )
        .otherwise(lit(False))
        .cast(BooleanType()),
    )
    .withColumn(
        "est_tx_payload_json",
        lit(None).cast(StringType()),
    )
    .withColumn(
        "aud_id_execucao_bronze",
        lit(None).cast(StringType()),
    )
    .withColumn(
        "aud_dh_ingestao_bronze",
        lit(None).cast(StringType()),
    )
    .withColumn(
        "aud_tx_endpoint_origem_bronze",
        lit(None).cast(StringType()),
    )
    .withColumn(
        "aud_tx_sistema_origem_bronze",
        lit("static_reference"),
    )
    .withColumn(
        "aud_tx_versao_pipeline_bronze",
        lit(PROJECT_VERSION),
    )
    .withColumn(
        "aud_tx_tipo_carga_bronze",
        lit(LOAD_TYPE),
    )
    .withColumn(
        "aud_tx_hash_registro_bronze",
        lit(None).cast(StringType()),
    )
)

# COMMAND ----------

# ==========================================================================================
# 7. Apply Quality Rules
# ==========================================================================================

estados_quality_df = (
    estados_base_df
    .withColumn(
        "est_fl_id_valido",
        (
            col("est_id_estado").isNotNull()
            & (col("est_id_estado") != "")
        ).cast(BooleanType()),
    )
    .withColumn(
        "est_fl_sigla_uf_valida",
        (
            col("est_tx_sigla_uf").isNotNull()
            & (col("est_tx_sigla_uf") != "")
            & (regexp_replace(col("est_tx_sigla_uf"), r"^[A-Z]{2}$", "") == "")
        ).cast(BooleanType()),
    )
    .withColumn(
        "est_fl_nome_valido",
        (
            col("est_tx_nome").isNotNull()
            & (col("est_tx_nome") != "")
        ).cast(BooleanType()),
    )
    .withColumn(
        "est_fl_regiao_valida",
        (
            col("est_tx_regiao").isNotNull()
            & (col("est_tx_regiao") != "")
        ).cast(BooleanType()),
    )
    .withColumn(
        "est_fl_registro_valido_silver",
        (
            col("est_fl_id_valido")
            & col("est_fl_sigla_uf_valida")
            & col("est_fl_nome_valido")
            & col("est_fl_regiao_valida")
        ).cast(BooleanType()),
    )
    .withColumn(
        "est_tx_motivo_rejeicao",
        when(
            ~col("est_fl_id_valido"),
            lit("EST_ID_NULO_OU_VAZIO"),
        )
        .when(
            ~col("est_fl_sigla_uf_valida"),
            lit("EST_SIGLA_UF_INVALIDA"),
        )
        .when(
            ~col("est_fl_nome_valido"),
            lit("EST_NOME_NULO_OU_VAZIO"),
        )
        .when(
            ~col("est_fl_regiao_valida"),
            lit("EST_REGIAO_NULA_OU_VAZIA"),
        )
        .otherwise(
            lit(None).cast(StringType())
        ),
    )
)

# COMMAND ----------

# ==========================================================================================
# 8. Build Mandatory Rejected Records
# ==========================================================================================

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=estados_quality_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="est_id_estado",
    validation_rule_column="est_tx_motivo_rejeicao",
    payload_column="est_tx_payload_json",
    valid_flag_column="est_fl_registro_valido_silver",
)

# COMMAND ----------

# ==========================================================================================
# 9. Keep Valid Records
# ==========================================================================================

valid_df = (
    estados_quality_df
    .filter(
        col("est_fl_registro_valido_silver") == True
    )
    .drop("est_tx_motivo_rejeicao")
)

# COMMAND ----------

# ==========================================================================================
# 10. Identify Technical Duplicates
# ==========================================================================================

dedup_window = (
    Window
    .partitionBy(
        "est_id_estado"
    )
    .orderBy(
        col("est_tx_sigla_uf").asc_nulls_last(),
    )
)

ranked_df = (
    valid_df
    .withColumn(
        "rn_deduplicacao",
        row_number().over(dedup_window),
    )
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=ranked_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="est_id_estado",
    payload_column="est_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="EST_REGISTRO_DUPLICADO_TECNICO",
    observation=(
        "Federative unit record kept only once by state identifier."
    ),
)

estados_dedup_df = (
    ranked_df
    .filter(
        col("rn_deduplicacao") == 1
    )
    .drop("rn_deduplicacao")
)

# COMMAND ----------

# ==========================================================================================
# 11. Persist Rejected and Discarded Records
# ==========================================================================================

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
            "Rejected and discarded estados records persisted "
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
            f"Failed writing rejected estados records "
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
        message="Failed writing rejected estados records.",
        error=error,
    )

    raise error

# COMMAND ----------

# ==========================================================================================
# 12. Add Silver Traceability Columns
# ==========================================================================================

silver_df = (
    estados_dedup_df
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
        lit("static_reference"),
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
        "aud_tx_regra_derivacao",
        lit(
            "Brazilian federative unit dimension derived from official static reference and observed UF values in available project sources."
        ),
    )
)

# COMMAND ----------

# ==========================================================================================
# 13. Add Silver Record Hash
# ==========================================================================================

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "est_id_estado",
        "est_tx_sigla_uf",
        "est_tx_nome",
        "est_tx_regiao",
        "est_tx_pais",
        "est_tx_codigo_pais",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# ==========================================================================================
# 14. Select Final Silver Columns
# ==========================================================================================

final_columns = [
    "est_id_estado",
    "est_tx_sigla_uf",
    "est_tx_nome",
    "est_tx_regiao",
    "est_tx_pais",
    "est_tx_codigo_pais",
    "est_tx_origem_registro",
    "est_tx_fontes_observadas",

    "est_fl_id_valido",
    "est_fl_sigla_uf_valida",
    "est_fl_nome_valido",
    "est_fl_regiao_valida",
    "est_fl_uf_observada_nas_fontes",
    "est_fl_registro_derivado",
    "est_fl_registro_valido_silver",

    "est_tx_payload_json",

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
    "aud_tx_regra_derivacao",
    "aud_tx_hash_registro_silver",
]

silver_df = silver_df.select(
    *final_columns
)

# COMMAND ----------

# ==========================================================================================
# 15. Persist Silver Table
# ==========================================================================================

records_written = silver_df.count()

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
            "Silver estados table persisted successfully "
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
            f"Failed writing Silver estados table "
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
        message="Failed writing Silver estados table.",
        error=error,
    )

    raise error

# COMMAND ----------

# ==========================================================================================
# 16. Apply Governance Comments
# ==========================================================================================

table_comment = """
Brazilian federative unit dimension in the Silver layer.

This table contains standardized Brazilian states and the Federal District,
including acronym, name, region and governance metadata. The table is derived
from a static official reference and enriched with indicators showing whether
each UF was observed in available project source tables.

Main characteristics:
- one row per Brazilian federative unit
- deterministic state identifier
- standardized UF acronym
- standardized state and region names
- source observation indicator
- preserved Silver governance metadata
- deterministic Silver record hash
"""

column_comments = {
    "est_id_estado": "Deterministic federative unit identifier generated from the UF acronym.",
    "est_tx_sigla_uf": "Brazilian federative unit acronym.",
    "est_tx_nome": "Brazilian federative unit name.",
    "est_tx_regiao": "Brazilian macro-region name.",
    "est_tx_pais": "Country name associated with the federative unit.",
    "est_tx_codigo_pais": "Country code associated with the federative unit.",
    "est_tx_origem_registro": "Description of the source used to derive the state record.",
    "est_tx_fontes_observadas": "List of project source tables where the UF acronym was observed.",

    "est_fl_id_valido": "Flag indicating whether the federative unit identifier is valid.",
    "est_fl_sigla_uf_valida": "Flag indicating whether the UF acronym is valid.",
    "est_fl_nome_valido": "Flag indicating whether the federative unit name is valid.",
    "est_fl_regiao_valida": "Flag indicating whether the macro-region name is valid.",
    "est_fl_uf_observada_nas_fontes": "Flag indicating whether the UF was observed in available project sources.",
    "est_fl_registro_derivado": "Flag indicating whether the state record was derived from a reference source.",
    "est_fl_registro_valido_silver": "Flag indicating whether the state record passed Silver validation rules.",

    "est_tx_payload_json": "Original payload preserved for traceability when available.",

    "aud_id_execucao_bronze": "Bronze execution identifier when available.",
    "aud_dh_ingestao_bronze": "Bronze ingestion timestamp when available.",
    "aud_tx_endpoint_origem_bronze": "Bronze source endpoint when available.",
    "aud_tx_sistema_origem_bronze": "Bronze source system or static reference marker.",
    "aud_tx_versao_pipeline_bronze": "Bronze pipeline version or project version for static references.",
    "aud_tx_tipo_carga_bronze": "Bronze load type or static reference load marker.",
    "aud_tx_hash_registro_bronze": "Bronze deterministic record hash when available.",

    "aud_id_execucao_silver": "Execution identifier for Silver state processing.",
    "aud_dh_processamento": "Timestamp when the state record was processed in Silver.",
    "aud_tx_camada_origem": "Source layer or reference type used during processing.",
    "aud_tx_tabela_origem": "Source table or reference used during state derivation.",
    "aud_tx_tabela_destino": "Target Silver table used during state derivation.",
    "aud_tx_versao_pipeline_silver": "Pipeline version used during Silver state processing.",
    "aud_tx_regra_derivacao": "Description of the Silver state derivation rule applied.",
    "aud_tx_hash_registro_silver": "Deterministic Silver state record hash.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# ==========================================================================================
# 17. Final Pipeline Log
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
        "Silver estados derivation completed successfully "
        "| grain=one Brazilian federative unit per UF acronym"
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
        f"Silver estados derivation completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER ESTADOS COMPLETED")
print("=" * 90)
print(f"Source Reference: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print("Grain: one Brazilian federative unit per UF acronym")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)
