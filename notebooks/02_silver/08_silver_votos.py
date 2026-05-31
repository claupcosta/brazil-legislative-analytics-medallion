# Databricks notebook source
# MAGIC %md
# MAGIC # 08 Silver — Votos Standardization
# MAGIC
# MAGIC **Notebook:** `08_silver_votos`  
# MAGIC **Layer:** `Silver`  
# MAGIC **Source:** `brazil_legislative_analytics.bronze.br_votos`  
# MAGIC **Target:** `brazil_legislative_analytics.silver.slv_votos`
# MAGIC
# MAGIC Standardizes individual deputy voting records from the Bronze layer and persists
# MAGIC validated, deduplicated and analytics-ready records into the Silver layer.
# MAGIC
# MAGIC This notebook defines the relationship between deputies and voting sessions,
# MAGIC enabling downstream analysis of parliamentary behavior, voting alignment,
# MAGIC party behavior and correlation between parliamentary fronts and votes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read individual deputy voting records from Bronze
# MAGIC - Standardize voting session, deputy and vote attributes
# MAGIC - Extract deputy party, UF, URI, photo and legislature attributes from JSON payload
# MAGIC - Normalize vote values into curated analytical categories
# MAGIC - Create analytical voting behavior flags
# MAGIC - Validate mandatory Silver fields
# MAGIC - Remove technical duplicate records
# MAGIC - Preserve one record per voting session and deputy
# MAGIC - Register rejected and duplicate records for traceability
# MAGIC - Preserve Bronze-to-Silver lineage
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
# MAGIC - The grain of this table is one deputy vote per voting session
# MAGIC - Party and UF are read from the JSON payload to avoid Spark Connect issues with void or unstable Bronze columns
# MAGIC - Invalid records are redirected to `slv_registros_rejeitados`
# MAGIC - Technical duplicates are also registered as rejected records
# MAGIC - Comments and documentation are written in English
# MAGIC - Naming conventions follow Portuguese mnemonic standards
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
    current_timestamp,
    row_number,
    when,
    concat_ws,
    sha2,
    coalesce,
    get_json_object,
    expr,
)

from pyspark.sql.window import Window

from pyspark.sql.types import (
    StringType,
    TimestampType,
)

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("08 - SILVER VOTOS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# NOTEBOOK CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "08_silver_votos"
LAYER_NAME = "silver"
ENTITY_NAME = "votos"

SOURCE_TABLE = get_bronze_table(
    BRONZE_TABLES["votos"]
)

TARGET_TABLE = get_silver_table(
    SILVER_TABLES["votos"]
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
# HELPER FUNCTIONS
# ============================================================

def clean_text(column_expression):
    """
    Cleans textual expressions by trimming and normalizing spaces.
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
    Cleans textual expressions and converts values to uppercase.
    """

    return upper(
        clean_text(column_expression)
    )

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
    message="Silver votos transformation started.",
    started_at=started_at,
    finished_at=None,
    duration_seconds=None,
    records_read=None,
    records_written=None,
)

log_info(
    pipeline_logger=logger,
    message="Starting Silver votos transformation.",
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
            f"Bronze votos table loaded successfully "
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
            f"Failed reading Bronze votos table "
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
        message="Failed reading Bronze votos table.",
        error=error,
    )

    raise error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Standardize Bronze Columns

# COMMAND ----------

silver_base_df = (
    bronze_df
    .select(
        clean_text(
            col("vot_id_votacao")
        ).alias("vot_id_votacao"),

        clean_text(
            col("vot_nr_ano_referencia")
        ).alias("vto_nr_ano_referencia"),

        clean_text(
            col("dep_id_deputado")
        ).alias("dep_id_deputado"),

        clean_upper_text(
            col("dep_tx_nome")
        ).alias("dep_tx_nome_coluna"),

        lit(None).cast(StringType())
            .alias("dep_tx_sigla_partido_coluna"),

        lit(None).cast(StringType())
            .alias("dep_tx_sigla_uf_coluna"),

        clean_text(
            col("vot_dt_registro_voto")
        ).alias("vot_dt_registro_voto_coluna"),

        clean_upper_text(
            col("vot_tx_tipo_voto")
        ).alias("vto_tx_voto_coluna"),

        clean_text(
            col("vot_tx_payload_json")
        ).alias("vto_tx_payload_json"),

        col("aud_id_execucao")
            .cast(StringType())
            .alias("aud_id_execucao_bronze"),

        col("aud_dh_ingestao")
            .cast(TimestampType())
            .alias("aud_dh_ingestao_bronze"),

        col("aud_tx_endpoint_origem")
            .cast(StringType())
            .alias("aud_tx_endpoint_origem_bronze"),

        col("aud_tx_sistema_origem")
            .cast(StringType())
            .alias("aud_tx_sistema_origem_bronze"),

        col("aud_tx_versao_pipeline")
            .cast(StringType())
            .alias("aud_tx_versao_pipeline_bronze"),

        col("aud_tx_tipo_carga")
            .cast(StringType())
            .alias("aud_tx_tipo_carga_bronze"),

        col("aud_tx_arquivo_origem")
            .cast(StringType())
            .alias("aud_tx_arquivo_origem_bronze"),

        col("aud_tx_hash_registro")
            .cast(StringType())
            .alias("aud_tx_hash_registro_bronze"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extract Deputy Attributes from JSON Payload

# COMMAND ----------

silver_enriched_df = (
    silver_base_df

    .withColumn(
        "dep_tx_uri",
        clean_text(
            get_json_object(
                col("vto_tx_payload_json"),
                "$.deputado_uri",
            )
        )
    )

    .withColumn(
        "dep_tx_url_foto",
        clean_text(
            get_json_object(
                col("vto_tx_payload_json"),
                "$.deputado_urlFoto",
            )
        )
    )

    .withColumn(
        "part_tx_uri",
        clean_text(
            get_json_object(
                col("vto_tx_payload_json"),
                "$.deputado_uriPartido",
            )
        )
    )

    .withColumn(
        "dep_tx_nome_json",
        clean_upper_text(
            get_json_object(
                col("vto_tx_payload_json"),
                "$.deputado_nome",
            )
        )
    )

    .withColumn(
        "dep_tx_sigla_partido_json",
        clean_upper_text(
            get_json_object(
                col("vto_tx_payload_json"),
                "$.deputado_siglaPartido",
            )
        )
    )

    .withColumn(
        "dep_tx_sigla_uf_json",
        clean_upper_text(
            get_json_object(
                col("vto_tx_payload_json"),
                "$.deputado_siglaUf",
            )
        )
    )

    .withColumn(
        "leg_id_legislatura",
        clean_text(
            get_json_object(
                col("vto_tx_payload_json"),
                "$.deputado_idLegislatura",
            )
        )
    )

    .withColumn(
        "vot_dt_registro_voto_json",
        clean_text(
            get_json_object(
                col("vto_tx_payload_json"),
                "$.dataHoraVoto",
            )
        )
    )

    .withColumn(
        "vto_tx_voto_json",
        clean_upper_text(
            get_json_object(
                col("vto_tx_payload_json"),
                "$.voto",
            )
        )
    )

    .withColumn(
        "dep_tx_nome",
        coalesce(
            col("dep_tx_nome_coluna"),
            col("dep_tx_nome_json"),
        )
    )

    .withColumn(
        "dep_tx_sigla_partido",
        coalesce(
            col("dep_tx_sigla_partido_json"),
            col("dep_tx_sigla_partido_coluna"),
        )
    )

    .withColumn(
        "dep_tx_sigla_uf",
        coalesce(
            col("dep_tx_sigla_uf_json"),
            col("dep_tx_sigla_uf_coluna"),
        )
    )

    .withColumn(
        "vot_dt_registro_voto",
        coalesce(
            col("vot_dt_registro_voto_coluna"),
            col("vot_dt_registro_voto_json"),
        )
    )

    .withColumn(
        "vto_tx_voto",
        coalesce(
            col("vto_tx_voto_coluna"),
            col("vto_tx_voto_json"),
        )
    )

    .drop(
        "dep_tx_nome_coluna",
        "dep_tx_nome_json",
        "dep_tx_sigla_partido_coluna",
        "dep_tx_sigla_partido_json",
        "dep_tx_sigla_uf_coluna",
        "dep_tx_sigla_uf_json",
        "vot_dt_registro_voto_coluna",
        "vot_dt_registro_voto_json",
        "vto_tx_voto_coluna",
        "vto_tx_voto_json",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Normalize Datetime and Create Vote Analytical Fields

# COMMAND ----------

silver_clean_df = (
    silver_enriched_df

    .withColumn(
        "vot_dh_registro_voto",
        coalesce(
            expr("try_to_timestamp(vot_dt_registro_voto, \"yyyy-MM-dd'T'HH:mm:ss\")"),
            expr("try_to_timestamp(vot_dt_registro_voto, \"yyyy-MM-dd'T'HH:mm\")"),
            expr("try_to_timestamp(vot_dt_registro_voto)")
        )
    )

    .withColumn(
        "vto_id_registro",
        concat_ws(
            "_",
            col("vot_id_votacao"),
            col("dep_id_deputado"),
        )
    )

    .withColumn(
        "vto_tx_dedup_key",
        sha2(
            concat_ws(
                "||",
                col("vot_id_votacao"),
                col("dep_id_deputado"),
            ),
            256,
        )
    )

    .withColumn(
        "vto_tx_voto_curado",
        when(
            upper(col("vto_tx_voto")) == "SIM",
            lit("SIM"),
        )
        .when(
            upper(col("vto_tx_voto")).isin("NÃO", "NAO"),
            lit("NAO"),
        )
        .when(
            upper(col("vto_tx_voto")).contains("ABST"),
            lit("ABSTENCAO"),
        )
        .when(
            upper(col("vto_tx_voto")).contains("OBSTRU"),
            lit("OBSTRUCAO"),
        )
        .otherwise(
            col("vto_tx_voto")
        )
    )

    .withColumn(
        "vto_fl_sim",
        when(
            col("vto_tx_voto_curado") == "SIM",
            lit(1)
        ).otherwise(lit(0))
    )

    .withColumn(
        "vto_fl_nao",
        when(
            col("vto_tx_voto_curado") == "NAO",
            lit(1)
        ).otherwise(lit(0))
    )

    .withColumn(
        "vto_fl_abstencao",
        when(
            col("vto_tx_voto_curado") == "ABSTENCAO",
            lit(1)
        ).otherwise(lit(0))
    )

    .withColumn(
        "vto_fl_obstrucao",
        when(
            col("vto_tx_voto_curado") == "OBSTRUCAO",
            lit(1)
        ).otherwise(lit(0))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply Silver Quality Rules

# COMMAND ----------

silver_quality_df = (
    silver_clean_df

    .withColumn(
        "vot_fl_id_votacao_valido",
        (
            col("vot_id_votacao").isNotNull()
            & (col("vot_id_votacao") != "")
        )
    )

    .withColumn(
        "dep_fl_id_deputado_valido",
        (
            col("dep_id_deputado").isNotNull()
            & (col("dep_id_deputado") != "")
        )
    )

    .withColumn(
        "vto_fl_voto_informado",
        (
            col("vto_tx_voto").isNotNull()
            & (col("vto_tx_voto") != "")
        )
    )

    .withColumn(
        "vto_fl_data_registro_informada",
        col("vot_dh_registro_voto").isNotNull()
    )

    .withColumn(
        "dep_fl_nome_informado",
        (
            col("dep_tx_nome").isNotNull()
            & (col("dep_tx_nome") != "")
        )
    )

    .withColumn(
        "dep_fl_partido_informado",
        (
            col("dep_tx_sigla_partido").isNotNull()
            & (col("dep_tx_sigla_partido") != "")
        )
    )

    .withColumn(
        "dep_fl_uf_informada",
        (
            col("dep_tx_sigla_uf").isNotNull()
            & (col("dep_tx_sigla_uf") != "")
        )
    )

    .withColumn(
        "vto_fl_registro_valido_silver",
        (
            col("vot_fl_id_votacao_valido")
            & col("dep_fl_id_deputado_valido")
            & col("vto_fl_voto_informado")
        )
    )

    .withColumn(
        "vto_tx_motivo_rejeicao",
        when(
            ~col("vot_fl_id_votacao_valido"),
            lit("VTO_ID_VOTACAO_NULO_OU_VAZIO"),
        )
        .when(
            ~col("dep_fl_id_deputado_valido"),
            lit("VTO_ID_DEPUTADO_NULO_OU_VAZIO"),
        )
        .when(
            ~col("vto_fl_voto_informado"),
            lit("VTO_VOTO_NULO_OU_VAZIO"),
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

mandatory_rejected_df = build_mandatory_rejected_records(
    dataframe=silver_quality_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="vto_id_registro",
    validation_rule_column="vto_tx_motivo_rejeicao",
    payload_column="vto_tx_payload_json",
    valid_flag_column="vto_fl_registro_valido_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Deduplicate Technical Records

# COMMAND ----------

valid_df = (
    silver_quality_df
    .filter(
        col("vto_fl_registro_valido_silver") == True
    )
    .drop("vto_tx_motivo_rejeicao")
)

dedup_window = (
    Window
    .partitionBy(
        "vot_id_votacao",
        "dep_id_deputado",
    )
    .orderBy(
        col("aud_dh_ingestao_bronze").desc_nulls_last()
    )
)

valid_ranked_df = (
    valid_df
    .withColumn(
        "rn_deduplicacao",
        row_number().over(dedup_window)
    )
)

duplicate_rejected_df = build_duplicate_rejected_records(
    dataframe=valid_ranked_df,
    execution_id=execution_id,
    source_table=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    project_version=PROJECT_VERSION,
    entity_name=ENTITY_NAME,
    record_id_column="vto_id_registro",
    payload_column="vto_tx_payload_json",
    dedup_rank_column="rn_deduplicacao",
    duplicate_rule_code="VTO_REGISTRO_DUPLICADO_TECNICO",
    observation=(
        "Record kept only once by vot_id_votacao and dep_id_deputado. "
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
# MAGIC ## 9. Persist Rejected Records

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
        f"Rejected and discarded votos records persisted "
        f"| records_rejected={records_rejected}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Add Silver Traceability Columns

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
        "aud_tx_regra_extracao_voto",
        lit(
            "Individual deputy vote standardized from Bronze columns and enriched from vto_tx_payload_json when available."
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Add Silver Record Hash

# COMMAND ----------

silver_df = add_hash(
    dataframe=silver_df,
    columns=[
        "vot_id_votacao",
        "dep_id_deputado",
        "vto_tx_voto",
        "vto_tx_voto_curado",
        "vot_dh_registro_voto",
    ],
    hash_column="aud_tx_hash_registro_silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Select Final Silver Columns

# COMMAND ----------

final_columns = [
    "vto_id_registro",
    "vto_tx_dedup_key",

    "vot_id_votacao",
    "vto_nr_ano_referencia",

    "dep_id_deputado",
    "dep_tx_uri",
    "dep_tx_nome",
    "dep_tx_sigla_partido",
    "part_tx_uri",
    "dep_tx_sigla_uf",
    "leg_id_legislatura",
    "dep_tx_url_foto",

    "vto_tx_voto",
    "vto_tx_voto_curado",
    "vto_fl_sim",
    "vto_fl_nao",
    "vto_fl_abstencao",
    "vto_fl_obstrucao",

    "vot_dt_registro_voto",
    "vot_dh_registro_voto",

    "vot_fl_id_votacao_valido",
    "dep_fl_id_deputado_valido",
    "vto_fl_voto_informado",
    "vto_fl_data_registro_informada",
    "dep_fl_nome_informado",
    "dep_fl_partido_informado",
    "dep_fl_uf_informada",
    "vto_fl_registro_valido_silver",

    "vto_tx_payload_json",

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
    "aud_tx_regra_extracao_voto",
    "aud_tx_hash_registro_silver",
]

silver_df = silver_df.select(
    *final_columns
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Persist Silver Table

# COMMAND ----------

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
        f"Silver votos table persisted successfully "
        f"| records_written={records_written}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Apply Governance Comments

# COMMAND ----------

table_comment = """
Standardized individual deputy votes table in the Silver layer.

This table contains cleaned, validated, deduplicated and analytics-ready
individual voting records derived from the Bronze ingestion layer.

The grain of this table is one deputy vote per voting session.
The table includes curated vote values, analytical voting flags, deputy
attributes extracted from the JSON payload when available, technical deduplication
keys and Bronze-to-Silver traceability.
"""

column_comments = {
    "vto_id_registro": "Synthetic vote record identifier composed by voting session and deputy identifiers.",
    "vto_tx_dedup_key": "Technical deterministic deduplication key based on voting session and deputy identifiers.",
    "vot_id_votacao": "Voting session identifier associated with the individual vote.",
    "vto_nr_ano_referencia": "Reference year extracted from the source file name or ingestion context.",
    "dep_id_deputado": "Deputy identifier associated with the individual vote.",
    "dep_tx_uri": "Deputy URI extracted from JSON payload when available.",
    "dep_tx_nome": "Standardized deputy name.",
    "dep_tx_sigla_partido": "Political party acronym extracted from JSON payload when available.",
    "part_tx_uri": "Political party URI extracted from JSON payload when available.",
    "dep_tx_sigla_uf": "Brazilian state acronym extracted from JSON payload when available.",
    "leg_id_legislatura": "Legislature identifier extracted from JSON payload when available.",
    "dep_tx_url_foto": "Deputy photo URL extracted from JSON payload when available.",
    "vto_tx_voto": "Standardized individual vote value.",
    "vto_tx_voto_curado": "Curated individual vote category used for analytical modeling.",
    "vto_fl_sim": "Flag indicating whether the deputy voted yes.",
    "vto_fl_nao": "Flag indicating whether the deputy voted no.",
    "vto_fl_abstencao": "Flag indicating whether the deputy abstained.",
    "vto_fl_obstrucao": "Flag indicating whether the deputy vote was obstruction.",
    "vot_dt_registro_voto": "Original vote registration datetime string from the Bronze layer.",
    "vot_dh_registro_voto": "Vote registration timestamp safely converted from the Bronze registration datetime field.",
    "vot_fl_id_votacao_valido": "Flag indicating whether the voting session identifier is valid.",
    "dep_fl_id_deputado_valido": "Flag indicating whether the deputy identifier is valid.",
    "vto_fl_voto_informado": "Flag indicating whether the vote value is informed.",
    "vto_fl_data_registro_informada": "Flag indicating whether the vote registration timestamp is available.",
    "dep_fl_nome_informado": "Flag indicating whether deputy name is informed.",
    "dep_fl_partido_informado": "Flag indicating whether political party information is informed.",
    "dep_fl_uf_informada": "Flag indicating whether UF information is informed.",
    "vto_fl_registro_valido_silver": "Flag indicating whether the record passed mandatory Silver validation.",
    "vto_tx_payload_json": "Original Bronze JSON payload preserved for traceability.",
    "aud_id_execucao_bronze": "Execution identifier from Bronze ingestion.",
    "aud_dh_ingestao_bronze": "Bronze ingestion timestamp.",
    "aud_tx_endpoint_origem_bronze": "Source endpoint or file path used during Bronze ingestion.",
    "aud_tx_sistema_origem_bronze": "Source system identified during Bronze ingestion.",
    "aud_tx_versao_pipeline_bronze": "Pipeline version used during Bronze ingestion.",
    "aud_tx_tipo_carga_bronze": "Load type applied during Bronze ingestion.",
    "aud_tx_arquivo_origem_bronze": "Original source file path captured during Bronze CSV ingestion.",
    "aud_tx_hash_registro_bronze": "Deterministic Bronze record hash.",
    "aud_id_execucao_silver": "Execution identifier for Silver transformation.",
    "aud_dh_processamento": "Timestamp when the record was processed in Silver.",
    "aud_tx_camada_origem": "Source Medallion layer used during processing.",
    "aud_tx_tabela_origem": "Source table used during processing.",
    "aud_tx_tabela_destino": "Target Silver table.",
    "aud_tx_versao_pipeline_silver": "Pipeline version used during Silver transformation.",
    "aud_tx_regra_extracao_voto": "Textual description of the vote extraction and enrichment rule.",
    "aud_tx_hash_registro_silver": "Deterministic Silver record hash.",
}

if APPLY_GOVERNANCE_COMMENTS:

    apply_governance_comments(
        table_name=TARGET_TABLE,
        table_comment=table_comment,
        column_comments=column_comments,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Final Pipeline Log

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
        f"Silver votos transformation completed successfully "
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
        f"Silver votos transformation completed "
        f"| duration_seconds={duration_seconds}"
    ),
)

print("=" * 90)
print("SILVER VOTOS COMPLETED")
print("=" * 90)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Rejected Table: {REJECTED_TABLE}")
print(f"Records Read: {records_read}")
print(f"Records Written: {records_written}")
print(f"Records Rejected: {records_rejected}")
print("Grain: one vote per voting session and deputy")
print(f"Execution Duration: {duration_seconds}")
print("=" * 90)

# COMMAND ----------

