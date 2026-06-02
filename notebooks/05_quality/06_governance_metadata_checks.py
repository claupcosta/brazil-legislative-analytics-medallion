# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Governance Layer — Metadata Quality Checks
# MAGIC
# MAGIC **Notebook:** `06_governance_metadata_checks`
# MAGIC **Layer:** `Governance`
# MAGIC **Source/Endpoint:** `Unity Catalog / Delta Table Metadata`
# MAGIC **Target:** `Governance metadata validation results and audit logs`
# MAGIC
# MAGIC Executes governance-oriented metadata validations for schemas and Delta tables
# MAGIC in the Brazil Legislative Analytics Medallion project.
# MAGIC
# MAGIC This notebook validates whether catalog, schema, table and column metadata are
# MAGIC available, traceable and structurally aligned with project governance standards.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC * Validate catalog availability
# MAGIC * Validate expected schema availability
# MAGIC * Discover governed tables across project layers
# MAGIC * Validate Delta table format
# MAGIC * Validate required audit / traceability columns
# MAGIC * Validate table comments / descriptions
# MAGIC * Validate column comments for business and audit columns
# MAGIC * Validate table ownership metadata when available
# MAGIC * Persist governance validation results into audit tables
# MAGIC * Generate governance metadata validation summary
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC * Validation results are persisted into audit quality logs
# MAGIC * Supports controlled exception handling per metadata object
# MAGIC * Failed validations may not block execution during early development
# MAGIC * Pipeline blocking behavior is controlled by `FAIL_ON_ERROR`
# MAGIC * Some Unity Catalog metadata fields may vary by workspace/runtime permissions
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC * `/docs/governance/data_quality_rules.md`
# MAGIC * `/docs/governance/metadata_management.md`
# MAGIC * `/docs/architecture/medallion_architecture.md`
# MAGIC

# COMMAND ----------

# MAGIC %run ../99_utils/utils_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_quality

# COMMAND ----------

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("06 - GOVERNANCE METADATA CHECKS")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print(f"Catalog: {CATALOG_NAME}")
print("Layer: governance")
print("=" * 90)

# COMMAND ----------

# ============================================================
# GOVERNANCE CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "06_governance_metadata_checks"
LAYER_NAME = "governance"

# During early development, schemas and tables may not exist yet.
FAIL_ON_ERROR = False

# Keep False while the physical model is being aligned.
# Set True when all curated tables must contain every mandatory audit column.
STRICT_TRACEABILITY_VALIDATION = False

DATA_QUALITY_LOG_TABLE = (
    f"{CATALOG_NAME}."
    f"{SCHEMA_AUDIT}."
    f"{AUD_TB_LOG_QUALIDADE_DADOS}"
)

EXPECTED_SCHEMAS = [
    SCHEMA_BRONZE,
    SCHEMA_SILVER,
    SCHEMA_GOLD,
    SCHEMA_MARTS,
    SCHEMA_AUDIT,
]

# Current physical model: most curated tables already carry processing timestamp.
# These columns are treated as mandatory only when strict validation is enabled.
GOVERNED_MANDATORY_TRACEABILITY_COLUMNS = [
    "aud_dh_processamento",
]

# Recommended full audit contract for target model alignment.
GOVERNED_RECOMMENDED_TRACEABILITY_COLUMNS = [
    "aud_id_execucao",
    "aud_dh_processamento",
    "aud_tx_versao_pipeline",
]

TRACEABILITY_REQUIRED_SCHEMAS = [
    SCHEMA_SILVER,
    SCHEMA_GOLD,
    SCHEMA_MARTS,
]

OPTIONAL_TRACEABILITY_SCHEMAS = [
    SCHEMA_BRONZE,
    SCHEMA_AUDIT,
]

EXCLUDED_TABLE_PREFIXES = [
    "_",
    "tmp_",
    "temp_",
    "sandbox_",
]

quality_results = []

# COMMAND ----------

# ============================================================
# GOVERNANCE HELPERS
# ============================================================

def add_governance_result(
    rule_name: str,
    rule_description: str,
    validation_status: str,
    total_records: int,
    invalid_records: int,
    invalid_percentage: float,
    message: str,
    entity_name: str,
    target_table: str,
) -> None:
    """
    Adds a governance metadata validation result to the in-memory result list.
    """

    quality_results.append({
        "nome_regra": rule_name,
        "descricao_regra": rule_description,
        "status_validacao": validation_status,
        "total_registros": int(total_records) if total_records is not None else 0,
        "registros_invalidos": int(invalid_records) if invalid_records is not None else 0,
        "percentual_invalidos": float(invalid_percentage) if invalid_percentage is not None else 0.0,
        "mensagem": message,
        "entity_name": entity_name,
        "target_table": target_table,
    })

# COMMAND ----------

def add_exception_result(
    entity_name: str,
    target_table: str,
    error: Exception,
) -> None:
    """
    Adds a controlled exception result to the governance result list.
    """

    add_governance_result(
        rule_name="governance_metadata_exception",
        rule_description="Captures unexpected errors during governance metadata validation.",
        validation_status=QUALITY_FAILED,
        total_records=1,
        invalid_records=1,
        invalid_percentage=100.0,
        message=f"Unexpected error during governance metadata validation: {str(error)}",
        entity_name=entity_name,
        target_table=target_table,
    )

# COMMAND ----------

def safe_sql(sql_text: str) -> DataFrame:
    """
    Executes a SQL statement and returns a Spark DataFrame.
    """

    return spark.sql(sql_text)

# COMMAND ----------

def catalog_exists(catalog_name: str) -> bool:
    """
    Checks whether a catalog exists and is accessible.
    """

    try:
        catalogs_df = safe_sql("SHOW CATALOGS")
        catalog_column = "catalog" if "catalog" in catalogs_df.columns else catalogs_df.columns[0]
        return catalogs_df.filter(F.col(catalog_column) == catalog_name).count() > 0

    except Exception:
        try:
            spark.catalog.setCurrentCatalog(catalog_name)
            return True
        except Exception:
            return False

# COMMAND ----------

def schema_exists(schema_name: str) -> bool:
    """
    Checks whether a schema exists in the configured catalog.
    """

    try:
        schemas_df = safe_sql(f"SHOW SCHEMAS IN {CATALOG_NAME}")
        schema_column = "databaseName" if "databaseName" in schemas_df.columns else schemas_df.columns[0]
        return schemas_df.filter(F.col(schema_column) == schema_name).count() > 0

    except Exception:
        try:
            return spark.catalog.databaseExists(f"{CATALOG_NAME}.{schema_name}")
        except Exception:
            return False

# COMMAND ----------

def table_exists(full_table_name: str) -> bool:
    """
    Checks whether a fully qualified table exists.
    """

    try:
        return spark.catalog.tableExists(full_table_name)
    except Exception:
        return False

# COMMAND ----------

def is_excluded_table(table_name: str) -> bool:
    """
    Applies governance exclusions for temporary/internal table names.
    """

    normalized_name = table_name.lower()

    return any(
        normalized_name.startswith(prefix)
        for prefix in EXCLUDED_TABLE_PREFIXES
    )

# COMMAND ----------

def discover_tables(schema_name: str) -> list:
    """
    Discovers managed and external tables in a schema.
    """

    if not schema_exists(schema_name):
        return []

    tables_df = safe_sql(f"SHOW TABLES IN {CATALOG_NAME}.{schema_name}")

    return [
        row.tableName
        for row in tables_df.collect()
        if not is_excluded_table(row.tableName)
    ]

# COMMAND ----------

def get_table_columns(full_table_name: str) -> list:
    """
    Returns the column names from a table.
    """

    return spark.table(full_table_name).columns

# COMMAND ----------

def get_describe_detail(full_table_name: str) -> dict:
    """
    Returns DESCRIBE DETAIL metadata as a dictionary.
    """

    try:
        rows = safe_sql(f"DESCRIBE DETAIL {full_table_name}").collect()

        if not rows:
            return {}

        return rows[0].asDict(recursive=True)

    except Exception:
        return {}

# COMMAND ----------

def get_extended_metadata(full_table_name: str) -> dict:
    """
    Returns key-value metadata from DESCRIBE TABLE EXTENDED.
    """

    metadata = {}

    try:
        describe_df = safe_sql(f"DESCRIBE TABLE EXTENDED {full_table_name}")

        for row in describe_df.collect():
            key = row.col_name
            value = row.data_type

            if key is not None and value is not None:
                metadata[str(key).strip()] = str(value).strip()

    except Exception:
        return {}

    return metadata

# COMMAND ----------

def get_column_comments(full_table_name: str) -> dict:
    """
    Returns table column comments from DESCRIBE TABLE EXTENDED when available.
    """

    comments = {}

    try:
        describe_df = safe_sql(f"DESCRIBE TABLE EXTENDED {full_table_name}")

        for row in describe_df.collect():
            column_name = row.col_name
            comment = row.comment

            if column_name and not str(column_name).startswith("#"):
                comments[column_name] = comment

    except Exception:
        return {}

    return comments

# COMMAND ----------

def calculate_percentage(
    invalid_records: int,
    total_records: int,
) -> float:
    """
    Calculates invalid percentage safely.
    """

    if total_records == 0:
        return 0.0

    return round((invalid_records / total_records) * 100, 2)

# COMMAND ----------

def validate_catalog_availability() -> None:
    """
    Validates whether the configured catalog is available.
    """

    exists = catalog_exists(CATALOG_NAME)

    add_governance_result(
        rule_name="governance_catalog_exists",
        rule_description="Validates whether the configured Unity Catalog catalog exists and is accessible.",
        validation_status=QUALITY_PASSED if exists else QUALITY_FAILED,
        total_records=1,
        invalid_records=0 if exists else 1,
        invalid_percentage=0.0 if exists else 100.0,
        message=(
            f"Catalog is accessible: {CATALOG_NAME}"
            if exists
            else f"Catalog is not accessible: {CATALOG_NAME}"
        ),
        entity_name="catalog",
        target_table=CATALOG_NAME,
    )

# COMMAND ----------

def validate_schema_availability(schema_name: str) -> None:
    """
    Validates whether an expected schema is available.
    """

    exists = schema_exists(schema_name)
    full_schema_name = f"{CATALOG_NAME}.{schema_name}"

    add_governance_result(
        rule_name="governance_schema_exists",
        rule_description="Validates whether the expected schema exists and is accessible.",
        validation_status=QUALITY_PASSED if exists else QUALITY_FAILED,
        total_records=1,
        invalid_records=0 if exists else 1,
        invalid_percentage=0.0 if exists else 100.0,
        message=(
            f"Schema is accessible: {full_schema_name}"
            if exists
            else f"Schema is not accessible: {full_schema_name}"
        ),
        entity_name=schema_name,
        target_table=full_schema_name,
    )

# COMMAND ----------

def validate_table_availability(
    schema_name: str,
    table_name: str,
) -> bool:
    """
    Validates whether a discovered table is accessible.
    """

    full_table_name = f"{CATALOG_NAME}.{schema_name}.{table_name}"
    exists = table_exists(full_table_name)

    add_governance_result(
        rule_name="governance_table_accessible",
        rule_description="Validates whether the governed table is accessible.",
        validation_status=QUALITY_PASSED if exists else QUALITY_FAILED,
        total_records=1,
        invalid_records=0 if exists else 1,
        invalid_percentage=0.0 if exists else 100.0,
        message=(
            f"Governed table is accessible: {full_table_name}"
            if exists
            else f"Governed table is not accessible: {full_table_name}"
        ),
        entity_name=f"{schema_name}.{table_name}",
        target_table=full_table_name,
    )

    return exists

# COMMAND ----------

def validate_delta_format(
    schema_name: str,
    table_name: str,
    full_table_name: str,
) -> None:
    """
    Validates whether a governed table uses Delta format.
    """

    detail = get_describe_detail(full_table_name)
    table_format = str(detail.get("format", "")).lower()
    is_delta = table_format == "delta"

    add_governance_result(
        rule_name="governance_delta_format",
        rule_description="Validates whether governed tables are stored in Delta format.",
        validation_status=QUALITY_PASSED if is_delta else QUALITY_WARNING,
        total_records=1,
        invalid_records=0 if is_delta else 1,
        invalid_percentage=0.0 if is_delta else 100.0,
        message=(
            f"Table format is Delta: {full_table_name}"
            if is_delta
            else f"Table format is not Delta or could not be determined. format={table_format}"
        ),
        entity_name=f"{schema_name}.{table_name}",
        target_table=full_table_name,
    )

# COMMAND ----------

def validate_traceability_columns(
    schema_name: str,
    table_name: str,
    full_table_name: str,
) -> None:
    """
    Validates traceability columns according to the current governance maturity policy.

    Current policy:
    - Bronze and audit tables: traceability columns are optional.
    - Silver, Gold and Marts: at least one traceability column should exist.
    - Missing recommended columns are WARNING while STRICT_TRACEABILITY_VALIDATION is False.
    - Missing mandatory columns are FAILED only when STRICT_TRACEABILITY_VALIDATION is True.
    """

    if schema_name not in TRACEABILITY_REQUIRED_SCHEMAS:
        add_governance_result(
            rule_name="governance_traceability_columns_scope",
            rule_description="Identifies tables where traceability columns are optional by layer policy.",
            validation_status=QUALITY_WARNING,
            total_records=1,
            invalid_records=0,
            invalid_percentage=0.0,
            message=f"Traceability columns are optional for schema: {schema_name}",
            entity_name=f"{schema_name}.{table_name}",
            target_table=full_table_name,
        )
        return

    columns = get_table_columns(full_table_name)

    mandatory_missing_columns = [
        column
        for column in GOVERNED_MANDATORY_TRACEABILITY_COLUMNS
        if column not in columns
    ]

    recommended_missing_columns = [
        column
        for column in GOVERNED_RECOMMENDED_TRACEABILITY_COLUMNS
        if column not in columns
    ]

    present_traceability_columns = [
        column
        for column in GOVERNED_RECOMMENDED_TRACEABILITY_COLUMNS
        if column in columns
    ]

    total_records = len(GOVERNED_RECOMMENDED_TRACEABILITY_COLUMNS)
    invalid_records = len(recommended_missing_columns)
    invalid_percentage = calculate_percentage(
        invalid_records=invalid_records,
        total_records=total_records,
    )

    if STRICT_TRACEABILITY_VALIDATION and mandatory_missing_columns:
        validation_status = QUALITY_FAILED
        message = (
            "Mandatory traceability columns are missing: "
            f"{mandatory_missing_columns}. "
            f"Recommended missing columns: {recommended_missing_columns}"
        )

    elif not present_traceability_columns:
        validation_status = QUALITY_FAILED
        message = (
            "No governed traceability columns found. "
            f"Expected at least one of: {GOVERNED_RECOMMENDED_TRACEABILITY_COLUMNS}"
        )

    elif recommended_missing_columns:
        validation_status = QUALITY_WARNING
        message = (
            "Traceability contract is partially implemented. "
            f"Present columns: {present_traceability_columns}. "
            f"Recommended missing columns: {recommended_missing_columns}"
        )

    else:
        validation_status = QUALITY_PASSED
        message = "Full recommended traceability metadata contract is available."

    add_governance_result(
        rule_name="governance_traceability_metadata_contract",
        rule_description="Validates mandatory and recommended governance traceability metadata columns.",
        validation_status=validation_status,
        total_records=total_records,
        invalid_records=invalid_records,
        invalid_percentage=invalid_percentage,
        message=message,
        entity_name=f"{schema_name}.{table_name}",
        target_table=full_table_name,
    )

# COMMAND ----------

def validate_table_comment(
    schema_name: str,
    table_name: str,
    full_table_name: str,
) -> None:
    """
    Validates whether a table has a business description/comment.
    """

    detail = get_describe_detail(full_table_name)
    extended_metadata = get_extended_metadata(full_table_name)

    comment = (
        detail.get("description")
        or detail.get("comment")
        or extended_metadata.get("Comment")
        or extended_metadata.get("Table Properties")
    )

    has_comment = comment is not None and len(str(comment).strip()) > 0

    add_governance_result(
        rule_name="governance_table_comment",
        rule_description="Validates whether the governed table has descriptive metadata.",
        validation_status=QUALITY_PASSED if has_comment else QUALITY_WARNING,
        total_records=1,
        invalid_records=0 if has_comment else 1,
        invalid_percentage=0.0 if has_comment else 100.0,
        message=(
            "Table descriptive metadata is available."
            if has_comment
            else "Table descriptive metadata is missing."
        ),
        entity_name=f"{schema_name}.{table_name}",
        target_table=full_table_name,
    )

# COMMAND ----------

def validate_column_comments(
    schema_name: str,
    table_name: str,
    full_table_name: str,
) -> None:
    """
    Validates whether table columns have comments where metadata is available.
    """

    columns = get_table_columns(full_table_name)
    column_comments = get_column_comments(full_table_name)

    if not columns:
        add_governance_result(
            rule_name="governance_column_comments",
            rule_description="Validates column-level metadata availability.",
            validation_status=QUALITY_WARNING,
            total_records=0,
            invalid_records=0,
            invalid_percentage=0.0,
            message="No columns found for column comment validation.",
            entity_name=f"{schema_name}.{table_name}",
            target_table=full_table_name,
        )
        return

    missing_comment_columns = [
        column
        for column in columns
        if not column_comments.get(column)
    ]

    invalid_records = len(missing_comment_columns)
    total_records = len(columns)
    invalid_percentage = calculate_percentage(
        invalid_records=invalid_records,
        total_records=total_records,
    )

    add_governance_result(
        rule_name="governance_column_comments",
        rule_description="Validates column-level metadata availability.",
        validation_status=(
            QUALITY_PASSED
            if invalid_records == 0
            else QUALITY_WARNING
        ),
        total_records=total_records,
        invalid_records=invalid_records,
        invalid_percentage=invalid_percentage,
        message=(
            "All columns have comments."
            if invalid_records == 0
            else f"Columns without comments: {', '.join(missing_comment_columns[:20])}"
        ),
        entity_name=f"{schema_name}.{table_name}",
        target_table=full_table_name,
    )

# COMMAND ----------

def validate_owner_metadata(
    schema_name: str,
    table_name: str,
    full_table_name: str,
) -> None:
    """
    Validates whether ownership metadata is available.
    """

    extended_metadata = get_extended_metadata(full_table_name)
    owner = extended_metadata.get("Owner")

    has_owner = owner is not None and len(str(owner).strip()) > 0

    add_governance_result(
        rule_name="governance_owner_metadata",
        rule_description="Validates whether table ownership metadata is available.",
        validation_status=QUALITY_PASSED if has_owner else QUALITY_WARNING,
        total_records=1,
        invalid_records=0 if has_owner else 1,
        invalid_percentage=0.0 if has_owner else 100.0,
        message=(
            f"Table owner metadata is available: {owner}"
            if has_owner
            else "Table owner metadata is missing or unavailable."
        ),
        entity_name=f"{schema_name}.{table_name}",
        target_table=full_table_name,
    )

# COMMAND ----------

def run_table_governance_checks(
    schema_name: str,
    table_name: str,
) -> None:
    """
    Executes all governance metadata checks for a single table.
    """

    full_table_name = f"{CATALOG_NAME}.{schema_name}.{table_name}"

    print("=" * 90)
    print(f"Running governance metadata checks for: {full_table_name}")
    print("=" * 90)

    try:
        if not validate_table_availability(
            schema_name=schema_name,
            table_name=table_name,
        ):
            return

        validate_delta_format(
            schema_name=schema_name,
            table_name=table_name,
            full_table_name=full_table_name,
        )

        validate_traceability_columns(
            schema_name=schema_name,
            table_name=table_name,
            full_table_name=full_table_name,
        )

        validate_table_comment(
            schema_name=schema_name,
            table_name=table_name,
            full_table_name=full_table_name,
        )

        validate_column_comments(
            schema_name=schema_name,
            table_name=table_name,
            full_table_name=full_table_name,
        )

        validate_owner_metadata(
            schema_name=schema_name,
            table_name=table_name,
            full_table_name=full_table_name,
        )

    except Exception as error:
        add_exception_result(
            entity_name=f"{schema_name}.{table_name}",
            target_table=full_table_name,
            error=error,
        )

# COMMAND ----------

def build_governance_quality_log() -> DataFrame:
    """
    Builds the final governance quality log DataFrame.
    """

    if not quality_results:
        add_governance_result(
            rule_name="governance_metadata_no_results",
            rule_description="Validates whether governance metadata checks produced results.",
            validation_status=QUALITY_WARNING,
            total_records=0,
            invalid_records=0,
            invalid_percentage=0.0,
            message="No governance metadata results were generated.",
            entity_name="governance",
            target_table=DATA_QUALITY_LOG_TABLE,
        )

    quality_base_df = spark.createDataFrame(quality_results)

    return (
        quality_base_df
        .withColumn("qlt_id_log", F.expr("uuid()"))
        .withColumn("aud_id_execucao", F.lit(RUN_ID))
        .withColumn("aud_tx_nome_projeto", F.lit(PROJECT_NAME))
        .withColumn("aud_tx_versao_pipeline", F.lit(PROJECT_VERSION))
        .withColumn("aud_tx_ambiente", F.lit(PROJECT_ENVIRONMENT))
        .withColumn("aud_tx_nome_notebook", F.lit(NOTEBOOK_NAME))
        .withColumn("aud_tx_nome_camada", F.lit(LAYER_NAME))
        .withColumn("aud_tx_nome_entidade", F.col("entity_name"))
        .withColumn("aud_tx_tabela_destino", F.col("target_table"))
        .withColumn("qlt_tx_nome_regra", F.col("nome_regra"))
        .withColumn("qlt_tx_descricao_regra", F.col("descricao_regra"))
        .withColumn("qlt_tx_status_validacao", F.col("status_validacao"))
        .withColumn("qlt_qt_total_registros", F.col("total_registros"))
        .withColumn("qlt_qt_registros_invalidos", F.col("registros_invalidos"))
        .withColumn("qlt_pc_registros_invalidos", F.col("percentual_invalidos"))
        .withColumn("qlt_dh_validacao", F.current_timestamp())
        .withColumn("qlt_tx_mensagem", F.col("mensagem"))
        .select(
            "qlt_id_log",
            "aud_id_execucao",
            "aud_tx_nome_projeto",
            "aud_tx_versao_pipeline",
            "aud_tx_ambiente",
            "aud_tx_nome_notebook",
            "aud_tx_nome_camada",
            "aud_tx_nome_entidade",
            "aud_tx_tabela_destino",
            "qlt_tx_nome_regra",
            "qlt_tx_descricao_regra",
            "qlt_tx_status_validacao",
            "qlt_qt_total_registros",
            "qlt_qt_registros_invalidos",
            "qlt_pc_registros_invalidos",
            "qlt_dh_validacao",
            "qlt_tx_mensagem",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Execute Catalog and Schema Governance Checks

# COMMAND ----------

validate_catalog_availability()

for schema_name in EXPECTED_SCHEMAS:
    validate_schema_availability(
        schema_name=schema_name,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Discover Governed Tables

# COMMAND ----------

governed_tables = []

for schema_name in EXPECTED_SCHEMAS:
    discovered_tables = discover_tables(schema_name)

    print(
        f"Discovered {len(discovered_tables)} governed table(s) "
        f"in schema {CATALOG_NAME}.{schema_name}"
    )

    for table_name in discovered_tables:
        governed_tables.append({
            "schema_name": schema_name,
            "table_name": table_name,
        })

add_governance_result(
    rule_name="governance_table_discovery",
    rule_description="Validates whether governed tables were discovered across expected schemas.",
    validation_status=QUALITY_PASSED if governed_tables else QUALITY_WARNING,
    total_records=len(EXPECTED_SCHEMAS),
    invalid_records=0 if governed_tables else len(EXPECTED_SCHEMAS),
    invalid_percentage=0.0 if governed_tables else 100.0,
    message=f"Discovered governed tables: {len(governed_tables)}",
    entity_name="governance",
    target_table=CATALOG_NAME,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execute Table Metadata Checks

# COMMAND ----------

for table_config in governed_tables:
    run_table_governance_checks(
        schema_name=table_config["schema_name"],
        table_name=table_config["table_name"],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persist Governance Results

# COMMAND ----------

governance_quality_log_df = build_governance_quality_log()

governance_quality_log_df.write.mode(
    "append"
).saveAsTable(DATA_QUALITY_LOG_TABLE)

print(
    f"Governance metadata quality results persisted into: "
    f"{DATA_QUALITY_LOG_TABLE}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Display Governance Results

# COMMAND ----------

display(governance_quality_log_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Governance Metadata Summary

# COMMAND ----------

failed_count = (
    governance_quality_log_df
    .filter("qlt_tx_status_validacao = 'FAILED'")
    .count()
)

warning_count = (
    governance_quality_log_df
    .filter("qlt_tx_status_validacao = 'WARNING'")
    .count()
)

passed_count = (
    governance_quality_log_df
    .filter("qlt_tx_status_validacao = 'PASSED'")
    .count()
)

print("=" * 90)
print("GOVERNANCE METADATA QUALITY SUMMARY")
print("=" * 90)
print(f"Passed validations: {passed_count}")
print(f"Warning validations: {warning_count}")
print(f"Failed validations: {failed_count}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# GOVERNANCE EXECUTION POLICY
# ============================================================

if failed_count > 0 and FAIL_ON_ERROR:
    raise Exception(
        f"Governance metadata validation failed with "
        f"{failed_count} failed validation(s)."
    )

if failed_count > 0:
    print(
        f"WARNING: Governance metadata validation finished with "
        f"{failed_count} failed validation(s)."
    )

if warning_count > 0:
    print(
        f"WARNING: Governance metadata validation finished with "
        f"{warning_count} warning validation(s). Review recommended metadata gaps."
    )

print("GOVERNANCE METADATA CHECKS COMPLETED")