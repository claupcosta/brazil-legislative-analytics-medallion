# Databricks notebook source
# MAGIC %md
# MAGIC # Utils — Hash Functions
# MAGIC
# MAGIC **Notebook:** `utils_hash`
# MAGIC
# MAGIC Provides reusable deterministic hashing functions used across Bronze,
# MAGIC Silver, Gold, Marts, Quality and Job notebooks.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - DataFrame validation helpers
# MAGIC - Column validation helpers
# MAGIC - Null normalization strategy
# MAGIC - SHA2-256 hash generation
# MAGIC - Record hash generation
# MAGIC - Business key hash generation
# MAGIC - Full-row hash generation
# MAGIC - Hash comparison helpers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Centralize deterministic hashing logic
# MAGIC - Standardize hash generation across Medallion layers
# MAGIC - Support deduplication and lineage use cases
# MAGIC - Support incremental and CDC-oriented processing
# MAGIC - Validate required hash input columns
# MAGIC - Normalize null values before hash generation
# MAGIC - Provide reusable hash utilities for all pipeline notebooks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - SHA2-256 is used as the standard hashing algorithm
# MAGIC - Null values are normalized using a fixed placeholder
# MAGIC - Hashes are deterministic and reproducible
# MAGIC - Hash columns support traceability and governance
# MAGIC - Naming conventions follow Portuguese mnemonic standards
# MAGIC - Comments and documentation are written in English
# MAGIC - This notebook must be executed with `%run` before calling hash utilities
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/governance/traceability.md`
# MAGIC - `/docs/standards/naming_conventions.md`
# MAGIC - `/docs/decisions/silver_layer_strategy.md`

# COMMAND ----------

# MAGIC %run ./utils_config

# COMMAND ----------

from typing import List, Optional

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

# COMMAND ----------

# ============================================================
# HASH CONFIGURATION
# ============================================================

HASH_NULL_VALUE = "__NULL__"
HASH_SEPARATOR = "||"
HASH_BITS = 256

# COMMAND ----------

# ============================================================
# VALIDATION FUNCTIONS
# ============================================================

def validate_dataframe(
    dataframe: DataFrame,
) -> None:
    """
    Validates whether the input object is a valid Spark DataFrame.
    """

    if dataframe is None:
        raise ValueError("Input DataFrame cannot be None.")

    if not hasattr(dataframe, "columns"):
        raise TypeError("Input object must be a Spark DataFrame.")

    if len(dataframe.columns) == 0:
        raise ValueError("Input DataFrame must contain at least one column.")


def validate_columns(
    dataframe: DataFrame,
    columns: List[str],
) -> None:
    """
    Validates whether all requested columns exist in the DataFrame.
    """

    validate_dataframe(
        dataframe=dataframe,
    )

    if columns is None or len(columns) == 0:
        raise ValueError("Column list cannot be empty.")

    missing_columns = [
        column_name
        for column_name in columns
        if column_name not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "The following columns do not exist in the DataFrame: "
            f"{missing_columns}"
        )

# COMMAND ----------

# ============================================================
# HASH EXPRESSION FUNCTIONS
# ============================================================

def normalize_column(
    column_name: str,
) -> Column:
    """
    Normalizes a column before hash generation.

    Null values are replaced by a fixed placeholder to ensure
    deterministic and reproducible hash outputs.
    """

    return F.coalesce(
        F.col(column_name).cast("string"),
        F.lit(HASH_NULL_VALUE),
    )


def generate_hash(
    columns: List[str],
) -> Column:
    """
    Generates a SHA2-256 hash expression based on a list of columns.
    """

    if columns is None or len(columns) == 0:
        raise ValueError("Column list cannot be empty.")

    normalized_columns = [
        normalize_column(
            column_name=column_name,
        )
        for column_name in columns
    ]

    return F.sha2(
        F.concat_ws(
            HASH_SEPARATOR,
            *normalized_columns,
        ),
        HASH_BITS,
    )

# COMMAND ----------

# ============================================================
# DATAFRAME HASH FUNCTIONS
# ============================================================

def add_hash(
    dataframe: DataFrame,
    columns: List[str],
    hash_column: str = "aud_tx_hash_registro",
) -> DataFrame:
    """
    Adds a deterministic record hash column to a Spark DataFrame.
    """

    validate_columns(
        dataframe=dataframe,
        columns=columns,
    )

    try:

        return dataframe.withColumn(
            hash_column,
            generate_hash(
                columns=columns,
            ),
        )

    except Exception as error:

        raise RuntimeError(
            f"Failed to add hash column '{hash_column}'. "
            f"Original error: {str(error)}"
        )


def add_all_hash(
    dataframe: DataFrame,
    hash_column: str = "aud_tx_hash_registro",
    excluded_columns: Optional[List[str]] = None,
) -> DataFrame:
    """
    Adds a hash column using all DataFrame columns except excluded columns.
    """

    validate_dataframe(
        dataframe=dataframe,
    )

    excluded_columns = excluded_columns or []

    hash_columns = [
        column_name
        for column_name in dataframe.columns
        if column_name not in excluded_columns
    ]

    return add_hash(
        dataframe=dataframe,
        columns=hash_columns,
        hash_column=hash_column,
    )


def add_key_hash(
    dataframe: DataFrame,
    key_columns: List[str],
    hash_column: str = "aud_tx_hash_chave_negocio",
) -> DataFrame:
    """
    Adds a deterministic business key hash column to a Spark DataFrame.
    """

    return add_hash(
        dataframe=dataframe,
        columns=key_columns,
        hash_column=hash_column,
    )


def get_hash_columns(
    dataframe: DataFrame,
    excluded_columns: Optional[List[str]] = None,
) -> List[str]:
    """
    Returns the list of columns eligible for hash generation.
    """

    validate_dataframe(
        dataframe=dataframe,
    )

    excluded_columns = excluded_columns or []

    return [
        column_name
        for column_name in dataframe.columns
        if column_name not in excluded_columns
    ]


def compare_hashes(
    source_dataframe: DataFrame,
    target_dataframe: DataFrame,
    hash_column: str = "aud_tx_hash_registro",
) -> DataFrame:
    """
    Returns source records that are not present in target based on the hash column.
    """

    validate_columns(
        dataframe=source_dataframe,
        columns=[hash_column],
    )

    validate_columns(
        dataframe=target_dataframe,
        columns=[hash_column],
    )

    return (
        source_dataframe.alias("source")
        .join(
            target_dataframe
            .select(hash_column)
            .alias("target"),
            on=hash_column,
            how="left_anti",
        )
    )

# COMMAND ----------

# ============================================================
# NOTEBOOK LOAD CONFIRMATION
# ============================================================

print("utils_hash loaded successfully.")
print("Available functions: add_hash, add_all_hash, add_key_hash, get_hash_columns, compare_hashes")