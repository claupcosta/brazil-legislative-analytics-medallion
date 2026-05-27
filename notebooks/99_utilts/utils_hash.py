# Databricks notebook source
# MAGIC %md
# MAGIC # Utils Layer — Hash Utilities
# MAGIC
# MAGIC **Notebook:** `utils_hash`  
# MAGIC **Layer:** `Utils`  
# MAGIC **Source/Endpoint:** `Spark DataFrames`  
# MAGIC **Target:** `Reusable hash generation and comparison functions`
# MAGIC
# MAGIC Provides reusable hash utilities for record traceability,
# MAGIC deduplication and incremental processing workflows.
# MAGIC
# MAGIC This notebook centralizes deterministic hash generation logic
# MAGIC used across Bronze, Silver, Gold and Marts layers.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Generate deterministic row-level hashes
# MAGIC - Validate hash input columns before execution
# MAGIC - Normalize null values during hash generation
# MAGIC - Support deduplication workflows
# MAGIC - Support incremental and replay processing strategies
# MAGIC - Compare records across pipeline executions
# MAGIC - Improve traceability across Medallion layers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Hash generation uses SHA2-256
# MAGIC - Null values are normalized before hash generation
# MAGIC - Supports business key and full-record hash strategies
# MAGIC - Shared utility notebook across Medallion workflows
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/governance/data_lineage.md`
# MAGIC - `/docs/standards/coding_standards.md`
# MAGIC - `/docs/architecture/medallion_architecture.md`

# COMMAND ----------

# MAGIC %run ./utils_config

# COMMAND ----------

from typing import List, Optional
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

# COMMAND ----------

HASH_NULL_VALUE = "__NULL__"
HASH_SEPARATOR = "||"
HASH_BITS = 256

# COMMAND ----------

def validate_dataframe(dataframe: DataFrame) -> None:
    """
    Validates whether the input DataFrame is valid for hash operations.
    """

    if dataframe is None:
        raise ValueError("Input DataFrame cannot be None.")

    if not hasattr(dataframe, "columns"):
        raise TypeError("Input object must be a Spark DataFrame.")

    if len(dataframe.columns) == 0:
        raise ValueError("Input DataFrame must contain at least one column.")

# COMMAND ----------

def validate_columns(
    dataframe: DataFrame,
    columns: List[str],
) -> None:
    """
    Validates whether all requested columns exist in the DataFrame.
    """

    validate_dataframe(dataframe)

    if columns is None or len(columns) == 0:
        raise ValueError("Column list cannot be empty.")

    missing_columns = [
        column
        for column in columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "The following columns do not exist in the DataFrame: "
            f"{missing_columns}"
        )

# COMMAND ----------

def normalize_column(column: str) -> Column:
    """
    Normalizes a column before hash generation.
    """

    return F.coalesce(
        F.col(column).cast("string"),
        F.lit(HASH_NULL_VALUE)
    )

# COMMAND ----------

def generate_hash(columns: List[str]) -> Column:
    """
    Generates a SHA2-256 hash expression based on a list of columns.
    """

    if columns is None or len(columns) == 0:
        raise ValueError("Column list cannot be empty.")

    normalized_columns = [
        normalize_column(column)
        for column in columns
    ]

    return F.sha2(
        F.concat_ws(HASH_SEPARATOR, *normalized_columns),
        HASH_BITS
    )

# COMMAND ----------

def add_hash(
    dataframe: DataFrame,
    columns: List[str],
    hash_column: str = "aud_tx_hash_registro",
) -> DataFrame:
    """
    Adds a deterministic record hash column to a DataFrame.
    """

    validate_columns(
        dataframe=dataframe,
        columns=columns
    )

    try:
        return dataframe.withColumn(
            hash_column,
            generate_hash(columns)
        )

    except Exception as error:
        raise RuntimeError(
            f"Failed to add hash column '{hash_column}'. "
            f"Original error: {str(error)}"
        )

# COMMAND ----------

def add_all_hash(
    dataframe: DataFrame,
    hash_column: str = "aud_tx_hash_registro",
    excluded_columns: Optional[List[str]] = None,
) -> DataFrame:
    """
    Adds a hash column using all DataFrame columns except excluded columns.
    """

    validate_dataframe(dataframe)

    excluded_columns = excluded_columns or []

    hash_columns = [
        column
        for column in dataframe.columns
        if column not in excluded_columns
    ]

    return add_hash(
        dataframe=dataframe,
        columns=hash_columns,
        hash_column=hash_column
    )

# COMMAND ----------

def add_key_hash(
    dataframe: DataFrame,
    key_columns: List[str],
    hash_column: str = "aud_tx_hash_chave_negocio",
) -> DataFrame:
    """
    Adds a deterministic business key hash column to a DataFrame.
    """

    return add_hash(
        dataframe=dataframe,
        columns=key_columns,
        hash_column=hash_column
    )

# COMMAND ----------

def get_hash_columns(
    dataframe: DataFrame,
    excluded_columns: Optional[List[str]] = None,
) -> List[str]:
    """
    Returns the list of columns eligible for hash generation.
    """

    validate_dataframe(dataframe)

    excluded_columns = excluded_columns or []

    return [
        column
        for column in dataframe.columns
        if column not in excluded_columns
    ]

# COMMAND ----------

def compare_hashes(
    source_dataframe: DataFrame,
    target_dataframe: DataFrame,
    hash_column: str = "aud_tx_hash_registro",
) -> DataFrame:
    """
    Returns source records that are not present in target based on the hash column.
    """

    validate_columns(source_dataframe, [hash_column])
    validate_columns(target_dataframe, [hash_column])

    return (
        source_dataframe.alias("source")
        .join(
            target_dataframe.select(hash_column).alias("target"),
            on=hash_column,
            how="left_anti"
        )
    )

# COMMAND ----------

print("utils_hash loaded successfully.")