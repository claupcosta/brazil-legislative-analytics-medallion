# Databricks notebook source
# MAGIC %md
# MAGIC # Utils — Text Standardization Functions
# MAGIC
# MAGIC **Notebook:** `utils_text`
# MAGIC
# MAGIC Provides reusable text normalization and standardization functions used across
# MAGIC Bronze, Silver, Gold, Marts, Quality and Job notebooks.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Text trimming utilities
# MAGIC - Null-like text normalization
# MAGIC - Uppercase and lowercase standardization
# MAGIC - Double whitespace cleanup
# MAGIC - Accent-safe normalization helpers
# MAGIC - Empty string handling
# MAGIC - Boolean informational flags
# MAGIC - Reusable textual quality utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Centralize text normalization logic
# MAGIC - Standardize textual transformations across Medallion layers
# MAGIC - Prevent inconsistent textual formatting
# MAGIC - Convert invalid textual representations into null values
# MAGIC - Support Silver quality validation rules
# MAGIC - Support deterministic and auditable text transformations
# MAGIC - Reduce duplicated transformation logic across notebooks
# MAGIC - Improve downstream analytical consistency
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze should preserve raw textual values whenever possible
# MAGIC - Silver is responsible for standardizing textual attributes
# MAGIC - Gold should consume already standardized fields
# MAGIC - Invalid textual values are safely converted to null
# MAGIC - Text normalization must be deterministic and reproducible
# MAGIC - Naming conventions follow Portuguese mnemonic standards
# MAGIC - Comments and documentation are written in English
# MAGIC - This notebook must be executed with `%run` before calling text utilities
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/decisions/silver_layer_strategy.md`
# MAGIC - `/docs/governance/data_quality.md`
# MAGIC - `/docs/governance/traceability.md`
# MAGIC - `/docs/standards/naming_conventions.md`

# COMMAND ----------


from typing import List, Optional

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

# COMMAND ----------

# ============================================================
# TEXT CONFIGURATION
# ============================================================

TEXT_NULL_VALUES = [
    "",
    " ",
    "None",
    "none",
    "NONE",
    "Null",
    "null",
    "NULL",
    "NaN",
    "nan",
    "NAN",
    "-",
]

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


def validate_column_exists(
    dataframe: DataFrame,
    column_name: str,
) -> None:
    """
    Validates whether a column exists in the DataFrame.
    """

    validate_dataframe(
        dataframe=dataframe,
    )

    if column_name not in dataframe.columns:
        raise ValueError(
            f"Column '{column_name}' does not exist in the DataFrame."
        )

# COMMAND ----------

# ============================================================
# BASIC NORMALIZATION FUNCTIONS
# ============================================================

def normalize_text(
    column_name: str,
) -> Column:
    """
    Applies standard text normalization rules.

    Rules applied:
    - Trim leading and trailing spaces
    - Replace multiple spaces with a single space
    - Convert null-like values into real nulls
    """

    normalized_column = F.trim(
        F.col(column_name).cast("string")
    )

    normalized_column = F.regexp_replace(
        normalized_column,
        r"\s+",
        " "
    )

    return (
        F.when(
            normalized_column.isNull(),
            F.lit(None)
        )
        .when(
            normalized_column.isin(TEXT_NULL_VALUES),
            F.lit(None)
        )
        .otherwise(normalized_column)
    )

# COMMAND ----------

# ============================================================
# CASE STANDARDIZATION FUNCTIONS
# ============================================================

def normalize_upper_text(
    column_name: str,
) -> Column:
    """
    Normalizes and converts text to uppercase.
    """

    return F.upper(
        normalize_text(column_name)
    )


def normalize_lower_text(
    column_name: str,
) -> Column:
    """
    Normalizes and converts text to lowercase.
    """

    return F.lower(
        normalize_text(column_name)
    )


def normalize_title_text(
    column_name: str,
) -> Column:
    """
    Normalizes and converts text to title case.
    """

    return F.initcap(
        normalize_text(column_name)
    )

# COMMAND ----------

# ============================================================
# DATAFRAME TRANSFORMATION FUNCTIONS
# ============================================================

def add_normalized_text_column(
    dataframe: DataFrame,
    source_column: str,
    target_column: Optional[str] = None,
) -> DataFrame:
    """
    Adds a normalized text column to a DataFrame.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    output_column = target_column or source_column

    return dataframe.withColumn(
        output_column,
        normalize_text(
            source_column
        )
    )


def add_upper_text_column(
    dataframe: DataFrame,
    source_column: str,
    target_column: str,
) -> DataFrame:
    """
    Adds an uppercase normalized text column.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    return dataframe.withColumn(
        target_column,
        normalize_upper_text(
            source_column
        )
    )


def add_lower_text_column(
    dataframe: DataFrame,
    source_column: str,
    target_column: str,
) -> DataFrame:
    """
    Adds a lowercase normalized text column.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    return dataframe.withColumn(
        target_column,
        normalize_lower_text(
            source_column
        )
    )


def add_title_text_column(
    dataframe: DataFrame,
    source_column: str,
    target_column: str,
) -> DataFrame:
    """
    Adds a title case normalized text column.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    return dataframe.withColumn(
        target_column,
        normalize_title_text(
            source_column
        )
    )

# COMMAND ----------

# ============================================================
# QUALITY FLAG FUNCTIONS
# ============================================================

def add_text_informed_flag(
    dataframe: DataFrame,
    source_column: str,
    flag_column: str,
) -> DataFrame:
    """
    Adds a boolean flag indicating whether a text field is informed.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    return dataframe.withColumn(
        flag_column,
        normalize_text(source_column).isNotNull()
    )


def add_text_length_column(
    dataframe: DataFrame,
    source_column: str,
    target_column: str,
) -> DataFrame:
    """
    Adds a column containing the normalized text length.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    return dataframe.withColumn(
        target_column,
        F.length(
            normalize_text(source_column)
        )
    )

# COMMAND ----------

# ============================================================
# FILTERING AND PROFILING FUNCTIONS
# ============================================================

def get_invalid_text_records(
    dataframe: DataFrame,
    source_column: str,
) -> DataFrame:
    """
    Returns records where the text field is invalid or empty.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    return dataframe.filter(
        normalize_text(source_column).isNull()
    )


def get_valid_text_records(
    dataframe: DataFrame,
    source_column: str,
) -> DataFrame:
    """
    Returns records where the text field is valid.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    return dataframe.filter(
        normalize_text(source_column).isNotNull()
    )

# COMMAND ----------

# ============================================================
# MULTI-COLUMN STANDARDIZATION
# ============================================================

def normalize_multiple_columns(
    dataframe: DataFrame,
    columns: List[str],
) -> DataFrame:
    """
    Applies text normalization to multiple columns.
    """

    validate_dataframe(
        dataframe=dataframe,
    )

    result_dataframe = dataframe

    for column_name in columns:

        validate_column_exists(
            dataframe=result_dataframe,
            column_name=column_name,
        )

        result_dataframe = result_dataframe.withColumn(
            column_name,
            normalize_text(column_name)
        )

    return result_dataframe

# COMMAND ----------

# ============================================================
# NOTEBOOK LOAD CONFIRMATION
# ============================================================

print("utils_text loaded successfully.")

print(
    "Available functions: "
    "normalize_text, "
    "normalize_upper_text, "
    "normalize_lower_text, "
    "normalize_title_text, "
    "add_normalized_text_column, "
    "add_upper_text_column, "
    "add_lower_text_column, "
    "add_title_text_column, "
    "add_text_informed_flag, "
    "add_text_length_column, "
    "get_invalid_text_records, "
    "get_valid_text_records, "
    "normalize_multiple_columns"
)