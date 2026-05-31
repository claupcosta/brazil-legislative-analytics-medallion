# Databricks notebook source
# MAGIC %md
# MAGIC # Utils — Date and Time Functions
# MAGIC
# MAGIC **Notebook:** `utils_datetime`
# MAGIC
# MAGIC Provides reusable date and timestamp normalization functions used across
# MAGIC Bronze, Silver, Gold, Marts, Quality and Job notebooks.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Null-like datetime normalization
# MAGIC - Safe timestamp conversion
# MAGIC - Safe date conversion
# MAGIC - Timestamp validation helpers
# MAGIC - Date validation helpers
# MAGIC - Standardized datetime quality flags
# MAGIC - Reusable temporal transformation utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Centralize datetime treatment logic
# MAGIC - Standardize timestamp parsing across Medallion layers
# MAGIC - Prevent malformed datetime values from breaking pipelines
# MAGIC - Convert textual null values into real nulls
# MAGIC - Preserve raw Bronze datetime fields
# MAGIC - Support Silver quality validation rules
# MAGIC - Support deterministic and auditable temporal transformations
# MAGIC - Reduce duplicated parsing logic across notebooks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Bronze should preserve raw datetime values
# MAGIC - Silver is responsible for standardizing datetime fields
# MAGIC - Gold should consume already standardized temporal fields
# MAGIC - Invalid datetime strings are safely converted to null
# MAGIC - Quality flags should be created after safe conversion
# MAGIC - Naming conventions follow Portuguese mnemonic standards
# MAGIC - Comments and documentation are written in English
# MAGIC - This notebook must be executed with `%run` before calling datetime utilities
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
# DATETIME CONFIGURATION
# ============================================================

DATETIME_NULL_VALUES = [
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

DEFAULT_TIMESTAMP_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd",
]

DEFAULT_DATE_FORMATS = [
    "yyyy-MM-dd",
    "dd/MM/yyyy",
    "yyyy/MM/dd",
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
# NULL NORMALIZATION FUNCTIONS
# ============================================================

def normalize_null_datetime_string(
    column_name: str,
) -> Column:
    """
    Converts common textual null representations into real null values.

    This function should be applied before timestamp or date parsing.
    """

    return (
        F.when(
            F.trim(F.col(column_name).cast("string")).isin(DATETIME_NULL_VALUES),
            F.lit(None),
        )
        .otherwise(
            F.trim(F.col(column_name).cast("string"))
        )
    )


def normalize_null_datetime_column(
    dataframe: DataFrame,
    source_column: str,
    target_column: Optional[str] = None,
) -> DataFrame:
    """
    Adds or replaces a column with normalized datetime string values.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    output_column = target_column or source_column

    return dataframe.withColumn(
        output_column,
        normalize_null_datetime_string(
            column_name=source_column,
        ),
    )

# COMMAND ----------

# ============================================================
# SAFE CONVERSION EXPRESSIONS
# ============================================================

def safe_to_timestamp(
    column_name: str,
) -> Column:
    """
    Safely converts a string column into timestamp.

    Invalid values are converted to null instead of failing the pipeline.
    """

    normalized_column = normalize_null_datetime_string(
        column_name=column_name,
    )

    return F.expr(
        f"try_cast({column_name} AS TIMESTAMP)"
    )


def safe_to_timestamp_multi_format(
    column_name: str,
    timestamp_formats: Optional[List[str]] = None,
) -> Column:
    """
    Safely converts a string column into timestamp using multiple known formats.

    The first successfully parsed timestamp is returned.
    """

    timestamp_formats = timestamp_formats or DEFAULT_TIMESTAMP_FORMATS

    normalized_column = normalize_null_datetime_string(
        column_name=column_name,
    )

    parsed_columns = [
        F.to_timestamp(
            normalized_column,
            timestamp_format,
        )
        for timestamp_format in timestamp_formats
    ]

    return F.coalesce(
        *parsed_columns
    )


def safe_to_date(
    column_name: str,
) -> Column:
    """
    Safely converts a string column into date.

    Invalid values are converted to null instead of failing the pipeline.
    """

    return F.expr(
        f"try_cast({column_name} AS DATE)"
    )


def safe_to_date_multi_format(
    column_name: str,
    date_formats: Optional[List[str]] = None,
) -> Column:
    """
    Safely converts a string column into date using multiple known formats.

    The first successfully parsed date is returned.
    """

    date_formats = date_formats or DEFAULT_DATE_FORMATS

    normalized_column = normalize_null_datetime_string(
        column_name=column_name,
    )

    parsed_columns = [
        F.to_date(
            normalized_column,
            date_format,
        )
        for date_format in date_formats
    ]

    return F.coalesce(
        *parsed_columns
    )

# COMMAND ----------

# ============================================================
# DATAFRAME TRANSFORMATION FUNCTIONS
# ============================================================

def add_safe_timestamp_column(
    dataframe: DataFrame,
    source_column: str,
    target_column: str,
    use_multi_format: bool = False,
    timestamp_formats: Optional[List[str]] = None,
) -> DataFrame:
    """
    Adds a safely parsed timestamp column to a DataFrame.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    normalized_source_column = f"{source_column}__normalized_tmp"

    dataframe_normalized = dataframe.withColumn(
        normalized_source_column,
        normalize_null_datetime_string(
            column_name=source_column,
        ),
    )

    if use_multi_format:

        result_dataframe = dataframe_normalized.withColumn(
            target_column,
            safe_to_timestamp_multi_format(
                column_name=normalized_source_column,
                timestamp_formats=timestamp_formats,
            ),
        )

    else:

        result_dataframe = dataframe_normalized.withColumn(
            target_column,
            F.expr(
                f"try_cast({normalized_source_column} AS TIMESTAMP)"
            ),
        )

    return result_dataframe.drop(
        normalized_source_column,
    )


def add_safe_date_column(
    dataframe: DataFrame,
    source_column: str,
    target_column: str,
    use_multi_format: bool = False,
    date_formats: Optional[List[str]] = None,
) -> DataFrame:
    """
    Adds a safely parsed date column to a DataFrame.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    normalized_source_column = f"{source_column}__normalized_tmp"

    dataframe_normalized = dataframe.withColumn(
        normalized_source_column,
        normalize_null_datetime_string(
            column_name=source_column,
        ),
    )

    if use_multi_format:

        result_dataframe = dataframe_normalized.withColumn(
            target_column,
            safe_to_date_multi_format(
                column_name=normalized_source_column,
                date_formats=date_formats,
            ),
        )

    else:

        result_dataframe = dataframe_normalized.withColumn(
            target_column,
            F.expr(
                f"try_cast({normalized_source_column} AS DATE)"
            ),
        )

    return result_dataframe.drop(
        normalized_source_column,
    )


def add_timestamp_quality_flag(
    dataframe: DataFrame,
    timestamp_column: str,
    flag_column: str,
) -> DataFrame:
    """
    Adds a boolean flag indicating whether a timestamp column is valid.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=timestamp_column,
    )

    return dataframe.withColumn(
        flag_column,
        F.col(timestamp_column).isNotNull().cast("boolean"),
    )


def add_date_quality_flag(
    dataframe: DataFrame,
    date_column: str,
    flag_column: str,
) -> DataFrame:
    """
    Adds a boolean flag indicating whether a date column is valid.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=date_column,
    )

    return dataframe.withColumn(
        flag_column,
        F.col(date_column).isNotNull().cast("boolean"),
    )

# COMMAND ----------

# ============================================================
# HIGH-LEVEL STANDARDIZATION FUNCTIONS
# ============================================================

def standardize_timestamp_field(
    dataframe: DataFrame,
    source_column: str,
    target_timestamp_column: str,
    validity_flag_column: Optional[str] = None,
    use_multi_format: bool = False,
    timestamp_formats: Optional[List[str]] = None,
) -> DataFrame:
    """
    Standardizes a raw datetime field into a parsed timestamp column
    and optionally creates a validity flag.
    """

    result_dataframe = add_safe_timestamp_column(
        dataframe=dataframe,
        source_column=source_column,
        target_column=target_timestamp_column,
        use_multi_format=use_multi_format,
        timestamp_formats=timestamp_formats,
    )

    if validity_flag_column:

        result_dataframe = add_timestamp_quality_flag(
            dataframe=result_dataframe,
            timestamp_column=target_timestamp_column,
            flag_column=validity_flag_column,
        )

    return result_dataframe


def standardize_date_field(
    dataframe: DataFrame,
    source_column: str,
    target_date_column: str,
    validity_flag_column: Optional[str] = None,
    use_multi_format: bool = False,
    date_formats: Optional[List[str]] = None,
) -> DataFrame:
    """
    Standardizes a raw date field into a parsed date column
    and optionally creates a validity flag.
    """

    result_dataframe = add_safe_date_column(
        dataframe=dataframe,
        source_column=source_column,
        target_column=target_date_column,
        use_multi_format=use_multi_format,
        date_formats=date_formats,
    )

    if validity_flag_column:

        result_dataframe = add_date_quality_flag(
            dataframe=result_dataframe,
            date_column=target_date_column,
            flag_column=validity_flag_column,
        )

    return result_dataframe

# COMMAND ----------

# ============================================================
# PROFILING HELPERS
# ============================================================

def get_invalid_timestamp_records(
    dataframe: DataFrame,
    source_column: str,
    parsed_timestamp_column: str,
) -> DataFrame:
    """
    Returns records where the raw datetime field is informed
    but the parsed timestamp column is null.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    validate_column_exists(
        dataframe=dataframe,
        column_name=parsed_timestamp_column,
    )

    return dataframe.filter(
        normalize_null_datetime_string(source_column).isNotNull()
        & F.col(parsed_timestamp_column).isNull()
    )


def get_invalid_date_records(
    dataframe: DataFrame,
    source_column: str,
    parsed_date_column: str,
) -> DataFrame:
    """
    Returns records where the raw date field is informed
    but the parsed date column is null.
    """

    validate_column_exists(
        dataframe=dataframe,
        column_name=source_column,
    )

    validate_column_exists(
        dataframe=dataframe,
        column_name=parsed_date_column,
    )

    return dataframe.filter(
        normalize_null_datetime_string(source_column).isNotNull()
        & F.col(parsed_date_column).isNull()
    )

# COMMAND ----------

# ============================================================
# NOTEBOOK LOAD CONFIRMATION
# ============================================================

print("utils_datetime loaded successfully.")
print(
    "Available functions: "
    "normalize_null_datetime_string, "
    "safe_to_timestamp, "
    "safe_to_timestamp_multi_format, "
    "safe_to_date, "
    "safe_to_date_multi_format, "
    "add_safe_timestamp_column, "
    "add_safe_date_column, "
    "add_timestamp_quality_flag, "
    "add_date_quality_flag, "
    "standardize_timestamp_field, "
    "standardize_date_field, "
    "get_invalid_timestamp_records, "
    "get_invalid_date_records"
)