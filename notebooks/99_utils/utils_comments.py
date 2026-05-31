# Databricks notebook source
# MAGIC %md
# MAGIC # Utils — Governance Comments Functions
# MAGIC
# MAGIC **Notebook:** `utils_comments`
# MAGIC
# MAGIC Provides reusable governance comment functions used across
# MAGIC Bronze, Silver, Gold, Marts, Audit and Quality notebooks.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Table comment application
# MAGIC - Column comment application
# MAGIC - Bulk governance comment execution
# MAGIC - Safe SQL comment escaping
# MAGIC - Metadata governance helpers
# MAGIC - Reusable documentation utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Centralize governance comment logic
# MAGIC - Standardize metadata documentation across Medallion layers
# MAGIC - Reduce duplicated SQL comment logic
# MAGIC - Improve catalog discoverability
# MAGIC - Support auditability and governance standards
# MAGIC - Support reusable metadata management
# MAGIC - Improve maintainability of notebook implementations
# MAGIC - Ensure consistent documentation across tables and columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Comments are applied after table creation
# MAGIC - Comments should always be written in English
# MAGIC - Table and column names follow Portuguese mnemonic standards
# MAGIC - Governance metadata is part of the project deliverables
# MAGIC - Unity Catalog comments improve traceability and discoverability
# MAGIC - This notebook should be executed with `%run`
# MAGIC - Safe escaping is automatically applied to SQL comments
# MAGIC - Supports Bronze, Silver, Gold, Marts and Audit schemas
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/governance/data_governance.md`
# MAGIC - `/docs/governance/naming_conventions.md`
# MAGIC - `/docs/governance/metadata_standards.md`
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC

# COMMAND ----------

from typing import Dict, Optional

# COMMAND ----------

# ============================================================
# VALIDATION FUNCTIONS
# ============================================================

def validate_table_name(
    table_name: str,
) -> None:
    """
    Validates whether the table name is properly informed.
    """

    if table_name is None:
        raise ValueError("Table name cannot be None.")

    if str(table_name).strip() == "":
        raise ValueError("Table name cannot be empty.")


def validate_column_comments(
    column_comments: Dict[str, str],
) -> None:
    """
    Validates whether the column comments dictionary is valid.
    """

    if column_comments is None:
        raise ValueError("Column comments dictionary cannot be None.")

    if len(column_comments) == 0:
        raise ValueError("Column comments dictionary cannot be empty.")

# COMMAND ----------

# ============================================================
# SQL ESCAPE FUNCTIONS
# ============================================================

def escape_sql_comment(
    comment: Optional[str],
) -> str:
    """
    Escapes single quotes from SQL comments.
    """

    if comment is None:
        return ""

    return str(comment).replace("'", "''").strip()

# COMMAND ----------

# ============================================================
# TABLE COMMENT FUNCTIONS
# ============================================================

def apply_table_comment(
    table_name: str,
    table_comment: str,
) -> None:
    """
    Applies a governance comment to a table.
    """

    validate_table_name(
        table_name=table_name,
    )

    escaped_comment = escape_sql_comment(
        comment=table_comment,
    )

    sql_statement = f"""
    COMMENT ON TABLE {table_name}
    IS '{escaped_comment}'
    """

    spark.sql(sql_statement)

    print(
        f"[SUCCESS] Table comment applied: {table_name}"
    )

# COMMAND ----------

# ============================================================
# COLUMN COMMENT FUNCTIONS
# ============================================================

def apply_column_comment(
    table_name: str,
    column_name: str,
    column_comment: str,
) -> None:
    """
    Applies a governance comment to a table column.
    """

    validate_table_name(
        table_name=table_name,
    )

    if column_name is None or str(column_name).strip() == "":
        raise ValueError("Column name cannot be empty.")

    escaped_comment = escape_sql_comment(
        comment=column_comment,
    )

    sql_statement = f"""
    ALTER TABLE {table_name}
    ALTER COLUMN {column_name}
    COMMENT '{escaped_comment}'
    """

    spark.sql(sql_statement)

    print(
        f"[SUCCESS] Column comment applied: "
        f"{table_name}.{column_name}"
    )

# COMMAND ----------

# ============================================================
# BULK COLUMN COMMENT FUNCTIONS
# ============================================================

def apply_column_comments(
    table_name: str,
    column_comments: Dict[str, str],
) -> None:
    """
    Applies governance comments to multiple columns.
    """

    validate_table_name(
        table_name=table_name,
    )

    validate_column_comments(
        column_comments=column_comments,
    )

    for column_name, column_comment in column_comments.items():

        apply_column_comment(
            table_name=table_name,
            column_name=column_name,
            column_comment=column_comment,
        )

# COMMAND ----------

# ============================================================
# FULL GOVERNANCE COMMENT APPLICATION
# ============================================================

def apply_governance_comments(
    table_name: str,
    table_comment: str,
    column_comments: Dict[str, str],
) -> None:
    """
    Applies table and column governance comments.
    """

    apply_table_comment(
        table_name=table_name,
        table_comment=table_comment,
    )

    apply_column_comments(
        table_name=table_name,
        column_comments=column_comments,
    )

    print(
        f"[SUCCESS] Governance comments fully applied: "
        f"{table_name}"
    )

# COMMAND ----------

# ============================================================
# COMMENT PROFILING FUNCTIONS
# ============================================================

def generate_missing_comment_report(
    table_name: str,
) -> None:
    """
    Displays columns without comments for governance validation.
    """

    validate_table_name(
        table_name=table_name,
    )

    query = f"""
    SELECT
        column_name,
        comment
    FROM information_schema.columns
    WHERE table_catalog = current_catalog()
      AND CONCAT(
            table_schema,
            '.',
            table_name
          ) = SPLIT('{table_name}', '.', 2)[1]
    """

    result_df = spark.sql(query)

    missing_comments_df = result_df.filter(
        "comment IS NULL OR TRIM(comment) = ''"
    )

    print(
        f"Missing comments validation for table: {table_name}"
    )

    display(missing_comments_df)

# COMMAND ----------

# ============================================================
# NOTEBOOK LOAD CONFIRMATION
# ============================================================

print("utils_comments loaded successfully.")

print(
    "Available functions: "
    "apply_table_comment, "
    "apply_column_comment, "
    "apply_column_comments, "
    "apply_governance_comments, "
    "generate_missing_comment_report"
)