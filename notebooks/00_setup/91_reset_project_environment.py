# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Layer — Project Environment Reset
# MAGIC
# MAGIC **Notebook:** `91_reset_project_environment`  
# MAGIC **Layer:** `Setup`  
# MAGIC **Source/Endpoint:** `Internal Spark SQL Commands`  
# MAGIC **Target:** `Project tables and optional schema cleanup`
# MAGIC
# MAGIC Resets the Brazil Legislative Analytics Medallion environment by removing
# MAGIC tables and optional schemas created during development and testing workflows.
# MAGIC
# MAGIC This notebook supports environment cleanup operations required for
# MAGIC development iterations, integration testing and architecture refactoring.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Remove audit tables
# MAGIC - Remove Bronze tables
# MAGIC - Remove Silver tables
# MAGIC - Remove Gold tables
# MAGIC - Remove Mart tables
# MAGIC - Optionally remove project schemas
# MAGIC - Validate remaining project structures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Intended for development environments only
# MAGIC - Permanently removes project objects
# MAGIC - Schema removal is optional and controlled by configuration
# MAGIC - Recommended to preserve schemas during standard reset operations
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/governance/data_governance.md`
# MAGIC - `/docs/operations/environment_reset.md`

# COMMAND ----------

# MAGIC %run ./01_project_config

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("91 - RESET PROJECT ENVIRONMENT")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Environment: {PROJECT_ENVIRONMENT}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# RESET CONFIGURATION
# ============================================================
#
# True  -> remove schemas after dropping tables
# False -> keep schemas and remove only tables
#
# ============================================================

DROP_SCHEMAS = False

# COMMAND ----------

# ============================================================
# SCHEMAS
# ============================================================

SCHEMAS_TO_RESET = [
    SCHEMA_AUDIT,
    SCHEMA_BRONZE,
    SCHEMA_SILVER,
    SCHEMA_GOLD,
    SCHEMA_MARTS,
]

# COMMAND ----------

# ============================================================
# TABLE COLLECTIONS
# ============================================================

AUDIT_TABLES = [
    AUD_TB_LOG_EXECUCAO_PIPELINE,
    AUD_TB_LOG_ERROS_PIPELINE,
    AUD_TB_LOG_QUALIDADE_DADOS,
]

BRONZE_TABLE_LIST = list(BRONZE_TABLES.values())
SILVER_TABLE_LIST = list(SILVER_TABLES.values())
GOLD_DIMENSION_TABLE_LIST = list(GOLD_DIMENSION_TABLES.values())
GOLD_FACT_TABLE_LIST = list(GOLD_FACT_TABLES.values())
MART_TABLE_LIST = list(MART_TABLES.values())

# COMMAND ----------

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def drop_table(
    schema_name: str,
    table_name: str,
) -> None:
    """
    Drops a table if it exists.
    """

    full_table_name = (
        f"{CATALOG_NAME}."
        f"{schema_name}."
        f"{table_name}"
    )

    spark.sql(f"""
    DROP TABLE IF EXISTS {full_table_name}
    """)

    print(f"Table removed: {full_table_name}")

# COMMAND ----------

def drop_schema(
    schema_name: str,
) -> None:
    """
    Drops a schema if it exists.
    """

    full_schema_name = (
        f"{CATALOG_NAME}."
        f"{schema_name}"
    )

    spark.sql(f"""
    DROP SCHEMA IF EXISTS {full_schema_name}
    """)

    print(f"Schema removed: {full_schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Remove Audit Tables

# COMMAND ----------

for table_name in AUDIT_TABLES:
    drop_table(
        schema_name=SCHEMA_AUDIT,
        table_name=table_name,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Remove Bronze Tables

# COMMAND ----------

for table_name in BRONZE_TABLE_LIST:
    drop_table(
        schema_name=SCHEMA_BRONZE,
        table_name=table_name,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Remove Silver Tables

# COMMAND ----------

for table_name in SILVER_TABLE_LIST:
    drop_table(
        schema_name=SCHEMA_SILVER,
        table_name=table_name,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Remove Gold Dimension Tables

# COMMAND ----------

for table_name in GOLD_DIMENSION_TABLE_LIST:
    drop_table(
        schema_name=SCHEMA_GOLD,
        table_name=table_name,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Remove Gold Fact Tables

# COMMAND ----------

for table_name in GOLD_FACT_TABLE_LIST:
    drop_table(
        schema_name=SCHEMA_GOLD,
        table_name=table_name,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Remove Analytical Mart Tables

# COMMAND ----------

for table_name in MART_TABLE_LIST:
    drop_table(
        schema_name=SCHEMA_MARTS,
        table_name=table_name,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Optional Schema Removal

# COMMAND ----------

if DROP_SCHEMAS:

    print("=" * 90)
    print("REMOVING SCHEMAS")
    print("=" * 90)

    for schema_name in SCHEMAS_TO_RESET:
        drop_schema(
            schema_name=schema_name,
        )

else:

    print("=" * 90)
    print("SCHEMA REMOVAL DISABLED")
    print("=" * 90)
    print("Schemas were preserved.")
    print("Only tables were removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Remaining Structures

# COMMAND ----------

remaining_schemas_df = spark.sql(f"""
SHOW SCHEMAS IN {CATALOG_NAME}
""")

display(remaining_schemas_df)

# COMMAND ----------

# ============================================================
# RESET SUMMARY
# ============================================================

print("=" * 90)
print("RESET SUMMARY")
print("=" * 90)
print(f"Catalog: {CATALOG_NAME}")
print(f"Drop Schemas Enabled: {DROP_SCHEMAS}")

print("=" * 90)
print("RESETTED SCHEMAS")

for schema_name in SCHEMAS_TO_RESET:
    print(f"{CATALOG_NAME}.{schema_name}")

print("=" * 90)
print("PROJECT ENVIRONMENT RESET COMPLETED")
print("=" * 90)
