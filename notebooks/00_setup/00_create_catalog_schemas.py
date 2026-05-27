# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Layer — Catalog and Schema Initialization
# MAGIC
# MAGIC **Notebook:** `00_create_catalog_schemas`  
# MAGIC **Layer:** `Setup`  
# MAGIC **Source/Endpoint:** `Internal Spark SQL Commands`  
# MAGIC **Target:** `brazil_legislative_analytics catalog and Medallion schemas`
# MAGIC
# MAGIC Creates the main catalog and schemas used by the Brazil Legislative Analytics Medallion project.
# MAGIC
# MAGIC This notebook initializes the project governance structure,
# MAGIC creating the required schemas for data ingestion, curation,
# MAGIC analytics and monitoring workflows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Create the project catalog
# MAGIC - Create Medallion architecture schemas
# MAGIC - Apply governance comments to schemas
# MAGIC - Set the active catalog context
# MAGIC - Validate schema creation results
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent setup execution
# MAGIC - Uses Spark SQL DDL statements
# MAGIC - Schema comments support governance and traceability
# MAGIC - Silver layer uses a unified architecture model
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/governance/data_governance.md`
# MAGIC - `/docs/standards/notebook_standards.md`

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

CATALOG_NAME = "brazil_legislative_analytics"

REQUIRED_SCHEMAS = {
    "audit": "Governance schema containing audit logs, error logs and data quality validation logs.",
    "bronze": "Raw ingestion layer containing data extracted from source APIs or CSV fallback files with minimal transformation.",
    "silver": "Unified curation layer responsible for cleansing, typing, standardization, deduplication, validation and enrichment.",
    "gold": "Dimensional analytical layer containing dimensions and fact tables following the Star Schema model.",
    "marts": "Analytical consumption layer containing business-oriented marts and final analytical outputs.",
}

# COMMAND ----------

execution_timestamp = datetime.now()

print("=" * 80)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("00 - CREATE CATALOG AND SCHEMAS")
print("=" * 80)
print(f"Execution timestamp: {execution_timestamp}")
print(f"Target catalog: {CATALOG_NAME}")
print("=" * 80)

# COMMAND ----------

# ============================================================
# CREATE PROJECT CATALOG
# ============================================================

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}
""")

print(f"Catalog validated: {CATALOG_NAME}")

# COMMAND ----------

# ============================================================
# CREATE MEDALLION AND GOVERNANCE SCHEMAS
# ============================================================

for schema_name, schema_comment in REQUIRED_SCHEMAS.items():
    full_schema_name = f"{CATALOG_NAME}.{schema_name}"

    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {full_schema_name}
    """)

    spark.sql(f"""
    COMMENT ON SCHEMA {full_schema_name}
    IS '{schema_comment}'
    """)

    print(f"Schema validated: {full_schema_name}")

# COMMAND ----------

# ============================================================
# SET ACTIVE CATALOG
# ============================================================
#
# The active catalog is explicitly set to avoid accidental
# table creation in the default workspace catalog.
#
# ============================================================

spark.sql(f"USE CATALOG {CATALOG_NAME}")

print(f"Active catalog set to: {CATALOG_NAME}")

# COMMAND ----------

# ============================================================
# VALIDATE CREATED SCHEMAS
# ============================================================

schemas_df = spark.sql(f"""
SHOW SCHEMAS IN {CATALOG_NAME}
""")

display(schemas_df)

# COMMAND ----------

# ============================================================
# SETUP VALIDATION SUMMARY
# ============================================================

existing_schemas = [
    row["databaseName"]
    for row in schemas_df.collect()
]

missing_schemas = [
    schema_name
    for schema_name in REQUIRED_SCHEMAS.keys()
    if schema_name not in existing_schemas
]

print("=" * 80)
print("SETUP VALIDATION SUMMARY")
print("=" * 80)
print(f"Catalog: {CATALOG_NAME}")

for schema_name in REQUIRED_SCHEMAS.keys():
    print(f"Schema: {schema_name}")

if missing_schemas:
    raise Exception(
        f"Catalog/schema setup validation failed. "
        f"Missing schemas: {missing_schemas}"
    )

print("=" * 80)
print("CATALOG AND SCHEMAS CREATED SUCCESSFULLY")
print("=" * 80)

