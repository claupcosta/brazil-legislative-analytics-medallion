{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "8700a1da-f3ad-4467-ae23-2222dfbe47d7",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "================================================================================\nBRAZIL LEGISLATIVE ANALYTICS MEDALLION\n00 - CREATE CATALOG AND SCHEMAS\n================================================================================\nExecution timestamp: 2026-05-17 04:06:45.449378\nTarget catalog: brazil_legislative_analytics\n================================================================================\nCatalog validated: brazil_legislative_analytics\nSchema validated: brazil_legislative_analytics.audit\nSchema validated: brazil_legislative_analytics.bronze\nSchema validated: brazil_legislative_analytics.silver_base\nSchema validated: brazil_legislative_analytics.silver_curated\nSchema validated: brazil_legislative_analytics.gold\nSchema validated: brazil_legislative_analytics.marts\nActive catalog set to: brazil_legislative_analytics\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>databaseName</th></tr></thead><tbody><tr><td>audit</td></tr><tr><td>bronze</td></tr><tr><td>gold</td></tr><tr><td>information_schema</td></tr><tr><td>marts</td></tr><tr><td>silver_base</td></tr><tr><td>silver_curated</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "audit"
        ],
        [
         "bronze"
        ],
        [
         "gold"
        ],
        [
         "information_schema"
        ],
        [
         "marts"
        ],
        [
         "silver_base"
        ],
        [
         "silver_curated"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "databaseName",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "================================================================================\nSETUP VALIDATION SUMMARY\n================================================================================\nCatalog: brazil_legislative_analytics\nSchema: audit\nSchema: bronze\nSchema: silver_base\nSchema: silver_curated\nSchema: gold\nSchema: marts\n================================================================================\nCATALOG AND SCHEMAS CREATED SUCCESSFULLY\n================================================================================\n"
     ]
    }
   ],
   "source": [
    "# Databricks notebook source\n",
    "# MAGIC %md\n",
    "# MAGIC # 00_create_catalog_schemas\n",
    "# MAGIC\n",
    "# MAGIC ## Purpose\n",
    "# MAGIC Create the main catalog and schemas used by the Brazil Legislative Analytics Medallion project.\n",
    "# MAGIC\n",
    "# MAGIC ## Layer\n",
    "# MAGIC Setup\n",
    "# MAGIC\n",
    "# MAGIC ## Inputs\n",
    "# MAGIC None.\n",
    "# MAGIC\n",
    "# MAGIC ## Outputs\n",
    "# MAGIC Catalog:\n",
    "# MAGIC - `brazil_legislative_analytics`\n",
    "# MAGIC\n",
    "# MAGIC Schemas:\n",
    "# MAGIC - `audit`\n",
    "# MAGIC - `bronze`\n",
    "# MAGIC - `silver_base`\n",
    "# MAGIC - `silver_curated`\n",
    "# MAGIC - `gold`\n",
    "# MAGIC - `marts`\n",
    "# MAGIC\n",
    "# MAGIC ## Documentation Standard\n",
    "# MAGIC This notebook follows the project documentation and naming standards.\n",
    "# MAGIC Schema comments are created to support governance, traceability and technical documentation.\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "from datetime import datetime\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "CATALOG_NAME = \"brazil_legislative_analytics\"\n",
    "\n",
    "SCHEMAS = {\n",
    "    \"audit\": \"Governance schema containing audit logs, error logs and data quality logs.\",\n",
    "    \"bronze\": \"Raw ingestion layer containing data extracted from source APIs with minimal transformation.\",\n",
    "    \"silver_base\": \"Technical standardization layer responsible for cleansing, typing, normalization and deduplication.\",\n",
    "    \"silver_curated\": \"Business-curated layer containing reusable and trusted entities for analytical modeling.\",\n",
    "    \"gold\": \"Dimensional analytical layer containing dimensions and fact tables following the Star Schema model.\",\n",
    "    \"marts\": \"Analytical consumption layer containing business-oriented marts and final analytical outputs.\"\n",
    "}\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "execution_timestamp = datetime.now()\n",
    "\n",
    "print(\"=\" * 80)\n",
    "print(\"BRAZIL LEGISLATIVE ANALYTICS MEDALLION\")\n",
    "print(\"00 - CREATE CATALOG AND SCHEMAS\")\n",
    "print(\"=\" * 80)\n",
    "print(f\"Execution timestamp: {execution_timestamp}\")\n",
    "print(f\"Target catalog: {CATALOG_NAME}\")\n",
    "print(\"=\" * 80)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# Create project catalog.\n",
    "spark.sql(f\"\"\"\n",
    "CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}\n",
    "\"\"\")\n",
    "\n",
    "print(f\"Catalog validated: {CATALOG_NAME}\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# Create Medallion and governance schemas with comments.\n",
    "for schema_name, schema_comment in SCHEMAS.items():\n",
    "    full_schema_name = f\"{CATALOG_NAME}.{schema_name}\"\n",
    "\n",
    "    spark.sql(f\"\"\"\n",
    "    CREATE SCHEMA IF NOT EXISTS {full_schema_name}\n",
    "    \"\"\")\n",
    "\n",
    "    spark.sql(f\"\"\"\n",
    "    COMMENT ON SCHEMA {full_schema_name}\n",
    "    IS '{schema_comment}'\n",
    "    \"\"\")\n",
    "\n",
    "    print(f\"Schema validated: {full_schema_name}\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# Set the active catalog to avoid accidental table creation in the default catalog/schema.\n",
    "spark.sql(f\"USE CATALOG {CATALOG_NAME}\")\n",
    "\n",
    "print(f\"Active catalog set to: {CATALOG_NAME}\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# Validate created schemas.\n",
    "schemas_df = spark.sql(f\"\"\"\n",
    "SHOW SCHEMAS IN {CATALOG_NAME}\n",
    "\"\"\")\n",
    "\n",
    "display(schemas_df)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"=\" * 80)\n",
    "print(\"SETUP VALIDATION SUMMARY\")\n",
    "print(\"=\" * 80)\n",
    "print(f\"Catalog: {CATALOG_NAME}\")\n",
    "\n",
    "for schema_name in SCHEMAS.keys():\n",
    "    print(f\"Schema: {schema_name}\")\n",
    "\n",
    "print(\"=\" * 80)\n",
    "print(\"CATALOG AND SCHEMAS CREATED SUCCESSFULLY\")\n",
    "print(\"=\" * 80)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# MAGIC %md\n",
    "# MAGIC ## Created Structures\n",
    "# MAGIC\n",
    "# MAGIC ### Catalog\n",
    "# MAGIC - `brazil_legislative_analytics`\n",
    "# MAGIC\n",
    "# MAGIC ### Schemas\n",
    "# MAGIC - `audit`\n",
    "# MAGIC - `bronze`\n",
    "# MAGIC - `silver_base`\n",
    "# MAGIC - `silver_curated`\n",
    "# MAGIC - `gold`\n",
    "# MAGIC - `marts`\n",
    "# MAGIC\n",
    "# MAGIC ## Governance Notes\n",
    "# MAGIC\n",
    "# MAGIC - All schemas include comments for documentation and governance.\n",
    "# MAGIC - The active catalog is explicitly set to avoid accidental table creation in the default schema.\n",
    "# MAGIC - Future notebooks must always write tables using the fully qualified name:\n",
    "# MAGIC\n",
    "# MAGIC ```text\n",
    "# MAGIC catalog.schema.table_name\n",
    "# MAGIC ```\n",
    "# MAGIC\n",
    "# MAGIC Example:\n",
    "# MAGIC\n",
    "# MAGIC ```text\n",
    "# MAGIC brazil_legislative_analytics.bronze.bronze_deputies_raw\n",
    "# MAGIC ```\n",
    "# MAGIC\n",
    "# MAGIC ## Next Step\n",
    "# MAGIC Execute:\n",
    "# MAGIC\n",
    "# MAGIC `01_project_config`"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "00_create_catalog_schemas",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}