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
     "nuid": "8250e869-b0d5-4732-b1b9-575df6e5f2ce",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# COMMAND ----------\n",
    "\n",
    "# ============================================================\n",
    "# LOAD GLOBAL PROJECT CONFIGURATION\n",
    "# ============================================================\n",
    "#\n",
    "# Loads catalog names, schemas, audit tables,\n",
    "# execution status values and reusable constants.\n",
    "#\n",
    "# ============================================================\n",
    "\n",
    "# MAGIC %run ./utils_config"
   ]
  },
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
     "nuid": "e092a7a8-5fb0-4081-af4c-854c897a37d0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%run ./utils_config"
   ]
  },
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
     "nuid": "41fa291a-d5b2-4fde-a0c9-085ca5ad5c95",
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
      "utils_table_logger loaded successfully.\n"
     ]
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Project configuration loaded successfully.\nProject: brazil_legislative_analytics\nCatalog: brazil_legislative_analytics\nEnvironment: dev\nDefault legislatures: [56, 57]\nAnalysis years: [2022, 2023, 2024, 2025, 2026]\n"
     ]
    }
   ],
   "source": [
    "# Databricks notebook source\n",
    "# MAGIC %md\n",
    "# MAGIC # 99 Utils — Pipeline Table Logger\n",
    "# MAGIC\n",
    "# MAGIC **Notebook:** `utils_table_logger`\n",
    "# MAGIC\n",
    "# MAGIC Persists structured pipeline execution events into the audit Delta table.\n",
    "# MAGIC\n",
    "# MAGIC ## Responsibility\n",
    "# MAGIC This notebook only appends log records into an existing audit table.\n",
    "# MAGIC Audit tables must be created previously by `00_setup/02_audit_tables`.\n",
    "# MAGIC\n",
    "# MAGIC ## Technical Notes\n",
    "# MAGIC - Python functions and variables are written in English.\n",
    "# MAGIC - Table and field names follow Portuguese mnemonic standards.\n",
    "# MAGIC - Table and column comments are written in English.\n",
    "# MAGIC - This notebook does not create audit tables.\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# MAGIC %run ./utils_config\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "from datetime import datetime\n",
    "from typing import Optional\n",
    "from pyspark.sql.types import (\n",
    "    StructType,\n",
    "    StructField,\n",
    "    StringType,\n",
    "    LongType,\n",
    "    DoubleType,\n",
    "    TimestampType,\n",
    ")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "PROJECT_NAME_VALUE = globals().get(\"PROJECT_NAME\", \"brazil_legislative_analytics\")\n",
    "PROJECT_VERSION_VALUE = globals().get(\"PROJECT_VERSION\", globals().get(\"PIPELINE_VERSION\", \"v1.0.0\"))\n",
    "PROJECT_ENVIRONMENT_VALUE = globals().get(\"PROJECT_ENVIRONMENT\", \"dev\")\n",
    "\n",
    "CATALOG_NAME_VALUE = globals().get(\"CATALOG_NAME\", \"brazil_legislative_analytics\")\n",
    "SCHEMA_AUDIT_VALUE = globals().get(\"SCHEMA_AUDIT\", \"audit\")\n",
    "AUDIT_LOG_TABLE_VALUE = globals().get(\"AUD_TB_LOG_EXECUCAO_PIPELINE\", \"aud_log_execucao_pipeline\")\n",
    "\n",
    "PIPELINE_LOG_TABLE = (\n",
    "    f\"{CATALOG_NAME_VALUE}.{SCHEMA_AUDIT_VALUE}.{AUDIT_LOG_TABLE_VALUE}\"\n",
    ")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "def get_pipeline_log_schema() -> StructType:\n",
    "    \"\"\"\n",
    "    Returns the schema used to append records into the pipeline execution audit table.\n",
    "    \"\"\"\n",
    "\n",
    "    return StructType([\n",
    "        StructField(\"aud_id_log\", StringType(), True),\n",
    "        StructField(\"aud_id_execucao\", StringType(), True),\n",
    "        StructField(\"aud_tx_nome_projeto\", StringType(), True),\n",
    "        StructField(\"aud_tx_versao_pipeline\", StringType(), True),\n",
    "        StructField(\"aud_tx_ambiente\", StringType(), True),\n",
    "        StructField(\"aud_tx_nome_notebook\", StringType(), True),\n",
    "        StructField(\"aud_tx_nome_camada\", StringType(), True),\n",
    "        StructField(\"aud_tx_nome_entidade\", StringType(), True),\n",
    "        StructField(\"aud_tx_tabela_destino\", StringType(), True),\n",
    "        StructField(\"aud_tx_status\", StringType(), True),\n",
    "        StructField(\"aud_dh_inicio\", TimestampType(), True),\n",
    "        StructField(\"aud_dh_fim\", TimestampType(), True),\n",
    "        StructField(\"aud_nr_duracao_segundos\", DoubleType(), True),\n",
    "        StructField(\"aud_qt_registros_lidos\", LongType(), True),\n",
    "        StructField(\"aud_qt_registros_gravados\", LongType(), True),\n",
    "        StructField(\"aud_tx_mensagem\", StringType(), True),\n",
    "    ])\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "def write_pipeline_log(\n",
    "    log_id: str,\n",
    "    execution_id: str,\n",
    "    notebook_name: str,\n",
    "    layer_name: str,\n",
    "    entity_name: str,\n",
    "    target_table: str,\n",
    "    status: str,\n",
    "    message: str,\n",
    "    started_at: Optional[datetime] = None,\n",
    "    finished_at: Optional[datetime] = None,\n",
    "    duration_seconds: Optional[float] = None,\n",
    "    records_read: Optional[int] = None,\n",
    "    records_written: Optional[int] = None,\n",
    "    project_name: str = PROJECT_NAME_VALUE,\n",
    "    project_version: str = PROJECT_VERSION_VALUE,\n",
    "    environment: str = PROJECT_ENVIRONMENT_VALUE,\n",
    ") -> None:\n",
    "    \"\"\"\n",
    "    Appends a structured pipeline execution log record into the audit table.\n",
    "    \"\"\"\n",
    "\n",
    "    log_schema = get_pipeline_log_schema()\n",
    "\n",
    "    log_df = spark.createDataFrame(\n",
    "        [(\n",
    "            log_id,\n",
    "            execution_id,\n",
    "            project_name,\n",
    "            project_version,\n",
    "            environment,\n",
    "            notebook_name,\n",
    "            layer_name,\n",
    "            entity_name,\n",
    "            target_table,\n",
    "            status,\n",
    "            started_at,\n",
    "            finished_at,\n",
    "            float(duration_seconds) if duration_seconds is not None else None,\n",
    "            int(records_read) if records_read is not None else None,\n",
    "            int(records_written) if records_written is not None else None,\n",
    "            message,\n",
    "        )],\n",
    "        log_schema,\n",
    "    )\n",
    "\n",
    "    log_df.write.mode(\"append\").saveAsTable(PIPELINE_LOG_TABLE)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"utils_table_logger loaded successfully.\")"
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
   "notebookName": "utils_table_logger",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}