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
     "nuid": "a8b4595f-3879-4dbf-8728-b32fc80b2dfd",
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
      "PROJECT CONFIGURATION LOADED SUCCESSFULLY\nPROJECT_NAME: brazil_legislative_analytics\nPROJECT_VERSION: v1.0.0\nPROJECT_ENVIRONMENT: dev\nCATALOG_NAME: brazil_legislative_analytics\nRUN_ID: 1fdda90e-cccf-4b73-8c34-2e769b1f0428\n"
     ]
    }
   ],
   "source": [
    "%run ../00_setup/01_project_config"
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
     "nuid": "863aff3e-ac2d-4244-afda-b52031ee4879",
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
      "utils_config loaded successfully.\n"
     ]
    }
   ],
   "source": [
    "# Databricks notebook source\n",
    "# MAGIC %md\n",
    "# MAGIC # 99 Utils — Configuration Loader\n",
    "# MAGIC\n",
    "# MAGIC **Notebook:** `utils_config`\n",
    "# MAGIC\n",
    "# MAGIC Lightweight configuration loader used across pipeline notebooks.\n",
    "# MAGIC\n",
    "# MAGIC ## Purpose\n",
    "# MAGIC Loads centralized project configuration variables from:\n",
    "# MAGIC\n",
    "# MAGIC ```text\n",
    "# MAGIC 00_setup/01_project_config\n",
    "# MAGIC ```\n",
    "# MAGIC\n",
    "# MAGIC ## Performance Note\n",
    "# MAGIC This notebook intentionally avoids:\n",
    "# MAGIC - catalog activation\n",
    "# MAGIC - heavy validation\n",
    "# MAGIC - large console outputs\n",
    "# MAGIC\n",
    "# MAGIC to reduce initialization overhead during `%run` execution.\n",
    "# MAGIC\n",
    "# MAGIC ## Documentation Standard\n",
    "# MAGIC - Python functions and variables are written in English.\n",
    "# MAGIC - Table and field names follow Portuguese mnemonic standards.\n",
    "# MAGIC - Comments and documentation are written in English.\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# MAGIC %run ../00_setup/01_project_config\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# ============================================================\n",
    "# LIGHTWEIGHT CONFIGURATION VALIDATION\n",
    "# ============================================================\n",
    "\n",
    "REQUIRED_CONFIG_OBJECTS = [\n",
    "    \"PROJECT_NAME\",\n",
    "    \"PROJECT_VERSION\",\n",
    "    \"PROJECT_ENVIRONMENT\",\n",
    "    \"CATALOG_NAME\",\n",
    "    \"SCHEMA_AUDIT\",\n",
    "    \"SCHEMA_BRONZE\",\n",
    "    \"SCHEMA_SILVER\",\n",
    "    \"SCHEMA_GOLD\",\n",
    "    \"SCHEMA_MARTS\",\n",
    "    \"AUD_TB_LOG_EXECUCAO_PIPELINE\",\n",
    "    \"AUD_TB_LOG_ERROS_PIPELINE\",\n",
    "    \"AUD_TB_LOG_QUALIDADE_DADOS\",\n",
    "    \"BRONZE_TABLES\",\n",
    "    \"SILVER_TABLES\",\n",
    "    \"GOLD_DIMENSION_TABLES\",\n",
    "    \"GOLD_FACT_TABLES\",\n",
    "    \"MART_TABLES\",\n",
    "    \"API_ENDPOINTS\",\n",
    "    \"TRACEABILITY_COLUMNS\",\n",
    "    \"QUALITY_PASSED\",\n",
    "    \"QUALITY_WARNING\",\n",
    "    \"QUALITY_FAILED\",\n",
    "    \"EXECUTION_STATUS_STARTED\",\n",
    "    \"EXECUTION_STATUS_SUCCESS\",\n",
    "    \"EXECUTION_STATUS_WARNING\",\n",
    "    \"EXECUTION_STATUS_FAILED\",\n",
    "]\n",
    "\n",
    "missing_config_objects = [\n",
    "    object_name\n",
    "    for object_name in REQUIRED_CONFIG_OBJECTS\n",
    "    if object_name not in globals()\n",
    "]\n",
    "\n",
    "if missing_config_objects:\n",
    "\n",
    "    raise Exception(\n",
    "        \"Project configuration was not loaded correctly. \"\n",
    "        f\"Missing objects: {missing_config_objects}\"\n",
    "    )\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# ============================================================\n",
    "# OPTIONAL CATALOG ACTIVATION\n",
    "# ============================================================\n",
    "#\n",
    "# Disabled by default to reduce overhead when this notebook is\n",
    "# imported through %run by other notebooks.\n",
    "#\n",
    "# Use fully qualified table names across the pipeline instead.\n",
    "#\n",
    "# ============================================================\n",
    "\n",
    "SET_ACTIVE_CATALOG = False\n",
    "\n",
    "if SET_ACTIVE_CATALOG:\n",
    "    spark.sql(f\"USE CATALOG {CATALOG_NAME}\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "print(\"utils_config loaded successfully.\")"
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
   "notebookName": "utils_config",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}