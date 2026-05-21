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
     "nuid": "66e45646-4403-455a-a722-a65599b8cbbd",
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
      "==========================================================================================\nBRAZIL LEGISLATIVE ANALYTICS MEDALLION\n01 - PROJECT CONFIGURATION\n==========================================================================================\nCONFIG LOADED SUCCESSFULLY\nPROJECT_NAME: brazil_legislative_analytics\nPROJECT_VERSION: v1.0.0\nPROJECT_ENVIRONMENT: dev\nCATALOG_NAME: brazil_legislative_analytics\nCAMARA_API_BASE_URL: https://dadosabertos.camara.leg.br/api/v2\nRUN_ID: d4aab8cd-7812-44ff-8691-6e377c1eeccc\n==========================================================================================\nPROJECT CONFIGURATION LOADED SUCCESSFULLY\n"
     ]
    }
   ],
   "source": [
    "%run ./01_project_config"
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
     "nuid": "51130f8c-a360-43b7-9ae0-e0928643a651",
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
      "================================================================================\nBRAZIL LEGISLATIVE ANALYTICS MEDALLION\n02 - CREATE AUDIT TABLES\n================================================================================\nExecution Timestamp: 2026-05-20 02:43:26.024649\nCatalog: brazil_legislative_analytics\nAudit Schema: audit\nProject Version: v1.0.0\n================================================================================\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>database</th><th>tableName</th><th>isTemporary</th></tr></thead><tbody><tr><td>audit</td><td>aud_log_erros_pipeline</td><td>false</td></tr><tr><td>audit</td><td>aud_log_execucao_pipeline</td><td>false</td></tr><tr><td>audit</td><td>aud_log_qualidade_dados</td><td>false</td></tr></tbody></table></div>"
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
         "audit",
         "aud_log_erros_pipeline",
         false
        ],
        [
         "audit",
         "aud_log_execucao_pipeline",
         false
        ],
        [
         "audit",
         "aud_log_qualidade_dados",
         false
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
         "name": "database",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "tableName",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "isTemporary",
         "type": "\"boolean\""
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
      "================================================================================\nAUDIT TABLES CREATED SUCCESSFULLY\n================================================================================\nTable: brazil_legislative_analytics.audit.aud_log_execucao_pipeline\nTable: brazil_legislative_analytics.audit.aud_log_erros_pipeline\nTable: brazil_legislative_analytics.audit.aud_log_qualidade_dados\n================================================================================\n"
     ]
    }
   ],
   "source": [
    "# Databricks notebook source\n",
    "# MAGIC %md\n",
    "# MAGIC # 02 Audit Tables\n",
    "# MAGIC\n",
    "# MAGIC Creates audit and governance tables used to monitor pipeline executions,\n",
    "# MAGIC pipeline errors and data quality validations across all Medallion layers.\n",
    "# MAGIC\n",
    "# MAGIC ## Tables Created\n",
    "# MAGIC - `audit.aud_log_execucao_pipeline`\n",
    "# MAGIC - `audit.aud_log_erros_pipeline`\n",
    "# MAGIC - `audit.aud_log_qualidade_dados`\n",
    "# MAGIC\n",
    "# MAGIC ## Documentation Standard\n",
    "# MAGIC - Python functions and variables are written in English.\n",
    "# MAGIC - Table and field names follow Portuguese mnemonic standards.\n",
    "# MAGIC - Table and column comments are written in English.\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# MAGIC %run ./01_project_config\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "from datetime import datetime\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# ============================================================\n",
    "# CONFIGURATION\n",
    "# ============================================================\n",
    "\n",
    "CATALOG_NAME = globals().get(\n",
    "    \"CATALOG_NAME\",\n",
    "    \"brazil_legislative_analytics\",\n",
    ")\n",
    "\n",
    "SCHEMA_AUDIT = globals().get(\n",
    "    \"SCHEMA_AUDIT\",\n",
    "    \"audit\",\n",
    ")\n",
    "\n",
    "PROJECT_VERSION = globals().get(\n",
    "    \"PROJECT_VERSION\",\n",
    "    \"v1.0.0\",\n",
    ")\n",
    "\n",
    "PIPELINE_LOG_TABLE_NAME = globals().get(\n",
    "    \"AUD_TB_LOG_EXECUCAO_PIPELINE\",\n",
    "    \"aud_log_execucao_pipeline\",\n",
    ")\n",
    "\n",
    "PIPELINE_ERROR_TABLE_NAME = globals().get(\n",
    "    \"AUD_TB_LOG_ERROS_PIPELINE\",\n",
    "    \"aud_log_erros_pipeline\",\n",
    ")\n",
    "\n",
    "DATA_QUALITY_LOG_TABLE_NAME = globals().get(\n",
    "    \"AUD_TB_LOG_QUALIDADE_DADOS\",\n",
    "    \"aud_log_qualidade_dados\",\n",
    ")\n",
    "\n",
    "PIPELINE_LOG_TABLE = (\n",
    "    f\"{CATALOG_NAME}.\"\n",
    "    f\"{SCHEMA_AUDIT}.\"\n",
    "    f\"{PIPELINE_LOG_TABLE_NAME}\"\n",
    ")\n",
    "\n",
    "PIPELINE_ERROR_TABLE = (\n",
    "    f\"{CATALOG_NAME}.\"\n",
    "    f\"{SCHEMA_AUDIT}.\"\n",
    "    f\"{PIPELINE_ERROR_TABLE_NAME}\"\n",
    ")\n",
    "\n",
    "DATA_QUALITY_LOG_TABLE = (\n",
    "    f\"{CATALOG_NAME}.\"\n",
    "    f\"{SCHEMA_AUDIT}.\"\n",
    "    f\"{DATA_QUALITY_LOG_TABLE_NAME}\"\n",
    ")\n",
    "\n",
    "spark.sql(f\"USE CATALOG {CATALOG_NAME}\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# ============================================================\n",
    "# EXECUTION HEADER\n",
    "# ============================================================\n",
    "\n",
    "print(\"=\" * 80)\n",
    "print(\"BRAZIL LEGISLATIVE ANALYTICS MEDALLION\")\n",
    "print(\"02 - CREATE AUDIT TABLES\")\n",
    "print(\"=\" * 80)\n",
    "print(f\"Execution Timestamp: {datetime.now()}\")\n",
    "print(f\"Catalog: {CATALOG_NAME}\")\n",
    "print(f\"Audit Schema: {SCHEMA_AUDIT}\")\n",
    "print(f\"Project Version: {PROJECT_VERSION}\")\n",
    "print(\"=\" * 80)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# ============================================================\n",
    "# CREATE PIPELINE EXECUTION LOG TABLE\n",
    "# ============================================================\n",
    "\n",
    "spark.sql(f\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS {PIPELINE_LOG_TABLE} (\n",
    "    aud_id_log STRING COMMENT 'Unique identifier for the pipeline log record.',\n",
    "    aud_id_execucao STRING COMMENT 'Unique execution identifier shared across notebooks within the same pipeline run.',\n",
    "    aud_tx_nome_projeto STRING COMMENT 'Project name associated with the execution.',\n",
    "    aud_tx_versao_pipeline STRING COMMENT 'Pipeline version executed.',\n",
    "    aud_tx_ambiente STRING COMMENT 'Execution environment, such as dev, qa or prod.',\n",
    "    aud_tx_nome_notebook STRING COMMENT 'Notebook responsible for the execution.',\n",
    "    aud_tx_nome_camada STRING COMMENT 'Medallion layer executed: setup, bronze, silver, gold, marts, quality or jobs.',\n",
    "    aud_tx_nome_entidade STRING COMMENT 'Business or technical entity processed during execution.',\n",
    "    aud_tx_tabela_destino STRING COMMENT 'Fully qualified target table processed during execution.',\n",
    "    aud_tx_status STRING COMMENT 'Execution status: STARTED, SUCCESS, FAILED or WARNING.',\n",
    "    aud_dh_inicio TIMESTAMP COMMENT 'Execution start timestamp.',\n",
    "    aud_dh_fim TIMESTAMP COMMENT 'Execution end timestamp.',\n",
    "    aud_nr_duracao_segundos DOUBLE COMMENT 'Execution duration in seconds.',\n",
    "    aud_qt_registros_lidos BIGINT COMMENT 'Number of records read during processing.',\n",
    "    aud_qt_registros_gravados BIGINT COMMENT 'Number of records written during processing.',\n",
    "    aud_tx_mensagem STRING COMMENT 'Additional execution message or processing note.'\n",
    ")\n",
    "USING DELTA\n",
    "COMMENT 'Audit table responsible for storing pipeline execution history across all Medallion layers.'\n",
    "\"\"\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# ============================================================\n",
    "# CREATE PIPELINE ERROR LOG TABLE\n",
    "# ============================================================\n",
    "\n",
    "spark.sql(f\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS {PIPELINE_ERROR_TABLE} (\n",
    "    err_id_erro STRING COMMENT 'Unique identifier for the error record.',\n",
    "    aud_id_execucao STRING COMMENT 'Execution identifier associated with the failed pipeline execution.',\n",
    "    aud_tx_nome_projeto STRING COMMENT 'Project name associated with the error.',\n",
    "    aud_tx_versao_pipeline STRING COMMENT 'Pipeline version executed.',\n",
    "    aud_tx_ambiente STRING COMMENT 'Execution environment where the error occurred.',\n",
    "    aud_tx_nome_notebook STRING COMMENT 'Notebook where the error occurred.',\n",
    "    aud_tx_nome_camada STRING COMMENT 'Medallion layer where the error occurred.',\n",
    "    aud_tx_nome_entidade STRING COMMENT 'Business or technical entity being processed when the error occurred.',\n",
    "    aud_tx_tabela_destino STRING COMMENT 'Fully qualified target table associated with the error.',\n",
    "    err_tx_nome_etapa STRING COMMENT 'Pipeline step where the error occurred.',\n",
    "    err_tx_tipo_erro STRING COMMENT 'Error classification or exception type.',\n",
    "    err_tx_mensagem STRING COMMENT 'Error message returned during execution.',\n",
    "    err_tx_stacktrace STRING COMMENT 'Complete stack trace captured for troubleshooting.',\n",
    "    err_dh_ocorrencia TIMESTAMP COMMENT 'Timestamp when the error occurred.'\n",
    ")\n",
    "USING DELTA\n",
    "COMMENT 'Audit table responsible for storing pipeline execution errors and troubleshooting details.'\n",
    "\"\"\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# ============================================================\n",
    "# CREATE DATA QUALITY LOG TABLE\n",
    "# ============================================================\n",
    "\n",
    "spark.sql(f\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS {DATA_QUALITY_LOG_TABLE} (\n",
    "    qlt_id_log STRING COMMENT 'Unique identifier for the quality validation log.',\n",
    "    aud_id_execucao STRING COMMENT 'Execution identifier associated with the quality validation.',\n",
    "    aud_tx_nome_projeto STRING COMMENT 'Project name associated with the validation.',\n",
    "    aud_tx_versao_pipeline STRING COMMENT 'Pipeline version executed.',\n",
    "    aud_tx_ambiente STRING COMMENT 'Execution environment associated with the validation.',\n",
    "    aud_tx_nome_notebook STRING COMMENT 'Notebook responsible for executing the quality validation.',\n",
    "    aud_tx_nome_camada STRING COMMENT 'Medallion layer validated.',\n",
    "    aud_tx_nome_entidade STRING COMMENT 'Business or technical entity validated.',\n",
    "    aud_tx_tabela_destino STRING COMMENT 'Fully qualified table validated.',\n",
    "    qlt_tx_nome_regra STRING COMMENT 'Name of the executed data quality rule.',\n",
    "    qlt_tx_descricao_regra STRING COMMENT 'Description of the executed data quality rule.',\n",
    "    qlt_tx_status_validacao STRING COMMENT 'Validation result: PASSED, FAILED or WARNING.',\n",
    "    qlt_qt_total_registros BIGINT COMMENT 'Total number of records evaluated.',\n",
    "    qlt_qt_registros_invalidos BIGINT COMMENT 'Number of invalid records identified.',\n",
    "    qlt_pc_registros_invalidos DOUBLE COMMENT 'Percentage of invalid records identified during validation.',\n",
    "    qlt_dh_validacao TIMESTAMP COMMENT 'Timestamp when the validation was executed.',\n",
    "    qlt_tx_mensagem STRING COMMENT 'Additional validation message or observation.'\n",
    ")\n",
    "USING DELTA\n",
    "COMMENT 'Audit table responsible for storing data quality validation results across all Medallion layers.'\n",
    "\"\"\")\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# ============================================================\n",
    "# VALIDATE CREATED AUDIT TABLES\n",
    "# ============================================================\n",
    "\n",
    "audit_tables_df = spark.sql(f\"\"\"\n",
    "SHOW TABLES IN {CATALOG_NAME}.{SCHEMA_AUDIT}\n",
    "\"\"\")\n",
    "\n",
    "display(audit_tables_df)\n",
    "\n",
    "# COMMAND ----------\n",
    "\n",
    "# ============================================================\n",
    "# EXECUTION SUMMARY\n",
    "# ============================================================\n",
    "\n",
    "print(\"=\" * 80)\n",
    "print(\"AUDIT TABLES CREATED SUCCESSFULLY\")\n",
    "print(\"=\" * 80)\n",
    "print(f\"Table: {PIPELINE_LOG_TABLE}\")\n",
    "print(f\"Table: {PIPELINE_ERROR_TABLE}\")\n",
    "print(f\"Table: {DATA_QUALITY_LOG_TABLE}\")\n",
    "print(\"=\" * 80)"
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
   "notebookName": "02_audit_tables",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}