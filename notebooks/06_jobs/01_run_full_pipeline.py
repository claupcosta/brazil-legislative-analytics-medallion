# Databricks notebook source
# MAGIC %md
# MAGIC # Jobs Layer — Full Pipeline Orchestration
# MAGIC
# MAGIC **Notebook:** `01_run_full_pipeline`  
# MAGIC **Layer:** `Jobs`  
# MAGIC **Source/Endpoint:** `Medallion Pipeline Notebooks`  
# MAGIC **Target:** `Pipeline execution orchestration and audit logs`
# MAGIC
# MAGIC Orchestrates the execution flow of the
# MAGIC Brazil Legislative Analytics Medallion project.
# MAGIC
# MAGIC This notebook manages pipeline step execution, operational logging,
# MAGIC error handling and execution monitoring across Setup, Bronze, Silver, Gold, Marts and Quality layers.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Execute Medallion pipeline notebooks across Bronze, Silver, Gold, Marts and Quality layers
# MAGIC - Control execution flow by configured steps
# MAGIC - Support fast and development execution modes
# MAGIC - Register execution logs and pipeline status
# MAGIC - Handle step-level failures and warnings
# MAGIC - Generate pipeline execution summary
# MAGIC - Execute governance and metadata quality validations
# MAGIC - Support API validation and replay scenarios
# MAGIC - Prioritize CSV fallback ingestion for unstable or high-volume sources
# MAGIC - Preserve disabled API steps for compatibility, validation and controlled reprocessing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Optimized for Databricks Free Edition execution
# MAGIC - Supports API and CSV fallback ingestion strategies
# MAGIC - CSV fallback is the preferred operational path for high-volume or unstable endpoints
# MAGIC - API notebooks are preserved mainly for validation, replay and controlled extraction scenarios
# MAGIC - The `/orgaos` endpoint may present timeout behavior and is handled operationally through CSV fallback
# MAGIC - Disabled steps can be preserved for compatibility and replay scenarios
# MAGIC - Pipeline interruption behavior is controlled by `FAIL_ON_STEP_ERROR`
# MAGIC - Execution status and step-level outcomes are persisted in audit logs
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/operations/pipeline_orchestration.md`
# MAGIC - `/docs/monitoring/observability.md`
# MAGIC - `/docs/decisions/api_limitations.md`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_logger

# COMMAND ----------

# MAGIC %run ../99_utils/utils_table_logger

# COMMAND ----------

from datetime import datetime
import uuid
import traceback

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("01 - RUN FULL PIPELINE")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Environment: {PROJECT_ENVIRONMENT}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# JOB CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "01_run_full_pipeline"
LAYER_NAME = "jobs"
ENTITY_NAME = "full_pipeline"

JOB_RUN_ID = str(uuid.uuid4())

RUN_MODE = "fast"

FAIL_ON_STEP_ERROR = False

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

# COMMAND ----------

# ============================================================
# PIPELINE STEP CONFIGURATION
# ============================================================

PIPELINE_STEPS = [

 
# ============================================================
#  SETUP
# ============================================================   
    {
        "step_name": "validate_project_setup",
        "notebook_path": "../00_setup/90_validate_project_setup",
        "layer_name": "setup",
        "entity_name": "project_setup",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "validate_api_connection",
        "notebook_path": "../00_setup/92_validate_api_connection",
        "layer_name": "setup",
        "entity_name": "api_connection",
        "enabled": False,
        "critical": False,
    },
# ============================================================
#  BRONZE
# ============================================================
    {
        "step_name": "bronze_deputados",
        "notebook_path": "../01_bronze/01_bronze_deputados",
        "layer_name": "bronze",
        "entity_name": "deputados",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "bronze_deputados_csv_fallback",
        "notebook_path": "../01_bronze/01a_bronze_deputados_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "deputados",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_frentes_api",
        "notebook_path": "../01_bronze/02_bronze_frentes",
        "layer_name": "bronze",
        "entity_name": "frentes",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "bronze_frentes_csv_fallback",
        "notebook_path": "../01_bronze/02a_bronze_frentes_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "frentes",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_frentes_membros_csv_fallback",
        "notebook_path": "../01_bronze/02b_bronze_frentes_membros_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "frentes_membros",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_eventos_api",
        "notebook_path": "../01_bronze/03_bronze_eventos",
        "layer_name": "bronze",
        "entity_name": "eventos",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "bronze_eventos_csv_fallback",
        "notebook_path": "../01_bronze/03a_bronze_eventos_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "eventos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_votacoes_api",
        "notebook_path": "../01_bronze/04_bronze_votacoes",
        "layer_name": "bronze",
        "entity_name": "votacoes",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "bronze_votacoes_csv_fallback",
        "notebook_path": "../01_bronze/04a_bronze_votacoes_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "votacoes",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_votos_api",
        "notebook_path": "../01_bronze/05_bronze_votos",
        "layer_name": "bronze",
        "entity_name": "votos",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "bronze_votos_csv_fallback",
        "notebook_path": "../01_bronze/05a_bronze_votos_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "votos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_despesas_ceap_api",
        "notebook_path": "../01_bronze/06_bronze_despesas_ceap",
        "layer_name": "bronze",
        "entity_name": "despesas_ceap",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "bronze_despesas_ceap_csv_fallback",
        "notebook_path": "../01_bronze/06a_bronze_despesas_ceap_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "despesas_ceap",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_orgaos_api",
        "notebook_path": "../01_bronze/07_bronze_orgaos",
        "layer_name": "bronze",
        "entity_name": "orgaos",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "bronze_orgaos_csv_fallback",
        "notebook_path": "../01_bronze/07a_bronze_orgaos_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "orgaos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_orgaos_membros_api",
        "notebook_path": "../01_bronze/08_bronze_orgaos_membros",
        "layer_name": "bronze",
        "entity_name": "orgaos_membros",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "bronze_orgaos_membros_csv_fallback",
        "notebook_path": "../01_bronze/08a_bronze_orgaos_membros_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "orgaos_membros",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_proposicoes_api",
        "notebook_path": "../01_bronze/09_bronze_proposicoes",
        "layer_name": "bronze",
        "entity_name": "proposicoes",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "bronze_proposicoes_csv_fallback",
        "notebook_path": "../01_bronze/09a_bronze_proposicoes_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "proposicoes",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_presencas_eventos_csv_fallback",
        "notebook_path": "../01_bronze/10_bronze_presencas_eventos_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "presencas_eventos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "bronze_quality_checkpoint",
        "notebook_path": "../01_bronze/99_bronze_quality_checkpoint",
        "layer_name": "bronze",
        "entity_name": "quality_checkpoint",
        "enabled": True,
        "critical": False,
    },
# ============================================================
#  SILVER
# ============================================================
    {
        "step_name": "silver_registros_rejeitados",
        "notebook_path": "../02_silver/00_silver_registros_rejeitados",
        "layer_name": "silver",
        "entity_name": "registros_rejeitados",
        "enabled": True,
        "critical": False,
    },
    {
        "step_name": "silver_deputados",
        "notebook_path": "../02_silver/01_silver_deputados",
        "layer_name": "silver",
        "entity_name": "deputados",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_partidos",
        "notebook_path": "../02_silver/02_silver_partidos",
        "layer_name": "silver",
        "entity_name": "partidos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_estados",
        "notebook_path": "../02_silver/03_silver_estados",
        "layer_name": "silver",
        "entity_name": "estados",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_frentes",
        "notebook_path": "../02_silver/04_silver_frentes",
        "layer_name": "silver",
        "entity_name": "frentes",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_frentes_membros",
        "notebook_path": "../02_silver/05_silver_frentes_membros",
        "layer_name": "silver",
        "entity_name": "frentes_membros",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_eventos",
        "notebook_path": "../02_silver/06_silver_eventos",
        "layer_name": "silver",
        "entity_name": "eventos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_votacoes",
        "notebook_path": "../02_silver/07_silver_votacoes",
        "layer_name": "silver",
        "entity_name": "votacoes",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_votos",
        "notebook_path": "../02_silver/08_silver_votos",
        "layer_name": "silver",
        "entity_name": "votos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_despesas_ceap",
        "notebook_path": "../02_silver/09_silver_despesas_ceap",
        "layer_name": "silver",
        "entity_name": "despesas_ceap",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_fornecedores",
        "notebook_path": "../02_silver/10_silver_fornecedores",
        "layer_name": "silver",
        "entity_name": "fornecedores",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_fornecedores_enrichment",
        "notebook_path": "../02_silver/11_silver_fornecedores_enrichment",
        "layer_name": "silver",
        "entity_name": "fornecedores_enrichment",
        "enabled": True,
        "critical": False,
    },
    {
        "step_name": "silver_cpis",
        "notebook_path": "../02_silver/12_silver_cpis",
        "layer_name": "silver",
        "entity_name": "cpis",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_cpi_eventos",
        "notebook_path": "../02_silver/13_silver_cpi_eventos",
        "layer_name": "silver",
        "entity_name": "cpi_eventos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_proposicoes",
        "notebook_path": "../02_silver/14_silver_proposicoes",
        "layer_name": "silver",
        "entity_name": "proposicoes",
        "enabled": True,
        "critical": False,
    },
    {
        "step_name": "silver_presencas_eventos",
        "notebook_path": "../02_silver/15_silver_presencas_eventos",
        "layer_name": "silver",
        "entity_name": "presencas_eventos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_orgaos",
        "notebook_path": "../02_silver/16_silver_orgaos",
        "layer_name": "silver",
        "entity_name": "orgaos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "silver_temas_pausado",
        "notebook_path": "../02_silver/17_silver_temas_Pausado",
        "layer_name": "silver",
        "entity_name": "temas",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "silver_tramitacoes_pausado",
        "notebook_path": "../02_silver/18_silver_tramitacoes_Pausado",
        "layer_name": "silver",
        "entity_name": "tramitacoes",
        "enabled": False,
        "critical": False,
    },
    {
        "step_name": "silver_quality_checkpoint",
        "notebook_path": "../02_silver/99_silver_quality_checkpoint",
        "layer_name": "silver",
        "entity_name": "quality_checkpoint",
        "enabled": True,
        "critical": False,
    },
# ============================================================
#  GOLD
# ============================================================
    {
        "step_name": "gold_inventory",
        "notebook_path": "../03_gold/00_gold_inventory",
        "layer_name": "gold",
        "entity_name": "inventory",
        "enabled": True,
        "critical": False,
    },
    {
        "step_name": "gold_dm_deputados",
        "notebook_path": "../03_gold/01_dm_deputados",
        "layer_name": "gold",
        "entity_name": "deputados",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_dm_partidos",
        "notebook_path": "../03_gold/02_dm_partidos",
        "layer_name": "gold",
        "entity_name": "partidos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_dm_estados",
        "notebook_path": "../03_gold/03_dm_estados",
        "layer_name": "gold",
        "entity_name": "estados",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_dm_datas",
        "notebook_path": "../03_gold/04_dm_datas",
        "layer_name": "gold",
        "entity_name": "datas",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_dm_frentes",
        "notebook_path": "../03_gold/05_dm_frentes",
        "layer_name": "gold",
        "entity_name": "frentes",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_dm_eventos",
        "notebook_path": "../03_gold/06_dm_eventos",
        "layer_name": "gold",
        "entity_name": "eventos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_dm_votacoes",
        "notebook_path": "../03_gold/07_dm_votacoes",
        "layer_name": "gold",
        "entity_name": "votacoes",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_dm_cpis",
        "notebook_path": "../03_gold/08_dm_cpis",
        "layer_name": "gold",
        "entity_name": "cpis",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_dm_fornecedores",
        "notebook_path": "../03_gold/09_dm_fornecedores",
        "layer_name": "gold",
        "entity_name": "fornecedores",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_ft_frentes_membros",
        "notebook_path": "../03_gold/10_ft_frentes_membros",
        "layer_name": "gold",
        "entity_name": "frentes_membros",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_ft_presencas_eventos",
        "notebook_path": "../03_gold/11_ft_presencas_eventos",
        "layer_name": "gold",
        "entity_name": "presencas_eventos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_ft_resultados_votacoes",
        "notebook_path": "../03_gold/12_ft_resultados_votacoes",
        "layer_name": "gold",
        "entity_name": "resultados_votacoes",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_ft_despesas_ceap",
        "notebook_path": "../03_gold/13_ft_despesas_ceap",
        "layer_name": "gold",
        "entity_name": "despesas_ceap",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_ft_eventos_cpis",
        "notebook_path": "../03_gold/14_ft_eventos_cpis",
        "layer_name": "gold",
        "entity_name": "eventos_cpis",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "gold_quality_checkpoint",
        "notebook_path": "../03_gold/99_gold_quality_checkpoint",
        "layer_name": "gold",
        "entity_name": "quality_checkpoint",
        "enabled": True,
        "critical": False,
    },
# ============================================================
#  MARTs
# ============================================================
    {
        "step_name": "mart_atlas_frentes",
        "notebook_path": "../04_marts/01_am_atlas_frentes",
        "layer_name": "marts",
        "entity_name": "atlas_frentes",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "mart_calendario_eventos_legislativos",
        "notebook_path": "../04_marts/02_am_calendario_eventos_legislativos",
        "layer_name": "marts",
        "entity_name": "calendario_eventos_legislativos",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "mart_correlacao_frentes_votacoes",
        "notebook_path": "../04_marts/03_am_correlacao_frentes_votacoes",
        "layer_name": "marts",
        "entity_name": "correlacao_frentes_votacoes",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "mart_visao_geral_despesas_ceap",
        "notebook_path": "../04_marts/04_am_visao_geral_despesas_ceap",
        "layer_name": "marts",
        "entity_name": "visao_geral_despesas_ceap",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "mart_auditoria_cpis",
        "notebook_path": "../04_marts/05_am_auditoria_cpis",
        "layer_name": "marts",
        "entity_name": "auditoria_cpis",
        "enabled": True,
        "critical": True,
    },
    {
        "step_name": "mart_monitor_presenca_absenteismo",
        "notebook_path": "../04_marts/06_am_monitor_presenca_absenteismo",
        "layer_name": "marts",
        "entity_name": "monitor_presenca_absenteismo",
        "enabled": True,
        "critical": True,
    },
# ============================================================
#  QUALITY
# ============================================================
    {
        "step_name": "quality_bronze_checks",
        "notebook_path": "../05_quality/01_quality_bronze_checks",
        "layer_name": "quality",
        "entity_name": "bronze_checks",
        "enabled": True,
        "critical": False,
    },
    {
        "step_name": "quality_silver_checks",
        "notebook_path": "../05_quality/02_quality_silver_checks",
        "layer_name": "quality",
        "entity_name": "silver_checks",
        "enabled": True,
        "critical": False,
    },
    {
        "step_name": "quality_gold_checks",
        "notebook_path": "../05_quality/03_quality_gold_checks",
        "layer_name": "quality",
        "entity_name": "gold_checks",
        "enabled": True,
        "critical": False,
    },
    {
        "step_name": "traceability_checks",
        "notebook_path": "../05_quality/04_traceability_checks",
        "layer_name": "quality",
        "entity_name": "traceability_checks",
        "enabled": True,
        "critical": False,
    },
    {
        "step_name": "quality_marts_checks",
        "notebook_path": "../05_quality/05_quality_marts_checks",
        "layer_name": "quality",
        "entity_name": "marts_checks",
        "enabled": True,
        "critical": False,
    },
    {
        "step_name": "governance_metadata_checks",
        "notebook_path": "../05_quality/06_governance_metadata_checks",
        "layer_name": "governance",
        "entity_name": "governance_metadata_checks",
        "enabled": True,
        "critical": False,
    },
]

# COMMAND ----------

def run_notebook_step(step_config: dict) -> dict:
    """
    Executes a configured notebook step and returns execution metadata.
    """

    step_name = step_config["step_name"]
    notebook_path = step_config["notebook_path"]
    step_layer = step_config["layer_name"]
    step_entity = step_config["entity_name"]
    is_enabled = step_config["enabled"]
    is_critical = step_config["critical"]

    step_log_id = str(uuid.uuid4())
    step_started_at = datetime.now()

    if not is_enabled:

        step_finished_at = datetime.now()
        duration_seconds = (
            step_finished_at - step_started_at
        ).total_seconds()

        write_pipeline_log(
            log_id=step_log_id,
            execution_id=JOB_RUN_ID,
            notebook_name=NOTEBOOK_NAME,
            layer_name=step_layer,
            entity_name=step_entity,
            target_table="not_applicable",
            status=EXECUTION_STATUS_WARNING,
            message=f"Step skipped because it is disabled: {step_name}",
            started_at=step_started_at,
            finished_at=step_finished_at,
            duration_seconds=duration_seconds,
            records_read=None,
            records_written=None,
        )

        log_warning(
            pipeline_logger=logger,
            message=f"Step skipped: {step_name}",
        )

        return {
            "step_name": step_name,
            "status": EXECUTION_STATUS_WARNING,
            "message": "Step skipped because it is disabled.",
            "duration_seconds": duration_seconds,
            "critical": is_critical,
        }

    try:

        write_pipeline_log(
            log_id=step_log_id,
            execution_id=JOB_RUN_ID,
            notebook_name=NOTEBOOK_NAME,
            layer_name=step_layer,
            entity_name=step_entity,
            target_table="not_applicable",
            status=EXECUTION_STATUS_STARTED,
            message=f"Step execution started: {step_name}",
            started_at=step_started_at,
            finished_at=None,
            duration_seconds=None,
            records_read=None,
            records_written=None,
        )

        log_info(
            pipeline_logger=logger,
            message=f"Starting step: {step_name}",
        )

        dbutils.notebook.run(
            notebook_path,
            timeout_seconds=0,
        )

        step_finished_at = datetime.now()
        duration_seconds = (
            step_finished_at - step_started_at
        ).total_seconds()

        write_pipeline_log(
            log_id=str(uuid.uuid4()),
            execution_id=JOB_RUN_ID,
            notebook_name=NOTEBOOK_NAME,
            layer_name=step_layer,
            entity_name=step_entity,
            target_table="not_applicable",
            status=EXECUTION_STATUS_SUCCESS,
            message=f"Step completed successfully: {step_name}",
            started_at=step_started_at,
            finished_at=step_finished_at,
            duration_seconds=duration_seconds,
            records_read=None,
            records_written=None,
        )

        log_success(
            pipeline_logger=logger,
            message=f"Step completed: {step_name}",
        )

        return {
            "step_name": step_name,
            "status": EXECUTION_STATUS_SUCCESS,
            "message": "Step completed successfully.",
            "duration_seconds": duration_seconds,
            "critical": is_critical,
        }

    except Exception as error:

        step_finished_at = datetime.now()
        duration_seconds = (
            step_finished_at - step_started_at
        ).total_seconds()

        error_message = str(error)
        error_stacktrace = traceback.format_exc()

        write_pipeline_log(
            log_id=str(uuid.uuid4()),
            execution_id=JOB_RUN_ID,
            notebook_name=NOTEBOOK_NAME,
            layer_name=step_layer,
            entity_name=step_entity,
            target_table="not_applicable",
            status=EXECUTION_STATUS_FAILED,
            message=f"Step failed: {step_name} | {error_message}",
            started_at=step_started_at,
            finished_at=step_finished_at,
            duration_seconds=duration_seconds,
            records_read=None,
            records_written=None,
        )

        log_error(
            pipeline_logger=logger,
            message=f"Step failed: {step_name}",
            error=error,
        )

        return {
            "step_name": step_name,
            "status": EXECUTION_STATUS_FAILED,
            "message": error_message,
            "duration_seconds": duration_seconds,
            "critical": is_critical,
            "stacktrace": error_stacktrace,
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Execute Pipeline Steps

# COMMAND ----------

pipeline_results = []
pipeline_started_at = datetime.now()

for step_config in PIPELINE_STEPS:

    step_result = run_notebook_step(
        step_config=step_config,
    )

    pipeline_results.append(step_result)

    if (
        step_result["status"] == EXECUTION_STATUS_FAILED
        and step_result["critical"]
        and FAIL_ON_STEP_ERROR
    ):

        raise Exception(
            f"Critical pipeline step failed: "
            f"{step_result['step_name']} | "
            f"{step_result['message']}"
        )

pipeline_finished_at = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline Summary

# COMMAND ----------

total_steps = len(pipeline_results)

success_steps = len([
    result
    for result in pipeline_results
    if result["status"] == EXECUTION_STATUS_SUCCESS
])

warning_steps = len([
    result
    for result in pipeline_results
    if result["status"] == EXECUTION_STATUS_WARNING
])

failed_steps = len([
    result
    for result in pipeline_results
    if result["status"] == EXECUTION_STATUS_FAILED
])

pipeline_duration_seconds = (
    pipeline_finished_at - pipeline_started_at
).total_seconds()

print("=" * 90)
print("FULL PIPELINE SUMMARY")
print("=" * 90)
print(f"Job Run ID: {JOB_RUN_ID}")
print(f"Run Mode: {RUN_MODE}")
print(f"Total steps: {total_steps}")
print(f"Successful steps: {success_steps}")
print(f"Warning/skipped steps: {warning_steps}")
print(f"Failed steps: {failed_steps}")
print(f"Pipeline duration seconds: {pipeline_duration_seconds}")
print("=" * 90)

# COMMAND ----------

final_status = (
    EXECUTION_STATUS_FAILED
    if failed_steps > 0
    else EXECUTION_STATUS_SUCCESS
)

write_pipeline_log(
    log_id=str(uuid.uuid4()),
    execution_id=JOB_RUN_ID,
    notebook_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
    entity_name=ENTITY_NAME,
    target_table="not_applicable",
    status=final_status,
    message=(
        f"Full pipeline finished. "
        f"run_mode={RUN_MODE}, "
        f"success={success_steps}, "
        f"warning={warning_steps}, "
        f"failed={failed_steps}"
    ),
    started_at=pipeline_started_at,
    finished_at=pipeline_finished_at,
    duration_seconds=pipeline_duration_seconds,
    records_read=None,
    records_written=None,
)

# COMMAND ----------

if failed_steps > 0:

    print(
        f"WARNING: Full pipeline finished with "
        f"{failed_steps} failed step(s)."
    )

    if FAIL_ON_STEP_ERROR:

        raise Exception(
            f"Full pipeline failed with "
            f"{failed_steps} failed step(s)."
        )

print("FULL PIPELINE EXECUTION COMPLETED")