# Databricks notebook source
# MAGIC %md
# MAGIC # Jobs Layer — Incremental CEAP Pipeline
# MAGIC
# MAGIC **Notebook:** `02_run_incremental_expenses_ceap`  
# MAGIC **Layer:** `Jobs`  
# MAGIC **Source/Endpoint:** `CEAP Medallion Pipeline Notebooks`  
# MAGIC **Target:** `Incremental CEAP pipeline orchestration and audit logs`
# MAGIC
# MAGIC Orchestrates the incremental execution flow for CEAP expenses in the
# MAGIC Brazil Legislative Analytics Medallion project.
# MAGIC
# MAGIC This notebook manages incremental CEAP ingestion, transformation,
# MAGIC analytical modeling and quality validation workflows across Medallion layers.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Execute incremental CEAP pipeline notebooks
# MAGIC - Control execution flow by configured run modes
# MAGIC - Support API and CSV fallback ingestion strategies
# MAGIC - Register execution logs and pipeline status
# MAGIC - Handle step-level failures and warnings
# MAGIC - Generate incremental pipeline execution summary
# MAGIC - Support validation-only execution scenarios
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Optimized for Databricks Free Edition execution
# MAGIC - CSV fallback is the recommended operational ingestion strategy
# MAGIC - API ingestion notebooks are preserved for compatibility and validation
# MAGIC - Pipeline interruption behavior is controlled by `FAIL_ON_STEP_ERROR`
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/operations/pipeline_orchestration.md`
# MAGIC - `/docs/operations/ceap_incremental_processing.md`

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

# ============================================================
# EXECUTION HEADER
# ============================================================

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("02 - RUN INCREMENTAL EXPENSES CEAP")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Environment: {PROJECT_ENVIRONMENT}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# JOB CONFIGURATION
# ============================================================

NOTEBOOK_NAME = "02_run_incremental_expenses_ceap"
LAYER_NAME = "jobs"
ENTITY_NAME = "incremental_expenses_ceap"

JOB_RUN_ID = str(uuid.uuid4())

# Options:
# - "fast"
# - "ceap_full"
# - "validation_only"
RUN_MODE = "fast"

FAIL_ON_STEP_ERROR = False

NOTEBOOK_TIMEOUT_SECONDS = 3600

logger = get_logger(
    logger_name=NOTEBOOK_NAME,
    layer_name=LAYER_NAME,
)

# COMMAND ----------

# ============================================================
# INCREMENTAL STEP CONFIGURATION
# ============================================================
#
# enabled_modes controls in which run mode each notebook is executed.
#
# Performance recommendation:
# - Keep API-heavy CEAP notebooks out of fast mode.
# - Use CSV fallback as the operational Bronze CEAP ingestion path.
# - Enable Silver, Gold and Marts only when those notebooks are available.
#
# ============================================================

INCREMENTAL_STEPS = [
    {
        "step_name": "validate_project_setup",
        "notebook_path": "../00_setup/90_validate_project_setup",
        "layer_name": "setup",
        "entity_name": "project_setup",
        "enabled_modes": ["validation_only", "ceap_full"],
        "critical": True,
    },
    {
        "step_name": "validate_api_connection",
        "notebook_path": "../00_setup/92_validate_api_connection",
        "layer_name": "setup",
        "entity_name": "api_connection",
        "enabled_modes": ["ceap_full"],
        "critical": False,
    },

    # ========================================================
    # BRONZE CEAP INGESTION
    # ========================================================

    {
        "step_name": "bronze_despesas_ceap_api",
        "notebook_path": "../01_bronze/06_bronze_despesas_ceap",
        "layer_name": "bronze",
        "entity_name": "despesas_ceap",
        "enabled_modes": ["ceap_full"],
        "critical": False,
    },
    {
        "step_name": "bronze_despesas_ceap_csv_fallback",
        "notebook_path": "../01_bronze/06a_bronze_despesas_ceap_csv_fallback",
        "layer_name": "bronze",
        "entity_name": "despesas_ceap",
        "enabled_modes": ["fast", "ceap_full"],
        "critical": True,
    },

    # ========================================================
    # SILVER CEAP TRANSFORMATIONS
    # Disabled until Silver notebooks are available and validated.
    # ========================================================

    {
        "step_name": "silver_despesas_ceap",
        "notebook_path": "../02_silver/09_silver_despesas_ceap",
        "layer_name": "silver",
        "entity_name": "despesas_ceap",
        "enabled_modes": ["ceap_full"],
        "critical": True,
    },
    {
        "step_name": "silver_fornecedores",
        "notebook_path": "../02_silver/10_silver_fornecedores",
        "layer_name": "silver",
        "entity_name": "fornecedores",
        "enabled_modes": ["ceap_full"],
        "critical": True,
    },

    # ========================================================
    # GOLD CEAP MODELING
    # Disabled until Gold notebooks are available and validated.
    # ========================================================

    {
        "step_name": "gold_dm_fornecedor",
        "notebook_path": "../03_gold/11_dm_fornecedor",
        "layer_name": "gold",
        "entity_name": "dm_fornecedor",
        "enabled_modes": ["ceap_full"],
        "critical": True,
    },
    {
        "step_name": "gold_ft_despesas_ceap",
        "notebook_path": "../03_gold/16_ft_despesas_ceap",
        "layer_name": "gold",
        "entity_name": "ft_despesas_ceap",
        "enabled_modes": ["ceap_full"],
        "critical": True,
    },

    # ========================================================
    # CEAP ANALYTICAL MART
    # Disabled until Marts notebooks are available and validated.
    # ========================================================

    {
        "step_name": "mart_panorama_despesas_ceap",
        "notebook_path": "../04_marts/04_am_panorama_despesas_ceap",
        "layer_name": "marts",
        "entity_name": "panorama_despesas_ceap",
        "enabled_modes": ["ceap_full"],
        "critical": True,
    },

    # ========================================================
    # QUALITY CHECKS
    # Disabled in fast mode to avoid unnecessary overhead.
    # ========================================================

    {
        "step_name": "quality_bronze_checks",
        "notebook_path": "../06_quality/01_quality_bronze_checks",
        "layer_name": "quality",
        "entity_name": "bronze_checks",
        "enabled_modes": ["ceap_full"],
        "critical": False,
    },
    {
        "step_name": "quality_silver_checks",
        "notebook_path": "../06_quality/02_quality_silver_checks",
        "layer_name": "quality",
        "entity_name": "silver_checks",
        "enabled_modes": ["ceap_full"],
        "critical": False,
    },
    {
        "step_name": "quality_gold_checks",
        "notebook_path": "../06_quality/03_quality_gold_checks",
        "layer_name": "quality",
        "entity_name": "gold_checks",
        "enabled_modes": ["ceap_full"],
        "critical": False,
    },
    {
        "step_name": "quality_traceability_checks",
        "notebook_path": "../06_quality/04_traceability_checks",
        "layer_name": "quality",
        "entity_name": "traceability_checks",
        "enabled_modes": ["ceap_full"],
        "critical": False,
    },
]

# COMMAND ----------

# ============================================================
# JOB HELPER FUNCTION
# ============================================================

def run_notebook_step(
    step_config: dict,
) -> dict:
    """
    Executes a configured notebook step and returns execution metadata.
    """

    step_name = step_config["step_name"]
    notebook_path = step_config["notebook_path"]
    step_layer = step_config["layer_name"]
    step_entity = step_config["entity_name"]
    enabled_modes = step_config["enabled_modes"]
    is_critical = step_config["critical"]

    step_log_id = str(uuid.uuid4())
    step_started_at = datetime.now()

    is_enabled = RUN_MODE in enabled_modes

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
            message=(
                f"Step skipped by RUN_MODE "
                f"| step={step_name} "
                f"| run_mode={RUN_MODE}"
            ),
            started_at=step_started_at,
            finished_at=step_finished_at,
            duration_seconds=duration_seconds,
            records_read=None,
            records_written=None,
        )

        log_warning(
            pipeline_logger=logger,
            message=(
                f"Step skipped by RUN_MODE "
                f"| step={step_name} "
                f"| run_mode={RUN_MODE}"
            ),
        )

        return {
            "step_name": step_name,
            "status": EXECUTION_STATUS_WARNING,
            "message": "Step skipped by RUN_MODE.",
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
            message=(
                f"Step execution started "
                f"| step={step_name} "
                f"| run_mode={RUN_MODE}"
            ),
            started_at=step_started_at,
            finished_at=None,
            duration_seconds=None,
            records_read=None,
            records_written=None,
        )

        log_info(
            pipeline_logger=logger,
            message=(
                f"Starting step "
                f"| step={step_name} "
                f"| run_mode={RUN_MODE}"
            ),
        )

        dbutils.notebook.run(
            notebook_path,
            timeout_seconds=NOTEBOOK_TIMEOUT_SECONDS,
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
            message=(
                f"Step completed successfully "
                f"| step={step_name} "
                f"| run_mode={RUN_MODE}"
            ),
            started_at=step_started_at,
            finished_at=step_finished_at,
            duration_seconds=duration_seconds,
            records_read=None,
            records_written=None,
        )

        log_success(
            pipeline_logger=logger,
            message=(
                f"Step completed "
                f"| step={step_name}"
            ),
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
            message=(
                f"Step failed "
                f"| step={step_name} "
                f"| error={error_message}"
            ),
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
# MAGIC ## 1. Execute Incremental CEAP Steps

# COMMAND ----------

incremental_results = []
incremental_started_at = datetime.now()

for step_config in INCREMENTAL_STEPS:

    step_result = run_notebook_step(
        step_config=step_config,
    )

    incremental_results.append(step_result)

    if (
        step_result["status"] == EXECUTION_STATUS_FAILED
        and step_result["critical"]
        and FAIL_ON_STEP_ERROR
    ):

        raise Exception(
            f"Critical incremental CEAP step failed: "
            f"{step_result['step_name']} | "
            f"{step_result['message']}"
        )

incremental_finished_at = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Incremental CEAP Summary

# COMMAND ----------

total_steps = len(incremental_results)

success_steps = len([
    result
    for result in incremental_results
    if result["status"] == EXECUTION_STATUS_SUCCESS
])

warning_steps = len([
    result
    for result in incremental_results
    if result["status"] == EXECUTION_STATUS_WARNING
])

failed_steps = len([
    result
    for result in incremental_results
    if result["status"] == EXECUTION_STATUS_FAILED
])

incremental_duration_seconds = (
    incremental_finished_at - incremental_started_at
).total_seconds()

print("=" * 90)
print("INCREMENTAL CEAP PIPELINE SUMMARY")
print("=" * 90)
print(f"Job Run ID: {JOB_RUN_ID}")
print(f"Run Mode: {RUN_MODE}")
print(f"Total steps: {total_steps}")
print(f"Successful steps: {success_steps}")
print(f"Warning/skipped steps: {warning_steps}")
print(f"Failed steps: {failed_steps}")
print(f"Pipeline duration seconds: {incremental_duration_seconds}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# FINAL JOB LOG
# ============================================================

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
        f"Incremental CEAP pipeline finished "
        f"| run_mode={RUN_MODE} "
        f"| success={success_steps} "
        f"| warning={warning_steps} "
        f"| failed={failed_steps}"
    ),
    started_at=incremental_started_at,
    finished_at=incremental_finished_at,
    duration_seconds=incremental_duration_seconds,
    records_read=None,
    records_written=None,
)

# COMMAND ----------

if failed_steps > 0:

    print(
        f"WARNING: Incremental CEAP pipeline finished with "
        f"{failed_steps} failed step(s)."
    )

    if FAIL_ON_STEP_ERROR:

        raise Exception(
            f"Incremental CEAP pipeline failed with "
            f"{failed_steps} failed step(s)."
        )

print("INCREMENTAL CEAP PIPELINE EXECUTION COMPLETED")