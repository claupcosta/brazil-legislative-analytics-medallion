# Databricks notebook source
# MAGIC %md
# MAGIC # Utils Layer — Console Logger
# MAGIC
# MAGIC **Notebook:** `utils_logger`  
# MAGIC **Layer:** `Utils`  
# MAGIC **Source/Endpoint:** `Python Logging Framework`  
# MAGIC **Target:** `Reusable console logging functions`
# MAGIC
# MAGIC Provides standardized console logging utilities for the
# MAGIC Brazil Legislative Analytics Medallion pipeline.
# MAGIC
# MAGIC This notebook centralizes operational log formatting and execution messaging
# MAGIC across Bronze, Silver, Gold, Marts, Quality and Jobs notebooks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Standardize operational logging across pipeline notebooks
# MAGIC - Support structured log levels such as INFO, WARNING, SUCCESS and ERROR
# MAGIC - Include Medallion layer context in log messages
# MAGIC - Improve execution traceability during notebook runs
# MAGIC - Support troubleshooting during ingestion and transformation workflows
# MAGIC - Provide reusable logging helper functions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Shared utility notebook across Medallion layers
# MAGIC - Logs are written to notebook execution output
# MAGIC - Persistent logging is handled by `utils_table_logger`
# MAGIC - SUCCESS messages are internally registered as INFO logs for compatibility
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/monitoring/observability.md`
# MAGIC - `/docs/standards/coding_standards.md`
# MAGIC - `/docs/architecture/medallion_architecture.md`

# COMMAND ----------

import logging
from typing import Optional

# COMMAND ----------

def get_logger(
    logger_name: str,
    layer_name: str = "pipeline",
) -> logging.Logger:
    """
    Creates or retrieves a standardized pipeline logger.
    """

    normalized_layer = layer_name.strip().lower()
    full_logger_name = f"{normalized_layer}.{logger_name}"

    pipeline_logger = logging.getLogger(full_logger_name)

    if pipeline_logger.handlers:
        return pipeline_logger

    pipeline_logger.setLevel(logging.INFO)
    pipeline_logger.propagate = False

    log_formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(layer)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_formatter)

    class LayerFilter(logging.Filter):
        """
        Adds the Medallion layer name to each log record.
        """

        def filter(self, log_record: logging.LogRecord) -> bool:
            log_record.layer = normalized_layer.upper()
            return True

    console_handler.addFilter(LayerFilter())
    pipeline_logger.addHandler(console_handler)

    return pipeline_logger

# COMMAND ----------

def log_info(
    pipeline_logger: logging.Logger,
    message: str,
) -> None:
    """
    Registers an informational console log message.
    """

    pipeline_logger.info(message)

# COMMAND ----------

def log_warning(
    pipeline_logger: logging.Logger,
    message: str,
) -> None:
    """
    Registers a warning console log message.
    """

    pipeline_logger.warning(message)

# COMMAND ----------

def log_error(
    pipeline_logger: logging.Logger,
    message: str,
    error: Optional[Exception] = None,
) -> None:
    """
    Registers an error console log message.
    """

    if error is not None:
        pipeline_logger.error(f"{message} | error_detail={str(error)}")
    else:
        pipeline_logger.error(message)

# COMMAND ----------

def log_success(
    pipeline_logger: logging.Logger,
    message: str,
) -> None:
    """
    Registers a success console log message.

    Notes
    -----
    Python logging does not provide a native SUCCESS level.
    For Databricks compatibility, SUCCESS is registered as INFO with a prefix.
    """

    pipeline_logger.info(f"[SUCCESS] {message}")

# COMMAND ----------

print("utils_logger loaded successfully.")