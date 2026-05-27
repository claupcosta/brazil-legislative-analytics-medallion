# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Layer — API Connectivity Validation
# MAGIC
# MAGIC **Notebook:** `92_validate_api_connection`  
# MAGIC **Layer:** `Setup`  
# MAGIC **Source/Endpoint:** `Câmara dos Deputados Open Data API`  
# MAGIC **Target:** `API validation results and audit quality logs`
# MAGIC
# MAGIC Validates connectivity, availability and response structure of the
# MAGIC Câmara dos Deputados Open Data API before executing Bronze ingestion pipelines.
# MAGIC
# MAGIC This notebook verifies whether the external API is reachable and
# MAGIC operational for Medallion ingestion workflows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Validate API endpoint connectivity
# MAGIC - Validate API response structure
# MAGIC - Validate parameterized endpoint accessibility
# MAGIC - Measure API response time
# MAGIC - Persist API validation results into audit tables
# MAGIC - Generate API validation summary
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Supports FAST and DEEP validation modes
# MAGIC - Validation results are persisted into audit quality logs
# MAGIC - Parameterized endpoint validation is optional
# MAGIC - API instability may generate warning validations
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/governance/data_governance.md`
# MAGIC - `/docs/monitoring/api_validation.md`

# COMMAND ----------

# MAGIC %run ./01_project_config

# COMMAND ----------

# MAGIC %run ../99_utils/utils_api_client

# COMMAND ----------

from datetime import datetime
from typing import Optional, Dict, Any
import time

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType

# COMMAND ----------

print("=" * 90)
print("BRAZIL LEGISLATIVE ANALYTICS MEDALLION")
print("92 - VALIDATE API CONNECTION")
print("=" * 90)
print(f"Execution Timestamp: {datetime.now()}")
print(f"API Base URL: {CAMARA_API_BASE_URL}")
print("=" * 90)

# COMMAND ----------

# ============================================================
# API VALIDATION CONFIGURATION
# ============================================================

VALIDATION_MODE = "FAST"
FAIL_ON_ERROR = False

FAST_REQUEST_TIMEOUT_SECONDS = 15
DEEP_REQUEST_TIMEOUT_SECONDS = 60

API_TEST_QUERY_PARAMS = {
    API_PAGE_PARAMETER_NAME: 1,
    API_PAGE_SIZE_PARAMETER_NAME: 1,
}

FAST_API_ENDPOINTS = [
    {"entity_name": "deputados", "endpoint_path": "/deputados"},
    {"entity_name": "frentes", "endpoint_path": "/frentes"},
    {"entity_name": "votacoes", "endpoint_path": "/votacoes"},
]

DEEP_API_ENDPOINTS = [
    {"entity_name": "eventos", "endpoint_path": "/eventos"},
    {"entity_name": "proposicoes", "endpoint_path": "/proposicoes"},
    {"entity_name": "orgaos", "endpoint_path": "/orgaos"},
]

if VALIDATION_MODE == "DEEP":
    API_ENDPOINTS_TO_VALIDATE = (
        FAST_API_ENDPOINTS
        + DEEP_API_ENDPOINTS
    )
    REQUEST_TIMEOUT_SECONDS = DEEP_REQUEST_TIMEOUT_SECONDS

else:
    API_ENDPOINTS_TO_VALIDATE = FAST_API_ENDPOINTS
    REQUEST_TIMEOUT_SECONDS = FAST_REQUEST_TIMEOUT_SECONDS

DATA_QUALITY_LOG_TABLE = (
    f"{CATALOG_NAME}."
    f"{SCHEMA_AUDIT}."
    f"{AUD_TB_LOG_QUALIDADE_DADOS}"
)

validation_results = []

# COMMAND ----------

# ============================================================
# API VALIDATION HELPERS
# ============================================================

def request_endpoint(
    endpoint_path: str,
    query_params: Optional[Dict[str, Any]] = None,
) -> dict:
    """
    Requests a Câmara API endpoint using the project API client.
    """

    return fetch_camara_api_data(
        endpoint_path=endpoint_path,
        query_params=query_params,
        request_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
        max_retry_attempts=1,
    )

# COMMAND ----------

def extract_first_id(
    api_response: dict,
    id_field: str = "id",
) -> Optional[str]:
    """
    Extracts the first identifier from an API response.
    """

    records = api_response.get(API_RESPONSE_DATA_FIELD, [])

    if not records:
        return None

    first_record = records[0]

    return first_record.get(id_field)

# COMMAND ----------

def register_api_validation(
    entity_name: str,
    endpoint_path: str,
    validation_status: str,
    validation_message: str,
    response_time_seconds: float,
    validation_type: str,
) -> None:
    """
    Stores API validation results for final reporting and persistence.
    """

    validation_results.append({
        "entity_name": entity_name,
        "endpoint_path": endpoint_path,
        "validation_type": validation_type,
        "validation_status": validation_status,
        "validation_message": validation_message,
        "response_time_seconds": response_time_seconds,
        "validated_at": datetime.now(),
    })

# COMMAND ----------

def validate_endpoint(
    entity_name: str,
    endpoint_path: str,
    query_params: Optional[Dict[str, Any]],
    validation_type: str,
    fail_as_warning: bool = False,
) -> Optional[dict]:
    """
    Validates a single API endpoint and stores the result.
    """

    print("=" * 90)
    print(f"Validating endpoint: {endpoint_path}")
    print("=" * 90)

    validation_start_time = time.time()

    try:
        api_response = request_endpoint(
            endpoint_path=endpoint_path,
            query_params=query_params,
        )

        response_time_seconds = round(
            time.time() - validation_start_time,
            2,
        )

        if not isinstance(api_response, dict):
            status = (
                "WARNING"
                if fail_as_warning
                else "FAILED"
            )

            register_api_validation(
                entity_name=entity_name,
                endpoint_path=endpoint_path,
                validation_status=status,
                validation_message="API response is not a dictionary structure.",
                response_time_seconds=response_time_seconds,
                validation_type=validation_type,
            )

            return None

        if API_RESPONSE_DATA_FIELD not in api_response:
            status = (
                "WARNING"
                if fail_as_warning
                else "FAILED"
            )

            register_api_validation(
                entity_name=entity_name,
                endpoint_path=endpoint_path,
                validation_status=status,
                validation_message="API response does not contain the expected 'dados' field.",
                response_time_seconds=response_time_seconds,
                validation_type=validation_type,
            )

            return None

        register_api_validation(
            entity_name=entity_name,
            endpoint_path=endpoint_path,
            validation_status="PASSED",
            validation_message="API endpoint validated successfully.",
            response_time_seconds=response_time_seconds,
            validation_type=validation_type,
        )

        return api_response

    except Exception as api_error:
        response_time_seconds = round(
            time.time() - validation_start_time,
            2,
        )

        status = (
            "WARNING"
            if fail_as_warning
            else "FAILED"
        )

        register_api_validation(
            entity_name=entity_name,
            endpoint_path=endpoint_path,
            validation_status=status,
            validation_message=str(api_error),
            response_time_seconds=response_time_seconds,
            validation_type=validation_type,
        )

        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validate API Endpoints

# COMMAND ----------

api_responses = {}

for endpoint_config in API_ENDPOINTS_TO_VALIDATE:
    entity_name = endpoint_config["entity_name"]
    endpoint_path = endpoint_config["endpoint_path"]

    api_response = validate_endpoint(
        entity_name=entity_name,
        endpoint_path=endpoint_path,
        query_params=API_TEST_QUERY_PARAMS,
        validation_type=(
            "fast_endpoint"
            if VALIDATION_MODE == "FAST"
            else "deep_endpoint"
        ),
        fail_as_warning=not FAIL_ON_ERROR,
    )

    api_responses[entity_name] = api_response

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate Parameterized API Endpoints

# COMMAND ----------

parameterized_endpoints = []

if VALIDATION_MODE == "DEEP":

    test_front_id = None
    test_voting_id = None
    test_deputy_id = None

    if api_responses.get("frentes") is not None:
        test_front_id = extract_first_id(
            api_response=api_responses["frentes"],
            id_field="id",
        )

    if api_responses.get("votacoes") is not None:
        test_voting_id = extract_first_id(
            api_response=api_responses["votacoes"],
            id_field="id",
        )

    if api_responses.get("deputados") is not None:
        test_deputy_id = extract_first_id(
            api_response=api_responses["deputados"],
            id_field="id",
        )

    print("=" * 90)
    print("DYNAMIC API IDENTIFIERS")
    print("=" * 90)
    print(f"Front ID: {test_front_id}")
    print(f"Voting ID: {test_voting_id}")
    print(f"Deputy ID: {test_deputy_id}")
    print("=" * 90)

    if test_front_id is not None:
        parameterized_endpoints.append({
            "entity_name": "frente_detalhe",
            "endpoint_path": f"/frentes/{test_front_id}",
        })

        parameterized_endpoints.append({
            "entity_name": "frentes_membros",
            "endpoint_path": f"/frentes/{test_front_id}/membros",
        })

    if test_voting_id is not None:
        parameterized_endpoints.append({
            "entity_name": "votos",
            "endpoint_path": f"/votacoes/{test_voting_id}/votos",
        })

    if test_deputy_id is not None:
        parameterized_endpoints.append({
            "entity_name": "despesas_ceap",
            "endpoint_path": f"/deputados/{test_deputy_id}/despesas",
        })

    if not parameterized_endpoints:
        register_api_validation(
            entity_name="parameterized_endpoints",
            endpoint_path="dynamic_identifier_resolution",
            validation_status="WARNING",
            validation_message="No dynamic identifiers were resolved for parameterized endpoint validation.",
            response_time_seconds=0.0,
            validation_type="parameterized_endpoint",
        )

    for endpoint_config in parameterized_endpoints:
        validate_endpoint(
            entity_name=endpoint_config["entity_name"],
            endpoint_path=endpoint_config["endpoint_path"],
            query_params=API_TEST_QUERY_PARAMS,
            validation_type="parameterized_endpoint",
            fail_as_warning=True,
        )

else:

    register_api_validation(
        entity_name="parameterized_endpoints",
        endpoint_path="parameterized_endpoint_validation",
        validation_status="WARNING",
        validation_message="Parameterized endpoint validation was skipped in FAST mode.",
        response_time_seconds=0.0,
        validation_type="parameterized_endpoint_skipped",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Display Validation Results

# COMMAND ----------

validation_df = spark.createDataFrame(
    validation_results
)

display(validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persist API Validation Results

# COMMAND ----------

api_quality_log_df = (
    validation_df
    .withColumn("qlt_id_log", F.expr("uuid()"))
    .withColumn("aud_id_execucao", F.lit(RUN_ID))
    .withColumn("aud_tx_nome_projeto", F.lit(PROJECT_NAME))
    .withColumn("aud_tx_versao_pipeline", F.lit(PROJECT_VERSION))
    .withColumn("aud_tx_ambiente", F.lit(PROJECT_ENVIRONMENT))
    .withColumn("aud_tx_nome_notebook", F.lit("92_validate_api_connection"))
    .withColumn("aud_tx_nome_camada", F.lit("setup"))
    .withColumn("aud_tx_nome_entidade", F.col("entity_name"))
    .withColumn("aud_tx_tabela_destino", F.lit(DATA_QUALITY_LOG_TABLE))
    .withColumn("qlt_tx_nome_regra", F.col("validation_type"))
    .withColumn(
        "qlt_tx_descricao_regra",
        F.concat(
            F.lit("Validates API endpoint connectivity and response structure: "),
            F.col("endpoint_path"),
        ),
    )
    .withColumn("qlt_tx_status_validacao", F.col("validation_status"))
    .withColumn("qlt_qt_total_registros", F.lit(1).cast(LongType()))
    .withColumn(
        "qlt_qt_registros_invalidos",
        F.when(
            F.col("validation_status") == "FAILED",
            F.lit(1),
        )
        .otherwise(F.lit(0))
        .cast(LongType()),
    )
    .withColumn(
        "qlt_pc_registros_invalidos",
        F.when(
            F.col("validation_status") == "FAILED",
            F.lit(100.0),
        )
        .otherwise(F.lit(0.0))
        .cast(DoubleType()),
    )
    .withColumn("qlt_dh_validacao", F.current_timestamp())
    .withColumn(
        "qlt_tx_mensagem",
        F.concat(
            F.lit("Endpoint: "),
            F.col("endpoint_path"),
            F.lit(" | Validation mode: "),
            F.lit(VALIDATION_MODE),
            F.lit(" | Validation type: "),
            F.col("validation_type"),
            F.lit(" | Response time: "),
            F.col("response_time_seconds").cast("string"),
            F.lit(" seconds | "),
            F.col("validation_message"),
        ),
    )
    .select(
        "qlt_id_log",
        "aud_id_execucao",
        "aud_tx_nome_projeto",
        "aud_tx_versao_pipeline",
        "aud_tx_ambiente",
        "aud_tx_nome_notebook",
        "aud_tx_nome_camada",
        "aud_tx_nome_entidade",
        "aud_tx_tabela_destino",
        "qlt_tx_nome_regra",
        "qlt_tx_descricao_regra",
        "qlt_tx_status_validacao",
        "qlt_qt_total_registros",
        "qlt_qt_registros_invalidos",
        "qlt_pc_registros_invalidos",
        "qlt_dh_validacao",
        "qlt_tx_mensagem",
    )
)

api_quality_log_df.write.mode(
    "append"
).saveAsTable(DATA_QUALITY_LOG_TABLE)

print(
    f"API validation results persisted into: "
    f"{DATA_QUALITY_LOG_TABLE}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation Summary

# COMMAND ----------

failed_count = (
    validation_df
    .filter("validation_status = 'FAILED'")
    .count()
)

warning_count = (
    validation_df
    .filter("validation_status = 'WARNING'")
    .count()
)

passed_count = (
    validation_df
    .filter("validation_status = 'PASSED'")
    .count()
)

print("=" * 90)
print("API VALIDATION SUMMARY")
print("=" * 90)
print(f"Validation mode: {VALIDATION_MODE}")
print(f"Passed validations: {passed_count}")
print(f"Warning validations: {warning_count}")
print(f"Failed validations: {failed_count}")
print("=" * 90)

# COMMAND ----------

if failed_count > 0 and FAIL_ON_ERROR:

    raise Exception(
        f"API validation failed with "
        f"{failed_count} failed endpoint validation(s)."
    )

if failed_count > 0:

    print(
        f"WARNING: API validation finished with "
        f"{failed_count} failed validation(s). "
        "This may be caused by API slowness or provider instability."
    )

if warning_count > 0:

    print(
        f"WARNING: {warning_count} warning(s) found. "
        "Review API validation results before Bronze ingestion."
    )

print("API VALIDATION COMPLETED")