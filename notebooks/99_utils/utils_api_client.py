# Databricks notebook source
# MAGIC %md
# MAGIC # Utils Layer — Câmara API Client
# MAGIC
# MAGIC **Notebook:** `utils_api_client`  
# MAGIC **Layer:** `Utils`  
# MAGIC **Source/Endpoint:** `Câmara dos Deputados Open Data API`  
# MAGIC **Target:** `Reusable API request and response handling functions`
# MAGIC
# MAGIC Provides reusable functions to request and process data from the
# MAGIC Câmara dos Deputados Open Data API.
# MAGIC
# MAGIC This notebook centralizes API access logic used across Bronze ingestion
# MAGIC and validation workflows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Execute HTTP GET requests against Câmara Open Data API
# MAGIC - Build standardized API request URLs
# MAGIC - Apply timeout and retry strategies
# MAGIC - Standardize JSON response handling
# MAGIC - Extract records from API payloads
# MAGIC - Validate endpoint availability
# MAGIC - Support resilient ingestion workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Shared utility notebook across ingestion workflows
# MAGIC - Supports configurable timeout and retry strategies
# MAGIC - Preserves detailed error messages for troubleshooting
# MAGIC - Optimized for reusable and resilient API integration patterns
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/integration/api_integration_patterns.md`
# MAGIC - `/docs/standards/coding_standards.md`

# COMMAND ----------

# MAGIC %run ./utils_config

# COMMAND ----------

# MAGIC %md
# MAGIC # 99 Utils — API Client
# MAGIC
# MAGIC **Notebook:** `utils_api_client`
# MAGIC
# MAGIC Provides reusable HTTP request utilities for the Câmara dos Deputados
# MAGIC Open Data API.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Centralize API request logic
# MAGIC - Apply timeout and retry strategy
# MAGIC - Standardize API error classification
# MAGIC - Return parsed JSON responses
# MAGIC - Extract API response records
# MAGIC - Preserve backward compatibility with older notebooks
# MAGIC - Support `utils_pagination`

# COMMAND ----------

# MAGIC %run ../00_setup/01_project_config

# COMMAND ----------

import time
import requests
from typing import Optional, Dict, Any, List

# COMMAND ----------

# ============================================================
# API CLIENT CONFIGURATION
# ============================================================

DEFAULT_REQUEST_TIMEOUT_SECONDS = globals().get(
    "API_REQUEST_TIMEOUT_SECONDS",
    120,
)

DEFAULT_MAX_RETRY_ATTEMPTS = globals().get(
    "API_MAX_RETRY_ATTEMPTS",
    3,
)

DEFAULT_RETRY_SLEEP_SECONDS = globals().get(
    "API_RETRY_SLEEP_SECONDS",
    2,
)

DEFAULT_RESPONSE_DATA_FIELD = globals().get(
    "API_RESPONSE_DATA_FIELD",
    "dados",
)

# COMMAND ----------

def classify_api_error(
    error: Exception,
) -> str:
    """
    Classifies API errors into operational categories.
    """

    error_text = str(error).lower()

    if "timeout" in error_text or "timed out" in error_text:
        return "timeout"

    if "connection" in error_text:
        return "connection"

    if "404" in error_text:
        return "not_found"

    if "400" in error_text:
        return "bad_request"

    if "429" in error_text:
        return "rate_limit"

    if (
        "500" in error_text
        or "502" in error_text
        or "503" in error_text
        or "504" in error_text
    ):
        return "server_error"

    return "unknown"

# COMMAND ----------

def build_api_url(
    endpoint_path: str,
) -> str:
    """
    Builds a full API URL from a relative endpoint path.
    """

    normalized_base_url = CAMARA_API_BASE_URL.rstrip("/")
    normalized_endpoint = endpoint_path.strip()

    if not normalized_endpoint.startswith("/"):
        normalized_endpoint = f"/{normalized_endpoint}"

    return f"{normalized_base_url}{normalized_endpoint}"

# COMMAND ----------

def make_api_request(
    endpoint_path: str,
    params: Optional[Dict[str, Any]] = None,
    request_timeout: Optional[int] = None,
    max_retries: Optional[int] = None,
    sleep_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Executes a GET request against the Câmara API and returns the JSON payload.

    Parameters
    ----------
    endpoint_path:
        Relative API endpoint path, for example `/deputados`.

    params:
        Optional query parameters.

    request_timeout:
        Request timeout in seconds.

    max_retries:
        Number of retry attempts.

    sleep_seconds:
        Seconds to wait between retries.

    Returns
    -------
    dict
        Parsed JSON response payload.
    """

    request_params = params or {}

    timeout_seconds = (
        request_timeout
        if request_timeout is not None
        else DEFAULT_REQUEST_TIMEOUT_SECONDS
    )

    retry_attempts = (
        max_retries
        if max_retries is not None
        else DEFAULT_MAX_RETRY_ATTEMPTS
    )

    retry_sleep = (
        sleep_seconds
        if sleep_seconds is not None
        else DEFAULT_RETRY_SLEEP_SECONDS
    )

    url = build_api_url(
        endpoint_path=endpoint_path,
    )

    last_error = None

    for attempt_number in range(
        1,
        retry_attempts + 1,
    ):

        try:

            response = requests.get(
                url=url,
                params=request_params,
                timeout=timeout_seconds,
                headers={
                    "accept": "application/json",
                    "User-Agent": "brazil-legislative-analytics/1.0",
                },
            )

            response.raise_for_status()

            response_payload = response.json()

            if response_payload is None:
                return {}

            return response_payload

        except Exception as error:

            last_error = error
            error_type = classify_api_error(
                error=error,
            )

            print(
                "[WARNING] API request failed "
                f"| endpoint={endpoint_path} "
                f"| attempt={attempt_number}/{retry_attempts} "
                f"| error_type={error_type} "
                f"| error={str(error)}"
            )

            if attempt_number < retry_attempts:
                time.sleep(retry_sleep)

    raise Exception(
        f"API request failed after {retry_attempts} attempt(s) "
        f"| endpoint={endpoint_path} "
        f"| params={request_params} "
        f"| last_error={str(last_error)}"
    )

# COMMAND ----------

def extract_response_records(
    api_response: Dict[str, Any],
    data_field: str = DEFAULT_RESPONSE_DATA_FIELD,
) -> List[Dict[str, Any]]:
    """
    Extracts the records list from a Câmara API response payload.

    This function is used by `utils_pagination`.
    """

    if api_response is None:
        return []

    if not isinstance(api_response, dict):
        return []

    records = api_response.get(
        data_field,
        [],
    )

    if records is None:
        return []

    if isinstance(records, list):
        return records

    return [records]

# COMMAND ----------

def get_api_data(
    endpoint_path: str,
    params: Optional[Dict[str, Any]] = None,
    request_timeout: Optional[int] = None,
    max_retries: Optional[int] = None,
    sleep_seconds: Optional[float] = None,
    data_field: str = DEFAULT_RESPONSE_DATA_FIELD,
) -> List[Dict[str, Any]]:
    """
    Executes an API request and returns only the data records list.
    """

    response_payload = make_api_request(
        endpoint_path=endpoint_path,
        params=params,
        request_timeout=request_timeout,
        max_retries=max_retries,
        sleep_seconds=sleep_seconds,
    )

    return extract_response_records(
        api_response=response_payload,
        data_field=data_field,
    )

# COMMAND ----------

def fetch_camara_api_data(
    endpoint_path: str,
    query_params: Optional[Dict[str, Any]] = None,
    request_timeout_seconds: Optional[int] = None,
    max_retry_attempts: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Backward-compatible wrapper used by `utils_pagination`
    and older Bronze notebooks.
    """

    return make_api_request(
        endpoint_path=endpoint_path,
        params=query_params or {},
        request_timeout=request_timeout_seconds,
        max_retries=max_retry_attempts,
    )

# COMMAND ----------

# ============================================================
# BACKWARD-COMPATIBILITY ALIASES
# ============================================================
#
# These aliases keep older notebooks working without requiring
# immediate refactoring.
#
# ============================================================

api_get = make_api_request
request_api = make_api_request
get_api_response = make_api_request

# COMMAND ----------

print("utils_api_client loaded successfully.")