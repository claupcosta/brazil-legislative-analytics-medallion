# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Utils Layer — Câmara API Pagination
# MAGIC
# MAGIC **Notebook:** `utils_pagination`  
# MAGIC **Layer:** `Utils`  
# MAGIC **Source/Endpoint:** `Câmara dos Deputados Open Data API`  
# MAGIC **Target:** `Reusable API pagination and extraction functions`
# MAGIC
# MAGIC Provides reusable pagination functions for Câmara dos Deputados
# MAGIC Open Data API endpoints.
# MAGIC
# MAGIC This notebook centralizes resilient page-based extraction logic
# MAGIC used across Bronze ingestion and validation workflows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Control page-based API extraction
# MAGIC - Apply pagination request parameters
# MAGIC - Reuse centralized API request functions
# MAGIC - Support retry behavior for unstable endpoints
# MAGIC - Handle empty and partial page responses safely
# MAGIC - Support controlled extraction limits for testing and development
# MAGIC - Provide reusable pagination helper functions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Shared utility notebook across ingestion workflows
# MAGIC - Designed for resilient API extraction patterns
# MAGIC - Supports configurable retry and timeout strategies
# MAGIC - Pagination logic intentionally remains simple for operational stability
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/integration/api_integration_patterns.md`
# MAGIC - `/docs/monitoring/api_validation.md`
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC

# COMMAND ----------

# MAGIC %run ./utils_api_client

# COMMAND ----------


from typing import (
    Optional,
    Dict,
    Any,
    List,
)

import time

# COMMAND ----------

# MAGIC %run ./utils_config

# COMMAND ----------

# MAGIC %run ./utils_api_client

# COMMAND ----------

def collect_pages(
    endpoint_path: str,
    base_params: Optional[Dict[str, Any]] = None,
    record_limit: Optional[int] = None,
    page_size: Optional[int] = None,
    request_timeout: Optional[int] = None,
    max_retries: Optional[int] = None,
    sleep_seconds: float = 0.5,
    max_pages: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Collects records from a paginated Câmara dos Deputados API endpoint.

    Parameters
    ----------
    endpoint_path : str
        API endpoint path.

    base_params : dict, optional
        Base query parameters.

    record_limit : int, optional
        Maximum number of records to collect.

    page_size : int, optional
        Number of records per page.

    request_timeout : int, optional
        API request timeout in seconds.

    max_retries : int, optional
        Maximum retry attempts per page.

    sleep_seconds : float
        Waiting time between requests.

    max_pages : int, optional
        Maximum number of pages to collect.

    Returns
    -------
    list
        List containing collected records.
    """

    if page_size is None:
        page_size = API_DEFAULT_PAGE_SIZE

    if request_timeout is None:
        request_timeout = API_REQUEST_TIMEOUT_SECONDS

    if max_retries is None:
        max_retries = API_MAX_RETRY_ATTEMPTS

    collected_records = []
    page_number = 1

    while True:

        if max_pages is not None and page_number > max_pages:

            print(
                f"[INFO] Pagination stopped by max_pages "
                f"| endpoint={endpoint_path} "
                f"| max_pages={max_pages}"
            )

            break

        page_params = dict(base_params or {})

        page_params[
            API_PAGE_PARAMETER_NAME
        ] = page_number

        page_params[
            API_PAGE_SIZE_PARAMETER_NAME
        ] = page_size

        last_error = None
        page_records = []

        for retry_number in range(
            1,
            max_retries + 1,
        ):

            try:

                api_response = (
                    fetch_camara_api_data(
                        endpoint_path=endpoint_path,
                        query_params=page_params,
                        request_timeout_seconds=request_timeout,
                        max_retry_attempts=1,
                    )
                )

                page_records = (
                    extract_response_records(
                        api_response=api_response,
                    )
                )

                break

            except Exception as error:

                last_error = error

                print(
                    f"[WARNING] Pagination request failed "
                    f"| endpoint={endpoint_path} "
                    f"| page={page_number} "
                    f"| attempt={retry_number}/{max_retries} "
                    f"| error={str(error)}"
                )

                if retry_number == max_retries:
                    raise last_error

                time.sleep(
                    sleep_seconds
                    * retry_number
                )

        if not page_records:

            print(
                f"[INFO] Pagination finished with empty response "
                f"| endpoint={endpoint_path} "
                f"| page={page_number}"
            )

            break

        collected_records.extend(
            page_records
        )

        print(
            f"[INFO] Page collected "
            f"| endpoint={endpoint_path} "
            f"| page={page_number} "
            f"| page_records={len(page_records)} "
            f"| total_records={len(collected_records)}"
        )

        if len(page_records) < page_size:

            print(
                f"[INFO] Pagination finished with partial last page "
                f"| endpoint={endpoint_path} "
                f"| page={page_number}"
            )

            break

        if (
            record_limit is not None
            and len(collected_records)
            >= record_limit
        ):

            print(
                f"[INFO] Pagination stopped by record limit "
                f"| endpoint={endpoint_path} "
                f"| limit={record_limit}"
            )

            break

        page_number += 1

        if sleep_seconds > 0:

            time.sleep(
                sleep_seconds
            )

    if record_limit is not None:

        return collected_records[
            :record_limit
        ]

    return collected_records

# COMMAND ----------

def collect_first_page(
    endpoint_path: str,
    base_params: Optional[Dict[str, Any]] = None,
    page_size: Optional[int] = None,
    request_timeout: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Collects only the first page from a Câmara API endpoint.
    """

    if page_size is None:
        page_size = API_DEFAULT_PAGE_SIZE

    if request_timeout is None:
        request_timeout = API_REQUEST_TIMEOUT_SECONDS

    first_page_params = dict(
        base_params or {}
    )

    first_page_params[
        API_PAGE_PARAMETER_NAME
    ] = 1

    first_page_params[
        API_PAGE_SIZE_PARAMETER_NAME
    ] = page_size

    api_response = (
        fetch_camara_api_data(
            endpoint_path=endpoint_path,
            query_params=first_page_params,
            request_timeout_seconds=request_timeout,
        )
    )

    return extract_response_records(
        api_response=api_response,
    )

# COMMAND ----------

print("utils_pagination loaded successfully.")