# Databricks notebook source
# MAGIC %md
# MAGIC # Utils — CNPJ and Supplier Document Utilities
# MAGIC
# MAGIC **Notebook:** `utils_cnpj`
# MAGIC
# MAGIC Provides reusable utilities for supplier document normalization, CNPJ
# MAGIC structural validation and optional public API enrichment.
# MAGIC
# MAGIC This notebook centralizes helper functions used to standardize supplier
# MAGIC documents, validate Brazilian legal entity identifiers and support supplier
# MAGIC quality checks across CEAP expense and supplier pipelines.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Supplier document cleaning rules
# MAGIC - CNPJ structural validation logic
# MAGIC - Repeated-digit document detection
# MAGIC - BrasilAPI CNPJ request handling
# MAGIC - API retry and timeout rules
# MAGIC - Structured API response standardization
# MAGIC - Reusable supplier document utilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Normalize supplier documents into numeric-only values
# MAGIC - Remove punctuation and non-numeric characters from CNPJ/CPF fields
# MAGIC - Identify missing, malformed or repeated supplier documents
# MAGIC - Validate CNPJ check digits without external API dependency
# MAGIC - Query public CNPJ data through BrasilAPI when explicitly used by enrichment notebooks
# MAGIC - Return structured API status and error information
# MAGIC - Support supplier data quality rules in CEAP and supplier pipelines
# MAGIC - Avoid persisting data directly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - This notebook is a utility notebook and does not persist Delta tables
# MAGIC - External API enrichment must be used for analytical enrichment only
# MAGIC - Supplier records must not be rejected because of API instability
# MAGIC - API failures return structured status instead of stopping the full pipeline
# MAGIC - CNPJ API validation should be executed in a separated enrichment notebook
# MAGIC - CNPJ/CPF structural validation can be used safely in Silver standardization
# MAGIC - Global utility notebooks are used to reduce duplicated logic
# MAGIC - Comments and documentation are written in English
# MAGIC - Naming conventions follow Portuguese mnemonic standards
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/decisions/silver_layer_strategy.md`
# MAGIC - `/docs/governance/data_quality.md`
# MAGIC - `/docs/governance/traceability.md`
# MAGIC - `/docs/operations/execution_guide.md`
# MAGIC - `/docs/standards/naming_conventions.md`

# COMMAND ----------

import re
import time
import requests

# COMMAND ----------

BRASILAPI_CNPJ_BASE_URL = "https://brasilapi.com.br/api/cnpj/v1"

REQUEST_TIMEOUT_SECONDS = 15
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 2

# COMMAND ----------

def clean_document_value(document):
    """
    Removes non-numeric characters from a supplier document.
    """

    if document is None:
        return None

    digits = re.sub(
        r"[^0-9]",
        "",
        str(document)
    )

    if len(digits) == 0:
        return None

    return digits

# COMMAND ----------

def is_repeated_document_value(document):
    """
    Identifies documents composed only by repeated digits.
    """

    document = clean_document_value(document)

    if document is None:
        return False

    return document == document[0] * len(document)

# COMMAND ----------

def is_valid_cnpj_digits(cnpj):
    """
    Validates CNPJ check digits.

    This function validates only CNPJ structure and check digits.
    It does not query Receita Federal or any external API.
    """

    cnpj = clean_document_value(cnpj)

    if cnpj is None:
        return False

    if len(cnpj) != 14:
        return False

    if is_repeated_document_value(cnpj):
        return False

    weights_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    weights_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    numbers = [int(digit) for digit in cnpj]

    sum_1 = sum(
        numbers[index] * weights_1[index]
        for index in range(12)
    )

    digit_1 = 11 - (sum_1 % 11)
    digit_1 = 0 if digit_1 >= 10 else digit_1

    sum_2 = sum(
        numbers[index] * weights_2[index]
        for index in range(13)
    )

    digit_2 = 11 - (sum_2 % 11)
    digit_2 = 0 if digit_2 >= 10 else digit_2

    return (
        numbers[12] == digit_1
        and numbers[13] == digit_2
    )

# COMMAND ----------

def fetch_cnpj_data(cnpj):
    """
    Fetches public CNPJ data from BrasilAPI.

    This function is resilient:
    - returns structured status
    - does not raise exception for API failures
    - supports retries
    """

    cnpj = clean_document_value(cnpj)

    base_response = {
        "forn_tx_documento_limpo": cnpj,
        "api_tx_status_consulta_cnpj": None,
        "api_tx_situacao_cadastral": None,
        "api_tx_razao_social": None,
        "api_tx_nome_fantasia": None,
        "api_tx_cnae_principal": None,
        "api_tx_uf": None,
        "api_tx_municipio": None,
        "api_tx_porte": None,
        "api_vl_capital_social": None,
        "api_cd_http_status": None,
        "api_tx_erro": None,
    }

    if cnpj is None:
        base_response["api_tx_status_consulta_cnpj"] = "INVALID_FORMAT"
        base_response["api_tx_erro"] = "Document is null or empty"
        return base_response

    if len(cnpj) != 14:
        base_response["api_tx_status_consulta_cnpj"] = "INVALID_FORMAT"
        base_response["api_tx_erro"] = "CNPJ must have 14 digits"
        return base_response

    if is_repeated_document_value(cnpj):
        base_response["api_tx_status_consulta_cnpj"] = "INVALID_FORMAT"
        base_response["api_tx_erro"] = "Repeated digits CNPJ"
        return base_response

    url = f"{BRASILAPI_CNPJ_BASE_URL}/{cnpj}"

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = requests.get(
                url,
                timeout=REQUEST_TIMEOUT_SECONDS
            )

            if response.status_code == 200:

                payload = response.json()

                return {
                    "forn_tx_documento_limpo": cnpj,
                    "api_tx_status_consulta_cnpj": "FOUND",
                    "api_tx_situacao_cadastral": payload.get("descricao_situacao_cadastral"),
                    "api_tx_razao_social": payload.get("razao_social"),
                    "api_tx_nome_fantasia": payload.get("nome_fantasia"),
                    "api_tx_cnae_principal": payload.get("cnae_fiscal_descricao"),
                    "api_tx_uf": payload.get("uf"),
                    "api_tx_municipio": payload.get("municipio"),
                    "api_tx_porte": payload.get("porte"),
                    "api_vl_capital_social": payload.get("capital_social"),
                    "api_cd_http_status": response.status_code,
                    "api_tx_erro": None,
                }

            if response.status_code == 404:

                base_response["api_tx_status_consulta_cnpj"] = "NOT_FOUND"
                base_response["api_cd_http_status"] = response.status_code
                base_response["api_tx_erro"] = "CNPJ not found"
                return base_response

            if attempt == MAX_RETRIES:

                base_response["api_tx_status_consulta_cnpj"] = "ERROR"
                base_response["api_cd_http_status"] = response.status_code
                base_response["api_tx_erro"] = response.text[:500]
                return base_response

        except Exception as error:

            if attempt == MAX_RETRIES:

                base_response["api_tx_status_consulta_cnpj"] = "ERROR"
                base_response["api_tx_erro"] = str(error)[:500]
                return base_response

        time.sleep(RETRY_SLEEP_SECONDS * attempt)

# COMMAND ----------

print("utils_cnpj loaded successfully.")