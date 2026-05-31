# Databricks notebook source
# MAGIC %md
# MAGIC # Utils Layer — Legislative Utilities
# MAGIC
# MAGIC **Notebook:** `utils_legislature`  
# MAGIC **Layer:** `Utils`  
# MAGIC **Source/Endpoint:** `Legislative Domain Reference Data`  
# MAGIC **Target:** `Reusable legislative normalization and validation functions`
# MAGIC
# MAGIC Provides reusable helper functions for legislative periods,
# MAGIC party acronyms, Brazilian states and Câmara dos Deputados domain standardization.
# MAGIC
# MAGIC This notebook centralizes legislative normalization and validation logic
# MAGIC used across Bronze, Silver, Gold and Marts transformations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Normalize political party acronyms
# MAGIC - Normalize Brazilian state acronyms
# MAGIC - Validate legislature identifiers
# MAGIC - Map years to legislative periods
# MAGIC - Validate supported legislative reference values
# MAGIC - Support reusable domain standardization workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Shared utility notebook across Medallion layers
# MAGIC - Does not persist data
# MAGIC - Supports domain consistency and standardization
# MAGIC - Centralizes legislative reference rules used throughout the project
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/governance/domain_standardization.md`
# MAGIC - `/docs/standards/coding_standards.md`
# MAGIC - `/docs/architecture/medallion_architecture.md`

# COMMAND ----------

# MAGIC %run ./utils_config

# COMMAND ----------

from typing import Optional, Dict, List

# COMMAND ----------

# ============================================================
# LEGISLATURE PERIODS
# ============================================================

LEGISLATURE_PERIODS = {
    56: {
        "start_year": 2019,
        "end_year": 2023,
        "description": "56th Legislature - 2019 to 2023",
    },
    57: {
        "start_year": 2023,
        "end_year": 2027,
        "description": "57th Legislature - 2023 to 2027",
    },
}

# COMMAND ----------

# ============================================================
# BRAZILIAN STATE ACRONYMS
# ============================================================

VALID_STATE_ACRONYMS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF",
    "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS",
    "RO", "RR", "SC", "SP", "SE", "TO",
]

# COMMAND ----------

# ============================================================
# COMMON PARTY ACRONYMS
# ============================================================

VALID_PARTY_ACRONYMS = [
    "AGIR",
    "AVANTE",
    "CIDADANIA",
    "DC",
    "MDB",
    "NOVO",
    "PCDOB",
    "PDT",
    "PL",
    "PMB",
    "PODE",
    "PP",
    "PRD",
    "PRTB",
    "PSB",
    "PSC",
    "PSD",
    "PSDB",
    "PSOL",
    "PSTU",
    "PT",
    "PV",
    "REDE",
    "REPUBLICANOS",
    "SOLIDARIEDADE",
    "UNIÃO",
    "UP",
]

PARTY_NORMALIZATION_MAP = {
    "UNIAO": "UNIÃO",
    "UNIÃO BRASIL": "UNIÃO",
    "UNIAO BRASIL": "UNIÃO",
    "PC DO B": "PCDOB",
    "PCdoB": "PCDOB",
    "REPUBLICANO": "REPUBLICANOS",
}

# COMMAND ----------

def normalize_text(value: Optional[str]) -> Optional[str]:
    """
    Normalizes a text value by trimming spaces and converting it to uppercase.

    Parameters
    ----------
    value : str, optional
        Input text value.

    Returns
    -------
    str or None
        Normalized text value.
    """

    if value is None:
        return None

    normalized_value = str(value).strip()

    if normalized_value == "":
        return None

    return normalized_value.upper()

# COMMAND ----------

def normalize_state(value: Optional[str]) -> Optional[str]:
    """
    Normalizes and validates a Brazilian state acronym.

    Parameters
    ----------
    value : str, optional
        State acronym.

    Returns
    -------
    str or None
        Normalized state acronym when valid, otherwise None.
    """

    normalized_value = normalize_text(value)

    if normalized_value in VALID_STATE_ACRONYMS:
        return normalized_value

    return None

# COMMAND ----------

def normalize_party(value: Optional[str]) -> Optional[str]:
    """
    Normalizes a political party acronym.

    Parameters
    ----------
    value : str, optional
        Party acronym.

    Returns
    -------
    str or None
        Normalized party acronym.
    """

    normalized_value = normalize_text(value)

    if normalized_value is None:
        return None

    if normalized_value in PARTY_NORMALIZATION_MAP:
        return PARTY_NORMALIZATION_MAP[normalized_value]

    return normalized_value

# COMMAND ----------

def is_valid_state(value: Optional[str]) -> bool:
    """
    Checks whether a value is a valid Brazilian state acronym.
    """

    return normalize_state(value) is not None

# COMMAND ----------

def is_valid_party(value: Optional[str]) -> bool:
    """
    Checks whether a value is a known party acronym.
    """

    normalized_value = normalize_party(value)

    if normalized_value is None:
        return False

    return normalized_value in VALID_PARTY_ACRONYMS

# COMMAND ----------

def is_valid_legislature(legislature_id: Optional[int]) -> bool:
    """
    Checks whether a legislature identifier is supported by the project scope.
    """

    if legislature_id is None:
        return False

    try:
        return int(legislature_id) in LEGISLATURE_PERIODS

    except Exception:
        return False

# COMMAND ----------

def get_legislature_period(legislature_id: int) -> Optional[Dict[str, int]]:
    """
    Returns the configured period for a legislature identifier.

    Parameters
    ----------
    legislature_id : int
        Legislature identifier.

    Returns
    -------
    dict or None
        Legislature period metadata.
    """

    try:
        return LEGISLATURE_PERIODS.get(int(legislature_id))

    except Exception:
        return None

# COMMAND ----------

def get_legislature_from_year(year: int) -> Optional[int]:
    """
    Returns the legislature identifier associated with a year.

    Parameters
    ----------
    year : int
        Calendar year.

    Returns
    -------
    int or None
        Legislature identifier when found.
    """

    if year is None:
        return None

    try:
        year = int(year)

    except Exception:
        return None

    for legislature_id, period in LEGISLATURE_PERIODS.items():
        if period["start_year"] <= year <= period["end_year"]:
            return legislature_id

    return None

# COMMAND ----------

def get_year_from_date(value) -> Optional[int]:
    """
    Extracts the year from a date-like value.

    Parameters
    ----------
    value : object
        Date, timestamp or string value.

    Returns
    -------
    int or None
        Extracted year when available.
    """

    if value is None:
        return None

    try:
        return int(str(value)[:4])

    except Exception:
        return None

# COMMAND ----------

def get_supported_legislatures() -> List[int]:
    """
    Returns the list of legislature identifiers supported by the project.
    """

    return list(LEGISLATURE_PERIODS.keys())

# COMMAND ----------

def get_valid_states() -> List[str]:
    """
    Returns the list of valid Brazilian state acronyms.
    """

    return VALID_STATE_ACRONYMS

# COMMAND ----------

def get_valid_parties() -> List[str]:
    """
    Returns the list of known political party acronyms.
    """

    return VALID_PARTY_ACRONYMS

# COMMAND ----------

print("utils_legislature loaded successfully.")