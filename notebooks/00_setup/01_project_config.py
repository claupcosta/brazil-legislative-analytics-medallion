# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup — Project Configuration
# MAGIC
# MAGIC **Notebook:** `01_project_config`
# MAGIC
# MAGIC Centralizes global project configuration used across Bronze, Silver,
# MAGIC Gold, Marts, Quality and Utility notebooks.
# MAGIC
# MAGIC This notebook defines:
# MAGIC
# MAGIC - Project metadata
# MAGIC - Catalog and schema configuration
# MAGIC - Volume and raw file locations
# MAGIC - API endpoints and operational settings
# MAGIC - Reference years and legislatures
# MAGIC - Naming conventions and table registries
# MAGIC - Traceability standards
# MAGIC - Execution status constants
# MAGIC - Helper functions used across the pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Centralize global pipeline configuration
# MAGIC - Standardize table naming conventions
# MAGIC - Centralize API operational settings
# MAGIC - Register Bronze, Silver, Gold and Mart table mappings
# MAGIC - Define audit and traceability standards
# MAGIC - Provide reusable helper functions
# MAGIC - Centralize reference year governance
# MAGIC - Support controlled analytical scope management
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - API ingestion notebooks use controlled reference years to improve stability
# MAGIC - CSV fallback notebooks use broader historical ranges available in Unity Catalog Volumes
# MAGIC - CSV fallback is the recommended operational ingestion strategy for high-volume datasets
# MAGIC - Naming conventions follow Portuguese mnemonic standards
# MAGIC - Comments and documentation are written in English
# MAGIC - Helper functions are shared across all Medallion layers
# MAGIC - Governance and traceability standards are centrally managed in this notebook
# MAGIC - Legislature scope currently prioritizes Legislatures 56 and 57
# MAGIC - CSV fallback historical scope currently covers files from 2021 to 2026
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/medallion_architecture.md`
# MAGIC - `/docs/governance/data_lineage.md`
# MAGIC - `/docs/governance/naming_conventions.md`

# COMMAND ----------

from datetime import datetime
import uuid

# COMMAND ----------

# ============================================================
# PROJECT METADATA
# ============================================================

PROJECT_NAME = "brazil_legislative_analytics"
PROJECT_VERSION = "v 2.0.0"
PROJECT_ENVIRONMENT = "dev"

PIPELINE_VERSION = PROJECT_VERSION

RUN_ID = str(uuid.uuid4())
EXECUTION_TIMESTAMP = datetime.now()

# COMMAND ----------

# ============================================================
# CATALOG CONFIGURATION
# ============================================================

CATALOG_NAME = "brazil_legislative_analytics"

# COMMAND ----------

# ============================================================
# SCHEMA NAMES
# ============================================================

SCHEMA_AUDIT = "audit"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_MARTS = "marts"

AUDIT_SCHEMA = f"{CATALOG_NAME}.{SCHEMA_AUDIT}"
BRONZE_SCHEMA = f"{CATALOG_NAME}.{SCHEMA_BRONZE}"
SILVER_SCHEMA = f"{CATALOG_NAME}.{SCHEMA_SILVER}"
GOLD_SCHEMA = f"{CATALOG_NAME}.{SCHEMA_GOLD}"
MARTS_SCHEMA = f"{CATALOG_NAME}.{SCHEMA_MARTS}"

# COMMAND ----------

# ============================================================
# VOLUME / RAW FILES CONFIGURATION
# ============================================================

VOLUME_RAW_FILES = (
    f"/Volumes/{CATALOG_NAME}/"
    f"{SCHEMA_BRONZE}/"
    "raw_files"
)

VOLUME_RAW_ORGAOS = f"{VOLUME_RAW_FILES}/orgaos"
VOLUME_RAW_VOTACOES = f"{VOLUME_RAW_FILES}/votacoes"
VOLUME_RAW_VOTOS = f"{VOLUME_RAW_FILES}/votacoes_votos"
VOLUME_RAW_CEAP = f"{VOLUME_RAW_FILES}/ceap"
VOLUME_RAW_ORGAOS_MEMBROS = f"{VOLUME_RAW_FILES}/orgaos_membros"
VOLUME_RAW_PROPOSICOES = f"{VOLUME_RAW_FILES}/proposicoes"

# COMMAND ----------

# ============================================================
# AUDIT TABLES
# ============================================================

AUD_TB_LOG_EXECUCAO_PIPELINE = "aud_log_execucao_pipeline"
AUD_TB_LOG_ERROS_PIPELINE = "aud_log_erros_pipeline"
AUD_TB_LOG_QUALIDADE_DADOS = "aud_log_qualidade_dados"

AUDIT_PIPELINE_LOGS = f"{AUDIT_SCHEMA}.{AUD_TB_LOG_EXECUCAO_PIPELINE}"
AUDIT_PIPELINE_ERRORS = f"{AUDIT_SCHEMA}.{AUD_TB_LOG_ERROS_PIPELINE}"
AUDIT_DATA_QUALITY_LOGS = f"{AUDIT_SCHEMA}.{AUD_TB_LOG_QUALIDADE_DADOS}"

# COMMAND ----------

# ============================================================
# API CONFIGURATION
# ============================================================

CAMARA_API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

API_REQUEST_TIMEOUT_SECONDS = 120
API_DEFAULT_PAGE_SIZE = 100
API_MAX_RETRY_ATTEMPTS = 3
API_RETRY_SLEEP_SECONDS = 2

API_PAGE_PARAMETER_NAME = "pagina"
API_PAGE_SIZE_PARAMETER_NAME = "itens"
API_RESPONSE_DATA_FIELD = "dados"

API_ENDPOINTS = {
    "deputados": "/deputados",
    "frentes": "/frentes",
    "frentes_detalhes": "/frentes/{id}",
    "frentes_membros": "/frentes/{id}/membros",
    "eventos": "/eventos",
    "votacoes": "/votacoes",
    "votos": "/votacoes/{id}/votos",
    "despesas_ceap": "/deputados/{id}/despesas",
    "orgaos": "/orgaos",
    "orgaos_membros": "/orgaos/{id}/membros",
    "proposicoes": "/proposicoes",
}

# COMMAND ----------

# ============================================================
# REFERENCE YEAR CONFIGURATION
# ============================================================
#
# API ingestion notebooks use controlled reference years
# to reduce timeout risk and improve execution stability.
#
# CSV fallback notebooks use the complete historical
# range currently available in Unity Catalog Volumes.
#
# ============================================================

# ------------------------------------------------------------
# API INGESTION REFERENCE YEARS
# ------------------------------------------------------------

DEFAULT_REFERENCE_YEARS = [
    2019,
    2020,
    2021,
    2022,
    2023,
    2024,
    2025,
    2026,
]

EVENTOS_REFERENCE_YEARS = [
    2025,
    2026,
]

PROPOSICOES_REFERENCE_YEARS = [
    2025,
    2026,
]

CEAP_REFERENCE_YEARS = [
    2025,
    2026,
]

# ------------------------------------------------------------
# CSV FALLBACK REFERENCE YEARS
# ------------------------------------------------------------
#
# These ranges represent the complete historical files
# currently available in Unity Catalog Volumes.
#
# CSV fallback is the recommended operational ingestion
# strategy for high-volume datasets.
#
# ------------------------------------------------------------

CSV_REFERENCE_YEARS = [
    2021,
    2022,
    2023,
    2024,
    2025,
    2026,
]

VOTACOES_CSV_REFERENCE_YEARS = CSV_REFERENCE_YEARS

VOTOS_CSV_REFERENCE_YEARS = CSV_REFERENCE_YEARS

PROPOSICOES_CSV_REFERENCE_YEARS = CSV_REFERENCE_YEARS

CEAP_CSV_REFERENCE_YEARS = CSV_REFERENCE_YEARS

# ------------------------------------------------------------
# LEGISLATURE REFERENCE CONFIGURATION
# ------------------------------------------------------------

REFERENCE_LEGISLATURES = [
    56,
    57,
]

# ------------------------------------------------------------
# CSV FALLBACK EXECUTION MODE
# ------------------------------------------------------------

CSV_REFERENCE_YEAR_MODE = "controlled_range"

# COMMAND ----------

# ============================================================
# NAMING CONVENTIONS
# ============================================================

TABLE_PREFIXES = {
    "br": "bronze",
    "slv": "silver",
    "dm": "dimension",
    "ft": "fact",
    "am": "analytical_mart",
    "ref": "reference",
    "aud": "audit",
}

COLUMN_PREFIXES = {
    "id": "identifier",
    "cd": "code",
    "tx": "text",
    "dt": "date",
    "dh": "datetime",
    "vl": "value",
    "qt": "quantity",
    "pc": "percentage",
    "fl": "flag",
    "nr": "number",
}

MNEMONICS = {
    "dep": "deputy",
    "prt": "party",
    "uf": "state",
    "leg": "legislature",
    "frn": "front",
    "evt": "event",
    "vot": "voting",
    "votres": "voting_result",
    "desp": "expense",
    "forn": "supplier",
    "cpi": "parliamentary_inquiry_commission",
    "prop": "proposition",
    "org": "organization",
    "mbr": "member",
    "aud": "audit",
    "qlt": "quality",
    "err": "error",
}

# COMMAND ----------

# ============================================================
# BRONZE TABLES
# ============================================================

BRONZE_TABLES = {
    "deputados": "br_deputados",
    "frentes": "br_frentes",
    "frentes_membros": "br_frentes_membros",
    "eventos": "br_eventos",
    "presencas_eventos": "br_presencas_eventos",
    "votacoes": "br_votacoes",
    "votos": "br_votos",
    "despesas_ceap": "br_despesas_ceap",
    "orgaos": "br_orgaos",
    "orgaos_membros": "br_orgaos_membros",
    "proposicoes": "br_proposicoes",
}

# COMMAND ----------

# ============================================================
# SILVER TABLES
# ============================================================

SILVER_TABLES = {
    "deputados": "slv_deputados",
    "partidos": "slv_partidos",
    "estados": "slv_estados",
    "frentes": "slv_frentes",
    "frentes_membros": "slv_frentes_membros",
    "eventos": "slv_eventos",
    "votacoes": "slv_votacoes",
    "votos": "slv_votos",
    "despesas_ceap": "slv_despesas_ceap",
    "fornecedores": "slv_fornecedores",
    "fornecedores_cnpj_api": "slv_fornecedores_cnpj_api",
    "fornecedores_enriched": "slv_fornecedores_enriched",
    "orgaos": "slv_orgaos",
    "orgaos_membros": "slv_orgaos_membros",
    "cpis": "slv_cpis",
    "proposicoes": "slv_proposicoes",
    "registros_rejeitados": "slv_registros_rejeitados",
    "cnpj_enriquecido": "slv_cnpj_enriquecido",
}

# COMMAND ----------

# ============================================================
# GOLD DIMENSION TABLES
# ============================================================

GOLD_DIMENSION_TABLES = {
    "deputados": "dm_deputados",
    "partidos": "dm_partidos",
    "estados": "dm_estados",
    "datas": "dm_datas",
    "frentes": "dm_frentes",
    "eventos": "dm_eventos",
    "votacoes": "dm_votacoes",
    "cpis": "dm_cpis",
    "fornecedores": "dm_fornecedores",
}

# COMMAND ----------

# ============================================================
# GOLD FACT TABLES
# ============================================================

GOLD_FACT_TABLES = {
    "frentes_membros": "ft_frentes_membros",
    "presencas_eventos": "ft_presencas_eventos",
    "resultados_votacoes": "ft_resultados_votacoes",
    "despesas_ceap": "ft_despesas_ceap",
    "eventos_cpis": "ft_eventos_cpis",
}

# COMMAND ----------

# ============================================================
# MART TABLES
# ============================================================

MART_TABLES = {
    "atlas_frentes": "am_atlas_frentes",
    "calendario_eventos": "am_calendario_eventos",
    "correlacao_frentes_votacoes": "am_correlacao_frentes_votacoes",
    "panorama_despesas_ceap": "am_panorama_despesas_ceap",
    "auditoria_cpis": "am_auditoria_cpis",
    "monitor_presenca_absenteismo": "am_monitor_presenca_absenteismo",
}

# COMMAND ----------

# ============================================================
# REFERENCE TABLES
# ============================================================

REFERENCE_TABLES = {
    "opcoes_voto": "ref_opcoes_voto",
    "tipos_evento": "ref_tipos_evento",
    "tipos_votacao": "ref_tipos_votacao",
    "tipos_despesa": "ref_tipos_despesa",
    "tipos_documento": "ref_tipos_documento",
}

# COMMAND ----------

# ============================================================
# TRACEABILITY COLUMNS
# ============================================================

TRACEABILITY_COLUMNS = [
    "aud_id_execucao",
    "aud_dh_ingestao",
    "aud_dh_processamento",
    "aud_tx_endpoint_origem",
    "aud_tx_sistema_origem",
    "aud_tx_versao_pipeline",
    "aud_tx_hash_registro",
]

BRONZE_REQUIRED_COLUMNS = [
    "aud_id_execucao",
    "aud_dh_ingestao",
    "aud_tx_endpoint_origem",
    "aud_tx_sistema_origem",
    "aud_tx_versao_pipeline",
    "aud_tx_hash_registro",
]

# COMMAND ----------

# ============================================================
# QUALITY STATUS
# ============================================================

QUALITY_PASSED = "PASSED"
QUALITY_WARNING = "WARNING"
QUALITY_FAILED = "FAILED"

# COMMAND ----------

# ============================================================
# EXECUTION STATUS VALUES
# ============================================================

EXECUTION_STATUS_STARTED = "STARTED"
EXECUTION_STATUS_SUCCESS = "SUCCESS"
EXECUTION_STATUS_WARNING = "WARNING"
EXECUTION_STATUS_FAILED = "FAILED"

LOAD_TYPE_FULL = "FULL"
LOAD_TYPE_INCREMENTAL = "INCREMENTAL"
LOAD_TYPE_REPLAY = "REPLAY"
LOAD_TYPE_FALLBACK = "FALLBACK"

# COMMAND ----------

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_full_table_name(
    schema_name: str,
    table_name: str,
) -> str:
    """
    Builds a fully qualified table name.
    """

    return (
        f"{CATALOG_NAME}."
        f"{schema_name}."
        f"{table_name}"
    )

# COMMAND ----------

def get_bronze_table(
    table_name: str,
) -> str:
    """
    Builds a fully qualified Bronze table name.
    """

    return get_full_table_name(
        schema_name=SCHEMA_BRONZE,
        table_name=table_name,
    )

# COMMAND ----------

def get_silver_table(
    table_name: str,
) -> str:
    """
    Builds a fully qualified Silver table name.
    """

    return get_full_table_name(
        schema_name=SCHEMA_SILVER,
        table_name=table_name,
    )

# COMMAND ----------

def get_gold_table(
    table_name: str,
) -> str:
    """
    Builds a fully qualified Gold table name.
    """

    return get_full_table_name(
        schema_name=SCHEMA_GOLD,
        table_name=table_name,
    )

# COMMAND ----------

def get_mart_table(
    table_name: str,
) -> str:
    """
    Builds a fully qualified Mart table name.
    """

    return get_full_table_name(
        schema_name=SCHEMA_MARTS,
        table_name=table_name,
    )

# COMMAND ----------

def get_audit_table(
    table_name: str,
) -> str:
    """
    Builds a fully qualified Audit table name.
    """

    return get_full_table_name(
        schema_name=SCHEMA_AUDIT,
        table_name=table_name,
    )

# COMMAND ----------

# ============================================================
# OPTIONAL SPARK CATALOG ACTIVATION
# ============================================================

SET_ACTIVE_CATALOG = False

if SET_ACTIVE_CATALOG:
    spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# ============================================================
# VALIDATION OUTPUT
# ============================================================

print("PROJECT CONFIGURATION LOADED SUCCESSFULLY")
print(f"PROJECT_NAME: {PROJECT_NAME}")
print(f"PROJECT_VERSION: {PROJECT_VERSION}")
print(f"PROJECT_ENVIRONMENT: {PROJECT_ENVIRONMENT}")
print(f"CATALOG_NAME: {CATALOG_NAME}")
print(f"VOLUME_RAW_FILES: {VOLUME_RAW_FILES}")
print(f"RUN_ID: {RUN_ID}")
print(f"DEFAULT_REFERENCE_YEARS: {DEFAULT_REFERENCE_YEARS}")
print(f"CSV_REFERENCE_YEARS: {CSV_REFERENCE_YEARS}")
print(f"REFERENCE_LEGISLATURES: {REFERENCE_LEGISLATURES}")