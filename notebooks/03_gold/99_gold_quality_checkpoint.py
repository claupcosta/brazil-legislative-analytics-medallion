# Databricks notebook source
# MAGIC %md
# MAGIC # 99 Gold — Quality Checkpoint
# MAGIC
# MAGIC **Notebook:** `99_gold_quality_checkpoint`
# MAGIC
# MAGIC Validates the completeness, quality, governance, referential integrity, and operational readiness of all Gold layer datasets before project delivery and analytical mart consumption.
# MAGIC
# MAGIC This notebook validates:
# MAGIC
# MAGIC - Gold table availability
# MAGIC - Gold record volume verification
# MAGIC - Dimension and fact completeness
# MAGIC - Data quality controls
# MAGIC - Duplicate detection
# MAGIC - Mandatory field validation
# MAGIC - Referential consistency indicators
# MAGIC - Governance comment coverage
# MAGIC - Gold execution audit coverage
# MAGIC - Hash traceability coverage
# MAGIC - Delivery checkpoint evidence generation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Responsibilities
# MAGIC
# MAGIC - Verify all mandatory Gold datasets were generated
# MAGIC - Confirm Gold record volumes are within expected ranges
# MAGIC - Validate critical business keys and surrogate keys
# MAGIC - Detect duplicate dimension and fact records
# MAGIC - Verify mandatory analytical attributes
# MAGIC - Assess referential coverage between facts and dimensions
# MAGIC - Confirm governance comments were applied
# MAGIC - Confirm Gold audit metadata was generated
# MAGIC - Confirm record hash traceability was generated
# MAGIC - Produce delivery evidence for the Gold layer
# MAGIC - Certify Gold readiness for analytical consumption
# MAGIC - Support final project acceptance validation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Notes
# MAGIC
# MAGIC - This notebook does not modify data
# MAGIC - All validations are read-only
# MAGIC - Results are intended for quality assurance purposes
# MAGIC - Validation failures must be investigated before delivery closure
# MAGIC - Record counts may vary according to source refresh dates
# MAGIC - Governance validations use Information Schema metadata
# MAGIC - Some referential gaps may be acceptable when caused by source historical limitations
# MAGIC - Gold datasets are considered approved when critical validations pass
# MAGIC - This notebook serves as the official Gold checkpoint
# MAGIC - Results should be archived as delivery evidence
# MAGIC
# MAGIC For additional architectural and governance details, refer to:
# MAGIC
# MAGIC - `/docs/architecture/README.md`
# MAGIC - `/docs/governance/data_quality.md`
# MAGIC - `/docs/governance/traceability.md`
# MAGIC - `/docs/operations/execution_guide.md`
# MAGIC - `/docs/standards/naming_conventions.md`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gold Table Availability

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     expected.table_name,
# MAGIC     CASE
# MAGIC         WHEN existing.table_name IS NOT NULL THEN 'AVAILABLE'
# MAGIC         ELSE 'MISSING'
# MAGIC     END AS availability_status
# MAGIC FROM (
# MAGIC     SELECT 'dm_deputados' AS table_name
# MAGIC     UNION ALL SELECT 'dm_partidos'
# MAGIC     UNION ALL SELECT 'dm_estados'
# MAGIC     UNION ALL SELECT 'dm_datas'
# MAGIC     UNION ALL SELECT 'dm_frentes'
# MAGIC     UNION ALL SELECT 'dm_eventos'
# MAGIC     UNION ALL SELECT 'dm_votacoes'
# MAGIC     UNION ALL SELECT 'dm_cpis'
# MAGIC     UNION ALL SELECT 'dm_fornecedores'
# MAGIC     UNION ALL SELECT 'ft_frentes_membros'
# MAGIC     UNION ALL SELECT 'ft_presencas_eventos'
# MAGIC     UNION ALL SELECT 'ft_resultados_votacoes'
# MAGIC     UNION ALL SELECT 'ft_despesas_ceap'
# MAGIC     UNION ALL SELECT 'ft_eventos_cpis'
# MAGIC ) expected
# MAGIC LEFT JOIN brazil_legislative_analytics.information_schema.tables existing
# MAGIC     ON expected.table_name = existing.table_name
# MAGIC    AND existing.table_schema = 'gold'
# MAGIC ORDER BY expected.table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold Record Volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dm_deputados' AS tabela, COUNT(*) AS total
# MAGIC FROM brazil_legislative_analytics.gold.dm_deputados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_partidos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_partidos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_estados', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_estados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_datas', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_datas
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_frentes', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_frentes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_eventos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_votacoes', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_cpis', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_cpis
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_fornecedores', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_fornecedores
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_frentes_membros', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_frentes_membros
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_presencas_eventos', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_presencas_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_resultados_votacoes', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_resultados_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_despesas_ceap', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_despesas_ceap
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_eventos_cpis', COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_eventos_cpis;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dimension Surrogate Key Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dm_deputados' AS tabela, COUNT(*) AS total, SUM(CASE WHEN dep_sk_deputado IS NULL THEN 1 ELSE 0 END) AS sem_sk
# MAGIC FROM brazil_legislative_analytics.gold.dm_deputados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_partidos', COUNT(*), SUM(CASE WHEN par_sk_partido IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.dm_partidos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_estados', COUNT(*), SUM(CASE WHEN est_sk_estado IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.dm_estados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_datas', COUNT(*), SUM(CASE WHEN dat_sk_data IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.dm_datas
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_frentes', COUNT(*), SUM(CASE WHEN frn_sk_frente IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.dm_frentes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_eventos', COUNT(*), SUM(CASE WHEN evt_sk_evento IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.dm_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_votacoes', COUNT(*), SUM(CASE WHEN vot_sk_votacao IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.dm_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_cpis', COUNT(*), SUM(CASE WHEN cpi_sk_cpi IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.dm_cpis
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_fornecedores', COUNT(*), SUM(CASE WHEN forn_sk_fornecedor IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.dm_fornecedores;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fact Surrogate Key Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'ft_frentes_membros' AS tabela, COUNT(*) AS total, SUM(CASE WHEN ffm_sk_frente_membro IS NULL THEN 1 ELSE 0 END) AS sem_sk
# MAGIC FROM brazil_legislative_analytics.gold.ft_frentes_membros
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_presencas_eventos', COUNT(*), SUM(CASE WHEN fpe_sk_presenca_evento IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.ft_presencas_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_resultados_votacoes', COUNT(*), SUM(CASE WHEN frv_sk_resultado_votacao IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.ft_resultados_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_despesas_ceap', COUNT(*), SUM(CASE WHEN fdc_sk_despesa_ceap IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.ft_despesas_ceap
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_eventos_cpis', COUNT(*), SUM(CASE WHEN fec_sk_evento_cpi IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.ft_eventos_cpis;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dimension Duplicate Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dm_partidos' AS tabela, par_id_partido AS chave, COUNT(*) AS total
# MAGIC FROM brazil_legislative_analytics.gold.dm_partidos
# MAGIC GROUP BY par_id_partido
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_estados', est_id_estado, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_estados
# MAGIC GROUP BY est_id_estado
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_frentes', frn_id_frente, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_frentes
# MAGIC GROUP BY frn_id_frente
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_eventos', evt_id_evento, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_eventos
# MAGIC GROUP BY evt_id_evento
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_votacoes', vot_id_votacao, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_votacoes
# MAGIC GROUP BY vot_id_votacao
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_cpis', cpi_id_orgao, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_cpis
# MAGIC GROUP BY cpi_id_orgao
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_fornecedores', forn_tx_chave_deduplicacao, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.dm_fornecedores
# MAGIC GROUP BY forn_tx_chave_deduplicacao
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fact Duplicate Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'ft_frentes_membros' AS tabela, CONCAT(frn_id_frente, ' | ', dep_id_deputado, ' | ', leg_id_legislatura) AS chave, COUNT(*) AS total
# MAGIC FROM brazil_legislative_analytics.gold.ft_frentes_membros
# MAGIC GROUP BY frn_id_frente, dep_id_deputado, leg_id_legislatura
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_presencas_eventos', CONCAT(evt_id_evento, ' | ', dep_id_deputado, ' | ', pev_nr_ano_evento), COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_presencas_eventos
# MAGIC GROUP BY evt_id_evento, dep_id_deputado, pev_nr_ano_evento
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_resultados_votacoes', CONCAT(vot_id_votacao, ' | ', dep_id_deputado), COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_resultados_votacoes
# MAGIC GROUP BY vot_id_votacao, dep_id_deputado
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_despesas_ceap', fdc_tx_business_key, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_despesas_ceap
# MAGIC GROUP BY fdc_tx_business_key
# MAGIC HAVING COUNT(*) > 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_eventos_cpis', cpi_evt_id_relacao, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_eventos_cpis
# MAGIC GROUP BY cpi_evt_id_relacao
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Fact Referential Coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'ft_frentes_membros' AS tabela,
# MAGIC     'frontes_deputados' AS validacao,
# MAGIC     COUNT(*) AS total,
# MAGIC     SUM(CASE WHEN ffm_fl_frente_encontrada_gold = true THEN 1 ELSE 0 END) AS chave_principal_1_encontrada,
# MAGIC     SUM(CASE WHEN ffm_fl_deputado_encontrado_gold = true THEN 1 ELSE 0 END) AS chave_principal_2_encontrada,
# MAGIC     SUM(CASE WHEN ffm_fl_dimensoes_principais_completas = true THEN 1 ELSE 0 END) AS dimensoes_principais_completas
# MAGIC FROM brazil_legislative_analytics.gold.ft_frentes_membros
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ft_presencas_eventos',
# MAGIC     'eventos_deputados_datas',
# MAGIC     COUNT(*),
# MAGIC     SUM(CASE WHEN fpe_fl_evento_encontrado_gold = true THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN fpe_fl_deputado_encontrado_gold = true THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN fpe_fl_dimensoes_principais_completas = true THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.ft_presencas_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ft_resultados_votacoes',
# MAGIC     'votacoes_deputados_datas',
# MAGIC     COUNT(*),
# MAGIC     SUM(CASE WHEN frv_fl_votacao_encontrada_gold = true THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN frv_fl_deputado_encontrado_gold = true THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN frv_fl_dimensoes_principais_completas = true THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.ft_resultados_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ft_despesas_ceap',
# MAGIC     'deputados_fornecedores_datas',
# MAGIC     COUNT(*),
# MAGIC     SUM(CASE WHEN fdc_fl_deputado_encontrado_gold = true THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN fdc_fl_fornecedor_encontrado_gold = true THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN fdc_fl_dimensoes_principais_completas = true THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.ft_despesas_ceap
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ft_eventos_cpis',
# MAGIC     'eventos_cpis_datas',
# MAGIC     COUNT(*),
# MAGIC     SUM(CASE WHEN fec_fl_evento_encontrado_gold = true THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN fec_fl_cpi_encontrada_gold = true THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN fec_fl_dimensoes_principais_completas = true THEN 1 ELSE 0 END)
# MAGIC FROM brazil_legislative_analytics.gold.ft_eventos_cpis;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Fact Gold Valid Records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'ft_frentes_membros' AS tabela, ffm_fl_registro_valido_gold AS valido_gold, COUNT(*) AS total
# MAGIC FROM brazil_legislative_analytics.gold.ft_frentes_membros
# MAGIC GROUP BY ffm_fl_registro_valido_gold
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_presencas_eventos', fpe_fl_registro_valido_gold, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_presencas_eventos
# MAGIC GROUP BY fpe_fl_registro_valido_gold
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_resultados_votacoes', frv_fl_registro_valido_gold, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_resultados_votacoes
# MAGIC GROUP BY frv_fl_registro_valido_gold
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_despesas_ceap', fdc_fl_registro_valido_gold, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_despesas_ceap
# MAGIC GROUP BY fdc_fl_registro_valido_gold
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_eventos_cpis', fec_fl_registro_valido_gold, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_eventos_cpis
# MAGIC GROUP BY fec_fl_registro_valido_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Fact Legislature Coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'ft_frentes_membros' AS tabela, leg_id_legislatura, COUNT(*) AS total
# MAGIC FROM brazil_legislative_analytics.gold.ft_frentes_membros
# MAGIC GROUP BY leg_id_legislatura
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_presencas_eventos', leg_id_legislatura, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_presencas_eventos
# MAGIC GROUP BY leg_id_legislatura
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_resultados_votacoes', leg_id_legislatura, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_resultados_votacoes
# MAGIC GROUP BY leg_id_legislatura
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_despesas_ceap', leg_id_legislatura, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_despesas_ceap
# MAGIC GROUP BY leg_id_legislatura
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_eventos_cpis', leg_id_legislatura, COUNT(*)
# MAGIC FROM brazil_legislative_analytics.gold.ft_eventos_cpis
# MAGIC GROUP BY leg_id_legislatura
# MAGIC ORDER BY tabela, leg_id_legislatura;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Gold Hash Traceability

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dm_deputados' AS tabela, COUNT(*) AS total, COUNT(DISTINCT aud_tx_hash_registro_gold) AS hashes_distintos
# MAGIC FROM brazil_legislative_analytics.gold.dm_deputados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_partidos', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_partidos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_estados', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_estados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_datas', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_datas
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_frentes', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_frentes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_eventos', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_votacoes', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_cpis', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_cpis
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_fornecedores', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_fornecedores
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_frentes_membros', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.ft_frentes_membros
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_presencas_eventos', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.ft_presencas_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_resultados_votacoes', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.ft_resultados_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_despesas_ceap', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.ft_despesas_ceap
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_eventos_cpis', COUNT(*), COUNT(DISTINCT aud_tx_hash_registro_gold)
# MAGIC FROM brazil_legislative_analytics.gold.ft_eventos_cpis;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Gold Execution Audit Coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dm_deputados' AS tabela, COUNT(DISTINCT aud_id_execucao_gold) AS execucoes, MIN(aud_dh_processamento_gold) AS primeiro_processamento, MAX(aud_dh_processamento_gold) AS ultimo_processamento
# MAGIC FROM brazil_legislative_analytics.gold.dm_deputados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_partidos', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_partidos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_estados', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_estados
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_datas', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_datas
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_frentes', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_frentes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_eventos', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_votacoes', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_cpis', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_cpis
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_fornecedores', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.dm_fornecedores
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_frentes_membros', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.ft_frentes_membros
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_presencas_eventos', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.ft_presencas_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_resultados_votacoes', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.ft_resultados_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_despesas_ceap', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.ft_despesas_ceap
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ft_eventos_cpis', COUNT(DISTINCT aud_id_execucao_gold), MIN(aud_dh_processamento_gold), MAX(aud_dh_processamento_gold)
# MAGIC FROM brazil_legislative_analytics.gold.ft_eventos_cpis;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Governance Comment Coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     COUNT(*) AS colunas,
# MAGIC     SUM(CASE WHEN comment IS NOT NULL THEN 1 ELSE 0 END) AS colunas_com_comentario,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN comment IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) * 100,
# MAGIC         2
# MAGIC     ) AS percentual_comentarios
# MAGIC FROM brazil_legislative_analytics.information_schema.columns
# MAGIC WHERE table_schema = 'gold'
# MAGIC   AND table_name IN (
# MAGIC       'dm_deputados',
# MAGIC       'dm_partidos',
# MAGIC       'dm_estados',
# MAGIC       'dm_datas',
# MAGIC       'dm_frentes',
# MAGIC       'dm_eventos',
# MAGIC       'dm_votacoes',
# MAGIC       'dm_cpis',
# MAGIC       'dm_fornecedores',
# MAGIC       'ft_frentes_membros',
# MAGIC       'ft_presencas_eventos',
# MAGIC       'ft_resultados_votacoes',
# MAGIC       'ft_despesas_ceap',
# MAGIC       'ft_eventos_cpis'
# MAGIC   )
# MAGIC GROUP BY table_name
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Business Metrics Sanity Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'ft_frentes_membros' AS tabela,
# MAGIC     COUNT(*) AS total_registros,
# MAGIC     COUNT(DISTINCT frn_id_frente) AS entidade_1_distinta,
# MAGIC     COUNT(DISTINCT dep_id_deputado) AS entidade_2_distinta,
# MAGIC     COUNT(DISTINCT leg_id_legislatura) AS legislaturas_distintas
# MAGIC FROM brazil_legislative_analytics.gold.ft_frentes_membros
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ft_presencas_eventos',
# MAGIC     COUNT(*),
# MAGIC     COUNT(DISTINCT evt_id_evento),
# MAGIC     COUNT(DISTINCT dep_id_deputado),
# MAGIC     COUNT(DISTINCT leg_id_legislatura)
# MAGIC FROM brazil_legislative_analytics.gold.ft_presencas_eventos
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ft_resultados_votacoes',
# MAGIC     COUNT(*),
# MAGIC     COUNT(DISTINCT vot_id_votacao),
# MAGIC     COUNT(DISTINCT dep_id_deputado),
# MAGIC     COUNT(DISTINCT leg_id_legislatura)
# MAGIC FROM brazil_legislative_analytics.gold.ft_resultados_votacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ft_despesas_ceap',
# MAGIC     COUNT(*),
# MAGIC     COUNT(DISTINCT dep_id_deputado),
# MAGIC     COUNT(DISTINCT forn_tx_chave_deduplicacao),
# MAGIC     COUNT(DISTINCT leg_id_legislatura)
# MAGIC FROM brazil_legislative_analytics.gold.ft_despesas_ceap
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ft_eventos_cpis',
# MAGIC     COUNT(*),
# MAGIC     COUNT(DISTINCT cpi_id_orgao),
# MAGIC     COUNT(DISTINCT evt_id_evento),
# MAGIC     COUNT(DISTINCT leg_id_legislatura)
# MAGIC FROM brazil_legislative_analytics.gold.ft_eventos_cpis;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Final Gold Readiness Assessment

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH availability AS (
# MAGIC     SELECT
# MAGIC         expected.table_name,
# MAGIC         CASE WHEN existing.table_name IS NOT NULL THEN 1 ELSE 0 END AS is_available
# MAGIC     FROM (
# MAGIC         SELECT 'dm_deputados' AS table_name
# MAGIC         UNION ALL SELECT 'dm_partidos'
# MAGIC         UNION ALL SELECT 'dm_estados'
# MAGIC         UNION ALL SELECT 'dm_datas'
# MAGIC         UNION ALL SELECT 'dm_frentes'
# MAGIC         UNION ALL SELECT 'dm_eventos'
# MAGIC         UNION ALL SELECT 'dm_votacoes'
# MAGIC         UNION ALL SELECT 'dm_cpis'
# MAGIC         UNION ALL SELECT 'dm_fornecedores'
# MAGIC         UNION ALL SELECT 'ft_frentes_membros'
# MAGIC         UNION ALL SELECT 'ft_presencas_eventos'
# MAGIC         UNION ALL SELECT 'ft_resultados_votacoes'
# MAGIC         UNION ALL SELECT 'ft_despesas_ceap'
# MAGIC         UNION ALL SELECT 'ft_eventos_cpis'
# MAGIC     ) expected
# MAGIC     LEFT JOIN brazil_legislative_analytics.information_schema.tables existing
# MAGIC         ON expected.table_name = existing.table_name
# MAGIC        AND existing.table_schema = 'gold'
# MAGIC ),
# MAGIC summary AS (
# MAGIC     SELECT
# MAGIC         COUNT(*) AS expected_tables,
# MAGIC         SUM(is_available) AS available_tables
# MAGIC     FROM availability
# MAGIC )
# MAGIC SELECT
# MAGIC     expected_tables,
# MAGIC     available_tables,
# MAGIC     CASE
# MAGIC         WHEN expected_tables = available_tables THEN 'GOLD_LAYER_AVAILABLE'
# MAGIC         ELSE 'GOLD_LAYER_INCOMPLETE'
# MAGIC     END AS gold_availability_status
# MAGIC FROM summary;