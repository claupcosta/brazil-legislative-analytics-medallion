# Runbook Operacional

## 1. Objetivo

Este documento descreve os procedimentos operacionais para execução, monitoramento, recuperação de falhas e suporte da plataforma **Brazil Legislative Analytics Medallion**.

O objetivo é fornecer orientações para garantir a disponibilidade, qualidade e rastreabilidade dos pipelines de dados implementados no Databricks.

---

# 2. Visão Geral da Solução

A plataforma segue a arquitetura Medallion e é composta pelas seguintes camadas:

```text
Bronze (Raw)
    ↓
Silver (Curated)
    ↓
Gold (Dimensional)
    ↓
Mat (Analytical Marts)
    ↓
Quality & Governance
```

---

# 3. Estrutura dos Pipelines

## Setup

```text
00_setup/
```

Responsável por:

* criação de catálogos
* criação de schemas
* configuração do projeto
* criação das tabelas de auditoria
* validação da conectividade

---

## Bronze

```text
01_bronze/
```

Responsável pela ingestão dos dados brutos provenientes da API da Câmara dos Deputados ou dos arquivos CSV de contingência.

---

## Silver

```text
02_silver/
```

Responsável por:

* limpeza dos dados
* padronização
* deduplicação
* enriquecimento
* tratamento de registros rejeitados

---

## Gold

```text
03_gold/
```

Responsável pela construção do modelo dimensional corporativo.

Dimensões:

```text
dm_deputados
dm_partidos
dm_estados
dm_datas
dm_frentes
dm_eventos
dm_votacoes
dm_cpis
dm_fornecedores
```

Fatos:

```text
ft_frentes_membros
ft_presencas_eventos
ft_resultados_votacoes
ft_despesas_ceap
ft_eventos_cpis
```

---

## Mat

```text
04_marts/
```

Responsável pela construção dos Data Marts analíticos.

Marts disponíveis:

```text
am_atlas_frentes
am_calendario_eventos_legislativos
am_correlacao_frentes_votacoes
am_visao_geral_despesas_ceap
am_auditoria_cpis
am_monitor_presenca_absenteismo
```

---

## Quality

```text
05_quality/
```

Responsável pela validação da qualidade, rastreabilidade e governança.

---

## Jobs

```text
06_jobs/
```

Responsável pela execução orquestrada dos pipelines.

---

## Utilitários

```text
99_utils/
```

Bibliotecas compartilhadas por toda a solução.

---

# 4. Ordem de Execução

A sequência recomendada é:

```text
00_setup
    ↓
01_bronze
    ↓
02_silver
    ↓
03_gold
    ↓
04_marts
    ↓
05_quality
```

---

# 5. Procedimento de Execução Completa

## Etapa 1 — Setup

Executar:

```text
00_create_catalog_schemas.py
01_project_config.py
02_audit_tables.py
90_validate_project_setup.py
92_validate_api_connection.py
```

Validar:

* conectividade
* schemas criados
* tabelas de auditoria

---

## Etapa 2 — Bronze

Executar os notebooks da camada Bronze.

Validar:

* volume carregado
* disponibilidade da API
* tabelas Bronze atualizadas

Exemplo:

```text
br_deputados
br_frentes
br_eventos
br_votacoes
br_despesas_ceap
```

---

## Etapa 3 — Silver

Executar os notebooks da camada Silver.

Validar:

* registros rejeitados
* duplicidades
* consistência dos dados

Exemplo:

```text
slv_deputados
slv_eventos
slv_votacoes
slv_cpis
```

---

## Etapa 4 — Gold

Executar:

```text
01_dm_deputados.py
02_dm_partidos.py
03_dm_estados.py
04_dm_datas.py
05_dm_frentes.py
06_dm_eventos.py
07_dm_votacoes.py
08_dm_cpis.py
09_dm_fornecedores.py
```

Validar:

* chaves substitutas
* integridade referencial
* cardinalidade

---

## Etapa 5 — Fatos

Executar:

```text
10_ft_frentes_membros.py
11_ft_presencas_eventos.py
12_ft_resultados_votacoes.py
13_ft_despesas_ceap.py
14_ft_eventos_cpis.py
```

Validar:

* relacionamentos
* integridade dimensional
* métricas calculadas

---

## Etapa 6 — Data Marts

Executar:

```text
01_am_atlas_frentes.py
02_am_calendario_eventos_legislativos.py
03_am_correlacao_frentes_votacoes.py
04_am_visao_geral_despesas_ceap.py
05_am_auditoria_cpis.py
06_am_monitor_presenca_absenteismo.py
```

Validar:

* volume produzido
* indicadores calculados
* consistência analítica

---

# 6. Execução das Validações

## Qualidade Bronze

```text
01_quality_bronze_checks.py
```

Valida:

* nulos críticos
* duplicidades
* campos obrigatórios

---

## Qualidade Silver

```text
02_quality_silver_checks.py
```

Valida:

* padronização
* regras de negócio
* consistência dos dados

---

## Qualidade Gold

```text
03_quality_gold_checks.py
```

Valida:

* dimensões
* fatos
* integridade referencial

---

## Rastreabilidade

```text
04_traceability_checks.py
```

Valida:

* Bronze → Silver
* Silver → Gold
* Gold → Marts

---

## Qualidade dos Marts

```text
05_quality_marts_checks.py
```

Valida:

* agregações
* indicadores
* métricas analíticas

---

## Governança

```text
06_governance_metadata_checks.py
```

Valida:

* comentários de tabelas
* comentários de colunas
* metadados obrigatórios
* padrões de nomenclatura

---

# 7. Recuperação de Falhas

## Falha da API

Sintoma:

```text
Timeout
Connection Error
HTTP Error
```

Ação:

1. Validar conectividade.
2. Reexecutar a ingestão.
3. Utilizar notebooks CSV Fallback.

Exemplo:

```text
01a_bronze_deputados_csv_fallback.py
03a_bronze_eventos_csv_fallback.py
06a_bronze_despesas_ceap_csv_fallback.py
```

---

## Falha na Silver

Validar:

```text
slv_registros_rejeitados
```

Ações:

* revisar regras de negócio
* corrigir registros inválidos
* reprocessar entidade

---

## Falha na Gold

Validar:

* dimensões
* surrogate keys
* integridade referencial

Ações:

1. Reprocessar dimensões.
2. Reprocessar fatos dependentes.

---

## Falha nos Marts

Validar:

* disponibilidade da camada Gold
* consistência dos indicadores

Ação:

Reexecutar somente o mart afetado.

---

# 8. Monitoramento

## Indicadores Operacionais

Monitorar:

* tempo de execução
* registros processados
* falhas por pipeline
* registros rejeitados
* volume por camada

---

## Logs

Todos os pipelines registram:

* início da execução
* fim da execução
* duração
* quantidade processada
* erros encontrados

---

# 9. Auditoria

As tabelas de auditoria armazenam:

* pipeline executado
* data e hora
* status
* duração
* quantidade de registros
* erros encontrados
* origem utilizada

---

# 10. Estratégia de Reprocessamento

O reprocessamento deve seguir a dependência das camadas.

```text
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Marts
```

Nunca reprocessar uma camada superior sem validar a camada imediatamente anterior.

---

# 11. Referências

* 01_solution_architecture.md
* 02_data_dictionary.md
* 03_pipeline_orchestration.md
* 04_data_quality_strategy.md
* 05_traceability.md
* 07_architectural_decisions.md
* 08_solution_adherence_Matrix.md

---

# Autor

Claudia Costa
