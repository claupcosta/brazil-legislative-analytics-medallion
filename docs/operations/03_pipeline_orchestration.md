# Pipeline Orchestration

## 1. Objetivo

Este documento descreve a estratégia de orquestração dos pipelines de dados da plataforma **Brazil Legislative Analytics Medallion** implementada no Databricks.

O objetivo é garantir ingestão confiável, processamento escalável, observabilidade, rastreabilidade, qualidade de dados e recuperação de falhas em todas as camadas da arquitetura.

---

# 2. Visão Geral

A solução utiliza Databricks Workflows para orquestrar os pipelines de ingestão, transformação, modelagem dimensional, construção dos Data Marts e validações de governança.

## Fluxo Geral

```text
API Câmara dos Deputados
         │
         ├─────────────┐
         │             │
         ▼             ▼
   API Loader     CSV Fallback
         │             │
         └──────┬──────┘
                ▼
             Bronze
                ▼
             Silver
                ▼
              Gold
                ▼
              Marts
                ▼
            Quality
                ▼
      Analytics & Consumption
```

---

# 3. Estrutura Física do Projeto

A implementação está organizada nos seguintes diretórios:

```text
notebooks/
├── 00_setup
├── 01_bronze
├── 02_silver
├── 03_gold
├── 04_marts
├── 05_quality
├── 06_jobs
└── 99_utils
```

---

# 4. Estratégia de Orquestração

Os pipelines são organizados em camadas sequenciais.

| Camada  | Objetivo                                 |
| ------- | ---------------------------------------- |
| Setup   | Inicialização do ambiente                |
| Bronze  | Ingestão e persistência dos dados brutos |
| Silver  | Curadoria, limpeza e enriquecimento      |
| Gold    | Modelo dimensional corporativo           |
| Marts   | Data Marts analíticos                    |
| Quality | Qualidade, rastreabilidade e governança  |
| Jobs    | Orquestração operacional                 |
| Utils   | Bibliotecas compartilhadas               |

---

# 5. Pipeline Setup

## Objetivo

Preparar o ambiente para execução dos pipelines.

## Notebooks

```text
00_create_catalog_schemas.py
01_project_config.py
02_audit_tables.py
90_validate_project_setup.py
91_reset_project_environment.py
92_validate_api_connection.py
```

## Responsabilidades

* Criação de catálogos
* Criação de schemas
* Configuração do projeto
* Criação de tabelas de auditoria
* Validação de conectividade

---

# 6. Pipeline Bronze

## Objetivo

Realizar a captura dos dados da Câmara dos Deputados preservando os dados originais para rastreabilidade e reprocessamento.

## Estratégia de Ingestão

A ingestão ocorre prioritariamente através da API Oficial de Dados Abertos.

### Fluxo Principal

```text
API Câmara
      │
      ▼
Ingestão API
      │
      ▼
Bronze Delta
```

## Estratégia de Fallback

Quando a API apresenta indisponibilidade, timeout ou falhas persistentes, o pipeline utiliza arquivos CSV previamente disponibilizados.

### Fluxo de Recuperação

```text
API Câmara
      │
      ├── Disponível ──► Bronze Delta
      │
      └── Falha
              │
              ▼
        CSV Fallback
              │
              ▼
         Bronze Delta
```

## Tabelas Principais

```text
br_deputados
br_frentes
br_frentes_membros
br_eventos
br_votacoes
br_votos
br_despesas_ceap
br_cpis
br_cpi_eventos
br_proposicoes
br_presencas_eventos
br_orgaos
br_orgaos_membros
```

---

# 7. Pipeline Silver

## Objetivo

Padronizar, limpar e consolidar os dados provenientes da camada Bronze.

## Principais Transformações

* Tratamento de valores nulos
* Deduplicação
* Padronização de tipos
* Normalização de atributos
* Aplicação de regras de negócio
* Enriquecimento de fornecedores
* Tratamento de registros rejeitados

## Tabelas Curadas

```text
slv_deputados
slv_partidos
slv_estados
slv_frentes
slv_frentes_membros
slv_eventos
slv_votacoes
slv_votos
slv_despesas_ceap
slv_fornecedores
slv_cpis
slv_cpi_eventos
slv_proposicoes
slv_presencas_eventos
slv_orgaos
```

## Tratamento de Rejeições

```text
slv_registros_rejeitados
```

---

# 8. Pipeline Gold

## Objetivo

Construir o modelo dimensional corporativo utilizado pelas análises.

## Dimensões

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

## Tabelas Fato

```text
ft_frentes_membros
ft_presencas_eventos
ft_resultados_votacoes
ft_despesas_ceap
ft_eventos_cpis
```

## Responsabilidades

* Surrogate Keys
* Integridade Referencial
* Modelo Estrela
* Dimensões Conformadas
* Fatos Analíticos

---

# 9. Pipeline Marts

## Objetivo

Disponibilizar Data Marts especializados para cada domínio analítico.

## Data Marts

### am_atlas_frentes

Composição, diversidade e representatividade das frentes parlamentares.

### am_calendario_eventos_legislativos

Calendário analítico de eventos legislativos.

### am_correlacao_frentes_votacoes

Análise de alinhamento parlamentar.

### am_visao_geral_despesas_ceap

Monitoramento de despesas parlamentares.

### am_auditoria_cpis

Rastreamento do ciclo de vida das CPIs.

### am_monitor_presenca_absenteismo

Análise de presença, absenteísmo e engajamento parlamentar.

---

# 10. Pipeline Quality

## Objetivo

Garantir qualidade, rastreabilidade e conformidade dos dados.

## Notebooks

```text
01_quality_bronze_checks.py
02_quality_silver_checks.py
03_quality_gold_checks.py
04_traceability_checks.py
05_quality_marts_checks.py
06_governance_metadata_checks.py
```

## Validações

* Qualidade Bronze
* Qualidade Silver
* Qualidade Gold
* Qualidade Marts
* Rastreabilidade
* Governança de Metadados

---

# 11. Estratégia de Carga Incremental

Sempre que suportado pela origem, os pipelines utilizam processamento incremental.

## Critérios

* ID da entidade
* Data de atualização
* Janela temporal
* Hash CDC

## Benefícios

* Menor custo computacional
* Menor volume processado
* Melhor desempenho
* Reprocessamento simplificado

---

# 12. Dependências Entre Camadas

```text
Setup
   │
   ▼
Bronze
   │
   ▼
Silver
   │
   ▼
Gold
   │
   ▼
Marts
   │
   ▼
Quality
```

Cada camada depende da conclusão bem-sucedida da etapa anterior.

---

# 13. Monitoramento

## Métricas Monitoradas

| Métrica           | Descrição                    |
| ----------------- | ---------------------------- |
| Tempo de execução | Duração do pipeline          |
| Volume processado | Quantidade de registros      |
| Taxa de erro      | Percentual de falhas         |
| Latência          | Tempo entre origem e consumo |
| Reprocessamentos  | Quantidade de replays        |

---

# 14. Auditoria

Todos os pipelines registram:

* Data da execução
* Nome do pipeline
* Quantidade de registros processados
* Status da execução
* Duração
* Erros encontrados
* Origem utilizada (API ou CSV_FALLBACK)

---

# 15. SLA Operacional

| Processo | SLA          |
| -------- | ------------ |
| Bronze   | < 30 minutos |
| Silver   | < 30 minutos |
| Gold     | < 20 minutos |
| Marts    | < 20 minutos |
| Quality  | < 15 minutos |

---

# 16. Referências

* architecture/01_solution_architecture.md
* architecture/07_architectural_decisions.md
* data_dictionary/02_data_dictionary.md
* governance/04_data_quality_strategy.md
* governance/05_traceability.md
* operations/06_runbook.md
* marts/README.md
* challenge/08_solution_adherence_matrix.md
