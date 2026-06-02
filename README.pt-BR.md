# Brazil Legislative Analytics Medallion

## Plataforma Analítica Legislativa baseada em Arquitetura Medalhão no Databricks


**Repositório GitHub:**
https://github.com/claupcosta/brazil-legislative-analytics-medallion

Projeto desenvolvido para ingestão, curadoria, modelagem dimensional e disponibilização analítica dos dados públicos da Câmara dos Deputados utilizando Databricks, Apache Spark, Delta Lake e Arquitetura Medalhão.


A solução foi construída seguindo práticas modernas de Engenharia de Dados, Analytics Engineering, Governança, Qualidade de Dados e Observabilidade, simulando padrões encontrados em ambientes corporativos de Data Platform.

---
# Como Avaliar Este Projeto

## Repositório

Repositório GitHub:

https://github.com/claupcosta/brazil-legislative-analytics-medallion

Todo o código-fonte, documentação, notebooks, diagramas, modelo dimensional, artefatos de governança e produtos analíticos estão disponíveis neste repositório.

---

## 1. Entendimento da Solução

* `docs/challenge/08_solution_adherence_matrix.md`

Matriz de aderência contendo o mapeamento completo entre os requisitos do desafio, implementação realizada e evidências técnicas.

---

## 2. Arquitetura

* `docs/architecture/01_solution_architecture.md`
* `docs/architecture/01_medallion_architecture_overview.png`
* `docs/architecture/02_end_to_end_data_flow.png`
* `docs/architecture/03_star_schema_model.png`

Documentação da arquitetura, fluxo de dados e modelo dimensional.

---

## 3. Dados

* `docs/data_dictionary/02_data_dictionary.md`
* `docs/data_dictionary/legislative_data_dictionary.xlsx`

Dicionário técnico contendo tabelas, colunas, regras de negócio e metadados.

---

## 4. Operação

* `docs/operations/03_pipeline_orchestration.md`
* `docs/operations/06_runbook.md`

Fluxo operacional, orquestração e procedimentos de execução.

---

## 5. Governança

* `docs/governance/04_data_quality_strategy.md`
* `docs/governance/05_traceability.md`

Estratégias de qualidade, rastreabilidade e governança implementadas.

---

# Resumo Executivo

A plataforma implementa uma Arquitetura Medalhão completa utilizando Databricks para processamento dos dados legislativos da Câmara dos Deputados.

## Principais Entregas

* Arquitetura Medalhão (Bronze, Silver, Gold e Marts)

* Modelo Dimensional (Star Schema)

* 6 Data Marts Analíticos

* Framework de Qualidade de Dados

* Framework de Rastreabilidade

* Governança de Metadados

* Processamento Incremental

* Estratégia de Fallback CSV

* Auditoria Operacional

* Replay e Recuperação

* Documentação Corporativa Completa

---

# Objetivo do Projeto

Este projeto foi desenvolvido para fins educacionais, estudo de Engenharia de Dados, construção de portfólio e demonstração de boas práticas de arquitetura moderna de dados.

A solução busca simular padrões encontrados em ambientes corporativos de Data Engineering, incluindo:

* Arquitetura Medalhão
* Governança de Dados
* Auditoria Operacional
* Qualidade de Dados
* Rastreabilidade
* Processamento Incremental
* Replay e Recuperação
* Modelagem Dimensional
* Data Marts Analíticos

---

# Arquitetura da Solução

```text
API Câmara dos Deputados
           │
           ▼
      01_Bronze
           │
           ▼
      02_Silver
           │
           ▼
       03_Gold
           │
           ▼
       04_Marts
           │
           ▼
05_Quality & Governance
```

## Camadas

| Camada  | Objetivo                                 |
| ------- | ---------------------------------------- |
| Bronze  | Ingestão e preservação dos dados brutos  |
| Silver  | Curadoria, padronização e enriquecimento |
| Gold    | Modelo dimensional corporativo           |
| Marts   | Produtos analíticos especializados       |
| Quality | Governança, qualidade e rastreabilidade  |

---

# Tecnologias Utilizadas

| Categoria       | Tecnologia             |
| --------------- | ---------------------- |
| Plataforma      | Databricks             |
| Linguagem       | Python                 |
| Processamento   | Apache Spark / PySpark |
| Armazenamento   | Delta Lake             |
| Governança      | Unity Catalog          |
| Orquestração    | Databricks Workflows   |
| Versionamento   | GitHub                 |
| Qualidade       | Framework próprio      |
| Rastreabilidade | Framework próprio      |

---

# Produtos Analíticos

A camada Marts disponibiliza os seguintes produtos analíticos:

| Data Mart                          | Objetivo                                            |
| ---------------------------------- | --------------------------------------------------- |
| am_atlas_frentes                   | Composição e diversidade das frentes parlamentares  |
| am_calendario_eventos_legislativos | Calendário legislativo e participação parlamentar   |
| am_correlacao_frentes_votacoes     | Correlação entre frentes e comportamento de votação |
| am_visao_geral_despesas_ceap       | Monitoramento de despesas parlamentares             |
| am_auditoria_cpis                  | Auditoria e acompanhamento de CPIs                  |
| am_monitor_presenca_absenteismo    | Indicadores de presença e engajamento               |

---

# Estrutura do Repositório

```text
BRAZIL-LEGISLATIVE-ANALYTICS-MEDALLION/
│
├── docs/
├── notebooks/
├── README.md
├── README.pt-BR.md
├── requirements.txt
└── .gitignore
```

## Estrutura dos Pipelines

```text
notebooks/
├── 00_setup/
├── 01_bronze/
├── 02_silver/
├── 03_gold/
├── 04_marts/
├── 05_quality/
├── 06_jobs/
└── 99_utils/
```

---

# Estrutura da Documentação

```text
docs/
├── architecture/
├── challenge/
├── data_dictionary/
├── governance/
├── marts/
├── operations/
└── changelog.md
```

---

# Governança e Observabilidade

A solução implementa mecanismos de governança ponta a ponta.

## Recursos Implementados

* Auditoria Operacional
* Data Quality Framework
* Traceability Framework
* Metadata Governance
* Logs Técnicos
* Replay por Camada
* Controle de Reprocessamento
* Registros Rejeitados
* Data Lineage
* Versionamento Delta Lake

---
# Links do Projeto

| Recurso              | Caminho                                                              |
| -------------------- | -------------------------------------------------------------------- |
| Repositório GitHub   | https://github.com/claupcosta/brazil-legislative-analytics-medallion |
| Arquitetura          | docs/architecture                                                    |
| Dicionário de Dados  | docs/data_dictionary                                                 |
| Governança           | docs/governance                                                      |
| Operação             | docs/operations                                                      |
| Data Marts           | docs/marts                                                           |
| Matriz de Aderência  | docs/challenge/08_solution_adherence_matrix.md                       |
| Histórico de Versões | docs/changelog.md                                                    |

---

## Documentação Recomendada

Para uma avaliação completa da solução, recomenda-se seguir a seguinte sequência:

1. Matriz de Aderência ao Desafio
2. Arquitetura da Solução
3. Modelo Dimensional
4. Dicionário de Dados
5. Orquestração de Pipelines
6. Estratégia de Qualidade de Dados
7. Estratégia de Rastreabilidade
8. Runbook Operacional
9. Decisões Arquiteturais
10. Changelog

---
# Autora

## Claudia Costa

Engenheira de Dados com foco em plataformas analíticas, arquitetura Lakehouse, Databricks, governança de dados, qualidade, rastreabilidade e soluções analíticas escaláveis.

Projeto desenvolvido para fins educacionais, portfólio técnico e demonstração de competências em Engenharia de Dados.

---

# Licença

Este projeto utiliza exclusivamente dados públicos disponibilizados pela Câmara dos Deputados e fontes públicas complementares.

Todos os artefatos analíticos foram desenvolvidos para fins educacionais, estudos técnicos e demonstração de arquitetura de dados.
