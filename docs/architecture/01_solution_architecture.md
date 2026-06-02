# Solution Architecture

## 1. Objetivo

Este documento descreve a arquitetura da solução **Brazil Legislative Analytics Medallion**, desenvolvida para ingestão, processamento, governança e disponibilização analítica dos dados públicos da Câmara dos Deputados.

A plataforma foi construída utilizando a Arquitetura Medalhão (Medallion Architecture), implementada no Databricks com Apache Spark e Delta Lake, seguindo princípios modernos de Engenharia de Dados, Analytics Engineering e Governança de Dados.

---

# 2. Visão Geral da Arquitetura

A solução realiza a ingestão dos dados da API Oficial da Câmara dos Deputados, executa processos de curadoria e modelagem dimensional e disponibiliza Data Marts especializados para consumo analítico.

## Fluxo Macro da Solução

```text
Fonte de Dados
(API Câmara dos Deputados)
          │
          ▼
01_Bronze (Raw)
          │
          ▼
02_Silver (Curated)
          │
          ▼
03_Gold (Dimensional)
          │
          ▼
04_Marts (Analytics)
          │
          ▼
05_Quality & Governance
```

---

# 3. Diagramas Arquiteturais

A arquitetura é complementada pelos seguintes diagramas:

| Arquivo                                | Descrição                           |
| -------------------------------------- | ----------------------------------- |
| 01_medallion_architecture_overview.png | Visão geral da arquitetura Medalhão |
| 02_end_to_end_data_flow.png            | Fluxo ponta a ponta dos dados       |
| 03_star_schema_model.png               | Modelo dimensional da camada Gold   |

---

# 4. Princípios Arquiteturais

A solução foi construída com base nos seguintes princípios:

## Separação de Responsabilidades

Cada camada possui uma responsabilidade específica.

| Camada  | Responsabilidade              |
| ------- | ----------------------------- |
| Bronze  | Persistência dos dados brutos |
| Silver  | Curadoria e padronização      |
| Gold    | Modelagem dimensional         |
| Marts   | Consumo analítico             |
| Quality | Governança e validações       |

---

## Reprocessamento Controlado

Todas as camadas permitem reprocessamento independente.

Benefícios:

* recuperação de falhas;
* auditoria;
* rastreabilidade;
* reexecução incremental.

---

## Governança Nativa

A governança é aplicada desde a ingestão até o consumo analítico.

Controles implementados:

* auditoria;
* qualidade de dados;
* rastreabilidade;
* versionamento;
* metadados técnicos.

---

# 5. Camada Bronze

## Objetivo

Armazenar os dados exatamente como recebidos das fontes de origem.

## Características

* dados brutos;
* sem transformações de negócio;
* preservação histórica;
* rastreabilidade completa;
* suporte a replay.

## Fontes

### Fonte Principal

API Dados Abertos da Câmara dos Deputados.

### Fonte Secundária

Arquivos CSV utilizados como mecanismo de contingência.

## Benefícios

* preservação da origem;
* recuperação simplificada;
* auditoria completa;
* reprocessamento seguro.

---

# 6. Camada Silver

## Objetivo

Consolidar e padronizar os dados provenientes da Bronze.

## Principais Processamentos

* deduplicação;
* padronização de tipos;
* tratamento de nulos;
* enriquecimento;
* validações de negócio;
* rejeição de registros inválidos.

## Resultado

Dados confiáveis e consistentes para modelagem dimensional.

---

# 7. Camada Gold

## Objetivo

Disponibilizar um modelo dimensional corporativo baseado em Star Schema.

## Características

* dimensões conformadas;
* surrogate keys;
* integridade referencial;
* fatos analíticos;
* granularidade controlada.

### Dimensões

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

### Tabelas Fato

```text
ft_frentes_membros
ft_presencas_eventos
ft_resultados_votacoes
ft_despesas_ceap
ft_eventos_cpis
```

---

# 8. Camada Marts

## Objetivo

Disponibilizar conjuntos analíticos especializados para consumo de negócio.

## Data Marts Disponíveis

### am_atlas_frentes

Análise de composição, representatividade e diversidade das frentes parlamentares.

### am_calendario_eventos_legislativos

Visão consolidada de eventos e participação parlamentar.

### am_correlacao_frentes_votacoes

Análise de alinhamento entre frentes parlamentares e votações.

### am_visao_geral_despesas_ceap

Monitoramento de despesas parlamentares, fornecedores e anomalias.

### am_auditoria_cpis

Rastreamento do ciclo de vida das CPIs.

### am_monitor_presenca_absenteismo

Indicadores de presença, absenteísmo e engajamento parlamentar.

---

# 9. Camada Quality & Governance

## Objetivo

Garantir confiabilidade, conformidade e rastreabilidade dos dados.

## Framework de Qualidade

Validações implementadas:

* qualidade Bronze;
* qualidade Silver;
* qualidade Gold;
* qualidade Marts;
* rastreabilidade;
* governança de metadados.

## Benefícios

* detecção precoce de inconsistências;
* redução de riscos analíticos;
* melhoria da confiabilidade dos dados.

---

# 10. Estratégia de Carga

A solução prioriza processamento incremental sempre que possível.

Critérios utilizados:

* identificadores naturais;
* data de atualização;
* janelas temporais;
* hashes técnicos.

Benefícios:

* menor custo computacional;
* maior desempenho;
* reprocessamento simplificado.

---

# 11. Monitoramento e Auditoria

Todos os pipelines registram informações operacionais.

## Informações Auditadas

* início da execução;
* fim da execução;
* duração;
* quantidade processada;
* status da execução;
* erros encontrados;
* origem utilizada.

## Objetivos

* observabilidade;
* rastreabilidade;
* troubleshooting;
* suporte operacional.

---

# 12. Segurança e Governança

A solução foi projetada considerando boas práticas de governança de dados.

Controles implementados:

* nomenclatura padronizada;
* metadados técnicos;
* documentação centralizada;
* auditoria operacional;
* controle de lineage;
* validações automáticas.

---

# 13. Tecnologias Utilizadas

| Tecnologia    | Finalidade                    |
| ------------- | ----------------------------- |
| Databricks    | Plataforma de processamento   |
| Apache Spark  | Processamento distribuído     |
| PySpark       | Transformações de dados       |
| Delta Lake    | Armazenamento transacional    |
| Unity Catalog | Governança e catálogo         |
| Python        | Desenvolvimento dos pipelines |
| Câmara API    | Fonte de dados oficial        |

---

# 14. Benefícios da Arquitetura

A arquitetura adotada proporciona:

* escalabilidade;
* governança;
* rastreabilidade;
* reprocessamento controlado;
* qualidade de dados;
* reutilização de componentes;
* manutenção simplificada;
* consumo analítico otimizado.

---

# 15. Documentação Relacionada

* 07_architectural_decisions.md
* 02_data_dictionary.md
* 03_pipeline_orchestration.md
* 04_data_quality_strategy.md
* 05_traceability.md
* 06_runbook.md
* 08_solution_adherence_matrix.md
* changelog.md

---

# Autor

Claudia Costa
