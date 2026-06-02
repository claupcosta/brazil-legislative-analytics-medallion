# Data Dictionary

## 1. Objetivo

Este documento descreve as principais entidades, relacionamentos e estruturas analíticas utilizadas na plataforma Brazil Legislative Analytics Medallion.

Seu objetivo é fornecer uma visão conceitual da organização dos dados ao longo das camadas Bronze, Silver, Gold e MAT.

O detalhamento completo de tabelas, colunas, tipos de dados, regras de negócio e metadados encontra-se no dicionário de dados oficial do projeto.

---

# 2. Referência Oficial do Dicionário de Dados

A documentação detalhada de todas as tabelas e colunas está disponível no artefato abaixo:

**Legislative Data Dictionary**

```text
data_dictionary/legislative_data_dictionary.xlsx
```

O arquivo contempla:

* Camada Bronze
* Camada Silver
* Camada Gold
* Camada MAT
* Campos técnicos
* Metadados de auditoria
* Descrições funcionais
* Tipos de dados

Este documento apresenta uma visão arquitetural e conceitual das entidades principais.

---

# 3. Organização das Camadas

A arquitetura segue o padrão Medalhão complementado por uma camada de Data Marts analíticos.

| Camada | Objetivo                               |
| ------ | -------------------------------------- |
| Bronze | Armazenamento dos dados brutos         |
| Silver | Limpeza, padronização e enriquecimento |
| Gold   | Modelo dimensional corporativo         |
| MAT    | Data Marts especializados              |

---

## Fluxo de Dados

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
               MAT
```

---

# 4. Camada Bronze

A camada Bronze armazena os dados recebidos diretamente das fontes de origem.

## Características

* Dados brutos
* Preservação do histórico
* Auditoria de ingestão
* Replay operacional
* Fallback CSV

## Principais Domínios

* Deputados
* Frentes Parlamentares
* Eventos
* Votações
* Proposições
* Órgãos
* Despesas CEAP

---

# 5. Camada Silver

A camada Silver concentra as transformações, validações e regras de negócio.

## Principais Processos

* Padronização
* Limpeza
* Deduplicação
* Tratamento de nulos
* Regras de qualidade
* Enriquecimento de dados

## Entidades Principais

* Deputados
* Partidos
* Frentes
* Eventos
* Votações
* Fornecedores
* CPIs
* Proposições

---

# 6. Framework de Funções Compartilhadas

A partir da camada Silver, os pipelines utilizam bibliotecas reutilizáveis para padronizar comportamentos.

## Principais Módulos

| Biblioteca             | Finalidade            |
| ---------------------- | --------------------- |
| utils_api_client       | Consumo da API        |
| utils_quality          | Qualidade de dados    |
| utils_hash             | Geração de hashes     |
| utils_logger           | Logging               |
| utils_table_logger     | Auditoria             |
| utils_datetime         | Datas                 |
| utils_pagination       | Paginação             |
| utils_rejected_records | Registros rejeitados  |
| utils_text             | Tratamento textual    |
| utils_config           | Configurações globais |

---

# 7. Camada Gold

A camada Gold implementa o modelo dimensional corporativo utilizado pelos processos analíticos.

## Dimensões Principais

* dm_deputados
* dm_partidos
* dm_estados
* dm_datas
* dm_fornecedores
* dm_frentes
* dm_eventos
* dm_votacoes
* dm_cpis

---

## Fatos Principais

* ft_frentes_membros
* ft_presencas_eventos
* ft_resultados_votacoes
* ft_despesas_ceap
* ft_eventos_cpis

---

# 8. Camada MAT

A camada MAT disponibiliza Data Marts especializados para consumo analítico.

## Data Marts

### am_atlas_frentes

Análise de composição e diversidade das frentes parlamentares.

### am_calendario_eventos

Análise temporal dos eventos legislativos.

### am_correlacao_frentes_votacoes

Análise de alinhamento parlamentar.

### am_panorama_despesas_ceap

Monitoramento e análise de despesas parlamentares.

### am_auditoria_cpis

Monitoramento das Comissões Parlamentares de Inquérito.

### am_monitor_presenca_absenteismo

Indicadores de participação parlamentar.

---

# 9. Relacionamentos

```text
dm_deputados
      │
      ├────────────┐
      │            │
      ▼            ▼
ft_votacoes   ft_eventos
      │
      ▼
ft_despesas

Fatos
  │
  ▼
Data Marts (MAT)
```

---

# 10. Convenções de Nomenclatura

## Tabelas

| Prefixo | Descrição       |
| ------- | --------------- |
| br_     | Bronze          |
| slv_    | Silver          |
| dm_     | Dimensão        |
| ft_     | Fato            |
| am_     | Analytical Mart |
| vw_     | View            |

---

## Campos

| Sufixo | Descrição        |
| ------ | ---------------- |
| _id    | Chave natural    |
| _sk    | Chave substituta |
| _dt    | Data             |
| _ts    | Timestamp        |

---

# 11. Campos Técnicos de Auditoria

Os campos de auditoria são padronizados em todas as camadas.

## Principais Campos

| Campo                  | Finalidade                |
| ---------------------- | ------------------------- |
| aud_id_execucao        | Identificação da execução |
| aud_dh_processamento   | Data de processamento     |
| aud_tx_hash_registro   | Hash do registro          |
| aud_tx_versao_pipeline | Versão do pipeline        |
| aud_tx_sistema_origem  | Origem dos dados          |

O detalhamento completo encontra-se no arquivo Excel do dicionário de dados.

---

# 12. Linhagem dos Dados

```text
API Câmara
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
   MAT
```

---

# 13. Artefatos Relacionados

| Documento                        | Descrição                              |
| -------------------------------- | -------------------------------------- |
| legislative_data_dictionary.xlsx | Catálogo completo de tabelas e colunas |
| 01_solution_architecture.md      | Arquitetura da solução                 |
| 03_pipeline_orchestration.md     | Fluxo dos pipelines                    |
| 04_data_quality.md               | Estratégia de qualidade                |
| 05_traceability.md               | Estratégia de rastreabilidade          |

---

# 14. Referências

* architecture/01_solution_architecture.md
* operations/03_pipeline_orchestration.md
* governance/04_data_quality.md
* governance/05_traceability.md
* governance/06_runbook.md
* data_dictionary/legislative_data_dictionary.xlsx
