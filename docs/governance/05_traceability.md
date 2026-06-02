# Traceability Strategy

## 1. Objetivo

Este documento descreve a estratégia de rastreabilidade implementada no projeto Brazil Legislative Analytics Medallion.

O objetivo é permitir o acompanhamento da origem, processamento, evolução e qualidade dos dados ao longo das camadas Bronze, Silver, Gold e Mat.

---

## 2. Escopo

A rastreabilidade é aplicada nas seguintes camadas:

| Camada | Aplicação                                         |
| ------ | ------------------------------------------------- |
| Bronze | Origem da ingestão, endpoint, API ou CSV fallback |
| Silver | Processamento, padronização e hash dos registros  |
| Gold   | Modelo dimensional e auditoria de processamento   |
| Mat    | Indicadores analíticos e consistência final       |
| Audit  | Logs de execução, erros e qualidade               |

---

## 3. Camadas do Fluxo

```text
API Câmara / CSV Fallback
        ↓
      Bronze
        ↓
      Silver
        ↓
       Gold
        ↓
        Mat
        ↓
 Dashboards e Analytics
```

---

## 4. Campos Técnicos de Rastreabilidade

O projeto utiliza campos técnicos padronizados para rastrear os dados.

### Bronze

| Campo                  | Descrição                  |
| ---------------------- | -------------------------- |
| aud_id_execucao        | Identificador da execução  |
| aud_dh_ingestao        | Data e hora da ingestão    |
| aud_tx_endpoint_origem | Endpoint ou origem do dado |
| aud_tx_sistema_origem  | Sistema de origem          |
| aud_tx_versao_pipeline | Versão do pipeline         |
| aud_tx_hash_registro   | Hash técnico do registro   |

---

### Silver

| Campo                  | Descrição                    |
| ---------------------- | ---------------------------- |
| aud_id_execucao        | Identificador da execução    |
| aud_dh_processamento   | Data e hora do processamento |
| aud_tx_versao_pipeline | Versão do pipeline           |
| aud_tx_hash_registro   | Hash técnico do registro     |

---

### Gold e Mat

| Campo                  | Descrição                    |
| ---------------------- | ---------------------------- |
| aud_id_execucao        | Identificador da execução    |
| aud_dh_processamento   | Data e hora do processamento |
| aud_tx_versao_pipeline | Versão do pipeline           |

---

## 5. Tabelas de Auditoria

O projeto utiliza o schema `audit` para centralizar logs operacionais e de qualidade.

| Tabela                    | Finalidade                            |
| ------------------------- | ------------------------------------- |
| aud_log_execucao_pipeline | Histórico de execução dos pipelines   |
| aud_log_erros_pipeline    | Registro de erros e exceções          |
| aud_log_qualidade_dados   | Resultado das validações de qualidade |

---

## 6. Log de Execução dos Pipelines

A tabela `aud_log_execucao_pipeline` registra o histórico das execuções.

Principais campos:

| Campo                     | Descrição                 |
| ------------------------- | ------------------------- |
| aud_id_execucao           | Identificador da execução |
| aud_tx_nome_projeto       | Nome do projeto           |
| aud_tx_versao_pipeline    | Versão executada          |
| aud_tx_ambiente           | Ambiente                  |
| aud_tx_nome_notebook      | Notebook executado        |
| aud_tx_nome_camada        | Camada processada         |
| aud_tx_nome_entidade      | Entidade processada       |
| aud_tx_tabela_destino     | Tabela de destino         |
| aud_tx_status             | Status da execução        |
| aud_dh_inicio             | Início da execução        |
| aud_dh_fim                | Fim da execução           |
| aud_nr_duracao_segundos   | Duração da execução       |
| aud_qt_registros_lidos    | Registros lidos           |
| aud_qt_registros_gravados | Registros gravados        |
| aud_tx_mensagem           | Mensagem operacional      |

---

## 7. Log de Erros

A tabela `aud_log_erros_pipeline` registra falhas durante a execução.

Principais campos:

| Campo                | Descrição             |
| -------------------- | --------------------- |
| err_id_erro          | Identificador do erro |
| aud_id_execucao      | Execução associada    |
| aud_tx_nome_notebook | Notebook com falha    |
| aud_tx_nome_camada   | Camada da falha       |
| aud_tx_nome_entidade | Entidade processada   |
| err_tx_nome_etapa    | Etapa do erro         |
| err_tx_tipo_erro     | Tipo do erro          |
| err_tx_mensagem      | Mensagem do erro      |
| err_tx_stacktrace    | Stack trace           |
| err_dh_ocorrencia    | Momento da ocorrência |

---

## 8. Log de Qualidade

A tabela `aud_log_qualidade_dados` registra os resultados das validações.

Principais campos:

| Campo                      | Descrição             |
| -------------------------- | --------------------- |
| qlt_id_log                 | Identificador do log  |
| aud_id_execucao            | Execução associada    |
| aud_tx_nome_camada         | Camada validada       |
| aud_tx_nome_entidade       | Entidade validada     |
| aud_tx_tabela_destino      | Tabela validada       |
| qlt_tx_nome_regra          | Nome da regra         |
| qlt_tx_descricao_regra     | Descrição da regra    |
| qlt_tx_status_validacao    | Status da validação   |
| qlt_qt_total_registros     | Total de registros    |
| qlt_qt_registros_invalidos | Registros inválidos   |
| qlt_pc_registros_invalidos | Percentual inválido   |
| qlt_dh_validacao           | Data da validação     |
| qlt_tx_mensagem            | Mensagem da validação |

---

## 9. Hash de Registro

O campo `aud_tx_hash_registro` é utilizado para rastrear alterações, apoiar deduplicação e identificar mudanças nos dados.

O projeto utiliza funções centralizadas no módulo:

```text
Notebooks/99_utils/utils_hash.py
```

Aplicações principais:

* Deduplicação
* Identificação de mudanças
* Controle incremental
* Auditoria de consistência
* Apoio à rastreabilidade entre camadas

---

## 10. Rastreabilidade por Camada

### Bronze

A camada Bronze registra:

* Endpoint de origem
* Sistema de origem
* Data/hora de ingestão
* Identificador da execução
* Hash do registro
* Origem API ou CSV fallback

---

### Silver

A camada Silver registra:

* Execução responsável pelo processamento
* Data/hora de processamento
* Hash do registro tratado
* Versão do pipeline
* Regras aplicadas de padronização e qualidade

---

### Gold

A camada Gold registra:

* Execução responsável pela geração dimensional
* Data/hora de processamento
* Versão do pipeline
* Tabelas dimensão e fato resultantes

---

### Mat

A camada Mat registra:

* Execução responsável pela geração do mart
* Data/hora de processamento
* Versão do pipeline
* Indicadores analíticos derivados

---

## 11. Validações de Rastreabilidade

O projeto possui notebook específico para validação de rastreabilidade:

```text
Notebooks/05_quality/04_traceability_checks.py
```

Esse processo valida:

* Existência das tabelas
* Existência dos campos técnicos
* Preenchimento dos campos críticos
* Disponibilidade do identificador de execução
* Disponibilidade da versão do pipeline
* Disponibilidade dos timestamps técnicos

---

## 12. Governança de Metadados

O projeto também possui validações de governança de metadados:

```text
Notebooks/05_quality/06_governance_metadata_checks.py
```

Essas validações verificam:

* Existência dos schemas esperados
* Existência das tabelas governadas
* Campos técnicos obrigatórios
* Comentários de tabelas
* Comentários de colunas
* Padronização de metadados

---

## 13. Tabelas Monitoradas

### Bronze

* br_deputados
* br_frentes
* br_frentes_membros
* br_eventos
* br_presencas_eventos
* br_votacoes
* br_votos
* br_despesas_ceap
* br_orgaos
* br_orgaos_membros
* br_proposicoes

### Silver

* slv_deputados
* slv_partidos
* slv_estados
* slv_frentes
* slv_frentes_membros
* slv_eventos
* slv_votacoes
* slv_votos
* slv_despesas_ceap
* slv_fornecedores
* slv_fornecedores_enriched
* slv_orgaos
* slv_orgaos_membros
* slv_cpis
* slv_proposicoes
* slv_registros_rejeitados

### Gold

Dimensões:

* dm_deputados
* dm_partidos
* dm_estados
* dm_datas
* dm_frentes
* dm_eventos
* dm_votacoes
* dm_cpis
* dm_fornecedores

Fatos:

* ft_frentes_membros
* ft_presencas_eventos
* ft_resultados_votacoes
* ft_despesas_ceap
* ft_eventos_cpis

### Mat

* am_atlas_frentes
* am_calendario_eventos
* am_correlacao_frentes_votacoes
* am_panorama_despesas_ceap
* am_auditoria_cpis
* am_monitor_presenca_absenteismo

---

## 14. Replay e Reprocessamento

A rastreabilidade permite identificar quais execuções, tabelas e entidades devem ser reprocessadas.

Cenários suportados:

| Cenário         | Ação                                               |
| --------------- | -------------------------------------------------- |
| Falha na API    | Usar CSV fallback ou reexecutar Bronze             |
| Falha na Silver | Reprocessar a partir da Bronze                     |
| Falha na Gold   | Reprocessar dimensões e fatos                      |
| Falha na Mat    | Reconstruir marts analíticos                       |
| Dados inválidos | Consultar logs de qualidade e registros rejeitados |

---

## 15. Fluxo de Investigação

```text
Erro ou inconsistência detectada
        ↓
Consulta aud_log_qualidade_dados
        ↓
Consulta aud_log_execucao_pipeline
        ↓
Consulta aud_log_erros_pipeline
        ↓
Identificação da camada afetada
        ↓
Reprocessamento controlado
```

---

## 16. Benefícios

A estratégia de rastreabilidade garante:

* Transparência operacional
* Facilidade de diagnóstico
* Reprocessamento controlado
* Histórico de execuções
* Governança sobre dados analíticos
* Confiabilidade dos indicadores finais

---

## 17. Referências

* architecture/01_solution_architecture.md
* data_dictionary/02_data_dictionary.md
* operations/03_pipeline_orchestration.md
* governance/04_data_quality.md
* operations/runbook.md
* Notebooks/00_setup/02_audit_tables.py
* Notebooks/05_quality/04_traceability_checks.py
* Notebooks/05_quality/06_governance_metadata_checks.py
* Notebooks/99_utils/utils_hash.py
