# Data Quality Strategy

## 1. Objetivo

Este documento descreve a estratégia de qualidade de dados implementada na plataforma analítica da Câmara dos Deputados.

O objetivo é garantir consistência, confiabilidade, integridade e rastreabilidade dos dados ao longo de todo o ciclo de processamento.

---

# 2. Escopo

A estratégia de qualidade é aplicada em todas as camadas da arquitetura.

| Camada | Objetivo                                |
| ------ | --------------------------------------- |
| Bronze | Garantir integridade da ingestão        |
| Silver | Garantir padronização e consistência    |
| Gold   | Garantir integridade analítica          |
| Mat    | Garantir confiabilidade dos indicadores |

---

# 3. Princípios de Qualidade

A solução foi construída seguindo cinco pilares:

* Completude
* Consistência
* Unicidade
* Integridade
* Rastreabilidade

---

# 4. Qualidade na Camada Bronze

## Objetivo

Garantir que os dados sejam recebidos corretamente da fonte de origem.

### Fontes de Dados

* API Dados Abertos da Câmara
* CSV Fallback

---

## Validações Técnicas

| Regra                  | Descrição                      |
| ---------------------- | ------------------------------ |
| Disponibilidade da API | Verifica resposta da API       |
| Disponibilidade do CSV | Verifica fallback              |
| Estrutura mínima       | Schema esperado                |
| Volume mínimo          | Quantidade mínima de registros |
| ForMato válido         | JSON ou CSV válido             |

---

## Metadados de Auditoria

| Campo               | Descrição              |
| ------------------- | ---------------------- |
| source_type         | API ou CSV_FALLBACK    |
| ingestion_timestamp | Momento da ingestão    |
| load_id             | Identificador da carga |
| processing_status   | Status da execução     |

---

# 5. Qualidade na Camada Silver

## Objetivo

Garantir dados consistentes, padronizados e preparados para integração.

---

## Validações de Completude

| Regra               | Exemplo     |
| ------------------- | ----------- |
| Campos obrigatórios | deputado_id |
| Datas válidas       | data_evento |
| Chaves preenchidas  | evento_id   |

---

## Validações de Unicidade

| Entidade | Chave       |
| -------- | ----------- |
| Deputado | deputado_id |
| Evento   | evento_id   |
| Votação  | votacao_id  |
| Despesa  | despesa_id  |

---

## Validações de Consistência

Exemplos:

* Datas inválidas
* Valores negativos
* Legislaturas inexistentes
* UFs inválidas
* Partidos inexistentes

---

## Deduplicação

São utilizados:

* Chaves naturais
* Hashes técnicos
* Regras de negócio

---

# 5.1 Framework de Funções Compartilhadas

A partir da camada Silver o projeto utiliza bibliotecas reutilizáveis responsáveis por padronizar comportamentos entre todos os pipelines.

## Bibliotecas Disponíveis

| Biblioteca             | Responsabilidade               |
| ---------------------- | ------------------------------ |
| utils_api_client       | Consumo padronizado da API     |
| utils_cnpj             | Validação de CNPJ              |
| utils_comments         | Comentários e metadados        |
| utils_config           | Configurações globais          |
| utils_datetime         | Tratamento de datas            |
| utils_hash             | Geração de hashes              |
| utils_legislature      | Regras de legislatura          |
| utils_logger           | Logs de execução               |
| utils_pagination       | Paginação da API               |
| utils_quality          | Regras de qualidade            |
| utils_rejected_records | Gestão de registros rejeitados |
| utils_table_logger     | Auditoria de tabelas           |
| utils_text             | Limpeza textual                |

---

## Aplicação nas Camadas

```text
Silver
 ├─ Validação
 ├─ Deduplicação
 ├─ Padronização
 └─ Auditoria

Gold
 ├─ Integridade Dimensional
 ├─ Hashes Técnicos
 └─ Auditoria

Mat
 ├─ Cálculo de Indicadores
 ├─ Consistência Analítica
 └─ Monitoramento
```

---

## Benefícios

* Reuso de código
* Padronização de regras
* Facilidade de manutenção
* Redução de erros
* Governança centralizada

---

# 6. Qualidade na Camada Gold

## Objetivo

Garantir confiabilidade do modelo dimensional.

---

## Integridade Referencial

Validações realizadas:

```text
fact_eventos      → dim_deputado
fact_votacoes     → dim_deputado
fact_despesas     → dim_deputado
fact_despesas     → dim_fornecedor
```

---

## Consistência Dimensional

Validações:

* Chaves substitutas válidas
* Dimensões órfãs
* Fatos órfãos
* Consistência temporal

---

# 7. Qualidade na Camada Mat

## Objetivo

Garantir confiabilidade dos indicadores disponibilizados para análise.

---

## Validações Analíticas

| Regra                    | Exemplo        |
| ------------------------ | -------------- |
| Percentuais válidos      | 0 a 100        |
| Scores válidos           | Faixa esperada |
| Totais consistentes      | Soma correta   |
| Séries temporais válidas | Sem lacunas    |

---

## Data Marts Monitorados

* Mat_atlas_frentes
* Mat_calendario_eventos
* Mat_correlacao_frentes_votacoes
* Mat_despesas_ceap
* Mat_auditoria_cpis
* Mat_presenca_absenteismo

---

# 8. Detecção de Anomalias

## Objetivo

Identificar padrões atípicos nos dados legislativos e financeiros.

---

## Caso de Uso: CEAP

Método utilizado:

* Z-Score

Dimensões analisadas:

```text
Categoria
    ×
Estado
    ×
Deputado
```

---

## Exemplos de Anomalias

* Gastos acima da média histórica
* Concentração excessiva por fornecedor
* Crescimento abrupto de despesas
* Valores incompatíveis com o perfil da categoria

---

## Classificação

| Z-Score | Classificação |
| ------- | ------------- |
| 0 – 2   | Normal        |
| 2 – 3   | Atenção       |
| > 3     | Anômalo       |

---

# 9. Tratamento de Registros Inválidos

Quando uma regra é violada:

1. Registrar ocorrência
2. Isolar registro
3. Permitir investigação
4. Possibilitar reprocessamento

---

## Quarantine Zone

Os registros inválidos são direcionados para estruturas específicas de rejeição.

Informações registradas:

* Registro original
* Motivo da rejeição
* Timestamp
* Pipeline responsável

---

# 10. Monitoramento da Qualidade

## Indicadores

| Métrica         | Meta        |
| --------------- | ----------- |
| Completude      | > 95%       |
| Duplicidade     | 0%          |
| Integridade     | > 99%       |
| Erros críticos  | 0           |
| Falhas de carga | Monitoradas |

---

# 11. Auditoria

Todos os pipelines registram:

* Data da execução
* Pipeline executado
* Registros processados
* Registros rejeitados
* Tempo de execução
* Origem utilizada
* Status final

---

# 12. Rastreabilidade

Cada registro pode ser rastreado através de:

| Campo               | Descrição              |
| ------------------- | ---------------------- |
| load_id             | Identificador da carga |
| source_type         | API ou CSV_FALLBACK    |
| ingestion_timestamp | Momento da ingestão    |
| processing_date     | Data de processamento  |

---

# 13. Fluxo de Correção

```text
Falha Detectada
       │
       ▼
Registro em Log
       │
       ▼
Quarantine
       │
       ▼
Análise
       │
       ▼
Correção
       │
       ▼
Reprocessamento
```

---

# 14. Responsabilidades

| Processo             | Responsável                        |
| -------------------- | ---------------------------------- |
| Ingestão             | Pipeline Bronze + utils_api_client |
| Validação            | Pipeline Silver + utils_quality    |
| Integridade          | Pipeline Gold + utils_hash         |
| Auditoria            | utils_logger + utils_table_logger  |
| Registros Rejeitados | utils_rejected_records             |
| Indicadores          | Pipeline Mat                       |
| Monitoramento        | Databricks Workflows               |

---

# 15. Referências

* architecture/01_solution_architecture.md
* data_dictionary/02_data_dictionary.md
* operations/03_pipeline_orchestration.md
* governance/05_traceability.md
* operations/runbook.md
