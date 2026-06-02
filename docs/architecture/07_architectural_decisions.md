# Architectural Decision Records (ADR)

## 1. Objetivo

Este documento registra as principais decisões arquiteturais adotadas no projeto Brazil Legislative Analytics Medallion.

O objetivo é documentar o contexto, as alternativas avaliadas e os motivos que justificaram cada escolha técnica.

---

# ADR-001 — Arquitetura Medalhão

## Status

Aprovada

---

## Contexto

O projeto necessita processar dados legislativos provenientes de múltiplos endpoints da Câmara dos Deputados, garantindo rastreabilidade, qualidade e reprocessamento.

---

## Decisão

Adotar a arquitetura Medalhão composta por:

```text
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Mat
```

---

## Justificativa

* Separação clara de responsabilidades
* Facilidade de auditoria
* Reprocessamento simplificado
* Escalabilidade
* Aderência às boas práticas Databricks

---

# ADR-002 — Utilização do Delta Lake

## Status

Aprovada

---

## Contexto

Os dados precisam suportar auditoria, versionamento e processamento incremental.

---

## Decisão

Utilizar Delta Lake como forMato padrão de armazenamento.

---

## Justificativa

* Transações ACID
* Performance
* Evolução de schema
* Integração nativa com Databricks
* Time Travel

---

# ADR-003 — API como Fonte Primária

## Status

Aprovada

---

## Contexto

A Câmara dos Deputados disponibiliza dados através da API Dados Abertos.

---

## Decisão

Consumir prioritariamente a API oficial.

---

## Justificativa

* Fonte oficial
* Dados atualizados
* Estrutura documentada
* Menor necessidade de manutenção

---

# ADR-004 — CSV como Estratégia de Fallback

## Status

Aprovada

---

## Contexto

A API pode apresentar indisponibilidade temporária ou limitações operacionais.

---

## Decisão

Implementar mecanismo de fallback para arquivos CSV.

---

## Fluxo

```text
API Disponível
        │
        ▼
     Bronze

API Indisponível
        │
        ▼
CSV Fallback
        │
        ▼
     Bronze
```

---

## Justificativa

* Continuidade operacional
* Redução de indisponibilidade
* Recuperação simplificada
* Maior resiliência

---

# ADR-005 — Camada Mat (Business Marts)

## Status

Aprovada

---

## Contexto

Os consumidores analíticos necessitam consultas simplificadas e indicadores prontos para uso.

---

## Decisão

Criar camada especializada de Business Marts.

---

## Data Marts Implementados

* am_atlas_frentes
* am_calendario_eventos
* am_correlacao_frentes_votacoes
* am_panorama_despesas_ceap
* am_auditoria_cpis
* am_monitor_presenca_absenteismo

---

## Justificativa

* Redução da complexidade analítica
* Melhor performance
* Reuso dos indicadores
* Facilidade de consumo

---

# ADR-006 — Funções Compartilhadas

## Status

Aprovada

---

## Contexto

Os pipelines utilizam diversas regras comuns.

---

## Decisão

Centralizar regras em bibliotecas reutilizáveis.

---

## Bibliotecas

* utils_api_client
* utils_hash
* utils_logger
* utils_quality
* utils_datetime
* utils_pagination
* utils_table_logger
* utils_rejected_records
* utils_text
* utils_config

---

## Justificativa

* Reuso de código
* Padronização
* Facilidade de manutenção
* Menor risco operacional

---

# ADR-007 — Auditoria Centralizada

## Status

Aprovada

---

## Contexto

O projeto necessita rastrear execuções, erros e qualidade.

---

## Decisão

Implementar schema dedicado de auditoria.

---

## Tabelas

* aud_log_execucao_pipeline
* aud_log_erros_pipeline
* aud_log_qualidade_dados

---

## Justificativa

* Observabilidade
* Diagnóstico de falhas
* Governança
* Rastreabilidade

---

# ADR-008 — Registros Rejeitados

## Status

Aprovada

---

## Contexto

Nem todos os registros recebidos atendem às regras de qualidade.

---

## Decisão

Separar registros inválidos em estrutura específica.

---

## Estrutura

```text
slv_registros_rejeitados
```

---

## Justificativa

* Não interromper cargas
* Facilitar investigação
* Melhorar governança

---

# ADR-009 — Hash de Registro

## Status

Aprovada

---

## Contexto

É necessário identificar alterações e duplicidades.

---

## Decisão

Utilizar hashes técnicos para rastreabilidade.

---

## Implementação

```text
utils_hash
```

---

## Aplicações

* Deduplicação
* Controle incremental
* Auditoria
* Consistência

---

# ADR-010 — Modelo Dimensional

## Status

Aprovada

---

## Contexto

As análises exigem alto desempenho e simplicidade de consulta.

---

## Decisão

Implementar modelo dimensional na camada Gold.

---

## Estruturas

Dimensões:

* dm_deputados
* dm_partidos
* dm_estados
* dm_datas
* dm_fornecedores

Fatos:

* ft_frentes_membros
* ft_presencas_eventos
* ft_resultados_votacoes
* ft_despesas_ceap
* ft_eventos_cpis

---

## Justificativa

* Melhor performance analítica
* Simplificação de consultas
* Escalabilidade

---

# ADR-011 — Estratégia de Qualidade

## Status

Aprovada

---

## Contexto

Os dados possuem múltiplas fontes e transformações.

---

## Decisão

Aplicar validações progressivas em cada camada.

---

## Distribuição

| Camada | Foco                         |
| ------ | ---------------------------- |
| Bronze | Integridade da ingestão      |
| Silver | Consistência e qualidade     |
| Gold   | Integridade dimensional      |
| Mat    | Consistência dos indicadores |

---

## Justificativa

* Redução de erros
* Melhor governança
* Maior confiabilidade

---

# ADR-012 — Estratégia de Replay

## Status

Aprovada

---

## Contexto

Falhas podem ocorrer em qualquer camada.

---

## Decisão

Permitir reprocessamento independente por camada.

---

## Escopo

* Replay Bronze
* Replay Silver
* Replay Gold
* Replay Mat

---

## Justificativa

* Recuperação rápida
* Menor custo operacional
* Maior disponibilidade

---
# ADR-013 — Consolidação da Camada Silver

## Status

Aprovada

---

## Contexto

Durante a evolução da solução, foi avaliada a possibilidade de subdividir a camada Silver em múltiplas etapas de processamento (Trusted, Business e Consumption).

Entretanto, a complexidade adicional não agregava valor proporcional ao volume de dados e aos requisitos do projeto.

---

## Decisão

Adotar uma única camada Silver curada responsável por:

* limpeza de dados;
* padronização;
* deduplicação;
* enriquecimento;
* validações de negócio;
* tratamento de registros rejeitados.

Fluxo final adotado:

```text
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Marts
```

---

## Justificativa

* simplificação operacional;
* menor custo de manutenção;
* redução da complexidade dos pipelines;
* melhor rastreabilidade;
* aderência ao escopo do projeto.

---

## Impacto

A estrutura final dos notebooks passa a seguir:

```text
00_setup/
01_bronze/
02_silver/
03_gold/
04_marts/
05_quality/
06_jobs/
99_utils/
```

sem subdivisões adicionais da camada Silver.

---

# ADR-014 — Framework de Qualidade e Governança

## Status

Aprovada

---

## Contexto

A solução exige mecanismos que garantam qualidade, rastreabilidade e governança ao longo de todo o ciclo de vida dos dados.

---

## Decisão

Implementar uma camada dedicada de validações e governança composta por notebooks especializados.

Estrutura adotada:

```text
05_quality/
├── 01_quality_bronze_checks.py
├── 02_quality_silver_checks.py
├── 03_quality_gold_checks.py
├── 04_traceability_checks.py
├── 05_quality_marts_checks.py
└── 06_governance_metadata_checks.py
```

---

## Objetivos

* validar qualidade dos dados;
* validar integridade referencial;
* validar rastreabilidade;
* validar metadados obrigatórios;
* garantir conformidade da documentação técnica.

---

## Justificativa

* aumento da confiabilidade analítica;
* melhoria da governança;
* detecção antecipada de inconsistências;
* maior observabilidade operacional.

---

## Impacto

A arquitetura final passa a incorporar uma camada transversal de qualidade e governança aplicada às camadas Bronze, Silver, Gold e Marts.

---

# Resumo das Decisões

| ADR     | Tema                    |
| ------- | ----------------------- |
| ADR-001 | Arquitetura Medalhão    |
| ADR-002 | Delta Lake              |
| ADR-003 | API Oficial             |
| ADR-004 | CSV Fallback            |
| ADR-005 | Camada Mat              |
| ADR-006 | Funções Compartilhadas  |
| ADR-007 | Auditoria Centralizada  |
| ADR-008 | Registros Rejeitados    |
| ADR-009 | Hash de Registro        |
| ADR-010 | Modelo Dimensional      |
| ADR-011 | Estratégia de Qualidade |
| ADR-012 | Estratégia de Replay    |
|ADR-013 | Consolidação da Camada Silver|
|ADR-014 | Framework de Qualidade e Governança|
---

# Referências

* architecture/01_solution_architecture.md
* data_dictionary/02_data_dictionary.md
* operations/03_pipeline_orchestration.md
* governance/04_data_quality.md
* governance/05_traceability.md
* operations/06_runbook.md
