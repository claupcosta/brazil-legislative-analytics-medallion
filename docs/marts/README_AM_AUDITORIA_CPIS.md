# Auditoria de CPIs

## Visão Geral

O Data Mart `am_auditoria_cpis` consolida indicadores analíticos, operacionais e de qualidade relacionados às Comissões Parlamentares de Inquérito (CPIs) da Câmara dos Deputados.

A solução foi construída a partir da dimensão de CPIs e dos relacionamentos entre CPIs e eventos legislativos identificados na camada Gold, permitindo análises de cobertura, consistência, rastreabilidade e monitoramento da atividade parlamentar.

O MART preserva todas as CPIs existentes na dimensão Gold, inclusive aquelas sem eventos relacionados, garantindo visibilidade completa para auditoria e governança.

---

## Objetivos Analíticos

Este Data Mart permite responder perguntas como:

* Quantas CPIs possuem eventos relacionados?
* Quantos eventos foram associados a cada CPI?
* Qual o volume de relações diretas e semânticas?
* Qual o percentual de relações de alta confiança?
* Quais CPIs apresentam maior atividade?
* Qual a duração das CPIs?
* Qual o nível de cobertura dimensional da camada Gold?
* Existem inconsistências temporais ou de relacionamento?

---

## Fontes de Dados

### Camada Gold

| Tabela            | Descrição                                                     |
| ----------------- | ------------------------------------------------------------- |
| `dm_cpis`         | Cadastro consolidado das Comissões Parlamentares de Inquérito |
| `ft_eventos_cpis` | Relacionamentos entre CPIs e eventos legislativos             |

---

## Principais Indicadores

### Cobertura

* Quantidade de CPIs
* Quantidade de eventos relacionados
* Quantidade de eventos distintos
* Cobertura dimensional

### Relacionamentos

* Quantidade de relações CPI-Evento
* Relações diretas
* Relações semânticas
* Relações de alta confiança

### Temporalidade

* Duração da CPI
* Período entre primeiro e último evento
* Consistência temporal

### Auditoria

* Indicador de completude
* Indicador de consistência
* Ranking de atividade das CPIs

---

## Cobertura

| Indicador                      |   Valor |
| ------------------------------ | ------: |
| CPIs publicadas                |     152 |
| Relações CPI-Evento            |      13 |
| Eventos distintos relacionados |      13 |
| Legislaturas identificadas     | 56 e 57 |

Resultados obtidos durante a homologação do MART.

---

## Qualidade dos Dados

### Validações Executadas

#### Integridade

* Chave substituta gerada corretamente
* Business Key sem duplicidades
* Hash determinístico validado
* Ausência de registros inválidos
* Cobertura Gold validada

#### Qualidade

* Nenhuma métrica negativa encontrada
* Nenhum percentual fora do intervalo esperado
* Nenhuma inconsistência temporal identificada
* Nenhuma inconsistência de relacionamento identificada

---

## Ressalvas Conhecidas

### Cobertura Histórica de Legislaturas

Durante a homologação foi identificado que parte das CPIs históricas não possui legislatura associada.

Resultado da análise:

| Classificação                       | Quantidade |
| ----------------------------------- | ---------: |
| Legislatura 56                      |          3 |
| Legislatura 57                      |          4 |
| Históricas fora da janela analítica |        145 |

Foi realizada validação complementar e não foram identificadas CPIs iniciadas a partir de 2019 sem legislatura associada.

Conclusão:

* Não há falha de modelagem.
* Não há perda de qualidade dos dados.
* Os registros históricos foram preservados para auditoria e rastreabilidade.

---

### Cobertura de Eventos Relacionados

Apenas uma pequena parcela das CPIs possui eventos relacionados na fato `ft_eventos_cpis`.

Esta condição reflete a cobertura atual da camada Gold e não representa falha do MART.

As CPIs sem eventos relacionados foram mantidas para garantir completude histórica e rastreabilidade.

---

## Casos de Uso

* Auditoria legislativa
* Monitoramento de CPIs
* Governança de dados legislativos
* Análise de atividade parlamentar
* Avaliação de cobertura dos relacionamentos CPI-Evento
* Estudos históricos sobre CPIs

---

## Governança

### Grain

Um registro por CPI.

### Chave de Negócio

```text
cpi_id_orgao
```

### Chave Substituta

```text
acp_sk_auditoria_cpi
```

### Versão

```text
marts_v1.0_cpi_audit
```

---

## Artefatos

### Delta Table

```sql
brazil_legislative_analytics.marts.am_auditoria_cpis
```

### Exportação CSV

```text
/Volumes/brazil_legislative_analytics/marts/exports/am_auditoria_cpis/
```

---

## Status

✅ Aprovado para consumo analítico

### Resultado da Homologação

| Indicador      | Status   |
| -------------- | -------- |
| Cobertura Gold | Aprovado |
| Integridade    | Aprovado |
| Qualidade      | Aprovado |
| Auditoria      | Aprovado |

O MART encontra-se validado e disponível para análises de auditoria, governança e monitoramento das Comissões Parlamentares de Inquérito.
