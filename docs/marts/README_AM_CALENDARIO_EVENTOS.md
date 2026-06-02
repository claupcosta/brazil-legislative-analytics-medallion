# Calendário de Eventos Legislativos

## Visão Geral

O Data Mart `am_calendario_eventos` consolida informações analíticas sobre os eventos legislativos da Câmara dos Deputados.

A solução permite analisar participação parlamentar, engajamento político, distribuição geográfica dos participantes e evolução temporal das atividades legislativas.

O MART foi construído a partir dos eventos registrados na camada Gold e dos respectivos registros de presença parlamentar.

---

## Objetivos Analíticos

Este Data Mart permite responder perguntas como:

* Quais eventos possuem maior participação parlamentar?
* Como os eventos estão distribuídos ao longo do tempo?
* Quais partidos participam mais dos eventos?
* Qual a distribuição geográfica da participação?
* Quais eventos apresentam maior engajamento parlamentar?

---

## Fontes de Dados

### Camada Gold

| Tabela                | Descrição                                     |
| --------------------- | --------------------------------------------- |
| `dm_eventos`          | Cadastro consolidado dos eventos legislativos |
| `ft_presenca_eventos` | Registros de presença parlamentar             |

---

## Principais Indicadores

* Quantidade de deputados participantes
* Quantidade total de presenças
* Quantidade total de ausências
* Percentual de presença
* Partido predominante
* UF predominante
* Ranking de engajamento
* Distribuição temporal dos eventos

Essas métricas foram definidas durante a construção do MART para suportar análises de participação e engajamento parlamentar.

---

## Cobertura

| Indicador                         |   Valor |
| --------------------------------- | ------: |
| Eventos analisados                |  14.926 |
| Registros de presença processados | 557.552 |

Valores obtidos durante a homologação do MART.

---

## Governança

### Grain

Um registro por evento legislativo.

### Camada

Mat (Analytical Mart)

### Origem

Gold Layer

### Rastreabilidade

O MART herda os mecanismos de auditoria, rastreabilidade e governança implementados nas camadas anteriores.

---

## Qualidade dos Dados

### Validações Executadas

#### Integridade

* Chaves obrigatórias preenchidas
* Ausência de duplicidades
* Cobertura completa da camada Gold
* Hash determinístico validado

#### Qualidade

* Nenhuma métrica negativa encontrada
* 100% dos registros aprovados nas validações

Resultados documentados durante a homologação da solução.

---

## Ressalvas Conhecidas

Os atributos:

```text
evt_tx_sigla_orgao
evt_tx_nome_orgao
```

apresentam valores nulos já na dimensão Gold (`dm_eventos`).

Esta condição é proveniente dos dados de origem e não representa falha de processamento do MART.

---

## Casos de Uso

* Calendário Legislativo
* Monitoramento de Eventos
* Análise de Participação Parlamentar
* Indicadores de Engajamento
* Estudos de Atividade Legislativa
* Distribuição Geográfica da Participação

---

## Artefatos

### Delta Table

```sql
brazil_legislative_analytics.marts.am_calendario_eventos
```

### Exportação CSV

```text
/Volumes/brazil_legislative_analytics/marts/exports/am_calendario_eventos/
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
| Publicação     | Aprovado |

O MART está validado e disponível para utilização analítica.
