# README_AM_PRESENCA_ABSENTEISMO

# MART Presença e Absenteísmo Parlamentar

## Visão Geral

O MART `am_presenca_absenteismo` foi desenvolvido para consolidar informações analíticas sobre participação parlamentar em eventos legislativos da Câmara dos Deputados.

A tabela foi construída a partir dos registros de presença parlamentar associados aos eventos legislativos e das dimensões de deputados, partidos, estados, eventos e calendário, permitindo análises de engajamento, participação institucional, comportamento parlamentar e indicadores de absenteísmo.

O MART possui caráter analítico e de monitoramento, permitindo avaliar padrões de participação parlamentar ao longo do tempo.

---

## Objetivo de Negócio

Disponibilizar um ativo analítico capaz de responder aos seguintes temas:

1. Participação parlamentar em eventos legislativos.
2. Indicadores de presença e absenteísmo.
3. Comparação entre parlamentares.
4. Comparação entre partidos políticos.
5. Comparação entre unidades federativas.
6. Evolução temporal do engajamento parlamentar.
7. Identificação de padrões de participação.
8. Indicadores de cobertura e qualidade dos dados.

---

## Fontes de Dados

### Camada Gold

| Tabela                | Finalidade                                  |
| --------------------- | ------------------------------------------- |
| `ft_presenca_eventos` | Registro de presença parlamentar em eventos |
| `dm_deputados`        | Cadastro consolidado de deputados           |
| `dm_partidos`         | Cadastro consolidado de partidos            |
| `dm_estados`          | Cadastro consolidado de estados             |
| `dm_eventos`          | Cadastro consolidado de eventos             |
| `dm_datas`            | Dimensão temporal                           |

---

## Volume Processado

| Indicador                         | Valor    |
| --------------------------------- | -------- |
| Registros de presença processados | Validado |
| Deputados analisados              | Validado |
| Eventos analisados                | Validado |
| Partidos analisados               | Validado |
| UFs analisadas                    | Validado |

---

## Principais Métricas

* Quantidade de eventos com participação parlamentar.
* Quantidade total de presenças.
* Quantidade total de ausências.
* Percentual de presença.
* Percentual de absenteísmo.
* Quantidade de eventos por parlamentar.
* Quantidade de eventos por partido.
* Quantidade de eventos por UF.
* Ranking de participação parlamentar.
* Ranking de absenteísmo.
* Índice de engajamento parlamentar.
* Indicadores de completude dimensional.

---

## Validações Executadas

### Integridade

* Chaves obrigatórias preenchidas.
* Chave substituta gerada corretamente.
* Ausência de duplicidades.
* Cobertura dimensional validada.
* Hash determinístico validado.
* Relacionamentos dimensionais validados.

### Qualidade

* Nenhuma métrica negativa identificada.
* Nenhum percentual fora da faixa esperada.
* Ausência de registros inconsistentes.
* Cobertura Gold validada.
* Regras de negócio validadas.
* Registros aprovados nas validações do MART.

---

## Resultado das Validações

### Integridade

Resultado:

**Aprovado**

### Cobertura Dimensional

Resultado:

**Aprovado**

Cobertura Gold validada.

### Qualidade

Resultado:

**Aprovado**

Nenhuma inconsistência estrutural identificada.

### Auditoria

Resultado:

**Aprovado**

Hashes distintos validados.

Execução rastreável.

---

## Principais Análises

### Participação Parlamentar

Permite identificar:

* Parlamentares mais participativos.
* Parlamentares com menor participação.
* Evolução individual de presença.

### Participação por Partido

Permite identificar:

* Partidos com maior engajamento.
* Distribuição de participação por legenda.
* Comparação entre bancadas.

### Participação por Unidade Federativa

Permite identificar:

* Distribuição geográfica do engajamento parlamentar.
* Estados com maior participação.
* Estados com maior absenteísmo.

### Evolução Temporal

Permite analisar:

* Participação por período.
* Tendências de presença.
* Tendências de absenteísmo.

---

## Resultado Final

**STATUS: APROVADO**

Cobertura Gold: 100%

Integridade: 100%

Qualidade: 100%

Pronto para consumo analítico e exportação CSV.

---

## Artefatos Gerados

### Delta Table

`brazil_legislative_analytics.marts.am_presenca_absenteismo`

### Exportação CSV

`/Volumes/brazil_legislative_analytics/marts/exports/am_presenca_absenteismo/`

---

## Governança

### Grain

Um registro por:

* Deputado
* Evento
* Data

### Chave de Negócio

```text
dep_id_deputado
evt_id_evento
dat_id_data
```

### Chave Substituta

```text
pab_sk_presenca_absenteismo
```

### Versão

```text
marts_v1.0_attendance_monitor
```
