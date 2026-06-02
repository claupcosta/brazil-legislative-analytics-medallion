# Atlas das Frentes Parlamentares

## Visão Geral

O Data Mart `am_atlas_frentes` consolida informações analíticas sobre as Frentes Parlamentares da Câmara dos Deputados.

O objetivo é fornecer uma visão integrada da composição das frentes, representatividade parlamentar, diversidade partidária e distribuição geográfica.

---

## Objetivos Analíticos

Este Data Mart permite responder perguntas como:

* Quantas Frentes Parlamentares existem?
* Qual a distribuição partidária das Frentes?
* Quais estados possuem maior participação?
* Quais Frentes possuem maior representatividade?
* Como está distribuída a liderança parlamentar?

---

## Fontes de Dados

### Camada Gold

| Tabela               | Descrição                                      |
| -------------------- | ---------------------------------------------- |
| `dm_frentes`         | Cadastro consolidado das Frentes Parlamentares |
| `ft_frentes_membros` | Relacionamento entre parlamentares e frentes   |

---

## Principais Indicadores

* Quantidade de membros por Frente
* Quantidade de partidos representados
* Quantidade de estados representados
* Partido predominante
* UF predominante
* Quantidade de lideranças
* Ranking de representatividade

---

## Cobertura

| Indicador                 | Valor   |
| ------------------------- | ------- |
| Frentes Parlamentares     | 1.442   |
| Relacionamentos agregados | 262.044 |

---

## Governança

### Grain

Um registro por Frente Parlamentar.

### Camada

Mat (Analytical Mart)

### Origem

Gold Layer

---

## Casos de Uso

* Atlas das Frentes Parlamentares
* Diversidade política
* Representatividade regional
* Estudos legislativos
* Monitoramento institucional

---

## Artefatos

### Delta Table

```sql
brazil_legislative_analytics.marts.am_atlas_frentes
```

### Exportação

```text
/Volumes/brazil_legislative_analytics/marts/exports/am_atlas_frentes/
```

---

## Status

✅ Aprovado para consumo analítico.
