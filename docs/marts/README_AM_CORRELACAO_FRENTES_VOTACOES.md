# Correlação entre Frentes Parlamentares e Votações

## Visão Geral

O Data Mart `am_correlacao_frentes_votacoes` consolida informações analíticas sobre o comportamento das Frentes Parlamentares da Câmara dos Deputados em votações legislativas.

A solução relaciona membros das Frentes Parlamentares com os resultados das votações, permitindo análises de participação, alinhamento político, coesão interna e comportamento de voto.

---

## Objetivos Analíticos

Este Data Mart permite responder perguntas como:

* Como os membros das Frentes votam?
* Qual o nível de coesão interna de cada Frente?
* Existe alinhamento entre o posicionamento da Frente e o resultado final da votação?
* Quais Frentes possuem maior participação nas votações?
* Como os votos se distribuem entre diferentes categorias legislativas?

---

## Fontes de Dados

### Camada Gold

| Tabela                   | Descrição                                              |
| ------------------------ | ------------------------------------------------------ |
| `ft_frentes_membros`     | Relacionamento entre deputados e Frentes Parlamentares |
| `ft_resultados_votacoes` | Resultado individual das votações                      |
| `dm_frentes`             | Cadastro consolidado das Frentes Parlamentares         |
| `dm_votacoes`            | Cadastro consolidado das votações                      |

---

## Principais Indicadores

### Participação

* Quantidade de membros da Frente
* Quantidade de membros votantes
* Quantidade de votos registrados
* Percentual de participação

### Comportamento de Voto

* Quantidade de votos SIM
* Quantidade de votos NÃO
* Quantidade de abstenções
* Quantidade de obstruções
* Quantidade de votos Artigo 17

### Alinhamento

* Percentual de coesão da Frente
* Voto predominante
* Indicador de alinhamento com o resultado da votação

As métricas foram desenvolvidas para suportar análises de comportamento parlamentar e alinhamento político.

---

## Cobertura

| Indicador               |     Valor |
| ----------------------- | --------: |
| Registros publicados    | 1.033.265 |
| Frentes analisadas      |  Validado |
| Votações analisadas     |  Validado |
| Legislaturas analisadas |  Validado |

Resultados obtidos durante a homologação do MART.

---

## Tratamento Especial – Artigo 17

Durante o desenvolvimento foi identificada divergência entre:

```text
Quantidade de votos registrados
```

e

```text
SoMatório das categorias de voto
```

A investigação demonstrou que parte dos registros correspondia à categoria legislativa:

```text
ARTIGO 17
```

Para preservar a consistência dos indicadores foi criado o campo:

```text
cfv_qt_votos_artigo_17
```

A regra de validação passou a considerar:

```text
cfv_qt_votos_registrados =
cfv_qt_votos_sim +
cfv_qt_votos_nao +
cfv_qt_abstencoes +
cfv_qt_obstrucoes +
cfv_qt_votos_artigo_17
```

Resultado:

* Divergência eliminada
* Consistência validada
* Informação legislativa preservada
* Auditoria completa das métricas

---

## Qualidade dos Dados

### Validações Executadas

#### Integridade

* Chaves obrigatórias preenchidas
* Ausência de duplicidades
* Hash determinístico validado
* Cobertura das dimensões Gold validada
* Cobertura das votações validada

#### Qualidade

* Nenhuma métrica negativa identificada
* Nenhum registro inconsistente encontrado
* Consistência dos votos validada
* Registros aprovados nas validações do MART

---

## Resultado das Validações

| Validação                | Resultado |
| ------------------------ | --------- |
| Consistência dos votos   | Aprovado  |
| Registros inconsistentes | 0         |
| Cobertura Gold           | Aprovado  |
| Integridade              | Aprovado  |
| Qualidade                | Aprovado  |

Além disso:

| Indicador                          |   Valor |
| ---------------------------------- | ------: |
| Registros contendo votos Artigo 17 | 222.729 |
| Votos Artigo 17 contabilizados     | 226.102 |

---

## Casos de Uso

* Monitoramento de alinhamento político
* Estudos legislativos
* Análise de comportamento parlamentar
* Avaliação de coesão das Frentes
* Estudos de representatividade
* Auditoria de votações

---

## Governança

### Grain

Um registro por:

* Frente Parlamentar
* Votação
* Legislatura

### Chave de Negócio

```text
frn_id_frente
vot_id_votacao
leg_id_legislatura
```

### Chave Substituta

```text
cfv_sk_correlacao_frente_votacao
```

### Versão

```text
marts_v1.0_front_voting_correlation
```

---

## Artefatos

### Delta Table

```sql
brazil_legislative_analytics.marts.am_correlacao_frentes_votacoes
```

### Exportação CSV

```text
/Volumes/brazil_legislative_analytics/marts/exports/am_correlacao_frentes_votacoes/
```

---

## Status

* Aprovado para consumo analítico

### Resultado da Homologação

| Indicador              | Status   |
| ---------------------- | -------- |
| Cobertura Gold         | Aprovado |
| Consistência dos votos | Aprovado |
| Integridade            | Aprovado |
| Qualidade              | Aprovado |

O MART encontra-se validado e disponível para utilização analítica.
