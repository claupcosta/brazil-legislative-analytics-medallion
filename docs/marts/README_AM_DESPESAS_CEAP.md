# Panorama de Despesas CEAP

## Visão Geral

O Data Mart `am_despesas_ceap` consolida informações analíticas sobre despesas da Cota para Exercício da Atividade Parlamentar (CEAP) da Câmara dos Deputados.

A solução foi desenvolvida para permitir análises financeiras, operacionais e de governança sobre os gastos parlamentares, oferecendo uma visão integrada por deputado, fornecedor, partido, estado e período.

---

## Objetivos Analíticos

Este Data Mart permite responder perguntas como:

* Quanto cada parlamentar gasta ao longo do tempo?
* Quais fornecedores recebem mais recursos?
* Quais categorias concentram maiores despesas?
* Como os gastos se distribuem geograficamente?
* Existem padrões atípicos de despesa?
* Quais despesas possuem glosas ou restituições?

---

## Fontes de Dados

### Camada Gold

| Tabela             | Descrição                                   |
| ------------------ | ------------------------------------------- |
| `ft_despesas_ceap` | Fato consolidada das despesas parlamentares |
| `dm_deputados`     | Cadastro parlamentar                        |
| `dm_fornecedores`  | Cadastro de fornecedores                    |
| `dm_partidos`      | Cadastro partidário                         |
| `dm_estados`       | Cadastro de estados                         |
| `dm_datas`         | Dimensão temporal                           |

---

## Principais Indicadores

### Financeiros

* Quantidade de despesas
* Valor total liquidado
* Valor médio por despesa
* Valor máximo por despesa
* Valor total por parlamentar
* Valor total por fornecedor

### Governança

* Percentual de glosa
* Percentual de restituição
* Cobertura dimensional
* Qualidade dos relacionamentos

### Analíticos

* Ranking de despesas por período
* Ranking de fornecedores
* Distribuição por categoria
* Identificação de anomalias estatísticas

---

## Cobertura

| Indicador              |    Valor |
| ---------------------- | -------: |
| Registros processados  |  652.195 |
| Deputados distintos    | Validado |
| Fornecedores distintos | Validado |
| Categorias de despesa  | Validado |

Resultados obtidos durante a homologação do MART.

---

## Detecção de Anomalias

O MART suporta análises estatísticas para identificação de comportamentos atípicos em despesas parlamentares.

### Casos de Uso

* Gastos acima do padrão histórico
* Concentração excessiva em fornecedores
* Variações abruptas de despesas
* Monitoramento de comportamento financeiro

### Método Utilizado

* Estatística descritiva
* Z-Score

A metodologia está alinhada à estratégia de qualidade e governança da plataforma.

---

## Qualidade dos Dados

### Validações Executadas

#### Integridade

* Chave substituta validada
* Hash determinístico validado
* Ausência de duplicidades
* Cobertura da camada Gold validada
* Regras de negócio validadas

#### Qualidade

* 100% dos registros válidos publicados
* Nenhuma inconsistência estrutural identificada
* Relacionamentos dimensionais validados

---

## Ressalvas Conhecidas

### Fornecedores sem Correspondência

Foram identificados:

```text
45.658 registros
```

sem correspondência na dimensão de fornecedores.

A investigação demonstrou que esses registros são originários principalmente da categoria:

```text
PASSAGEM AÉREA - SIGEPA
```

com fornecedores como:

* TAM
* GOL
* AZUL

registrados sem CNPJ ou CPF válido na origem. Esta condição não representa falha do MART.

---

### Datas sem Correspondência

Foram identificados:

```text
4 registros
```

sem correspondência na dimensão de datas.

Composição:

* 3 registros de telefonia sem data de emissão informada
* 1 registro com data inconsistente na origem

A inconsistência já existia na camada Gold e foi preservada para fins de rastreabilidade.

---

## Casos de Uso

* Transparência pública
* Governança financeira
* Auditoria parlamentar
* Monitoramento de fornecedores
* Controle de gastos
* Estudos legislativos
* Detecção de anomalias

---

## Governança

### Grain

Um registro por despesa parlamentar.

### Camada

Mat (Analytical Mart)

### Origem

Gold Layer

### Rastreabilidade

O MART mantém rastreabilidade completa desde a origem até a publicação analítica.

---

## Artefatos

### Delta Table

```sql
brazil_legislative_analytics.marts.am_despesas_ceap
```

### Exportação CSV

```text
/Volumes/brazil_legislative_analytics/marts/exports/am_despesas_ceap/
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

O MART encontra-se validado e disponível para análises financeiras e de governança.
