# Descoberta de Business Keys

Este guia documenta completamente a funcionalidade implementada em `04_discover_business_keys.py` para sugerir Business Keys (BKs) em tabelas Bronze do Databricks.

## Objetivo

Uma Business Key é um campo, ou conjunto de campos, que identifica uma entidade segundo o sistema de origem. Alguns exemplos são:

- `crm_customer_id` para um cliente do CRM;
- `order_id` para um pedido;
- `order_id + payment_sequential` para uma ocorrência de pagamento.

O notebook automatiza a parte técnica da investigação. Ele mede unicidade, nulos, estabilidade e características do nome das colunas, produzindo uma lista priorizada de candidatas.

O resultado é uma recomendação, não uma decisão automática. Uma coluna tecnicamente única pode ser uma surrogate key, pode mudar no futuro ou não possuir significado para o negócio.

## Fluxo da análise

```text
Tabelas Bronze
     |
     v
Remoção de metadados e tipos inadequados
     |
     v
Profiling aproximado em lote
     |
     v
Seleção de candidatas promissoras
     |
     v
Confirmação com contagem exata
     |
     v
Remoção de superchaves redundantes
     |
     v
Teste de estabilidade entre snapshots
     |
     v
Score, classificação e justificativa
     |
     +--> Exibição no notebook
     |
     +--> Tabela Delta opcional
```

## Como executar

1. Abra `ingestion/04_discover_business_keys.py` no Databricks.
2. Anexe o notebook a um compute com acesso ao Unity Catalog.
3. Confirme os widgets no topo do notebook.
4. Execute todas as células.
5. Revise o DataFrame `result_df` exibido no final.

Com os valores padrão, o notebook analisa:

```text
lakehouse.bronze.crm_customers
lakehouse.bronze.ecommerce_customers
lakehouse.bronze.ecommerce_products
lakehouse.bronze.ecommerce_sellers
lakehouse.bronze.erp_customers
lakehouse.bronze.erp_orders
lakehouse.bronze.erp_payments
lakehouse.bronze.loyalty_customers
```

## Parâmetros

| Widget | Padrão | Finalidade |
|---|---|---|
| `catalog` | `lakehouse` | Catálogo do Unity Catalog que contém as tabelas. |
| `schema` | `bronze` | Schema que contém as tabelas. |
| `tables` | oito tabelas separadas por vírgula | Tabelas que serão analisadas. |
| `max_columns` | `2` | Tamanho máximo de uma BK composta. Aceita `1`, `2` ou `3`. |
| `uniqueness_threshold` | `0.999` | Taxa mínima de unicidade para uma candidata ser retornada. |
| `max_candidates` | `10` | Quantidade máxima de candidatas retornadas por tabela. |
| `combination_candidate_limit` | `12` | Quantidade máxima de colunas usadas para gerar combinações. |
| `aggregation_batch_size` | `50` | Candidatas avaliadas juntas em cada agregação Spark. |
| `approx_rsd` | `0.01` | Erro relativo esperado na triagem com `approx_count_distinct`. |
| `latest_snapshot_only` | `true` | Analisa a unicidade no snapshot mais recente. |
| `persist_results` | `false` | Define se o resultado será gravado em Delta. |
| `results_table` | `lakehouse.metadata.business_key_candidates` | Tabela de destino das sugestões persistidas. |

### Exemplo: analisar somente ERP

Configure:

```text
catalog = lakehouse
schema = bronze
tables = erp_customers,erp_orders,erp_payments
max_columns = 2
```

### Exemplo: procurar apenas chaves simples

```text
max_columns = 1
```

Isso evita qualquer combinação e procura somente campos como `order_id` e `erp_customer_id`.

### Exemplo: aceitar somente unicidade perfeita

```text
uniqueness_threshold = 1.0
```

Com `0.999`, uma coluna com 99,9% de valores distintos ainda pode aparecer para revisão. Com `1.0`, somente candidatas completamente únicas passam.

## Colunas e tipos excluídos

Metadados de ingestão não podem virar BK. Por isso, o notebook exclui campos como:

```text
_source_file
_source_date
_ingested_at
_rescued_data
batch_id
record_source
```

Também são excluídos tipos normalmente inadequados para identificação:

- `boolean`;
- `float` e `double`;
- `array`;
- `map`;
- `struct`.

Um `double` não é recomendado por causa da representação de ponto flutuante. Um `boolean`, por possuir apenas dois valores, não consegue identificar entidades em uma tabela comum.

## Estratégia de performance

### 1. Profiling simples em lote

Em vez de executar uma leitura Spark para cada coluna, o notebook agrupa várias expressões na mesma agregação.

Conceitualmente:

```python
df.agg(
    approx_count_distinct("customer_id"),
    count_nulls("customer_id"),
    approx_count_distinct("email"),
    count_nulls("email"),
)
```

Isso reduz o número de scans sobre a Bronze.

### 2. Triagem aproximada

`approx_count_distinct` é usado para eliminar rapidamente colunas com baixa cardinalidade. Uma margem de tolerância evita descartar candidatas próximas ao limite.

Os valores aproximados nunca são publicados como resultado final.

### 3. Confirmação exata

As candidatas aprovadas na triagem são recalculadas com `countDistinct`. As colunas `distinct_values` e `uniqueness_ratio` exibidas são, portanto, métricas exatas.

### 4. Controle de combinações

O total de combinações cresce rapidamente. Com 30 colunas existem 435 pares e 4.060 trios.

`combination_candidate_limit=12` limita a geração às 12 colunas mais promissoras. Com 12 colunas existem apenas 66 pares.

### 5. Estabilidade apenas para finalistas

O histórico por `_source_date` é avaliado somente depois que as candidatas ruins e redundantes foram removidas. Todas as finalistas são medidas na mesma agregação por snapshot.

## Unicidade

A taxa de unicidade é calculada como:

```text
uniqueness_ratio = distinct_values / total_rows
```

Exemplo:

```text
total_rows       = 100.000
distinct_values  = 100.000
uniqueness_ratio = 1,0
```

Isso indica 100% de unicidade.

Outro exemplo:

```text
total_rows       = 100.000
distinct_values  = 99.950
uniqueness_ratio = 0,9995
```

Essa candidata passa quando o threshold é `0.999`, mas requer revisão porque possui valores repetidos.

## Estabilidade entre snapshots

Uma BK não deve ser válida somente em um dia. O notebook agrupa os registros por `_source_date` e verifica em quantos snapshots a candidata atende ao threshold e não contém nulos.

```text
Snapshots analisados: 4
Snapshots válidos:    3
stability_ratio:      0,75
```

Uma candidata com `stability_ratio=1.0` foi válida em todos os snapshots disponíveis.

Se a tabela não possui `_source_date`, `stability_ratio` fica nulo e a estabilidade não participa do score.

## Score

O score ordena as candidatas que devem ser revisadas primeiro. Ele não é probabilidade nem confirmação de BK.

```text
score = uniqueness_ratio * 100
      + stability_ratio * 20
      + identifier_name_count * 10
      - key_size * 5
```

O componente de estabilidade é omitido quando não há snapshots.

### Exemplo 1: chave simples forte

Para `loyalty_customer_id`:

```text
100 pontos: unicidade de 100%
 20 pontos: estabilidade de 100%
 10 pontos: nome contém _id
 -5 pontos: chave com uma coluna
--------------------------------
125 pontos
```

### Exemplo 2: chave composta

Para `order_id + payment_sequential`:

```text
100 pontos: unicidade de 100%
 20 pontos: estabilidade de 100%
 10 pontos: order_id possui nome de identificador
-10 pontos: chave com duas colunas
--------------------------------
120 pontos
```

### Exemplo 3: atributo mutável

Um `email` poderia ser único no snapshot atual, mas válido em apenas 75% dos snapshots:

```text
100 pontos: unicidade atual de 100%
 15 pontos: estabilidade de 75%
 10 pontos: nome reconhecido como identificador
 -5 pontos: uma coluna
--------------------------------
120 pontos
```

O score ainda pode ser alto, mas a classificação e a justificativa mostram a instabilidade. A revisão de negócio deve rejeitar `email` como BK quando ele puder mudar.

## Classificações

| Classificação | Significado |
|---|---|
| `STRONG` | 100% única, sem nulos e estável em todos os snapshots disponíveis. |
| `REVIEW` | Próxima da unicidade, mas possui duplicidade ou instabilidade que exige análise. |
| `WEAK` | Não atende aos critérios fortes. Normalmente é removida pelo threshold antes da saída. |

Mesmo uma candidata `STRONG` precisa ser validada com o responsável pelo sistema de origem.

## Chaves mínimas e superchaves

Se `loyalty_customer_id` já é único, estas combinações também serão únicas:

```text
loyalty_customer_id + tier
loyalty_customer_id + points
loyalty_customer_id + active_member
```

Essas combinações são superchaves redundantes. O notebook as remove porque a primeira coluna já identifica o registro sozinha.

O resultado preserva chaves compostas quando nenhum subconjunto perfeito existe. Por exemplo:

```text
order_id + payment_sequential
```

## Exemplo completo de resultado

| table | rank | columns | uniqueness_ratio | stability_ratio | classification | score |
|---|---:|---|---:|---:|---|---:|
| `erp_orders` | 1 | `["order_id"]` | 1.0 | 1.0 | `STRONG` | 125.0 |
| `erp_payments` | 1 | `["order_id", "payment_sequential"]` | 1.0 | 1.0 | `STRONG` | 120.0 |
| `crm_customers` | 2 | `["email"]` | 1.0 | 0.75 | `REVIEW` | 120.0 |

Interpretação:

- `order_id` é a primeira candidata para ERP Orders;
- a ocorrência de pagamento necessita de uma combinação;
- `email` deve ser analisado manualmente por ser mutável entre snapshots.

## Persistência em Delta

Para gravar os resultados:

```text
persist_results = true
results_table = lakehouse.metadata.business_key_candidates
```

O notebook cria o schema de destino quando necessário e acrescenta uma nova execução com `mode("append")`.

Cada execução recebe um `analysis_id`, permitindo consultar um resultado específico:

```sql
SELECT *
FROM lakehouse.metadata.business_key_candidates
WHERE analysis_id = '56d681da-36d5-4e18-99f6-012ab50df17c'
ORDER BY table, rank;
```

### Consultar somente sugestões fortes

```sql
SELECT
    table,
    columns,
    score,
    reason
FROM lakehouse.metadata.business_key_candidates
WHERE classification = 'STRONG'
ORDER BY table, score DESC;
```

### Confirmar uma BK

```sql
UPDATE lakehouse.metadata.business_key_candidates
SET status = 'CONFIRMED'
WHERE table = 'erp_orders'
  AND columns = array('order_id');
```

### Rejeitar um atributo mutável

```sql
UPDATE lakehouse.metadata.business_key_candidates
SET status = 'REJECTED'
WHERE table = 'crm_customers'
  AND columns = array('email');
```

## Ajuste de performance

Para uma tabela estreita e dados críticos:

```text
combination_candidate_limit = 15
aggregation_batch_size = 50
approx_rsd = 0.005
```

Isso amplia a busca e aumenta a precisão da triagem, consumindo mais recursos.

Para uma tabela larga e exploração inicial:

```text
combination_candidate_limit = 8
aggregation_batch_size = 75
approx_rsd = 0.02
```

Isso reduz combinações e privilegia velocidade.

Evite `max_columns=3` em tabelas largas sem reduzir `combination_candidate_limit`.

## Limitações

- Unicidade técnica não prova significado de negócio.
- Uma UUID pode ser uma surrogate key e não uma BK.
- O histórico depende da qualidade de `_source_date`.
- Uma BK válida pode conter nulos por erro de qualidade e ser eliminada.
- O limite de colunas para combinações pode excluir uma composição inesperada.
- O notebook não detecta automaticamente se um atributo é legalmente sensível.
- O algoritmo não substitui catálogo de dados, data owner ou documentação da fonte.

## Recomendações de governança

1. Execute o notebook para gerar sugestões.
2. Revise as candidatas `STRONG` e `REVIEW`.
3. Confirme a semântica com o proprietário do sistema.
4. Registre `CONFIRMED` ou `REJECTED`.
5. Use somente BKs confirmadas nos Hubs do Data Vault.
6. Reavalie as chaves quando o schema ou comportamento da fonte mudar.

## Troubleshooting

### Nenhuma candidata foi encontrada

Possíveis causas:

- threshold alto demais;
- colunas com nulos;
- chave composta maior que `max_columns`;
- colunas necessárias ficaram fora de `combination_candidate_limit`.

Teste:

```text
uniqueness_threshold = 0.99
max_columns = 2
combination_candidate_limit = 20
```

Use esse ajuste apenas para investigação e confirme os resultados depois.

### Execução demorada

- reduza `combination_candidate_limit`;
- mantenha `max_columns=2`;
- aumente moderadamente `aggregation_batch_size`;
- analise menos tabelas por execução;
- mantenha `latest_snapshot_only=true`.

### Resultado contém uma surrogate key

O algoritmo não consegue confirmar a semântica. Marque a candidata como `REJECTED` e documente a BK correta com o data owner.

### Stability ratio inesperadamente baixo

Verifique se `_source_date` representa a data lógica da carga e se um mesmo snapshot não foi carregado parcialmente ou duplicado.

## Referência das colunas de saída

Consulte `BUSINESS_KEY_DISCOVERY_DATA_DICTIONARY.md` para o tipo, descrição e exemplo de cada coluna produzida pelo notebook.

