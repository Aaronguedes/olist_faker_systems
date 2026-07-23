# Descoberta de Business Keys

Este guia documenta completamente a funcionalidade implementada em `04_discover_business_keys.py` para sugerir Business Keys (BKs) em tabelas Bronze do Databricks.

## Objetivo

Uma Business Key Ã© um campo, ou conjunto de campos, que identifica uma entidade segundo o sistema de origem. Alguns exemplos sÃ£o:

- `crm_customer_id` para um cliente do CRM;
- `order_id` para um pedido;
- `order_id + payment_sequential` para uma ocorrÃªncia de pagamento.

O notebook automatiza a parte tÃ©cnica da investigaÃ§Ã£o. Ele mede unicidade, nulos, estabilidade e caracterÃ­sticas do nome das colunas, produzindo uma lista priorizada de candidatas.

O resultado Ã© uma recomendaÃ§Ã£o, nÃ£o uma decisÃ£o automÃ¡tica. Uma coluna tecnicamente Ãºnica pode ser uma surrogate key, pode mudar no futuro ou nÃ£o possuir significado para o negÃ³cio.

## Fluxo da anÃ¡lise

```text
Tabelas Bronze
     |
     v
RemoÃ§Ã£o de metadados e tipos inadequados
     |
     v
Profiling aproximado em lote
     |
     v
SeleÃ§Ã£o de candidatas promissoras
     |
     v
ConfirmaÃ§Ã£o com contagem exata
     |
     v
RemoÃ§Ã£o de superchaves redundantes
     |
     v
Teste de estabilidade entre snapshots
     |
     v
Score, classificaÃ§Ã£o e justificativa
     |
     +--> ExibiÃ§Ã£o no notebook
     |
     +--> Tabela Delta opcional
```

## Como executar

1. Abra `ingestion/04_discover_business_keys.py` no Databricks.
2. Anexe o notebook a um compute com acesso ao Unity Catalog.
3. Confirme os widgets no topo do notebook.
4. Execute todas as cÃ©lulas.
5. Revise o DataFrame `result_df` exibido no final.

Com os valores padrÃ£o, o notebook analisa:

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

## ParÃ¢metros

| Widget | PadrÃ£o | Finalidade |
|---|---|---|
| `catalog` | `lakehouse` | CatÃ¡logo do Unity Catalog que contÃ©m as tabelas. |
| `schema` | `bronze` | Schema que contÃ©m as tabelas. |
| `tables` | oito tabelas separadas por vÃ­rgula | Tabelas que serÃ£o analisadas. |
| `max_columns` | `2` | Tamanho mÃ¡ximo de uma BK composta. Aceita `1`, `2` ou `3`. |
| `uniqueness_threshold` | `0.999` | Taxa mÃ­nima de unicidade para uma candidata ser retornada. |
| `max_candidates` | `10` | Quantidade mÃ¡xima de candidatas retornadas por tabela. |
| `combination_candidate_limit` | `12` | Quantidade mÃ¡xima de colunas usadas para gerar combinaÃ§Ãµes. |
| `aggregation_batch_size` | `50` | Candidatas avaliadas juntas em cada agregaÃ§Ã£o Spark. |
| `approx_rsd` | `0.01` | Erro relativo esperado na triagem com `approx_count_distinct`. |
| `latest_snapshot_only` | `true` | Analisa a unicidade no snapshot mais recente. |
| `persist_results` | `false` | Define se o resultado serÃ¡ gravado em Delta. |
| `results_table` | `lakehouse.metadata.business_key_candidates` | Tabela de destino das sugestÃµes persistidas. |
| `use_ai_interpretation` | `true` | Executa uma interpretaÃ§Ã£o final com `ai_query()`, uma vez por tabela. |
| `ai_model_endpoint` | `databricks-gpt-5-mini` | Endpoint de Model Serving usado pelo `ai_query()`. |
| `persist_ai_results` | `false` | Grava as recomendaÃ§Ãµes da IA em uma tabela Delta. |
| `ai_results_table` | `lakehouse.metadata.business_key_recommendations` | Destino opcional das recomendaÃ§Ãµes da IA. |

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

Isso evita qualquer combinaÃ§Ã£o e procura somente campos como `order_id` e `erp_customer_id`.

### Exemplo: aceitar somente unicidade perfeita

```text
uniqueness_threshold = 1.0
```

Com `0.999`, uma coluna com 99,9% de valores distintos ainda pode aparecer para revisÃ£o. Com `1.0`, somente candidatas completamente Ãºnicas passam.

## Colunas e tipos excluÃ­dos

Metadados de ingestÃ£o nÃ£o podem virar BK. Por isso, o notebook exclui campos como:

```text
_source_file
_source_date
_ingested_at
_rescued_data
batch_id
record_source
```

TambÃ©m sÃ£o excluÃ­dos tipos normalmente inadequados para identificaÃ§Ã£o:

- `boolean`;
- `float` e `double`;
- `array`;
- `map`;
- `struct`.

Um `double` nÃ£o Ã© recomendado por causa da representaÃ§Ã£o de ponto flutuante. Um `boolean`, por possuir apenas dois valores, nÃ£o consegue identificar entidades em uma tabela comum.

## EstratÃ©gia de performance

### 1. Profiling simples em lote

Em vez de executar uma leitura Spark para cada coluna, o notebook agrupa vÃ¡rias expressÃµes na mesma agregaÃ§Ã£o.

Conceitualmente:

```python
df.agg(
    approx_count_distinct("customer_id"),
    count_nulls("customer_id"),
    approx_count_distinct("email"),
    count_nulls("email"),
)
```

Isso reduz o nÃºmero de scans sobre a Bronze.

### 2. Triagem aproximada

`approx_count_distinct` Ã© usado para eliminar rapidamente colunas com baixa cardinalidade. Uma margem de tolerÃ¢ncia evita descartar candidatas prÃ³ximas ao limite.

Os valores aproximados nunca sÃ£o publicados como resultado final.

### 3. ConfirmaÃ§Ã£o exata

As candidatas aprovadas na triagem sÃ£o recalculadas com `countDistinct`. As colunas `distinct_values` e `uniqueness_ratio` exibidas sÃ£o, portanto, mÃ©tricas exatas.

### 4. Controle de combinaÃ§Ãµes

O total de combinaÃ§Ãµes cresce rapidamente. Com 30 colunas existem 435 pares e 4.060 trios.

`combination_candidate_limit=12` limita a geraÃ§Ã£o Ã s 12 colunas mais promissoras. Com 12 colunas existem apenas 66 pares.

### 5. Estabilidade apenas para finalistas

O histÃ³rico por `_source_date` Ã© avaliado somente depois que as candidatas ruins e redundantes foram removidas. Todas as finalistas sÃ£o medidas na mesma agregaÃ§Ã£o por snapshot.

## Unicidade

A taxa de unicidade Ã© calculada como:

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

Essa candidata passa quando o threshold Ã© `0.999`, mas requer revisÃ£o porque possui valores repetidos.

## Estabilidade entre snapshots

Uma BK nÃ£o deve ser vÃ¡lida somente em um dia. O notebook agrupa os registros por `_source_date` e verifica em quantos snapshots a candidata atende ao threshold e nÃ£o contÃ©m nulos.

```text
Snapshots analisados: 4
Snapshots vÃ¡lidos:    3
stability_ratio:      0,75
```

Uma candidata com `stability_ratio=1.0` foi vÃ¡lida em todos os snapshots disponÃ­veis.

Se a tabela nÃ£o possui `_source_date`, `stability_ratio` fica nulo e a estabilidade nÃ£o participa do score.

## Score

O score ordena as candidatas que devem ser revisadas primeiro. Ele nÃ£o Ã© probabilidade nem confirmaÃ§Ã£o de BK.

```text
score = uniqueness_ratio * 100
      + stability_ratio * 20
      + identifier_name_count * 10
      - key_size * 5
```

O componente de estabilidade Ã© omitido quando nÃ£o hÃ¡ snapshots.

### Exemplo 1: chave simples forte

Para `loyalty_customer_id`:

```text
100 pontos: unicidade de 100%
 20 pontos: estabilidade de 100%
 10 pontos: nome contÃ©m _id
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

### Exemplo 3: atributo mutÃ¡vel

Um `email` poderia ser Ãºnico no snapshot atual, mas vÃ¡lido em apenas 75% dos snapshots:

```text
100 pontos: unicidade atual de 100%
 15 pontos: estabilidade de 75%
 10 pontos: nome reconhecido como identificador
 -5 pontos: uma coluna
--------------------------------
120 pontos
```

O score ainda pode ser alto, mas a classificaÃ§Ã£o e a justificativa mostram a instabilidade. A revisÃ£o de negÃ³cio deve rejeitar `email` como BK quando ele puder mudar.

## ClassificaÃ§Ãµes

| ClassificaÃ§Ã£o | Significado |
|---|---|
| `STRONG` | 100% Ãºnica, sem nulos e estÃ¡vel em todos os snapshots disponÃ­veis. |
| `REVIEW` | PrÃ³xima da unicidade, mas possui duplicidade ou instabilidade que exige anÃ¡lise. |
| `WEAK` | NÃ£o atende aos critÃ©rios fortes. Normalmente Ã© removida pelo threshold antes da saÃ­da. |

Mesmo uma candidata `STRONG` precisa ser validada com o responsÃ¡vel pelo sistema de origem.

## Chaves mÃ­nimas e superchaves

Se `loyalty_customer_id` jÃ¡ Ã© Ãºnico, estas combinaÃ§Ãµes tambÃ©m serÃ£o Ãºnicas:

```text
loyalty_customer_id + tier
loyalty_customer_id + points
loyalty_customer_id + active_member
```

Essas combinaÃ§Ãµes sÃ£o superchaves redundantes. O notebook as remove porque a primeira coluna jÃ¡ identifica o registro sozinha.

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

InterpretaÃ§Ã£o:

- `order_id` Ã© a primeira candidata para ERP Orders;
- a ocorrÃªncia de pagamento necessita de uma combinaÃ§Ã£o;
- `email` deve ser analisado manualmente por ser mutÃ¡vel entre snapshots.

## PersistÃªncia em Delta

Para gravar os resultados:

```text
persist_results = true
results_table = lakehouse.metadata.business_key_candidates
```

O notebook cria o schema de destino quando necessÃ¡rio e acrescenta uma nova execuÃ§Ã£o com `mode("append")`.

Cada execuÃ§Ã£o recebe um `analysis_id`, permitindo consultar um resultado especÃ­fico:

```sql
SELECT *
FROM lakehouse.metadata.business_key_candidates
WHERE analysis_id = '56d681da-36d5-4e18-99f6-012ab50df17c'
ORDER BY table, rank;
```

## InterpretaÃ§Ã£o final com `ai_query()`

Quando `use_ai_interpretation=true`, o notebook agrupa todas as candidatas de uma tabela e envia apenas metadados para o modelo. Valores reais de CPF, e-mail, nome ou outros atributos nÃ£o sÃ£o enviados.

Ã‰ feita uma chamada por tabela. A IA recebe:

- nomes das colunas candidatas;
- unicidade e nulos;
- estabilidade entre snapshots;
- classificaÃ§Ã£o, score e justificativa determinÃ­stica.

O prompt instrui o modelo a favorecer identificadores locais, rejeitar atributos mutÃ¡veis, nÃ£o inventar colunas e retornar `NEEDS_REVIEW` quando nÃ£o houver candidata adequada.

O `responseFormat` usa um Ãºnico campo no nÃ­vel superior, conforme exigido pelo formato DDL do Azure Databricks:

```text
STRUCT<bk_recommendation:STRUCT<
  recommended_columns:ARRAY<STRING>,
  decision:STRING,
  confidence:STRING,
  explanation:STRING,
  warnings:ARRAY<STRING>
>>
```

Com `failOnError=false`, o Azure Databricks Runtime utilizado pelo projeto retorna um struct com os campos `result` e `errorMessage`. Nesse runtime, `result` Ã© uma string JSON contendo diretamente os campos internos (`recommended_columns`, `decision`, `confidence`, `explanation` e `warnings`), sem o wrapper `bk_recommendation`. O notebook mantÃ©m o wrapper no `responseFormat`, onde ele Ã© obrigatÃ³rio, mas aplica `from_json(result, ai_result_schema)` usando um schema plano. Erros do endpoint ficam em `ai_query_result.errorMessage`; formatos inesperados sÃ£o registrados em `ai_parse_error`.

A saÃ­da final Ã© `final_bk_df`:

| Coluna | Exemplo | Finalidade |
|---|---|---|
| `recommended_bk` | `["order_id"]` | BK recomendada pela IA. |
| `ai_decision` | `RECOMMENDED` | DecisÃ£o estruturada retornada pelo modelo. |
| `confidence` | `HIGH` | ConfianÃ§a qualitativa, nÃ£o uma probabilidade calibrada. |
| `explanation` | `order_id is the source order identifier...` | Justificativa da seleÃ§Ã£o. |
| `warnings` | `["Only one snapshot was available"]` | Riscos que exigem revisÃ£o. |
| `ai_error` | `null` | Erro retornado pelo endpoint sem interromper outras tabelas. |
| `status` | `AI_RECOMMENDED` | Estado de governanÃ§a produzido pelo notebook. |

Tabelas sem candidatas tambÃ©m sÃ£o enviadas com uma lista vazia. Nesse caso, o modelo deve retornar:

```text
recommended_bk = []
status = NEEDS_REVIEW
```

Para persistir:

```text
persist_ai_results = true
ai_results_table = lakehouse.metadata.business_key_recommendations
```

O fluxo recomendado continua exigindo confirmaÃ§Ã£o humana:

```text
SUGGESTED -> AI_RECOMMENDED -> HUMAN_CONFIRMED
```

Nunca crie automaticamente Hubs do Data Vault usando apenas `AI_RECOMMENDED`.

### Requisitos do Databricks

- endpoint de Model Serving disponÃ­vel no workspace;
- permissÃ£o para consultar o endpoint;
- compute Serverless e runtime compatÃ­vel com `ai_query()` e structured output;
- regiÃ£o com suporte ao Model Serving utilizado.

O endpoint pode ser substituÃ­do pelo widget `ai_model_endpoint` sem alterar o cÃ³digo.

### Consultar somente sugestÃµes fortes

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

### Rejeitar um atributo mutÃ¡vel

```sql
UPDATE lakehouse.metadata.business_key_candidates
SET status = 'REJECTED'
WHERE table = 'crm_customers'
  AND columns = array('email');
```

## Ajuste de performance

Para uma tabela estreita e dados crÃ­ticos:

```text
combination_candidate_limit = 15
aggregation_batch_size = 50
approx_rsd = 0.005
```

Isso amplia a busca e aumenta a precisÃ£o da triagem, consumindo mais recursos.

Para uma tabela larga e exploraÃ§Ã£o inicial:

```text
combination_candidate_limit = 8
aggregation_batch_size = 75
approx_rsd = 0.02
```

Isso reduz combinaÃ§Ãµes e privilegia velocidade.

Evite `max_columns=3` em tabelas largas sem reduzir `combination_candidate_limit`.

## LimitaÃ§Ãµes

- Unicidade tÃ©cnica nÃ£o prova significado de negÃ³cio.
- Uma UUID pode ser uma surrogate key e nÃ£o uma BK.
- O histÃ³rico depende da qualidade de `_source_date`.
- Uma BK vÃ¡lida pode conter nulos por erro de qualidade e ser eliminada.
- O limite de colunas para combinaÃ§Ãµes pode excluir uma composiÃ§Ã£o inesperada.
- O notebook nÃ£o detecta automaticamente se um atributo Ã© legalmente sensÃ­vel.
- O algoritmo nÃ£o substitui catÃ¡logo de dados, data owner ou documentaÃ§Ã£o da fonte.

## RecomendaÃ§Ãµes de governanÃ§a

1. Execute o notebook para gerar sugestÃµes.
2. Revise as candidatas `STRONG` e `REVIEW`.
3. Confirme a semÃ¢ntica com o proprietÃ¡rio do sistema.
4. Registre `CONFIRMED` ou `REJECTED`.
5. Use somente BKs confirmadas nos Hubs do Data Vault.
6. Reavalie as chaves quando o schema ou comportamento da fonte mudar.

## Troubleshooting

### Nenhuma candidata foi encontrada

PossÃ­veis causas:

- threshold alto demais;
- colunas com nulos;
- chave composta maior que `max_columns`;
- colunas necessÃ¡rias ficaram fora de `combination_candidate_limit`.

Teste:

```text
uniqueness_threshold = 0.99
max_columns = 2
combination_candidate_limit = 20
```

Use esse ajuste apenas para investigaÃ§Ã£o e confirme os resultados depois.

### ExecuÃ§Ã£o demorada

- reduza `combination_candidate_limit`;
- mantenha `max_columns=2`;
- aumente moderadamente `aggregation_batch_size`;
- analise menos tabelas por execuÃ§Ã£o;
- mantenha `latest_snapshot_only=true`.

### Resultado contÃ©m uma surrogate key

O algoritmo nÃ£o consegue confirmar a semÃ¢ntica. Marque a candidata como `REJECTED` e documente a BK correta com o data owner.

### Stability ratio inesperadamente baixo

Verifique se `_source_date` representa a data lÃ³gica da carga e se um mesmo snapshot nÃ£o foi carregado parcialmente ou duplicado.

## ReferÃªncia das colunas de saÃ­da

Consulte `BUSINESS_KEY_DISCOVERY_DATA_DICTIONARY.md` para o tipo, descriÃ§Ã£o e exemplo de cada coluna produzida pelo notebook.

