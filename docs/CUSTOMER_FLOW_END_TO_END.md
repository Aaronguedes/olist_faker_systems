# Fluxo de Customer de ponta a ponta

Este documento acompanha uma cliente fictÃ­cia, Ana Silva, desde os arquivos de
origem atÃ© a camada Silver em Raw Data Vault.

## VisÃ£o geral

```mermaid
flowchart LR
    A[Landing Parquet] --> B[Bronze]
    B --> C[Hub Customer]
    B --> D[Customer Satellites]
    C --> D
    B --> E[Same-As Link]
    C --> E
    B --> F[Customer-Order Link]
    C --> F
```

O fluxo tem trÃªs objetivos:

1. preservar os dados recebidos de cada sistema;
2. manter a identidade e o histÃ³rico de cada cliente;
3. registrar quando identidades diferentes representam a mesma pessoa.

---

## 1. Dados recebidos na Landing

Ana existe em trÃªs sistemas, mas cada sistema usa um ID diferente.

### CRM

Arquivo:

```text
vault/crm/customers/2026/07/27/customers.parquet
```

| crm_customer_id | full_name | cpf | email | city | status |
|---|---|---|---|---|---|
| `CRM-100` | Ana Silva | `12345678900` | `ana@email.com` | SÃ£o Paulo | ACTIVE |

### ERP

Arquivo:

```text
vault/erp/customers/2026/07/27/customers.parquet
```

| erp_customer_id | customer_name | cpf | invoice_email | credit_limit |
|---|---|---|---|---:|
| `ERP-900` | ANA SILVA | `12345678900` | `ana@email.com` | 5000.00 |

### E-commerce

Arquivo:

```text
vault/ecommerce/customers/2026/07/27/customers.parquet
```

| ecommerce_customer_id | display_name | email_address | newsletter_optin |
|---|---|---|---|
| `ECOM-300` | Ana | `ana@email.com` | true |

Nesse momento existem trÃªs IDs:

```text
CRM-100
ERP-900
ECOM-300
```

Os arquivos nÃ£o possuem uma chave global dizendo que os trÃªs registros sÃ£o da
mesma pessoa.

---

## 2. IngestÃ£o para a Bronze

O Auto Loader executa uma task para cada fonte:

```text
crm_customers
erp_customers
ecommerce_customers
```

Ele copia os dados para tabelas Delta e adiciona metadados tÃ©cnicos.

### Linha em `bronze.crm_customers`

| crm_customer_id | full_name | cpf | city | status | _source_system | _source_date | _ingested_at |
|---|---|---|---|---|---|---|---|
| `CRM-100` | Ana Silva | `12345678900` | SÃ£o Paulo | ACTIVE | crm | 2026-07-27 | 2026-07-27 20:00 |

### Linha em `bronze.erp_customers`

| erp_customer_id | customer_name | cpf | credit_limit | _source_system | _source_date | _ingested_at |
|---|---|---|---:|---|---|---|
| `ERP-900` | ANA SILVA | `12345678900` | 5000.00 | erp | 2026-07-27 | 2026-07-27 20:02 |

### Linha em `bronze.ecommerce_customers`

| ecommerce_customer_id | display_name | email_address | newsletter_optin | _source_system | _source_date | _ingested_at |
|---|---|---|---|---|---|---|
| `ECOM-300` | Ana | `ana@email.com` | true | ecommerce | 2026-07-27 | 2026-07-27 20:04 |

A Bronze:

- preserva as colunas da origem;
- nÃ£o junta clientes;
- nÃ£o escolhe o melhor nome ou e-mail;
- registra arquivo, sistema e momento da ingestÃ£o;
- permite reprocessar apenas arquivos novos atravÃ©s do checkpoint.

---

## 3. CriaÃ§Ã£o do `hub_customer`

O Hub guarda somente identidade e auditoria. Os atributos descritivos ficam nos
Satellites.

Antes do hash, cada Business Key Ã© combinada com o contexto do sistema:

```text
CRM||CRM-100
ERP||ERP-900
ECOMMERCE||ECOM-300
```

Depois Ã© aplicado SHA-256:

```text
customer_hk = SHA256(contexto || business_key)
```

Para facilitar a leitura, os hashes abaixo estÃ£o abreviados.

| customer_hk | customer_bk | business_key_context | load_datetime | record_source |
|---|---|---|---|---|
| `HK-CRM-100` | `CRM-100` | CRM | 2026-07-27 20:00 | crm |
| `HK-ERP-900` | `ERP-900` | ERP | 2026-07-27 20:02 | erp |
| `HK-ECOM-300` | `ECOM-300` | ECOMMERCE | 2026-07-27 20:04 | ecommerce |

O Hub possui trÃªs linhas, e nÃ£o uma. Isso preserva a identidade original de
cada sistema.

Se a carga for executada novamente, o `MERGE` encontra os mesmos
`customer_hk` e nÃ£o insere duplicatas.

---

## 4. CriaÃ§Ã£o dos Customer Satellites

Cada fonte possui um Satellite prÃ³prio.

### `sat_customer_crm`

| customer_hk | full_name | cpf | email | city | status | hashdiff | load_datetime |
|---|---|---|---|---|---|---|---|
| `HK-CRM-100` | Ana Silva | `12345678900` | `ana@email.com` | SÃ£o Paulo | ACTIVE | `HD-CRM-A` | 2026-07-27 20:00 |

### `sat_customer_erp`

| customer_hk | customer_name | cpf | invoice_email | credit_limit | hashdiff | load_datetime |
|---|---|---|---|---:|---|---|
| `HK-ERP-900` | ANA SILVA | `12345678900` | `ana@email.com` | 5000.00 | `HD-ERP-A` | 2026-07-27 20:02 |

### `sat_customer_ecommerce`

| customer_hk | display_name | email_address | newsletter_optin | hashdiff | load_datetime |
|---|---|---|---|---|---|
| `HK-ECOM-300` | Ana | `ana@email.com` | true | `HD-ECOM-A` | 2026-07-27 20:04 |

O `hashdiff` representa todos os atributos daquela versÃ£o. Se um atributo
mudar, o hashdiff tambÃ©m muda.

---

## 5. CriaÃ§Ã£o do `same_as_link_customer`

O Same-As Link procura evidÃªncias exatas entre identidades de sistemas
diferentes.

### EvidÃªncias encontradas

| customer_hk | contexto | match_rule | match_value |
|---|---|---|---|
| `HK-CRM-100` | CRM | EXACT_CPF | `12345678900` |
| `HK-ERP-900` | ERP | EXACT_CPF | `12345678900` |
| `HK-CRM-100` | CRM | EXACT_EMAIL | `ANA@EMAIL.COM` |
| `HK-ECOM-300` | ECOMMERCE | EXACT_EMAIL | `ANA@EMAIL.COM` |

Os valores sÃ£o normalizados com `trim` e `upper`.

### Links criados

| same_as_customer_hk | customer_hk_left | customer_hk_right | match_rule | match_score | match_status |
|---|---|---|---|---:|---|
| `SAL-1` | `HK-CRM-100` | `HK-ERP-900` | EXACT_CPF | 1.0 | AUTO_MATCHED |
| `SAL-2` | `HK-CRM-100` | `HK-ECOM-300` | EXACT_EMAIL | 1.0 | AUTO_MATCHED |

O Same-As Link nÃ£o apaga nem consolida as trÃªs linhas do Hub. Ele registra que
elas pertencem ao mesmo grupo de identidade.

Nomes nÃ£o sÃ£o usados como correspondÃªncia automÃ¡tica porque pessoas diferentes
podem ter o mesmo nome.

---

## 6. Relacionamento entre Customer e Order

Ana faz um pedido no ERP:

### Linha em `bronze.erp_orders`

| order_id | erp_customer_id | order_status | order_purchase_timestamp |
|---|---|---|---|
| `ORDER-500` | `ERP-900` | CREATED | 2026-07-27 21:00 |

Primeiro, `ORDER-500` Ã© inserido em `hub_order`:

| order_hk | order_bk | business_key_context |
|---|---|---|
| `HK-ORDER-500` | `ORDER-500` | ERP |

Depois Ã© criado o relacionamento:

```text
customer_order_hk = SHA256(customer_hk || order_hk)
```

### Linha em `link_customer_order`

| customer_order_hk | customer_hk | order_hk | load_datetime | record_source |
|---|---|---|---|---|
| `LINK-1` | `HK-ERP-900` | `HK-ORDER-500` | 2026-07-27 21:05 | erp |

Agora Ã© possÃ­vel navegar:

```text
pedido ORDER-500
  â†’ cliente ERP-900
  â†’ mesma pessoa que CRM-100
  â†’ mesma pessoa que ECOM-300
```

---

## 7. Chegada de uma carga incremental

No dia seguinte, o CRM envia uma nova versÃ£o:

| crm_customer_id | full_name | cpf | city | status |
|---|---|---|---|---|
| `CRM-100` | Ana Silva | `12345678900` | Rio de Janeiro | ACTIVE |

A Ãºnica mudanÃ§a foi:

```text
city: SÃ£o Paulo â†’ Rio de Janeiro
```

### Bronze

A nova linha Ã© adicionada, sem alterar a anterior:

| crm_customer_id | city | _source_date | _ingested_at |
|---|---|---|---|
| `CRM-100` | SÃ£o Paulo | 2026-07-27 | 2026-07-27 20:00 |
| `CRM-100` | Rio de Janeiro | 2026-07-28 | 2026-07-28 20:00 |

### Hub

Nada Ã© inserido. A identidade `CRM||CRM-100` jÃ¡ existe:

| customer_hk | customer_bk | quantidade |
|---|---|---:|
| `HK-CRM-100` | `CRM-100` | 1 |

### Satellite

O novo conjunto de atributos produz outro hashdiff:

| customer_hk | city | hashdiff | load_datetime |
|---|---|---|---|
| `HK-CRM-100` | SÃ£o Paulo | `HD-CRM-A` | 2026-07-27 20:00 |
| `HK-CRM-100` | Rio de Janeiro | `HD-CRM-B` | 2026-07-28 20:00 |

As duas versÃµes sÃ£o preservadas.

### Same-As Link

Nada novo Ã© inserido porque a relaÃ§Ã£o `CRM-100 â†” ERP-900` jÃ¡ existe.

---

## 8. Carga incremental sem mudanÃ§a

No terceiro dia, o CRM envia exatamente os mesmos atributos:

| crm_customer_id | city | status |
|---|---|---|
| `CRM-100` | Rio de Janeiro | ACTIVE |

O hashdiff calculado continua sendo:

```text
HD-CRM-B
```

Como Ã© igual Ã  versÃ£o anterior, o Satellite ignora essa linha.

Resultado:

| Data recebida | Estado | Inserido no Satellite? |
|---|---|---|
| 27/07 | SÃ£o Paulo | Sim |
| 28/07 | Rio de Janeiro | Sim |
| 29/07 | Rio de Janeiro | NÃ£o |

---

## 9. Ordem de execuÃ§Ã£o no Job

```mermaid
flowchart TD
    BCRM[Bronze CRM]
    BERP[Bronze ERP]
    BECOM[Bronze Ecommerce]
    BORDER[Bronze Orders]

    HUBC[hub_customer]
    HUBO[hub_order]

    SATCRM[sat_customer_crm]
    SATERP[sat_customer_erp]
    SATECOM[sat_customer_ecommerce]

    SAL[same_as_link_customer]
    LCO[link_customer_order]

    BCRM --> HUBC
    BERP --> HUBC
    BECOM --> HUBC
    BORDER --> HUBC
    BORDER --> HUBO

    HUBC --> SATCRM
    HUBC --> SATERP
    HUBC --> SATECOM
    HUBC --> SAL

    HUBC --> LCO
    HUBO --> LCO
```

Os Satellites e o Same-As Link sÃ³ iniciam depois de `hub_customer`. O
`link_customer_order` espera tanto `hub_customer` quanto `hub_order`.

---

## 10. Consultas de validaÃ§Ã£o

### Identidades do cliente

```sql
SELECT *
FROM lakehouse.silver.hub_customer
WHERE customer_bk IN ('CRM-100', 'ERP-900', 'ECOM-300');
```

### HistÃ³rico do CRM

```sql
SELECT
    hub.customer_bk,
    sat.city,
    sat.status,
    sat.load_datetime
FROM lakehouse.silver.hub_customer AS hub
INNER JOIN lakehouse.silver.sat_customer_crm AS sat
    ON hub.customer_hk = sat.customer_hk
WHERE hub.customer_bk = 'CRM-100'
ORDER BY sat.load_datetime;
```

### Identidades equivalentes

```sql
SELECT *
FROM lakehouse.silver.same_as_link_customer
WHERE customer_hk_left = 'HK-CRM-100'
   OR customer_hk_right = 'HK-CRM-100';
```

Os exemplos usam hashes abreviados para facilitar a leitura. Nas tabelas reais,
os campos `*_hk` e `hashdiff` contÃªm hashes SHA-256 completos.

---

## Resumo

| Etapa | O que acontece com Ana? |
|---|---|
| Landing | TrÃªs sistemas entregam trÃªs registros e trÃªs IDs |
| Bronze | Os registros sÃ£o preservados com metadados tÃ©cnicos |
| Hub | Cada identidade de origem recebe um Hash Key |
| Satellites | Os atributos sÃ£o separados por fonte e versionados |
| Same-As Link | CPF e e-mail relacionam as identidades equivalentes |
| Customer-Order Link | O cliente ERP Ã© relacionado ao pedido |
| Incremental | O Hub nÃ£o duplica e o Satellite grava somente mudanÃ§as |


