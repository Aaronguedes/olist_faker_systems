# Olist Faker Systems

Projeto de engenharia de dados que utiliza o dataset pÃºblico Olist como referÃªncia para gerar dados sintÃ©ticos de clientes em mÃºltiplos sistemas empresariais. O objetivo Ã© criar uma fonte realista para estudos de ingestÃ£o incremental, qualidade de dados, integraÃ§Ã£o de identidades e modelagem Data Vault em Databricks.

Os mesmos clientes recebem representaÃ§Ãµes diferentes em CRM, ERP, e-commerce e fidelidade. Isso permite testar resoluÃ§Ã£o de identidade, historizaÃ§Ã£o, regras de negÃ³cio e relacionamentos entre fontes independentes.

## Fluxo do projeto

```text
Dataset Olist
     |
     v
Master Customer Profile
     |
     +---- CRM
     +---- ERP
     +---- Ecommerce
     +---- Loyalty
     |
     v
Landing Parquet particionada por data
     |
     v
Bronze -> Raw Vault -> Business Vault -> Gold
```

## Notebooks

### `01_generate_initial_data`

Cria a carga inicial completa:

- lÃª clientes, pedidos, pagamentos, produtos e vendedores do Olist;
- gera o perfil mestre canÃ´nico;
- cria identificadores prÃ³prios para cada sistema de origem;
- produz atributos sintÃ©ticos com Faker;
- calcula mÃ©tricas de compra;
- injeta variaÃ§Ãµes de formato e problemas controlados de qualidade;
- grava snapshots Parquet na landing zone.

### `02_simulate_incremental_loads`

Simula novos dias de operaÃ§Ã£o a partir do estado anterior:

- novos clientes;
- mudanÃ§as de telefone, e-mail e endereÃ§o;
- novos pedidos e pagamentos;
- logins e alteraÃ§Ãµes de dispositivo;
- resgate e recÃ¡lculo de pontos;
- geraÃ§Ã£o de incrementais contendo apenas registros modificados;
- persistÃªncia do estado para a prÃ³xima execuÃ§Ã£o.

## DicionÃ¡rio de dados

Tipos sÃ£o apresentados como tipos lÃ³gicos recomendados. Como os notebooks usam inferÃªncia de schema entre Pandas e Spark, o tipo fÃ­sico pode variar conforme os valores presentes.

### Master Profile

**GrÃ£o:** um registro por cliente canÃ´nico.

| Campo | Tipo | Chave | DescriÃ§Ã£o |
|---|---|---|---|
| `global_customer_id` | string/UUID | PK | Identificador global do cliente. |
| `customer_unique_id` | string | BK | Identificador Olist usado para correlacionar os sistemas. |
| `first_name` | string |  | Primeiro nome sintÃ©tico. |
| `middle_name` | string |  | Nome do meio opcional. |
| `last_name` | string |  | Sobrenome sintÃ©tico. |
| `full_name` | string |  | Nome completo. |
| `birth_date` | date |  | Data de nascimento. |
| `gender` | string |  | GÃªnero sintÃ©tico. |
| `cpf` | string |  | CPF sintÃ©tico. |
| `personal_email` | string |  | E-mail pessoal. |
| `phone` | string |  | Telefone celular. |
| `street` | string |  | Logradouro. |
| `neighborhood` | string |  | Bairro. |
| `city` | string |  | Cidade do cadastro Olist. |
| `state` | string |  | UF do cadastro Olist. |
| `crm_customer_id` | string | AK | Identificador do cliente no CRM. |
| `erp_customer_id` | string | AK | Identificador do cliente no ERP. |
| `ecommerce_customer_id` | string | AK | Identificador da conta no e-commerce. |
| `loyalty_customer_id` | string | AK | Identificador do membro de fidelidade. |

### CRM Customers

**GrÃ£o:** um registro por cliente no CRM.  
**Chaves:** `crm_customer_id` (PK), `customer_unique_id` (BK).

| Grupo | Campos | DescriÃ§Ã£o |
|---|---|---|
| IdentificaÃ§Ã£o | `crm_customer_id`, `customer_unique_id`, `cpf` | Identificadores do CRM, chave de integraÃ§Ã£o e documento. |
| Nome | `first_name`, `middle_name`, `last_name`, `full_name` | Componentes e apresentaÃ§Ã£o do nome. |
| Perfil | `birth_date`, `gender`, `marital_status`, `profession`, `annual_income` | InformaÃ§Ãµes demogrÃ¡ficas e profissionais. |
| Contato | `email`, `phone`, `street`, `city`, `state` | Dados sujeitos a mudanÃ§as e problemas de qualidade. |
| GestÃ£o | `status`, `risk_score`, `created_at`, `updated_at` | Estado, risco e datas do registro. |

DomÃ­nios principais:

- `status`: `ACTIVE`, `INACTIVE`, `PROSPECT`;
- `risk_score`: 0 a 100;
- `annual_income`: 25.000 a 350.000.

### ERP Customers

**GrÃ£o:** um registro por cliente no ERP.  
**Chaves:** `erp_customer_id` (PK), `customer_unique_id` (BK).

| Campo | Tipo | DescriÃ§Ã£o |
|---|---|---|
| `erp_customer_id` | string | Identificador do cliente no ERP. |
| `customer_unique_id` | string | Chave de correlaÃ§Ã£o entre sistemas. |
| `customer_name` | string | Nome com possÃ­vel variaÃ§Ã£o de apresentaÃ§Ã£o. |
| `cpf` | string | CPF sintÃ©tico. |
| `tax_category` | string | `Normal`, `Simples` ou `MEI`. |
| `customer_group` | string | `Retail`, `SMB`, `Corporate` ou `Government`. |
| `payment_terms` | string | `NET15`, `NET30`, `NET45` ou `NET60`. |
| `credit_limit` | decimal(18,2) | Limite de crÃ©dito calculado/atribuÃ­do. |
| `account_manager` | string | Gerente responsÃ¡vel. |
| `invoice_email` | string | E-mail de faturamento. |
| `total_orders` | integer | Quantidade acumulada de pedidos. |
| `first_purchase` | timestamp | Primeira compra. |
| `last_purchase` | timestamp | Compra mais recente. |
| `total_spent` | decimal(18,2) | Valor total gasto. |
| `average_ticket` | decimal(18,2) | Valor mÃ©dio por pedido. |

### ERP Orders

**GrÃ£o:** um registro por pedido.  
**Chave:** `order_id` (PK).

| Campo | Tipo | DescriÃ§Ã£o |
|---|---|---|
| `order_id` | string/UUID | Identificador do pedido. |
| `customer_id` | string | Identificador Olist presente no snapshot inicial. |
| `customer_unique_id` | string | Chave do cliente usada no incremental. |
| `order_status` | string | Status do pedido. |
| `order_purchase_timestamp` | timestamp | Data/hora da compra. |
| `order_approved_at` | timestamp | AprovaÃ§Ã£o; presente no snapshot Olist. |
| `order_delivered_carrier_date` | timestamp | Entrega Ã  transportadora. |
| `order_delivered_customer_date` | timestamp | Entrega ao cliente. |
| `order_estimated_delivery_date` | timestamp | PrevisÃ£o de entrega. |

### ERP Payments

**GrÃ£o:** um registro por ocorrÃªncia de pagamento.  
**Relacionamento:** `order_id` referencia ERP Orders.

| Campo | Tipo | DescriÃ§Ã£o |
|---|---|---|
| `payment_id` | string/UUID | Identificador criado para pagamentos incrementais. |
| `order_id` | string/UUID | Pedido relacionado. |
| `payment_sequential` | integer | SequÃªncia do pagamento no snapshot Olist. |
| `payment_type` | string | Meio de pagamento. |
| `payment_installments` | integer | Quantidade de parcelas. |
| `payment_value` | decimal(18,2) | Valor pago. |

### Ecommerce Customers

**GrÃ£o:** uma conta por cliente.  
**Chaves:** `ecommerce_customer_id` (PK), `customer_unique_id` (BK).

| Grupo | Campos | DescriÃ§Ã£o |
|---|---|---|
| Identidade | `ecommerce_customer_id`, `customer_unique_id`, `username`, `display_name` | Conta e identificaÃ§Ã£o do cliente. |
| Contato | `email_address`, `phone_number` | Dados de contato na plataforma. |
| Tecnologia | `browser`, `operating_system`, `device`, `last_login` | Contexto de acesso mais recente. |
| PreferÃªncias | `marketing_source`, `preferred_language`, `newsletter_optin`, `preferred_category` | AquisiÃ§Ã£o, consentimento e preferÃªncias. |
| Comportamento | `wishlist_items`, `abandoned_carts`, `account_created` | Indicadores de uso da conta. |

### Ecommerce Products

**GrÃ£o:** um registro por produto.  
**Chave:** `product_id` (PK).

| Grupo | Campos | DescriÃ§Ã£o |
|---|---|---|
| IdentificaÃ§Ã£o | `product_id`, `product_category_name` | Produto e categoria Olist. |
| ConteÃºdo | `product_name_lenght`, `product_description_lenght`, `product_photos_qty` | Metadados do catÃ¡logo original. |
| DimensÃµes | `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm` | Peso e dimensÃµes fÃ­sicas. |
| Enriquecimento | `brand`, `supplier_code`, `is_active`, `launch_year` | Atributos sintÃ©ticos adicionados pelo projeto. |

### Ecommerce Sellers

**GrÃ£o:** um registro por vendedor.  
**Chave:** `seller_id` (PK).

| Campo | Tipo | DescriÃ§Ã£o |
|---|---|---|
| `seller_id` | string | Identificador Olist do vendedor. |
| `seller_zip_code_prefix` | string/integer | Prefixo do CEP. |
| `seller_city` | string | Cidade. |
| `seller_state` | string | UF. |
| `seller_rating` | decimal(3,2) | Nota sintÃ©tica entre 3,5 e 5,0. |
| `premium_partner` | boolean | Indicador de parceiro premium. |
| `account_manager` | string | Gerente responsÃ¡vel. |

### Loyalty Customers

**GrÃ£o:** uma associaÃ§Ã£o por cliente.  
**Chaves:** `loyalty_customer_id` (PK), `customer_unique_id` (BK).

| Campo | Tipo | DescriÃ§Ã£o |
|---|---|---|
| `loyalty_customer_id` | string | Identificador no programa. |
| `customer_unique_id` | string | Chave de correlaÃ§Ã£o entre sistemas. |
| `member_name` | string | Nome apresentado no programa. |
| `join_date` | date/timestamp | Data de adesÃ£o. |
| `points` | integer | Pontos acumulados. |
| `redeemed_points` | integer | Pontos jÃ¡ resgatados. |
| `available_points` | integer | Pontos disponÃ­veis. |
| `tier` | string | `Bronze`, `Silver`, `Gold` ou `Platinum`. |
| `wallet_balance` | decimal(18,2) | Saldo equivalente aos pontos. |
| `preferred_store` | string | Loja preferencial. |
| `last_campaign` | string | Ãšltima campanha atribuÃ­da. |
| `active_member` | boolean | Indica associaÃ§Ã£o ativa. |

### Simulation Control

**GrÃ£o:** um registro por execuÃ§Ã£o da simulaÃ§Ã£o.

| Campo | Tipo | DescriÃ§Ã£o |
|---|---|---|
| `last_execution` | timestamp | Data processada pela execuÃ§Ã£o atual. |
| `next_execution` | timestamp | PrÃ³xima data prevista. |
| `crm_records` | integer | Total de registros no estado do CRM. |
| `erp_records` | integer | Total de registros no estado do ERP. |
| `web_records` | integer | Total de contas no estado do e-commerce. |
| `loyalty_records` | integer | Total de membros no estado de fidelidade. |
| `orders` | integer | Total acumulado de pedidos. |
| `payments` | integer | Total acumulado de pagamentos. |

## Snapshot, incremental e estado

- A carga inicial grava snapshots completos na landing.
- O Notebook 2 grava apenas clientes alterados e novas transaÃ§Ãµes na partiÃ§Ã£o diÃ¡ria.
- O diretÃ³rio de estado mantÃ©m a visÃ£o completa apÃ³s a aplicaÃ§Ã£o dos eventos e nÃ£o deve ser confundido com a landing consumida pelo pipeline.
- `customer_unique_id` Ã© a principal Business Key para integraÃ§Ã£o e modelagem Data Vault.

Para uma ingestÃ£o mais robusta, recomenda-se adicionar a todos os datasets os metadados `load_datetime`, `record_source`, `batch_id` e `operation_type` (`I`, `U`, `D`). TambÃ©m Ã© recomendÃ¡vel alinhar explicitamente os schemas de Orders, Payments e novos clientes entre snapshot e incremental.
