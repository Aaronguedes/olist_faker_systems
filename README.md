# Olist Faker Systems

Projeto de engenharia de dados que utiliza o dataset público Olist como referência para gerar dados sintéticos de clientes em múltiplos sistemas empresariais. O objetivo é criar uma fonte realista para estudos de ingestão incremental, qualidade de dados, integração de identidades e modelagem Data Vault em Databricks.

Os mesmos clientes recebem representações diferentes em CRM, ERP, e-commerce e fidelidade. Cada dataset publicado expõe somente a chave local do respectivo sistema; a chave interna `customer_unique_id` permanece restrita ao simulador. Isso permite testar resolução de identidade, historização, regras de negócio e relacionamentos entre fontes independentes.

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

- lê clientes, pedidos, pagamentos, produtos e vendedores do Olist;
- gera o perfil mestre canônico;
- cria identificadores próprios para cada sistema de origem;
- produz atributos sintéticos com Faker;
- calcula métricas de compra;
- injeta variações de formato e problemas controlados de qualidade;
- grava snapshots Parquet na landing zone.

### `02_simulate_incremental_loads`

Simula novos dias de operação a partir do estado anterior:

- novos clientes;
- mudanças de telefone, e-mail e endereço;
- novos pedidos e pagamentos;
- logins e alterações de dispositivo;
- resgate e recálculo de pontos;
- geração de incrementais contendo apenas registros modificados;
- persistência do estado para a próxima execução.

### `03_bronze_autoloader`

Ingere todas as entidades da landing para tabelas Delta na camada Bronze:

- utiliza Databricks Auto Loader com `cloudFiles` e formato Parquet;
- mantém schema location e checkpoint independentes por entidade;
- suporta evolução aditiva de schema e `_rescued_data`;
- adiciona metadados de arquivo, sistema, entidade, data de origem e ingestão;
- usa `availableNow=True`, adequado para execução recorrente em Workflows;
- grava as tabelas em `<bronze_catalog>.<bronze_schema>`.

Tabelas produzidas: `crm_customers`, `erp_customers`, `erp_orders`,
`erp_payments`, `ecommerce_customers`, `ecommerce_products`,
`ecommerce_sellers` e `loyalty_customers`.

## Dicionário de dados

Tipos são apresentados como tipos lógicos recomendados. Como os notebooks usam inferência de schema entre Pandas e Spark, o tipo físico pode variar conforme os valores presentes.

### Master Profile

**Grão:** um registro por cliente canônico.

| Campo | Tipo | Chave | Descrição |
|---|---|---|---|
| `global_customer_id` | string/UUID | PK | Identificador global do cliente. |
| `customer_unique_id` | string | BK | Identificador Olist usado para correlacionar os sistemas. |
| `first_name` | string |  | Primeiro nome sintético. |
| `middle_name` | string |  | Nome do meio opcional. |
| `last_name` | string |  | Sobrenome sintético. |
| `full_name` | string |  | Nome completo. |
| `birth_date` | date |  | Data de nascimento. |
| `gender` | string |  | Gênero sintético. |
| `cpf` | string |  | CPF sintético. |
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

**Grão:** um registro por cliente no CRM.  
**Chave publicada:** `crm_customer_id` (PK/BK da fonte).

| Grupo | Campos | Descrição |
|---|---|---|
| Identificação | `crm_customer_id`, `cpf` | Identificador do CRM e documento usado no matching. |
| Nome | `first_name`, `middle_name`, `last_name`, `full_name` | Componentes e apresentação do nome. |
| Perfil | `birth_date`, `gender`, `marital_status`, `profession`, `annual_income` | Informações demográficas e profissionais. |
| Contato | `email`, `phone`, `street`, `city`, `state` | Dados sujeitos a mudanças e problemas de qualidade. |
| Gestão | `status`, `risk_score`, `created_at`, `updated_at` | Estado, risco e datas do registro. |

Domínios principais:

- `status`: `ACTIVE`, `INACTIVE`, `PROSPECT`;
- `risk_score`: 0 a 100;
- `annual_income`: 25.000 a 350.000.

### ERP Customers

**Grão:** um registro por cliente no ERP.  
**Chave publicada:** `erp_customer_id` (PK/BK da fonte).

| Campo | Tipo | Descrição |
|---|---|---|
| `erp_customer_id` | string | Identificador do cliente no ERP. |
| `customer_name` | string | Nome com possível variação de apresentação. |
| `cpf` | string | CPF sintético. |
| `tax_category` | string | `Normal`, `Simples` ou `MEI`. |
| `customer_group` | string | `Retail`, `SMB`, `Corporate` ou `Government`. |
| `payment_terms` | string | `NET15`, `NET30`, `NET45` ou `NET60`. |
| `credit_limit` | decimal(18,2) | Limite de crédito calculado/atribuído. |
| `account_manager` | string | Gerente responsável. |
| `invoice_email` | string | E-mail de faturamento. |
| `total_orders` | integer | Quantidade acumulada de pedidos. |
| `first_purchase` | timestamp | Primeira compra. |
| `last_purchase` | timestamp | Compra mais recente. |
| `total_spent` | decimal(18,2) | Valor total gasto. |
| `average_ticket` | decimal(18,2) | Valor médio por pedido. |

### ERP Orders

**Grão:** um registro por pedido.  
**Chave:** `order_id` (PK).

| Campo | Tipo | Descrição |
|---|---|---|
| `order_id` | string/UUID | Identificador do pedido. |
| `customer_id` | string | Identificador Olist presente no snapshot inicial. |
| `erp_customer_id` | string | Cliente ERP relacionado ao pedido incremental. |
| `order_status` | string | Status do pedido. |
| `order_purchase_timestamp` | timestamp | Data/hora da compra. |
| `order_approved_at` | timestamp | Aprovação; presente no snapshot Olist. |
| `order_delivered_carrier_date` | timestamp | Entrega à transportadora. |
| `order_delivered_customer_date` | timestamp | Entrega ao cliente. |
| `order_estimated_delivery_date` | timestamp | Previsão de entrega. |

### ERP Payments

**Grão:** um registro por ocorrência de pagamento.  
**Relacionamento:** `order_id` referencia ERP Orders.

| Campo | Tipo | Descrição |
|---|---|---|
| `payment_id` | string/UUID | Identificador criado para pagamentos incrementais. |
| `order_id` | string/UUID | Pedido relacionado. |
| `payment_sequential` | integer | Sequência do pagamento no snapshot Olist. |
| `payment_type` | string | Meio de pagamento. |
| `payment_installments` | integer | Quantidade de parcelas. |
| `payment_value` | decimal(18,2) | Valor pago. |

### Ecommerce Customers

**Grão:** uma conta por cliente.  
**Chave publicada:** `ecommerce_customer_id` (PK/BK da fonte).

| Grupo | Campos | Descrição |
|---|---|---|
| Identidade | `ecommerce_customer_id`, `username`, `display_name` | Conta e identificação do cliente. |
| Contato | `email_address`, `phone_number` | Dados de contato na plataforma. |
| Tecnologia | `browser`, `operating_system`, `device`, `last_login` | Contexto de acesso mais recente. |
| Preferências | `marketing_source`, `preferred_language`, `newsletter_optin`, `preferred_category` | Aquisição, consentimento e preferências. |
| Comportamento | `wishlist_items`, `abandoned_carts`, `account_created` | Indicadores de uso da conta. |

### Ecommerce Products

**Grão:** um registro por produto.  
**Chave:** `product_id` (PK).

| Grupo | Campos | Descrição |
|---|---|---|
| Identificação | `product_id`, `product_category_name` | Produto e categoria Olist. |
| Conteúdo | `product_name_lenght`, `product_description_lenght`, `product_photos_qty` | Metadados do catálogo original. |
| Dimensões | `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm` | Peso e dimensões físicas. |
| Enriquecimento | `brand`, `supplier_code`, `is_active`, `launch_year` | Atributos sintéticos adicionados pelo projeto. |

### Ecommerce Sellers

**Grão:** um registro por vendedor.  
**Chave:** `seller_id` (PK).

| Campo | Tipo | Descrição |
|---|---|---|
| `seller_id` | string | Identificador Olist do vendedor. |
| `seller_zip_code_prefix` | string/integer | Prefixo do CEP. |
| `seller_city` | string | Cidade. |
| `seller_state` | string | UF. |
| `seller_rating` | decimal(3,2) | Nota sintética entre 3,5 e 5,0. |
| `premium_partner` | boolean | Indicador de parceiro premium. |
| `account_manager` | string | Gerente responsável. |

### Loyalty Customers

**Grão:** uma associação por cliente.  
**Chave publicada:** `loyalty_customer_id` (PK/BK da fonte).

| Campo | Tipo | Descrição |
|---|---|---|
| `loyalty_customer_id` | string | Identificador no programa. |
| `member_name` | string | Nome apresentado no programa. |
| `join_date` | date/timestamp | Data de adesão. |
| `points` | integer | Pontos acumulados. |
| `redeemed_points` | integer | Pontos já resgatados. |
| `available_points` | integer | Pontos disponíveis. |
| `tier` | string | `Bronze`, `Silver`, `Gold` ou `Platinum`. |
| `wallet_balance` | decimal(18,2) | Saldo equivalente aos pontos. |
| `preferred_store` | string | Loja preferencial. |
| `last_campaign` | string | Última campanha atribuída. |
| `active_member` | boolean | Indica associação ativa. |

### Simulation Control

**Grão:** um registro por execução da simulação.

| Campo | Tipo | Descrição |
|---|---|---|
| `last_execution` | timestamp | Data processada pela execução atual. |
| `next_execution` | timestamp | Próxima data prevista. |
| `crm_records` | integer | Total de registros no estado do CRM. |
| `erp_records` | integer | Total de registros no estado do ERP. |
| `web_records` | integer | Total de contas no estado do e-commerce. |
| `loyalty_records` | integer | Total de membros no estado de fidelidade. |
| `orders` | integer | Total acumulado de pedidos. |
| `payments` | integer | Total acumulado de pagamentos. |

## Snapshot, incremental e estado

- A carga inicial grava snapshots completos na landing.
- O Notebook 2 grava apenas clientes alterados e novas transações na partição diária.
- O diretório de estado mantém a visão completa após a aplicação dos eventos e não deve ser confundido com a landing consumida pelo pipeline.
- `customer_unique_id` existe somente no estado interno do simulador e não é publicado na landing.
- Cada fonte usa seu próprio ID como Business Key: `crm_customer_id`, `erp_customer_id`, `ecommerce_customer_id` ou `loyalty_customer_id`.
- A associação global deve ser resolvida por atributos como CPF, e-mail, telefone e nome/data de nascimento, registrando método e confiança do match.

Para uma ingestão mais robusta, recomenda-se adicionar a todos os datasets os metadados `load_datetime`, `record_source`, `batch_id` e `operation_type` (`I`, `U`, `D`). Também é recomendável alinhar explicitamente os schemas de Orders, Payments e novos clientes entre snapshot e incremental.