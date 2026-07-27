# Databricks notebook source
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# COMMAND ----------

DEFAULTS = {
    "bronze_catalog": "poc",
    "bronze_schema": "bronze",
    "silver_catalog": "poc",
    "silver_schema": "silver",
}

for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)

params = {name: dbutils.widgets.get(name).strip() for name in DEFAULTS}
bronze_catalog = params["bronze_catalog"]
bronze_schema = params["bronze_schema"]
silver_catalog = params["silver_catalog"]
silver_schema = params["silver_schema"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{silver_catalog}`.`{silver_schema}`")

TECHNICAL_COLUMNS = {
    "_rescued_data",
    "_source_file",
    "_source_file_name",
    "_source_file_size",
    "_source_file_modification_time",
    "_source_system",
    "_source_entity",
    "_source_date",
    "_ingested_at",
}


# COMMAND ----------

def table_name(layer, name):
    schema = bronze_schema if layer == "bronze" else silver_schema
    catalog = bronze_catalog if layer == "bronze" else silver_catalog
    return f"`{catalog}`.`{schema}`.`{name}`"


def read_bronze(name):
    return spark.table(table_name("bronze", name))


def normalized_string(column):
    return F.upper(F.trim(column.cast("string")))


def hash_columns(*columns):
    columns = [F.col(column) if isinstance(column, str) else column for column in columns]
    values = [
        F.coalesce(normalized_string(column), F.lit("^^"))
        for column in columns
    ]
    return F.sha2(F.concat_ws("||", *values), 256)


def source_load_datetime(df):
    if "_ingested_at" in df.columns:
        return F.col("_ingested_at").cast("timestamp")
    return F.current_timestamp()


def source_record_source(df, fallback):
    if "_source_system" in df.columns:
        return F.coalesce(F.col("_source_system"), F.lit(fallback))
    return F.lit(fallback)


def merge_insert_only(df, target, keys):
    """Create a Delta table or insert rows that do not already exist."""
    target_name = table_name("silver", target)
    if not spark.catalog.tableExists(f"{silver_catalog}.{silver_schema}.{target}"):
        df.write.format("delta").mode("overwrite").saveAsTable(target_name)
        return

    condition = " AND ".join(f"target.`{key}` <=> source.`{key}`" for key in keys)
    (
        DeltaTable.forName(spark, f"{silver_catalog}.{silver_schema}.{target}")
        .alias("target")
        .merge(df.alias("source"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def union_all(frames):
    return reduce(lambda left, right: left.unionByName(right), frames)


def load_hub(target, frames, hk_column, bk_column):
    staged = (
        union_all(frames)
        .filter(F.col(bk_column).isNotNull() & (F.trim(F.col(bk_column)) != ""))
        .groupBy(hk_column, bk_column, "business_key_context")
        .agg(
            F.min("load_datetime").alias("load_datetime"),
            F.min("record_source").alias("record_source"),
        )
    )
    merge_insert_only(staged, target, [hk_column])


def hub_stage(df, source_column, context, hk_column, bk_column):
    bk = normalized_string(F.col(source_column))
    return df.select(
        hash_columns(F.lit(context), bk).alias(hk_column),
        bk.alias(bk_column),
        F.lit(context).alias("business_key_context"),
        source_load_datetime(df).alias("load_datetime"),
        source_record_source(df, context.lower()).alias("record_source"),
    )


def load_link(target, df, link_hk, identity_columns):
    staged = (
        df.filter(reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in identity_columns]))
        .dropDuplicates([link_hk])
    )
    merge_insert_only(staged, target, [link_hk])


def load_satellite(target, source_df, parent_hk, attribute_columns, fallback_source):
    """
    Rebuild the ordered source history and insert only changes.

    Reprocessing is idempotent because the merge key is the parent hash key and
    the original Bronze ingestion timestamp.
    """
    existing_attributes = [name for name in attribute_columns if name in source_df.columns]
    staged = source_df.select(
        F.col(parent_hk),
        source_load_datetime(source_df).alias("load_datetime"),
        source_record_source(source_df, fallback_source).alias("record_source"),
        *[F.col(name) for name in existing_attributes],
    ).filter(F.col(parent_hk).isNotNull())

    staged = staged.withColumn(
        "hashdiff",
        hash_columns(*[F.col(name) for name in existing_attributes]),
    )

    # A Bronze load should contain at most one state per key and ingestion time.
    staged = staged.dropDuplicates([parent_hk, "load_datetime"])
    history_window = Window.partitionBy(parent_hk).orderBy("load_datetime")
    staged = (
        staged
        .withColumn("_previous_hashdiff", F.lag("hashdiff").over(history_window))
        .filter(
            F.col("_previous_hashdiff").isNull()
            | (F.col("_previous_hashdiff") != F.col("hashdiff"))
        )
        .drop("_previous_hashdiff")
    )
    merge_insert_only(staged, target, [parent_hk, "load_datetime"])


# COMMAND ----------

# Read every Bronze source once. Spark remains lazy until the Delta merges run.
crm = read_bronze("crm_customers")
erp_customers = read_bronze("erp_customers")
ecommerce_customers = read_bronze("ecommerce_customers")
loyalty = read_bronze("loyalty_customers")
orders = read_bronze("erp_orders")
payments = read_bronze("erp_payments")
products = read_bronze("ecommerce_products")
sellers = read_bronze("ecommerce_sellers")


# COMMAND ----------

# Hubs
customer_hub_frames = [
    hub_stage(crm, "crm_customer_id", "CRM", "customer_hk", "customer_bk"),
    hub_stage(erp_customers, "erp_customer_id", "ERP", "customer_hk", "customer_bk"),
    hub_stage(
        ecommerce_customers,
        "ecommerce_customer_id",
        "ECOMMERCE",
        "customer_hk",
        "customer_bk",
    ),
    hub_stage(loyalty, "loyalty_customer_id", "LOYALTY", "customer_hk", "customer_bk"),
]

# Orders can introduce a customer before its customer master record arrives.
if "erp_customer_id" in orders.columns:
    customer_hub_frames.append(
        hub_stage(orders, "erp_customer_id", "ERP", "customer_hk", "customer_bk")
    )
if "customer_id" in orders.columns:
    customer_hub_frames.append(
        hub_stage(orders, "customer_id", "ECOMMERCE", "customer_hk", "customer_bk")
    )

load_hub("hub_customer", customer_hub_frames, "customer_hk", "customer_bk")

load_hub(
    "hub_order",
    [
        hub_stage(orders, "order_id", "ERP", "order_hk", "order_bk"),
        hub_stage(payments, "order_id", "ERP", "order_hk", "order_bk"),
    ],
    "order_hk",
    "order_bk",
)

load_hub(
    "hub_product",
    [hub_stage(products, "product_id", "ECOMMERCE", "product_hk", "product_bk")],
    "product_hk",
    "product_bk",
)

load_hub(
    "hub_seller",
    [hub_stage(sellers, "seller_id", "ECOMMERCE", "seller_hk", "seller_bk")],
    "seller_hk",
    "seller_bk",
)


# COMMAND ----------

# Business Links
order_customer_sources = []
if "erp_customer_id" in orders.columns:
    order_customer_sources.append(
        orders.select(
            hash_columns(F.lit("ERP"), F.col("order_id")).alias("order_hk"),
            hash_columns(F.lit("ERP"), F.col("erp_customer_id")).alias("customer_hk"),
            source_load_datetime(orders).alias("load_datetime"),
            source_record_source(orders, "erp").alias("record_source"),
        ).filter(F.col("erp_customer_id").isNotNull())
    )
if "customer_id" in orders.columns:
    order_customer_sources.append(
        orders.select(
            hash_columns(F.lit("ERP"), F.col("order_id")).alias("order_hk"),
            hash_columns(F.lit("ECOMMERCE"), F.col("customer_id")).alias("customer_hk"),
            source_load_datetime(orders).alias("load_datetime"),
            source_record_source(orders, "erp").alias("record_source"),
        ).filter(F.col("customer_id").isNotNull())
    )

if order_customer_sources:
    link_customer_order = (
        union_all(order_customer_sources)
        .withColumn("customer_order_hk", hash_columns("customer_hk", "order_hk"))
        .select(
            "customer_order_hk",
            "customer_hk",
            "order_hk",
            "load_datetime",
            "record_source",
        )
    )
    load_link(
        "link_customer_order",
        link_customer_order,
        "customer_order_hk",
        ["customer_hk", "order_hk"],
    )

payment_occurrence = (
    payments
    .filter(F.col("order_id").isNotNull() & F.col("payment_sequential").isNotNull())
    .select(
        hash_columns(F.lit("ERP"), F.col("order_id")).alias("order_hk"),
        normalized_string(F.col("payment_sequential")).alias("payment_sequence_bk"),
        source_load_datetime(payments).alias("load_datetime"),
        source_record_source(payments, "erp").alias("record_source"),
    )
    .withColumn(
        "order_payment_hk",
        hash_columns("order_hk", "payment_sequence_bk"),
    )
)
load_link(
    "link_order_payment_occurrence",
    payment_occurrence,
    "order_payment_hk",
    ["order_hk", "payment_sequence_bk"],
)


# COMMAND ----------

# Customer Same-As Link: exact deterministic evidence only.
def identity_projection(df, id_column, context, evidence_column, evidence_type):
    return (
        df.filter(F.col(id_column).isNotNull() & F.col(evidence_column).isNotNull())
        .select(
            hash_columns(F.lit(context), F.col(id_column)).alias("customer_hk"),
            F.lit(context).alias("business_key_context"),
            normalized_string(F.col(evidence_column)).alias("match_value"),
            F.lit(evidence_type).alias("match_rule"),
            source_load_datetime(df).alias("load_datetime"),
            source_record_source(df, context.lower()).alias("record_source"),
        )
        .filter(F.length("match_value") > 0)
    )


identity_sources = [
    identity_projection(crm, "crm_customer_id", "CRM", "cpf", "EXACT_CPF"),
    identity_projection(erp_customers, "erp_customer_id", "ERP", "cpf", "EXACT_CPF"),
    identity_projection(crm, "crm_customer_id", "CRM", "email", "EXACT_EMAIL"),
    identity_projection(
        ecommerce_customers,
        "ecommerce_customer_id",
        "ECOMMERCE",
        "email_address",
        "EXACT_EMAIL",
    ),
    identity_projection(
        erp_customers,
        "erp_customer_id",
        "ERP",
        "invoice_email",
        "EXACT_EMAIL",
    ),
]

identity_evidence = union_all(identity_sources).dropDuplicates(
    ["customer_hk", "match_rule", "match_value"]
)
left = identity_evidence.alias("left")
right = identity_evidence.alias("right")

same_as = (
    left.join(
        right,
        (F.col("left.match_rule") == F.col("right.match_rule"))
        & (F.col("left.match_value") == F.col("right.match_value"))
        & (
            F.col("left.business_key_context")
            != F.col("right.business_key_context")
        )
        & (F.col("left.customer_hk") < F.col("right.customer_hk")),
    )
    .select(
        F.col("left.customer_hk").alias("customer_hk_left"),
        F.col("right.customer_hk").alias("customer_hk_right"),
        F.col("left.business_key_context").alias("context_left"),
        F.col("right.business_key_context").alias("context_right"),
        F.col("left.match_rule").alias("match_rule"),
        F.col("left.match_value").alias("match_value"),
        F.lit(1.0).alias("match_score"),
        F.lit("AUTO_MATCHED").alias("match_status"),
        F.greatest(F.col("left.load_datetime"), F.col("right.load_datetime")).alias(
            "load_datetime"
        ),
        F.concat_ws("+", F.col("left.record_source"), F.col("right.record_source")).alias(
            "record_source"
        ),
    )
    .withColumn(
        "same_as_customer_hk",
        hash_columns("customer_hk_left", "customer_hk_right", "match_rule"),
    )
)
load_link(
    "same_as_link_customer",
    same_as,
    "same_as_customer_hk",
    ["customer_hk_left", "customer_hk_right", "match_rule"],
)


# COMMAND ----------

# Satellites
crm_sat_source = crm.withColumn(
    "customer_hk", hash_columns(F.lit("CRM"), F.col("crm_customer_id"))
)
load_satellite(
    "sat_customer_crm",
    crm_sat_source,
    "customer_hk",
    [
        column
        for column in crm.columns
        if column not in TECHNICAL_COLUMNS | {"crm_customer_id"}
    ],
    "crm",
)

erp_customer_sat_source = erp_customers.withColumn(
    "customer_hk", hash_columns(F.lit("ERP"), F.col("erp_customer_id"))
)
load_satellite(
    "sat_customer_erp",
    erp_customer_sat_source,
    "customer_hk",
    [
        column
        for column in erp_customers.columns
        if column not in TECHNICAL_COLUMNS | {"erp_customer_id"}
    ],
    "erp",
)

ecommerce_customer_sat_source = ecommerce_customers.withColumn(
    "customer_hk",
    hash_columns(F.lit("ECOMMERCE"), F.col("ecommerce_customer_id")),
)
load_satellite(
    "sat_customer_ecommerce",
    ecommerce_customer_sat_source,
    "customer_hk",
    [
        column
        for column in ecommerce_customers.columns
        if column not in TECHNICAL_COLUMNS | {"ecommerce_customer_id"}
    ],
    "ecommerce",
)

loyalty_sat_source = loyalty.withColumn(
    "customer_hk", hash_columns(F.lit("LOYALTY"), F.col("loyalty_customer_id"))
)
load_satellite(
    "sat_customer_loyalty",
    loyalty_sat_source,
    "customer_hk",
    [
        column
        for column in loyalty.columns
        if column not in TECHNICAL_COLUMNS | {"loyalty_customer_id"}
    ],
    "loyalty",
)

order_sat_source = orders.withColumn(
    "order_hk", hash_columns(F.lit("ERP"), F.col("order_id"))
)
load_satellite(
    "sat_order_erp",
    order_sat_source,
    "order_hk",
    [
        column
        for column in orders.columns
        if column
        not in TECHNICAL_COLUMNS | {"order_id", "customer_id", "erp_customer_id"}
    ],
    "erp",
)

payment_sat_source = (
    payments
    .withColumn(
        "order_payment_hk",
        hash_columns(
            hash_columns(F.lit("ERP"), F.col("order_id")),
            normalized_string(F.col("payment_sequential")),
        ),
    )
)
load_satellite(
    "sat_order_payment_erp",
    payment_sat_source,
    "order_payment_hk",
    [
        column
        for column in payments.columns
        if column
        not in TECHNICAL_COLUMNS | {"order_id", "payment_sequential", "payment_id"}
    ],
    "erp",
)

product_sat_source = products.withColumn(
    "product_hk", hash_columns(F.lit("ECOMMERCE"), F.col("product_id"))
)
load_satellite(
    "sat_product_ecommerce",
    product_sat_source,
    "product_hk",
    [
        column
        for column in products.columns
        if column not in TECHNICAL_COLUMNS | {"product_id"}
    ],
    "ecommerce",
)

seller_sat_source = sellers.withColumn(
    "seller_hk", hash_columns(F.lit("ECOMMERCE"), F.col("seller_id"))
)
load_satellite(
    "sat_seller_ecommerce",
    seller_sat_source,
    "seller_hk",
    [
        column
        for column in sellers.columns
        if column not in TECHNICAL_COLUMNS | {"seller_id"}
    ],
    "ecommerce",
)


# COMMAND ----------

silver_tables = [
    "hub_customer",
    "hub_order",
    "hub_product",
    "hub_seller",
    "link_customer_order",
    "link_order_payment_occurrence",
    "same_as_link_customer",
    "sat_customer_crm",
    "sat_customer_erp",
    "sat_customer_ecommerce",
    "sat_customer_loyalty",
    "sat_order_erp",
    "sat_order_payment_erp",
    "sat_product_ecommerce",
    "sat_seller_ecommerce",
]

display(
    spark.createDataFrame([(name,) for name in silver_tables], "table STRING")
    .withColumn("catalog", F.lit(silver_catalog))
    .withColumn("schema", F.lit(silver_schema))
    .select("catalog", "schema", "table")
)

