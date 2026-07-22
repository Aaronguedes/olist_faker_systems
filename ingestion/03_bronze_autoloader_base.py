# Databricks notebook source
from pyspark.sql import functions as F


# COMMAND ----------

# One job task invokes this notebook for one source dataset. Keeping the
# registry here prevents paths and table names from being duplicated in jobs.
DATASETS = {
    ("crm", "customers"): "crm_customers",
    ("erp", "customers"): "erp_customers",
    ("erp", "orders"): "erp_orders",
    ("erp", "payments"): "erp_payments",
    ("ecommerce", "customers"): "ecommerce_customers",
    ("ecommerce", "products"): "ecommerce_products",
    ("ecommerce", "sellers"): "ecommerce_sellers",
    ("loyalty", "customers"): "loyalty_customers",
}

DEFAULTS = {
    "source_system": "crm",
    "entity": "customers",
    "landing_root": "abfss://landing@stglkhdev.dfs.core.windows.net/vault",
    "checkpoint_root": "abfss://landing@stglkhdev.dfs.core.windows.net/vault/_checkpoints/bronze",
    "bronze_catalog": "poc",
    "bronze_schema": "bronze",
    "include_existing_files": "true",
}

for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)

params = {name: dbutils.widgets.get(name).strip() for name in DEFAULTS}
dataset_key = (params["source_system"].lower(), params["entity"].lower())

if dataset_key not in DATASETS:
    valid = ", ".join(f"{system}/{entity}" for system, entity in DATASETS)
    raise ValueError(f"Unsupported dataset {dataset_key!r}. Valid datasets: {valid}")

table_name = DATASETS[dataset_key]
source_path = f'{params["landing_root"].rstrip("/")}/{dataset_key[0]}/{dataset_key[1]}'
state_path = f'{params["checkpoint_root"].rstrip("/")}/{dataset_key[0]}/{dataset_key[1]}'
schema_path = f"{state_path}/schema"
checkpoint_path = f"{state_path}/checkpoint"
target_table = f'{params["bronze_catalog"]}.{params["bronze_schema"]}.{table_name}'

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {params['bronze_catalog']}.{params['bronze_schema']}")


# COMMAND ----------

bronze_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.includeExistingFiles", params["include_existing_files"].lower())
    .option("rescuedDataColumn", "_rescued_data")
    .load(source_path)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_source_file_name", F.col("_metadata.file_name"))
    .withColumn("_source_file_size", F.col("_metadata.file_size"))
    .withColumn("_source_file_modification_time", F.col("_metadata.file_modification_time"))
    .withColumn("_source_system", F.lit(dataset_key[0]))
    .withColumn("_source_entity", F.lit(dataset_key[1]))
    .withColumn(
        "_source_date",
        F.to_date(F.regexp_extract(F.col("_metadata.file_path"), r"/(\d{4}/\d{2}/\d{2})(?:/|$)", 1), "yyyy/MM/dd"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    bronze_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()


