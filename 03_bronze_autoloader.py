# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion with Auto Loader
# MAGIC
# MAGIC Incrementally ingests every source-system dataset from ADLS into
# MAGIC Unity Catalog Delta tables. Each source has independent schema and
# MAGIC checkpoint locations so failures and schema evolution remain isolated.

# COMMAND ----------

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery


DEFAULT_LANDING_ROOT = (
    "abfss://landing@stglkhdev.dfs.core.windows.net/vault"
)
DEFAULT_CHECKPOINT_ROOT = f"{DEFAULT_LANDING_ROOT}/_checkpoints/bronze"

dbutils.widgets.text("landing_root", DEFAULT_LANDING_ROOT)
dbutils.widgets.text("checkpoint_root", DEFAULT_CHECKPOINT_ROOT)
dbutils.widgets.text("bronze_catalog", "poc")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.dropdown("include_existing_files", "true", ["true", "false"])

LANDING_ROOT = dbutils.widgets.get("landing_root").rstrip("/")
CHECKPOINT_ROOT = dbutils.widgets.get("checkpoint_root").rstrip("/")
BRONZE_CATALOG = dbutils.widgets.get("bronze_catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
INCLUDE_EXISTING_FILES = dbutils.widgets.get("include_existing_files")

if not LANDING_ROOT.startswith("abfss://"):
    raise ValueError("landing_root must be an abfss:// path")

if not CHECKPOINT_ROOT.startswith("abfss://"):
    raise ValueError("checkpoint_root must be an abfss:// path")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{BRONZE_CATALOG}`.`{BRONZE_SCHEMA}`")

# COMMAND ----------


@dataclass(frozen=True)
class BronzeDataset:
    source_system: str
    entity: str
    relative_path: str
    target_table: str

    @property
    def dataset_key(self) -> str:
        return f"{self.source_system}_{self.entity}"

    @property
    def source_path(self) -> str:
        return f"{LANDING_ROOT}/{self.relative_path}"

    @property
    def schema_path(self) -> str:
        return f"{CHECKPOINT_ROOT}/_schemas/{self.dataset_key}"

    @property
    def checkpoint_path(self) -> str:
        return f"{CHECKPOINT_ROOT}/_streams/{self.dataset_key}"

    @property
    def full_table_name(self) -> str:
        return (
            f"`{BRONZE_CATALOG}`.`{BRONZE_SCHEMA}`.`{self.target_table}`"
        )


DATASETS = [
    BronzeDataset("crm", "customers", "crm/customers", "crm_customers"),
    BronzeDataset("erp", "customers", "erp/customers", "erp_customers"),
    BronzeDataset("erp", "orders", "erp/orders", "erp_orders"),
    BronzeDataset("erp", "payments", "erp/payments", "erp_payments"),
    BronzeDataset(
        "ecommerce",
        "customers",
        "ecommerce/customers",
        "ecommerce_customers",
    ),
    BronzeDataset(
        "ecommerce",
        "products",
        "ecommerce/products",
        "ecommerce_products",
    ),
    BronzeDataset(
        "ecommerce",
        "sellers",
        "ecommerce/sellers",
        "ecommerce_sellers",
    ),
    BronzeDataset(
        "loyalty",
        "customers",
        "loyalty/customers",
        "loyalty_customers",
    ),
]

# COMMAND ----------


def read_bronze_stream(dataset: BronzeDataset) -> DataFrame:
    """Create one Auto Loader stream with isolated schema tracking."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", dataset.schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.includeExistingFiles", INCLUDE_EXISTING_FILES)
        .option("rescuedDataColumn", "_rescued_data")
        .load(dataset.source_path)
    )


def add_ingestion_metadata(
    dataframe: DataFrame,
    dataset: BronzeDataset,
) -> DataFrame:
    """Add lineage metadata without changing source-system columns."""
    date_pattern = r"/(\d{4})/(\d{2})/(\d{2})(?:/|$)"

    return (
        dataframe
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_name", F.col("_metadata.file_name"))
        .withColumn("_source_file_size", F.col("_metadata.file_size"))
        .withColumn(
            "_source_file_modification_time",
            F.col("_metadata.file_modification_time"),
        )
        .withColumn("_source_system", F.lit(dataset.source_system))
        .withColumn("_source_entity", F.lit(dataset.entity))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "_source_date",
            F.to_date(
                F.concat_ws(
                    "-",
                    F.regexp_extract("_source_file", date_pattern, 1),
                    F.regexp_extract("_source_file", date_pattern, 2),
                    F.regexp_extract("_source_file", date_pattern, 3),
                )
            ),
        )
    )


def start_bronze_ingestion(dataset: BronzeDataset) -> StreamingQuery:
    """Run one idempotent, available-now Bronze ingestion."""
    bronze_df = add_ingestion_metadata(
        read_bronze_stream(dataset),
        dataset,
    )

    return (
        bronze_df.writeStream
        .queryName(f"bronze_{dataset.dataset_key}")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", dataset.checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(dataset.full_table_name)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start every source stream
# MAGIC
# MAGIC `availableNow=True` processes every unconsumed file and stops. Schedule
# MAGIC this notebook as a Databricks Workflow for recurring incremental loads.

# COMMAND ----------

queries = {
    dataset.dataset_key: start_bronze_ingestion(dataset)
    for dataset in DATASETS
}

for dataset_key, query in queries.items():
    query.awaitTermination()
    if query.exception() is not None:
        raise RuntimeError(
            f"Bronze ingestion failed for {dataset_key}: {query.exception()}"
        )

print("Bronze ingestion completed for every configured dataset.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

validation_rows = []

for dataset in DATASETS:
    table_df = spark.table(dataset.full_table_name)
    validation_rows.append(
        (
            dataset.source_system,
            dataset.entity,
            dataset.full_table_name.replace("`", ""),
            table_df.count(),
            table_df.select("_source_file").distinct().count(),
            table_df.select(F.max("_ingested_at")).first()[0],
        )
    )

validation_df = spark.createDataFrame(
    validation_rows,
    [
        "source_system",
        "entity",
        "bronze_table",
        "row_count",
        "source_file_count",
        "last_ingested_at",
    ],
)

display(validation_df.orderBy("source_system", "entity"))