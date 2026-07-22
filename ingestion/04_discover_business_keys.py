# Databricks notebook source
from itertools import combinations

from pyspark.sql import functions as F


# COMMAND ----------

# Widgets make the notebook reusable from an interactive run or a Databricks job.
DEFAULTS = {
    "catalog": "lakehouse",
    "schema": "bronze",
    "tables": "crm_customers,ecommerce_customers,ecommerce_products,ecommerce_sellers,erp_customers,erp_orders,erp_payments,loyalty_customers",
    "max_columns": "2",
    "uniqueness_threshold": "0.999",
    "max_candidates": "10",
    "latest_snapshot_only": "true",
}

for widget_name, default_value in DEFAULTS.items():
    dbutils.widgets.text(widget_name, default_value)

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
tables = [name.strip() for name in dbutils.widgets.get("tables").split(",") if name.strip()]
max_columns = int(dbutils.widgets.get("max_columns"))
uniqueness_threshold = float(dbutils.widgets.get("uniqueness_threshold"))
max_candidates = int(dbutils.widgets.get("max_candidates"))
latest_snapshot_only = dbutils.widgets.get("latest_snapshot_only").strip().lower() == "true"

if max_columns not in (1, 2, 3):
    raise ValueError("max_columns must be 1, 2, or 3")
if not 0 < uniqueness_threshold <= 1:
    raise ValueError("uniqueness_threshold must be greater than 0 and at most 1")
if max_candidates < 1:
    raise ValueError("max_candidates must be at least 1")


# COMMAND ----------

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
    "load_datetime",
    "batch_id",
    "record_source",
}

PREFERRED_PATTERNS = (
    "_id",
    "_code",
    "_number",
    "cpf",
    "cnpj",
    "email",
    "sku",
)


def quote_identifier(identifier):
    """Quote catalog, schema, table, and column names safely."""
    return f"`{identifier.replace('`', '``')}`"


def prepare_discovery_frame(df, latest_only=True):
    """Optionally analyze only the most recent landing snapshot."""
    if latest_only and "_source_date" in df.columns:
        latest_date = df.agg(F.max("_source_date").alias("value")).first()["value"]
        if latest_date is not None:
            return df.filter(F.col("_source_date") == latest_date), latest_date
    return df, None


def discover_business_keys(
    df,
    max_columns=2,
    uniqueness_threshold=1.0,
    max_candidates=20,
):
    """Return ranked BK candidates based on completeness, uniqueness, and names."""
    total_rows = df.count()
    if total_rows == 0:
        return []

    candidate_columns = [
        column for column in df.columns if column not in TECHNICAL_COLUMNS
    ]
    candidate_columns.sort(
        key=lambda column: (
            not any(pattern in column.lower() for pattern in PREFERRED_PATTERNS),
            column,
        )
    )

    results = []

    for key_size in range(1, max_columns + 1):
        for key_columns in combinations(candidate_columns, key_size):
            null_condition = F.lit(False)
            for column in key_columns:
                null_condition = null_condition | F.col(column).isNull()

            metrics = df.agg(
                F.countDistinct(
                    F.struct(*[F.col(column) for column in key_columns])
                ).alias("distinct_values"),
                F.sum(F.when(null_condition, 1).otherwise(0)).alias("null_rows"),
            ).first()

            distinct_values = metrics["distinct_values"]
            null_rows = metrics["null_rows"] or 0
            uniqueness_ratio = distinct_values / total_rows

            if null_rows == 0 and uniqueness_ratio >= uniqueness_threshold:
                name_bonus = sum(
                    any(pattern in column.lower() for pattern in PREFERRED_PATTERNS)
                    for column in key_columns
                )
                score = uniqueness_ratio * 100 + name_bonus * 10 - key_size * 5

                results.append(
                    {
                        "columns": list(key_columns),
                        "key_size": key_size,
                        "total_rows": total_rows,
                        "distinct_values": distinct_values,
                        "null_rows": null_rows,
                        "uniqueness_ratio": float(uniqueness_ratio),
                        "score": float(score),
                    }
                )

    return sorted(
        results,
        key=lambda result: (-result["score"], result["key_size"], result["columns"]),
    )[:max_candidates]


# COMMAND ----------

all_results = []

for table in tables:
    full_table_name = ".".join(
        quote_identifier(part) for part in (catalog, schema, table)
    )
    source_df = spark.table(full_table_name)
    discovery_df, snapshot_date = prepare_discovery_frame(
        source_df,
        latest_only=latest_snapshot_only,
    )

    candidates = discover_business_keys(
        discovery_df,
        max_columns=max_columns,
        uniqueness_threshold=uniqueness_threshold,
        max_candidates=max_candidates,
    )

    for rank, candidate in enumerate(candidates, start=1):
        all_results.append(
            {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "snapshot_date": snapshot_date,
                "rank": rank,
                **candidate,
            }
        )

if all_results:
    result_df = spark.createDataFrame(all_results).orderBy("table", "rank")
    display(result_df)
else:
    print("No candidates met the configured thresholds.")


