# Databricks notebook source
from datetime import datetime, timezone
from itertools import combinations
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, FloatType, DoubleType, ArrayType, MapType, StructType


# COMMAND ----------

DEFAULTS = {
    "catalog": "lakehouse",
    "schema": "bronze",
    "tables": "crm_customers,ecommerce_customers,ecommerce_products,ecommerce_sellers,erp_customers,erp_orders,erp_payments,loyalty_customers",
    "max_columns": "2",
    "uniqueness_threshold": "0.999",
    "max_candidates": "10",
    "latest_snapshot_only": "true",
    "persist_results": "false",
    "results_table": "lakehouse.metadata.business_key_candidates",
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
persist_results = dbutils.widgets.get("persist_results").strip().lower() == "true"
results_table = dbutils.widgets.get("results_table").strip()

if max_columns not in (1, 2, 3):
    raise ValueError("max_columns must be 1, 2, or 3")
if not 0 < uniqueness_threshold <= 1:
    raise ValueError("uniqueness_threshold must be greater than 0 and at most 1")
if max_candidates < 1:
    raise ValueError("max_candidates must be at least 1")


# COMMAND ----------

TECHNICAL_COLUMNS = {
    "_rescued_data", "_source_file", "_source_file_name", "_source_file_size",
    "_source_file_modification_time", "_source_system", "_source_entity",
    "_source_date", "_ingested_at", "load_datetime", "batch_id", "record_source",
}

PREFERRED_PATTERNS = ("_id", "_code", "_number", "cpf", "cnpj", "email", "sku")
EXCLUDED_TYPES = (BooleanType, FloatType, DoubleType, ArrayType, MapType, StructType)


def quote_identifier(identifier):
    return f"`{identifier.replace('`', '``')}`"


def prepare_discovery_frame(df, latest_only=True):
    if latest_only and "_source_date" in df.columns:
        latest_date = df.agg(F.max("_source_date").alias("value")).first()["value"]
        if latest_date is not None:
            return df.filter(F.col("_source_date") == latest_date), latest_date
    return df, None


def eligible_columns(df):
    """Remove ingestion metadata and types that are unsafe or poor identifiers."""
    return [
        field.name
        for field in df.schema.fields
        if field.name not in TECHNICAL_COLUMNS
        and not isinstance(field.dataType, EXCLUDED_TYPES)
    ]


def profile_key(df, key_columns):
    null_condition = F.lit(False)
    for column in key_columns:
        null_condition = null_condition | F.col(column).isNull()

    row = df.agg(
        F.count(F.lit(1)).alias("total_rows"),
        F.countDistinct(F.struct(*[F.col(column) for column in key_columns])).alias("distinct_values"),
        F.sum(F.when(null_condition, 1).otherwise(0)).alias("null_rows"),
    ).first()
    total_rows = row["total_rows"]
    distinct_values = row["distinct_values"]
    null_rows = row["null_rows"] or 0
    return total_rows, distinct_values, null_rows


def snapshot_stability(df, key_columns, threshold):
    """Measure the fraction of snapshots in which the candidate remains valid."""
    if "_source_date" not in df.columns:
        return None, 0

    null_condition = F.lit(False)
    for column in key_columns:
        null_condition = null_condition | F.col(column).isNull()

    snapshots = (
        df.filter(F.col("_source_date").isNotNull())
        .groupBy("_source_date")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct(F.struct(*[F.col(column) for column in key_columns])).alias("distinct_values"),
            F.sum(F.when(null_condition, 1).otherwise(0)).alias("null_rows"),
        )
        .withColumn("uniqueness_ratio", F.col("distinct_values") / F.col("rows"))
        .collect()
    )
    if not snapshots:
        return None, 0

    valid_snapshots = sum(
        row["null_rows"] == 0 and row["uniqueness_ratio"] >= threshold
        for row in snapshots
    )
    return valid_snapshots / len(snapshots), len(snapshots)


def classify_candidate(uniqueness_ratio, null_rows, stability_ratio):
    stable = stability_ratio is None or stability_ratio == 1.0
    if uniqueness_ratio == 1.0 and null_rows == 0 and stable:
        return "STRONG"
    if uniqueness_ratio >= 0.999 and null_rows == 0:
        return "REVIEW"
    return "WEAK"


def explain_candidate(key_columns, uniqueness_ratio, null_rows, stability_ratio, snapshots):
    parts = [f"{uniqueness_ratio:.4%} unique", f"{null_rows} null rows"]
    if stability_ratio is not None:
        parts.append(f"valid in {stability_ratio:.1%} of {snapshots} snapshots")
    if any(any(pattern in column.lower() for pattern in PREFERRED_PATTERNS) for column in key_columns):
        parts.append("identifier-like column name")
    if len(key_columns) > 1:
        parts.append(f"composite key with {len(key_columns)} columns")
    return "; ".join(parts)


def remove_redundant_superkeys(results):
    """Drop combinations containing a smaller, perfectly unique candidate."""
    minimal = []
    for candidate in sorted(results, key=lambda item: (item["key_size"], -item["score"])):
        columns = set(candidate["columns"])
        redundant = any(
            existing["uniqueness_ratio"] == 1.0
            and set(existing["columns"]).issubset(columns)
            for existing in minimal
        )
        if not redundant:
            minimal.append(candidate)
    return minimal


def discover_business_keys(full_df, analysis_df, max_columns, threshold, limit):
    columns = eligible_columns(analysis_df)
    columns.sort(key=lambda name: (not any(p in name.lower() for p in PREFERRED_PATTERNS), name))
    candidates = []

    for key_size in range(1, max_columns + 1):
        for key_columns in combinations(columns, key_size):
            total_rows, distinct_values, null_rows = profile_key(analysis_df, key_columns)
            if total_rows == 0:
                continue
            uniqueness_ratio = distinct_values / total_rows
            if null_rows > 0 or uniqueness_ratio < threshold:
                continue

            stability_ratio, snapshot_count = snapshot_stability(full_df, key_columns, threshold)
            name_bonus = sum(
                any(pattern in column.lower() for pattern in PREFERRED_PATTERNS)
                for column in key_columns
            )
            stability_points = 0 if stability_ratio is None else stability_ratio * 20
            score = uniqueness_ratio * 100 + stability_points + name_bonus * 10 - key_size * 5

            candidates.append({
                "columns": list(key_columns),
                "key_size": key_size,
                "total_rows": total_rows,
                "distinct_values": distinct_values,
                "null_rows": null_rows,
                "uniqueness_ratio": float(uniqueness_ratio),
                "snapshot_count": snapshot_count,
                "stability_ratio": stability_ratio,
                "classification": classify_candidate(uniqueness_ratio, null_rows, stability_ratio),
                "score": float(score),
                "reason": explain_candidate(
                    key_columns, uniqueness_ratio, null_rows, stability_ratio, snapshot_count
                ),
            })

    minimal = remove_redundant_superkeys(candidates)
    return sorted(minimal, key=lambda item: (-item["score"], item["key_size"], item["columns"]))[:limit]


# COMMAND ----------

analysis_id = str(uuid4())
analyzed_at = datetime.now(timezone.utc)
all_results = []

for table in tables:
    full_table_name = ".".join(quote_identifier(part) for part in (catalog, schema, table))
    source_df = spark.table(full_table_name)
    analysis_df, snapshot_date = prepare_discovery_frame(source_df, latest_snapshot_only)
    candidates = discover_business_keys(
        source_df, analysis_df, max_columns, uniqueness_threshold, max_candidates
    )

    for rank, candidate in enumerate(candidates, start=1):
        all_results.append({
            "analysis_id": analysis_id,
            "analyzed_at": analyzed_at,
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "snapshot_date": snapshot_date,
            "rank": rank,
            "status": "SUGGESTED",
            **candidate,
        })

if all_results:
    result_df = spark.createDataFrame(all_results).orderBy("table", "rank")
    display(result_df)

    if persist_results:
        target_parts = results_table.split(".")
        if len(target_parts) != 3:
            raise ValueError("results_table must use catalog.schema.table format")
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(target_parts[0])}.{quote_identifier(target_parts[1])}"
        )
        quoted_results_table = ".".join(quote_identifier(part) for part in target_parts)
        result_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(quoted_results_table)
        print(f"Candidates appended to {results_table}")
else:
    print("No candidates met the configured thresholds.")

