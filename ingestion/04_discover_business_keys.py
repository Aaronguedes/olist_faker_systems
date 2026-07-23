# Databricks notebook source
from datetime import datetime, timezone
from itertools import combinations
import re
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
    "combination_candidate_limit": "12",
    "aggregation_batch_size": "50",
    "approx_rsd": "0.01",
    "latest_snapshot_only": "true",
    "persist_results": "false",
    "results_table": "lakehouse.metadata.business_key_candidates",
    "use_ai_interpretation": "true",
    "ai_model_endpoint": "databricks-gpt-5-mini",
    "persist_ai_results": "false",
    "ai_results_table": "lakehouse.metadata.business_key_recommendations",
}

for widget_name, default_value in DEFAULTS.items():
    dbutils.widgets.text(widget_name, default_value)

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
tables = [name.strip() for name in dbutils.widgets.get("tables").split(",") if name.strip()]
max_columns = int(dbutils.widgets.get("max_columns"))
uniqueness_threshold = float(dbutils.widgets.get("uniqueness_threshold"))
max_candidates = int(dbutils.widgets.get("max_candidates"))
combination_candidate_limit = int(dbutils.widgets.get("combination_candidate_limit"))
aggregation_batch_size = int(dbutils.widgets.get("aggregation_batch_size"))
approx_rsd = float(dbutils.widgets.get("approx_rsd"))
latest_snapshot_only = dbutils.widgets.get("latest_snapshot_only").strip().lower() == "true"
persist_results = dbutils.widgets.get("persist_results").strip().lower() == "true"
results_table = dbutils.widgets.get("results_table").strip()
use_ai_interpretation = dbutils.widgets.get("use_ai_interpretation").strip().lower() == "true"
ai_model_endpoint = dbutils.widgets.get("ai_model_endpoint").strip()
persist_ai_results = dbutils.widgets.get("persist_ai_results").strip().lower() == "true"
ai_results_table = dbutils.widgets.get("ai_results_table").strip()

if max_columns not in (1, 2, 3):
    raise ValueError("max_columns must be 1, 2, or 3")
if not 0 < uniqueness_threshold <= 1:
    raise ValueError("uniqueness_threshold must be greater than 0 and at most 1")
if max_candidates < 1:
    raise ValueError("max_candidates must be at least 1")
if combination_candidate_limit < 2 or aggregation_batch_size < 1:
    raise ValueError("combination_candidate_limit must be at least 2 and aggregation_batch_size at least 1")
if not 0 < approx_rsd <= 0.1:
    raise ValueError("approx_rsd must be greater than 0 and at most 0.1")
if not re.fullmatch(r"[A-Za-z0-9_.:-]+", ai_model_endpoint):
    raise ValueError("ai_model_endpoint contains unsupported characters")


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


def null_condition(key_columns):
    condition = F.lit(False)
    for column in key_columns:
        condition = condition | F.col(column).isNull()
    return condition


def profile_keys(df, key_sets, total_rows, batch_size, approximate=False, rsd=0.01):
    """Profile many candidates per Spark aggregation instead of scanning once per key."""
    profiles = {}
    for offset in range(0, len(key_sets), batch_size):
        batch = key_sets[offset:offset + batch_size]
        expressions = []
        for index, key_columns in enumerate(batch):
            value = F.struct(*[F.col(column) for column in key_columns])
            distinct = F.approx_count_distinct(value, rsd) if approximate else F.countDistinct(value)
            expressions.extend([
                distinct.alias(f"d_{index}"),
                F.sum(F.when(null_condition(key_columns), 1).otherwise(0)).alias(f"n_{index}"),
            ])
        row = df.agg(*expressions).first()
        for index, key_columns in enumerate(batch):
            profiles[tuple(key_columns)] = {
                "total_rows": total_rows,
                "distinct_values": row[f"d_{index}"],
                "null_rows": row[f"n_{index}"] or 0,
            }
    return profiles


def snapshot_stability_batch(df, key_sets, threshold):
    """Profile every finalist across all snapshots in one grouped aggregation."""
    if "_source_date" not in df.columns or not key_sets:
        return {tuple(key): (None, 0) for key in key_sets}

    expressions = [F.count(F.lit(1)).alias("snapshot_rows")]
    for index, key_columns in enumerate(key_sets):
        value = F.struct(*[F.col(column) for column in key_columns])
        expressions.extend([
            F.countDistinct(value).alias(f"d_{index}"),
            F.sum(F.when(null_condition(key_columns), 1).otherwise(0)).alias(f"n_{index}"),
        ])

    snapshots = (
        df.filter(F.col("_source_date").isNotNull())
        .groupBy("_source_date")
        .agg(*expressions)
        .collect()
    )
    if not snapshots:
        return {tuple(key): (None, 0) for key in key_sets}

    result = {}
    for index, key_columns in enumerate(key_sets):
        valid = sum(
            (row[f"n_{index}"] or 0) == 0
            and row[f"d_{index}"] / row["snapshot_rows"] >= threshold
            for row in snapshots
        )
        result[tuple(key_columns)] = (valid / len(snapshots), len(snapshots))
    return result


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


def discover_business_keys(
    full_df, analysis_df, max_columns, threshold, limit,
    combination_limit, batch_size, rsd,
):
    columns = eligible_columns(analysis_df)
    columns.sort(key=lambda name: (not any(p in name.lower() for p in PREFERRED_PATTERNS), name))
    total_rows = analysis_df.count()
    if total_rows == 0:
        return []

    tolerance = max(0.02, rsd * 3)
    single_keys = [(column,) for column in columns]
    approximate_singles = profile_keys(
        analysis_df, single_keys, total_rows, batch_size, approximate=True, rsd=rsd
    )
    shortlisted_singles = [
        key for key, profile in approximate_singles.items()
        if profile["null_rows"] == 0
        and profile["distinct_values"] / total_rows >= threshold - tolerance
    ]
    exact_profiles = profile_keys(
        analysis_df, shortlisted_singles, total_rows, batch_size
    )

    perfect_singletons = {
        key for key, profile in exact_profiles.items()
        if profile["null_rows"] == 0 and profile["distinct_values"] == total_rows
    }

    approximate_ratios = {
        key[0]: profile["distinct_values"] / total_rows
        for key, profile in approximate_singles.items()
    }
    combination_columns = sorted(
        [
            column for column in columns
            if any(pattern in column.lower() for pattern in PREFERRED_PATTERNS)
            or approximate_ratios[column] >= 0.01
        ],
        key=lambda column: (
            not any(pattern in column.lower() for pattern in PREFERRED_PATTERNS),
            -approximate_ratios[column],
            column,
        ),
    )[:combination_limit]

    composite_keys = []
    for key_size in range(2, max_columns + 1):
        composite_keys.extend(
            key for key in combinations(combination_columns, key_size)
            if not any(set(single).issubset(key) for single in perfect_singletons)
        )

    approximate_composites = profile_keys(
        analysis_df, composite_keys, total_rows, batch_size, approximate=True, rsd=rsd
    )
    shortlisted_composites = [
        key for key, profile in approximate_composites.items()
        if profile["null_rows"] == 0
        and profile["distinct_values"] / total_rows >= threshold - tolerance
    ]
    exact_profiles.update(
        profile_keys(analysis_df, shortlisted_composites, total_rows, batch_size)
    )

    candidates = []
    for key_columns, profile in exact_profiles.items():
        distinct_values = profile["distinct_values"]
        null_rows = profile["null_rows"]
        uniqueness_ratio = distinct_values / total_rows
        if null_rows > 0 or uniqueness_ratio < threshold:
            continue
        key_size = len(key_columns)
        name_bonus = sum(
            any(pattern in column.lower() for pattern in PREFERRED_PATTERNS)
            for column in key_columns
        )
        preliminary_score = uniqueness_ratio * 100 + name_bonus * 10 - key_size * 5
        candidates.append({
            "columns": list(key_columns),
            "key_size": key_size,
            "total_rows": total_rows,
            "distinct_values": distinct_values,
            "null_rows": null_rows,
            "uniqueness_ratio": float(uniqueness_ratio),
            "score": float(preliminary_score),
        })

    minimal = remove_redundant_superkeys(candidates)
    finalists = sorted(minimal, key=lambda item: -item["score"])[:limit]
    stability = snapshot_stability_batch(
        full_df, [candidate["columns"] for candidate in finalists], threshold
    )

    for candidate in finalists:
        key_columns = candidate["columns"]
        key_size = candidate["key_size"]
        uniqueness_ratio = candidate["uniqueness_ratio"]
        null_rows = candidate["null_rows"]
        stability_ratio, snapshot_count = stability[tuple(key_columns)]
        name_bonus = sum(
            any(pattern in column.lower() for pattern in PREFERRED_PATTERNS)
            for column in key_columns
        )
        stability_points = 0 if stability_ratio is None else stability_ratio * 20
        score = uniqueness_ratio * 100 + stability_points + name_bonus * 10 - key_size * 5

        candidate.update({
            "snapshot_count": snapshot_count,
            "stability_ratio": stability_ratio,
            "classification": classify_candidate(uniqueness_ratio, null_rows, stability_ratio),
            "score": float(score),
            "reason": explain_candidate(
                key_columns, uniqueness_ratio, null_rows, stability_ratio, snapshot_count
            ),
        })

    return sorted(finalists, key=lambda item: (-item["score"], item["key_size"], item["columns"]))


# COMMAND ----------

analysis_id = str(uuid4())
analyzed_at = datetime.now(timezone.utc)
all_results = []
result_df = None

for table in tables:
    full_table_name = ".".join(quote_identifier(part) for part in (catalog, schema, table))
    source_df = spark.table(full_table_name)
    analysis_df, snapshot_date = prepare_discovery_frame(source_df, latest_snapshot_only)
    candidates = discover_business_keys(
        source_df, analysis_df, max_columns, uniqueness_threshold, max_candidates,
        combination_candidate_limit, aggregation_batch_size, approx_rsd,
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


# COMMAND ----------

# AI interpretation sends only candidate metadata. Source values and PII are
# never included in the prompt. One request is made per table, not per row.
if use_ai_interpretation:
    candidate_fields = [
        "columns", "key_size", "classification", "score", "uniqueness_ratio",
        "null_rows", "snapshot_count", "stability_ratio", "reason",
    ]

    if result_df is not None:
        candidates_by_table = (
            result_df
            .groupBy("catalog", "schema", "table")
            .agg(
                F.to_json(
                    F.collect_list(F.struct(*[F.col(name) for name in candidate_fields]))
                ).alias("candidates_json")
            )
        )
    else:
        candidates_by_table = spark.createDataFrame(
            [], "catalog STRING, schema STRING, table STRING, candidates_json STRING"
        )

    expected_tables_df = (
        spark.createDataFrame([(table,) for table in tables], "table STRING")
        .withColumn("catalog", F.lit(catalog))
        .withColumn("schema", F.lit(schema))
    )

    ai_input_df = (
        expected_tables_df
        .join(candidates_by_table, ["catalog", "schema", "table"], "left")
        .withColumn("candidates_json", F.coalesce(F.col("candidates_json"), F.lit("[]")))
        .withColumn(
            "prompt",
            F.concat(
                F.lit("""You are a senior data architect reviewing candidate Business Keys.

Select the most likely Business Key using only the supplied candidate metadata.

Rules:
1. Prefer the smallest stable key.
2. Prefer a source-system identifier such as customer_id, order_id, product_id, seller_id, or payment_id.
3. Never select ingestion metadata.
4. Reject mutable attributes such as email, address, name, points, income, status, login timestamp, and purchase timestamp.
5. A technically unique value is not necessarily a Business Key.
6. For an orders table, prefer order_id over customer_id.
7. Use a composite key only when no smaller candidate identifies the entity.
8. One snapshot does not prove long-term stability; mention this as a warning.
9. Select only columns present in the candidate list. Never invent a column or combination.
10. If no suitable candidate exists, return an empty recommended_columns array and decision NEEDS_REVIEW.

Table: """),
                F.col("catalog"), F.lit("."), F.col("schema"), F.lit("."), F.col("table"),
                F.lit("\n\nCandidates:\n"), F.col("candidates_json"),
            ),
        )
    )

    endpoint_literal = "'" + ai_model_endpoint.replace("'", "''") + "'"
    ai_response_format = (
        "STRUCT<bk_recommendation:STRUCT<recommended_columns:ARRAY<STRING>,"
        "decision:STRING,confidence:STRING,explanation:STRING,"
        "warnings:ARRAY<STRING>>>"
    )

    ai_scored_df = (
        ai_input_df
        .withColumn(
            "ai_query_result",
            F.expr(f"""
                ai_query(
                    {endpoint_literal},
                    prompt,
                    responseFormat => '{ai_response_format}',
                    failOnError => false,
                    modelParameters => named_struct('temperature', 0.0)
                )
            """),
        )
        # With failOnError=false, this Azure runtime exposes the successful
        # structured response as a JSON string in result. Parse it explicitly.
        .withColumn(
            "parsed_ai_result",
            F.from_json(F.col("ai_query_result.result"), ai_response_format),
        )
        .withColumn(
            "ai_parse_error",
            F.when(
                F.col("ai_query_result.result").isNotNull()
                & F.col("parsed_ai_result").isNull(),
                F.lit("AI result was not valid JSON for the configured response schema"),
            ),
        )
    )

    final_bk_df = (
        ai_scored_df
        .select(
            F.lit(analysis_id).alias("analysis_id"),
            F.lit(analyzed_at).alias("analyzed_at"),
            "catalog", "schema", "table",
            F.col("parsed_ai_result.bk_recommendation.recommended_columns").alias("recommended_bk"),
            F.col("parsed_ai_result.bk_recommendation.decision").alias("ai_decision"),
            F.col("parsed_ai_result.bk_recommendation.confidence").alias("confidence"),
            F.col("parsed_ai_result.bk_recommendation.explanation").alias("explanation"),
            F.col("parsed_ai_result.bk_recommendation.warnings").alias("warnings"),
            F.coalesce(
                F.col("ai_query_result.errorMessage"),
                F.col("ai_parse_error"),
            ).alias("ai_error"),
        )
        .withColumn(
            "status",
            F.when(F.col("ai_error").isNotNull(), F.lit("AI_ERROR"))
            .when(
                F.coalesce(F.size("recommended_bk"), F.lit(0)) == 0,
                F.lit("NEEDS_REVIEW"),
            )
            .otherwise(F.lit("AI_RECOMMENDED")),
        )
        .orderBy("table")
        .cache()
    )

    # Materialize once so display and optional persistence do not call the
    # serving endpoint more than once for the same table.
    final_bk_df.count()
    display(final_bk_df)

    if persist_ai_results:
        ai_target_parts = ai_results_table.split(".")
        if len(ai_target_parts) != 3:
            raise ValueError("ai_results_table must use catalog.schema.table format")
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{quote_identifier(ai_target_parts[0])}.{quote_identifier(ai_target_parts[1])}"
        )
        quoted_ai_results_table = ".".join(quote_identifier(part) for part in ai_target_parts)
        (
            final_bk_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quoted_ai_results_table)
        )
        print(f"AI recommendations appended to {ai_results_table}")
else:
    print("AI interpretation disabled. Set use_ai_interpretation=true to enable it.")

