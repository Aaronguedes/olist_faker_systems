# Business Key Discovery — Data Dictionary

This document describes the output of `04_discover_business_keys.py`. The notebook suggests candidates; it does not replace business confirmation.

| Column | Type | Description | Example |
|---|---|---|---|
| `analysis_id` | string/UUID | Identifier shared by every result produced in one notebook execution. | `56d681da-36d5-4e18-99f6-012ab50df17c` |
| `analyzed_at` | timestamp | UTC timestamp at which the analysis started. | `2026-07-22 22:45:00` |
| `catalog` | string | Unity Catalog catalog containing the analyzed table. | `lakehouse` |
| `schema` | string | Schema containing the analyzed table. | `bronze` |
| `table` | string | Name of the analyzed source table. | `erp_payments` |
| `snapshot_date` | date | Most recent `_source_date` used for current-snapshot profiling; null when all rows are analyzed. | `2026-07-21` |
| `rank` | integer | Candidate position within the table after sorting by score. | `1` |
| `status` | string | Governance state. The notebook writes `SUGGESTED`; reviewers may later set `CONFIRMED` or `REJECTED`. | `SUGGESTED` |
| `columns` | array<string> | Ordered columns forming the candidate BK. | `["order_id", "payment_sequential"]` |
| `key_size` | integer | Number of columns in the candidate. | `2` |
| `total_rows` | long | Rows evaluated in the current analysis frame. | `103886` |
| `distinct_values` | long | Distinct values or distinct column combinations found. | `103886` |
| `null_rows` | long | Rows where at least one candidate column is null. Candidates with nulls are rejected before output. | `0` |
| `uniqueness_ratio` | double | `distinct_values / total_rows`. A value of `1.0` means complete uniqueness. | `1.0` |
| `snapshot_count` | integer | Number of non-null `_source_date` snapshots used in the stability test. | `3` |
| `stability_ratio` | double | Fraction of snapshots where the candidate meets the configured uniqueness threshold and has no nulls. | `1.0` |
| `classification` | string | `STRONG` for perfectly unique and stable candidates, `REVIEW` for near-unique candidates, otherwise `WEAK`. | `STRONG` |
| `score` | double | Ranking heuristic: uniqueness points + up to 20 stability points + name bonus − key-size penalty. It is not a probability. | `120.0` |
| `reason` | string | Human-readable evidence supporting the recommendation. | `100.0000% unique; 0 null rows; valid in 100.0% of 3 snapshots; identifier-like column name; composite key with 2 columns` |

## Score formula

```text
score = uniqueness_ratio * 100
      + stability_ratio * 20        (when snapshots exist)
      + identifier_name_count * 10
      - key_size * 5
```

A higher score places a candidate earlier in the review list. It does not prove that the candidate is a business key.

## Minimal-key behavior

If a perfectly unique smaller key exists, combinations containing it are removed. For example, when `loyalty_customer_id` is unique, the output omits redundant superkeys such as `loyalty_customer_id + tier`.

## Persistence

Set the `persist_results` widget to `true` to append results to the Delta table configured by `results_table`. The default is `lakehouse.metadata.business_key_candidates`.

