# Databricks notebook source
from raw_vault import RawVaultLoader

DEFAULTS = {
    "object_name": "hub_customer",
    "bronze_catalog": "lakehouse",
    "bronze_schema": "bronze",
    "silver_catalog": "lakehouse",
    "silver_schema": "silver",
}
for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)
params = {name: dbutils.widgets.get(name).strip() for name in DEFAULTS}

RawVaultLoader(
    spark, params["bronze_catalog"], params["bronze_schema"],
    params["silver_catalog"], params["silver_schema"],
).load_hub(params["object_name"])


