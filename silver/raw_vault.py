from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from raw_vault_config import HUBS, LINKS, SAME_AS_SOURCES, SATELLITES


TECHNICAL_COLUMNS = {
    "_rescued_data", "_source_file", "_source_file_name", "_source_file_size",
    "_source_file_modification_time", "_source_system", "_source_entity",
    "_source_date", "_ingested_at",
}


class RawVaultLoader:
    def __init__(self, spark, bronze_catalog, bronze_schema, silver_catalog, silver_schema):
        self.spark = spark
        self.bronze_catalog = bronze_catalog
        self.bronze_schema = bronze_schema
        self.silver_catalog = silver_catalog
        self.silver_schema = silver_schema
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{silver_catalog}`.`{silver_schema}`")

    @staticmethod
    def normalize(column):
        return F.upper(F.trim(column.cast("string")))

    @classmethod
    def hash(cls, *columns):
        columns = [F.col(column) if isinstance(column, str) else column for column in columns]
        return F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(cls.normalize(column), F.lit("^^")) for column in columns],
            ),
            256,
        )

    def bronze(self, table):
        return self.spark.table(
            f"`{self.bronze_catalog}`.`{self.bronze_schema}`.`{table}`"
        )

    def target(self, table):
        return f"`{self.silver_catalog}`.`{self.silver_schema}`.`{table}`"

    @staticmethod
    def load_time(df):
        return (
            F.col("_ingested_at").cast("timestamp")
            if "_ingested_at" in df.columns
            else F.current_timestamp()
        )

    @staticmethod
    def record_source(df, fallback):
        return (
            F.coalesce(F.col("_source_system"), F.lit(fallback))
            if "_source_system" in df.columns
            else F.lit(fallback)
        )

    def merge_insert_only(self, df, table, keys):
        unquoted = f"{self.silver_catalog}.{self.silver_schema}.{table}"
        if not self.spark.catalog.tableExists(unquoted):
            df.write.format("delta").mode("overwrite").saveAsTable(self.target(table))
            return
        condition = " AND ".join(
            f"target.`{key}` <=> source.`{key}`" for key in keys
        )
        (
            DeltaTable.forName(self.spark, unquoted)
            .alias("target")
            .merge(df.alias("source"), condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

    def load_hub(self, object_name):
        config = HUBS[object_name]
        frames = []
        for table, column, context in config["sources"]:
            df = self.bronze(table)
            if column not in df.columns:
                continue
            bk = self.normalize(F.col(column))
            frames.append(
                df.select(
                    self.hash(F.lit(context), bk).alias(config["hk"]),
                    bk.alias(config["bk"]),
                    F.lit(context).alias("business_key_context"),
                    self.load_time(df).alias("load_datetime"),
                    self.record_source(df, context.lower()).alias("record_source"),
                )
            )
        if not frames:
            raise ValueError(f"No valid sources found for {object_name}")
        staged = (
            reduce(lambda left, right: left.unionByName(right), frames)
            .filter(F.col(config["bk"]).isNotNull() & (F.col(config["bk"]) != ""))
            .groupBy(config["hk"], config["bk"], "business_key_context")
            .agg(
                F.min("load_datetime").alias("load_datetime"),
                F.min("record_source").alias("record_source"),
            )
        )
        self.merge_insert_only(staged, object_name, [config["hk"]])

    def load_link(self, object_name):
        config = LINKS[object_name]
        df = self.bronze(config["source"])
        if config["kind"] == "customer_order":
            frames = []
            for column, context in [
                ("erp_customer_id", "ERP"),
                ("customer_id", "ECOMMERCE"),
            ]:
                if column in df.columns:
                    frames.append(
                        df.filter(F.col(column).isNotNull()).select(
                            self.hash(F.lit(context), F.col(column)).alias("customer_hk"),
                            self.hash(F.lit("ERP"), F.col("order_id")).alias("order_hk"),
                            self.load_time(df).alias("load_datetime"),
                            self.record_source(df, "erp").alias("record_source"),
                        )
                    )
            staged = reduce(lambda left, right: left.unionByName(right), frames)
            staged = staged.withColumn(
                "customer_order_hk", self.hash("customer_hk", "order_hk")
            ).dropDuplicates(["customer_order_hk"])
            self.merge_insert_only(staged, object_name, ["customer_order_hk"])
            return

        staged = (
            df.filter(F.col("order_id").isNotNull() & F.col("payment_sequential").isNotNull())
            .select(
                self.hash(F.lit("ERP"), F.col("order_id")).alias("order_hk"),
                self.normalize(F.col("payment_sequential")).alias("payment_sequence_bk"),
                self.load_time(df).alias("load_datetime"),
                self.record_source(df, "erp").alias("record_source"),
            )
            .withColumn("order_payment_hk", self.hash("order_hk", "payment_sequence_bk"))
            .dropDuplicates(["order_payment_hk"])
        )
        self.merge_insert_only(staged, object_name, ["order_payment_hk"])

    def load_satellite(self, object_name):
        table, bk_column, context, parent_hk, extra_excluded = SATELLITES[object_name]
        df = self.bronze(table)
        if context == "PAYMENT_OCCURRENCE":
            df = df.withColumn(
                parent_hk,
                self.hash(
                    self.hash(F.lit("ERP"), F.col("order_id")),
                    self.normalize(F.col("payment_sequential")),
                ),
            )
        else:
            df = df.withColumn(parent_hk, self.hash(F.lit(context), F.col(bk_column)))

        excluded = TECHNICAL_COLUMNS | {bk_column, *extra_excluded}
        attributes = [column for column in df.columns if column not in excluded | {parent_hk}]
        staged = (
            df.select(
                parent_hk,
                self.load_time(df).alias("load_datetime"),
                self.record_source(df, context.lower()).alias("record_source"),
                *attributes,
            )
            .filter(F.col(parent_hk).isNotNull())
            .withColumn("hashdiff", self.hash(*attributes))
            .dropDuplicates([parent_hk, "load_datetime"])
        )
        window = Window.partitionBy(parent_hk).orderBy("load_datetime")
        staged = (
            staged.withColumn("_previous_hashdiff", F.lag("hashdiff").over(window))
            .filter(
                F.col("_previous_hashdiff").isNull()
                | (F.col("_previous_hashdiff") != F.col("hashdiff"))
            )
            .drop("_previous_hashdiff")
        )
        self.merge_insert_only(staged, object_name, [parent_hk, "load_datetime"])

    def load_customer_same_as(self):
        frames = []
        for table, id_column, context, evidence_column, rule in SAME_AS_SOURCES:
            df = self.bronze(table)
            frames.append(
                df.filter(F.col(id_column).isNotNull() & F.col(evidence_column).isNotNull())
                .select(
                    self.hash(F.lit(context), F.col(id_column)).alias("customer_hk"),
                    F.lit(context).alias("business_key_context"),
                    self.normalize(F.col(evidence_column)).alias("match_value"),
                    F.lit(rule).alias("match_rule"),
                    self.load_time(df).alias("load_datetime"),
                    self.record_source(df, context.lower()).alias("record_source"),
                )
            )
        evidence = reduce(lambda left, right: left.unionByName(right), frames).dropDuplicates(
            ["customer_hk", "match_rule", "match_value"]
        )
        left, right = evidence.alias("left"), evidence.alias("right")
        staged = (
            left.join(
                right,
                (F.col("left.match_rule") == F.col("right.match_rule"))
                & (F.col("left.match_value") == F.col("right.match_value"))
                & (F.col("left.business_key_context") != F.col("right.business_key_context"))
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
                F.greatest(
                    F.col("left.load_datetime"), F.col("right.load_datetime")
                ).alias("load_datetime"),
                F.concat_ws(
                    "+", F.col("left.record_source"), F.col("right.record_source")
                ).alias("record_source"),
            )
            .withColumn(
                "same_as_customer_hk",
                self.hash("customer_hk_left", "customer_hk_right", "match_rule"),
            )
            .dropDuplicates(["same_as_customer_hk"])
        )
        self.merge_insert_only(staged, "same_as_link_customer", ["same_as_customer_hk"])


