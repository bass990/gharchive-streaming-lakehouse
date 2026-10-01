"""Debezium CDC topic -> SCD2 dimension in Iceberg via MERGE INTO.

Debezium (Postgres, pgoutput) publishes one JSON message per row change:
  {"op": "c|u|d|r", "before": {...}, "after": {...}, "source": {"lsn": ..., "ts_ms": ...}, "ts_ms": ...}

Each micro-batch:
  1. keep the LAST change per key within the batch (ordered by LSN), so a row
     updated three times in one batch produces one new SCD2 version;
  2. close the current version of every key that changed (valid_to = change time);
  3. insert the new current version (unless the op was a delete).
Idempotent because the MERGE is keyed on (repo_name, source_lsn): re-running a
batch after a crash matches existing rows and changes nothing.
"""
from __future__ import annotations

import sys

from common import CATALOG, DB, checkpoint_root, ensure_tables, env, spark_session
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

ROW = T.StructType([
    T.StructField("repo_name", T.StringType()), T.StructField("tier", T.StringType()),
    T.StructField("owner_team", T.StringType()), T.StructField("notes", T.StringType()),
    T.StructField("updated_at", T.StringType()),
])
ENVELOPE = T.StructType([
    T.StructField("op", T.StringType()), T.StructField("before", ROW), T.StructField("after", ROW),
    T.StructField("source", T.StructType([T.StructField("lsn", T.LongType()), T.StructField("ts_ms", T.LongType())])),
    T.StructField("ts_ms", T.LongType()),
])

TARGET = f"{CATALOG}.{DB}.repo_watchlist_scd2"
STAGING = f"{CATALOG}.{DB}.cdc_staging"


def apply_batch(spark: SparkSession, batch: DataFrame, epoch: int) -> None:
    if batch.isEmpty():
        return
    changes = (batch.select(F.from_json(F.col("value").cast("string"), ENVELOPE).alias("e"))
               .select("e.*")
               .withColumn("key", F.coalesce(F.col("after.repo_name"), F.col("before.repo_name")))
               .withColumn("change_ts", (F.col("source.ts_ms") / 1000).cast("timestamp"))
               .withColumn("rn", F.row_number().over(Window.partitionBy("key").orderBy(F.col("source.lsn").desc())))
               .filter("rn = 1").drop("rn"))
    # a staging Iceberg table rather than a temp view: with the Iceberg catalog as the session default,
    # SQL cannot resolve session temp views, and a staging table also leaves an audit trail per batch
    changes.select("key", "op", "change_ts", F.col("source.lsn").alias("lsn"),
                   F.col("after.tier").alias("tier"), F.col("after.owner_team").alias("owner_team"), F.col("after.notes").alias("notes")
                   ).writeTo(STAGING).createOrReplace()
    # 1. close current versions of changed keys
    spark.sql(f"""
        MERGE INTO {TARGET} t
        USING (SELECT key AS repo_name, change_ts, lsn FROM {STAGING}) c
        ON t.repo_name = c.repo_name AND t.is_current = true AND t.source_lsn < c.lsn
        WHEN MATCHED THEN UPDATE SET t.valid_to = c.change_ts, t.is_current = false
    """)
    # 2. insert new current versions (not for deletes); MERGE on (key, lsn) makes replays no-ops
    spark.sql(f"""
        MERGE INTO {TARGET} t
        USING (
            SELECT key AS repo_name, tier, owner_team, notes,
                   change_ts AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to, true AS is_current,
                   op AS source_op, lsn AS source_lsn
            FROM {STAGING} WHERE op <> 'd'
        ) c
        ON t.repo_name = c.repo_name AND t.source_lsn = c.source_lsn
        WHEN NOT MATCHED THEN INSERT *
    """)
    n = changes.count()
    print(f"epoch {epoch}: applied {n} row changes")


def main() -> int:
    spark = spark_session("gh-cdc-merge")
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)
    topic = env("TOPIC_CDC", "cdc.public.repo_watchlist")
    src = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP", "redpanda:9092"))
           .option("subscribe", topic).option("startingOffsets", "earliest").option("failOnDataLoss", "false").load())
    q = (src.writeStream.foreachBatch(lambda df, epoch: apply_batch(spark, df, epoch))
         .option("checkpointLocation", f"{checkpoint_root()}/cdc_merge")
         .trigger(processingTime=env("TRIGGER", "20 seconds")).start())
    print(f"cdc: {topic} -> {TARGET} (SCD2)")
    q.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
