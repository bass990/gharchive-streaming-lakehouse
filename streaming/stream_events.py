"""Kafka (Avro) -> Iceberg, exactly once, with event-time windows.

Exactly-once, end to end:
  * the producer is idempotent (no broker-side duplicates on retry);
  * Spark tracks Kafka offsets in the checkpoint and Iceberg commits are
    atomic; a micro-batch that fails after writing but before committing is
    replayed and Iceberg sees one commit per epoch, never two;
  * `dropDuplicatesWithinWatermark("id")` removes the one class of duplicates
    the above cannot: the same event re-published by a replay / backfill.
  * aggregates use append output mode, so a window is written once, when the
    watermark passes its end. Late events beyond the watermark are counted in
    the `late_events` metric, not silently dropped.

Two queries share one Kafka source (one consumer group each; that is normal):
  events            every event, partitioned by hours(created_at), type
  repo_activity_5m  5-minute tumbling windows per (repo, type)
"""
from __future__ import annotations

import sys

from common import CATALOG, DB, checkpoint_root, ensure_tables, env, spark_session, target
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro


def main() -> int:
    spark = spark_session("gh-stream-events")
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    with open("/app/schemas/gh_event.avsc", encoding="utf-8") as f:
        schema_json = f.read()

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP", "redpanda:9092"))
           .option("subscribe", env("TOPIC_EVENTS", "gh.events.v1"))
           .option("startingOffsets", env("STARTING_OFFSETS", "earliest"))
           .option("maxOffsetsPerTrigger", env("MAX_OFFSETS_PER_TRIGGER", "50000"))
           .option("failOnDataLoss", "false")
           .load())

    # Confluent framing: 1 magic byte + 4 byte schema id, then Avro binary
    events = (raw
              .withColumn("avro", F.expr("substring(value, 6, length(value) - 5)"))
              .withColumn("ev", from_avro(F.col("avro"), schema_json, {"mode": "PERMISSIVE"}))
              # Avro timestamp-millis -> Spark TimestampType directly; no manual epoch arithmetic
              .select("ev.*", F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"))
              .withWatermark("created_at", env("WATERMARK", "15 minutes"))
              .dropDuplicatesWithinWatermark(["id"]))

    checkpoints = checkpoint_root()

    queries = []
    queries.append(events.withColumn("committed_at", F.current_timestamp())
                .writeStream.format("iceberg").outputMode("append")
                .option("checkpointLocation", f"{checkpoints}/events")
                .option("fanout-enabled", "true")
                .trigger(processingTime=env("TRIGGER", "30 seconds"))
                .toTable(f"{CATALOG}.{DB}.events"))

    activity = (events
                .groupBy(F.window("created_at", "5 minutes"), "repo_name", "type")
                .agg(F.count("*").alias("n_events"), F.approx_count_distinct("actor_id").alias("n_actors"))
                .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                        "repo_name", "type", "n_events", "n_actors", F.current_timestamp().alias("committed_at")))

    queries.append(activity.writeStream.format("iceberg").outputMode("append")
                  .option("checkpointLocation", f"{checkpoints}/repo_activity_5m")
                  .trigger(processingTime=env("TRIGGER", "30 seconds"))
                  .toTable(f"{CATALOG}.{DB}.repo_activity_5m"))

    print(f"streaming {len(queries)} queries -> target={target()}: events + repo_activity_5m (Ctrl-C to stop; checkpoints make restart exactly-once)")
    # one progress line per query per trigger: the numbers an operator needs (input rows, rows dropped as late,
    # rows written, watermark) without turning on Spark's INFO firehose
    import json  # noqa: PLC0415
    import time  # noqa: PLC0415
    seen: dict[str, int] = {}
    # STOP_WHEN_IDLE_BATCHES=n turns the stream into a bounded backfill: exit once the events query has seen
    # n consecutive empty triggers (topic caught up). 0 = run forever (the default for a live deployment).
    stop_after = int(env("STOP_WHEN_IDLE_BATCHES", "0"))
    idle = 0
    while any(q.isActive for q in queries):
        time.sleep(10)
        if stop_after and idle >= stop_after:
            print(f"caught up: {idle} consecutive empty triggers, stopping (STOP_WHEN_IDLE_BATCHES={stop_after})")
            for q in queries:
                q.stop()
            break
        for q in queries:
            p = q.lastProgress
            if not p or seen.get(q.name or q.id, -1) == p["batchId"]:
                continue
            seen[q.name or q.id] = p["batchId"]
            if q is queries[0]:  # the events query drives the idle count (queryName is not kept by toTable)
                idle = idle + 1 if not p.get("numInputRows") else 0
            state = p.get("stateOperators") or [{}]
            print(json.dumps({
                "query": p["name"] or str(q.id)[:8], "batch": p["batchId"], "input_rows": p.get("numInputRows"),
                "dropped_late": sum(int(s.get("numRowsDroppedByWatermark", 0)) for s in state),
                "state_rows": sum(int(s.get("numRowsTotal", 0)) for s in state),
                "output_rows": (p.get("sink") or {}).get("numOutputRows"), "watermark": (p.get("eventTime") or {}).get("watermark"),
                "max_event_time": (p.get("eventTime") or {}).get("max"),
            }), flush=True)
    for q in queries:
        if q.exception():
            raise q.exception()
    return 0


if __name__ == "__main__":
    sys.exit(main())
