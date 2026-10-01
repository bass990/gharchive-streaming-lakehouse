# Architecture

```
GH Archive (hourly .json.gz) ─▶ producer (Avro, idempotent, keyed by repo) ─▶ Redpanda topic gh.events.v1 (6 partitions)
                                                    │ schema registered, BACKWARD compatibility enforced
                                                    ▼
                               Spark Structured Streaming (checkpointed, RocksDB state)
                                 ├─ dropDuplicatesWithinWatermark(id) ──▶ Iceberg gh.events         (append, hours(created_at)/type)
                                 └─ 5-min tumbling windows per repo/type ──▶ Iceberg gh.repo_activity_5m (append after watermark)

Postgres repo_watchlist ─▶ Debezium (pgoutput) ─▶ topic cdc.public.repo_watchlist ─▶ Spark foreachBatch MERGE ─▶ Iceberg gh.repo_watchlist_scd2

Redpanda /public_metrics ─▶ Prometheus ◀─ exporter (freshness, e2e latency, file counts, SCD2 invariant) ─▶ Grafana
```

## Delivery semantics, precisely

**Producer to broker: no duplicates on retry.** `enable.idempotence=true`
with `acks=all`; the broker de-duplicates producer retries by sequence number.

**Broker to Iceberg: exactly once.** Spark stores the consumed Kafka offsets
in the checkpoint *before* the write and Iceberg commits are atomic. If the
job dies after writing data files but before the Iceberg commit, the restarted
job re-reads the same offsets, re-writes, and commits once; the orphaned files
from the failed attempt are removed by `remove_orphan_files`. If it dies after
the commit but before the checkpoint advances, Spark's Iceberg sink checks the
epoch id recorded in the snapshot summary and skips the duplicate commit. This
is the documented Iceberg behaviour; it is why "Kafka + Spark + Iceberg" is
the standard exactly-once stack and Kafka + Spark + plain Parquet is not.

**A bug the tests did not catch, stated.** The first run wrote every
`created_at` as 1970: `from_avro` already returns Spark timestamps for
Avro `timestamp-millis`, and an extra "divide by 1000 and cast" on top shifted
them. The exporter's freshness gauge (56 years) is what caught it, which is
the argument for exporting business-level metrics and not only consumer lag.
The events tables were dropped, checkpoints cleared, and the topic replayed
from offset 0.

**Exactly-once is per checkpoint lineage, stated.** Two runs of the same job
against the same topic with *different* checkpoint directories are two
independent consumers; each writes the whole topic once, so the table ends up
with every event twice. That happened on the GCS target during cloud
verification (309,762 rows for 169,767 events) and was fixed by wiping the
table and running once. In production this is why checkpoint locations are
part of the deployment contract and never regenerated casually.

**Replays: idempotent within the watermark.** The archive can be re-produced
at any time. `dropDuplicatesWithinWatermark(["id"])` drops any event id already
seen whose event time is within the watermark (15 minutes by default), so a
restarted or duplicated replay of a recent hour writes nothing. For older
hours, `replay.py` deletes the hour's rows first (Iceberg row-level delete,
one atomic snapshot) and then re-produces.

**Known limitation, stated.** Windowed aggregates for a replayed *old* hour
are not recomputed by the running query: those windows were finalised when the
watermark passed and Spark will not re-open them. The correct procedure for an
old-hour backfill is: delete the hour from both tables, stop the query, and
run the stream once more with `STARTING_OFFSETS` at the replayed offsets and a
fresh checkpoint for the aggregate query. That is a real operational cost of
watermark-based streaming and is the reason many teams keep aggregates as
batch jobs over the event table instead; `repo_activity_5m` exists to show the
streaming version, and the batch version is one DuckDB/dbt query away.

## Schema evolution

The topic's value schema lives in the registry under `gh.events.v1-value`
with `BACKWARD` compatibility. Adding a field with a default, or removing a
field that had a default, is allowed; renaming or changing a type is refused
at registration time, before any message is produced. The Spark job reads with
the schema file mounted into the container; in production it would fetch the
writer schema by id from the registry (the 4-byte id is right there in the
frame) and the reader schema from the repo.

`payload_json` is deliberately a string: GitHub's ~15 payload shapes evolve
independently and are queried by a handful of consumers who can `json_extract`
what they need. Modelling them all as Avro unions would make every producer
change a registry negotiation.

## CDC as SCD2

Debezium emits `before`/`after` per row change with the WAL LSN. The merge
job keeps the last change per key per micro-batch (ordered by LSN), closes the
open version of each changed key (`valid_to = change time, is_current = false`)
and inserts the new version keyed by `(repo_name, source_lsn)`; a delete only
closes. Because the insert `MERGE` matches on LSN, re-applying a micro-batch
after a failure is a no-op. The exporter checks the invariant "at most one
current row per key" (a deleted key has none) every 30 seconds and Grafana shows it in red if it breaks.

Two things testing taught about this leg. First, with the Iceberg catalog as
the session default, Spark SQL cannot see session temp views, so the batch's
changes go to a small staging Iceberg table (`gh.cdc_staging`) that the
`MERGE` reads; that also leaves an audit trail of the last batch. Second, the
Iceberg Spark catalog caches table metadata for 30 seconds by default, and two
micro-batches 1 second apart made the second `MERGE` read the *previous*
batch's staging rows: an insert and a delete were silently skipped. The cache
is disabled for this catalog. Re-running the same two `MERGE` statements
against the staged batch then applied exactly the two missing changes and
nothing else, which is the idempotency the LSN key is there to provide.

## Table maintenance (the part interviews ask about)

Streaming writes produce small files. `maintenance.py` runs Iceberg's
`rewrite_data_files` (target 128 MB), `expire_snapshots` (3 days retained for
time travel, at least 5 snapshots) and `remove_orphan_files`, and prints file
counts before/after. `docs/finops.md` translates those counts into dollars.

## Portability

Local: Redpanda + MinIO + Iceberg REST + Debezium Connect + Spark in
containers. AWS: `deploy/aws` provisions S3 + Glue + budget (applied) and MSK
Serverless behind `create_msk` (not applied, cost). The Spark jobs switch to
the Glue catalog by changing `spark.sql.catalog.lh.type` to `glue` and
dropping the S3 endpoint override; EMR Serverless and Glue Streaming both run
them as-is.
