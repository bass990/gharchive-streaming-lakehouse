# GitHub Archive streaming lakehouse

Real-time data engineering on the public GitHub event firehose, built the way
a platform team would build it and priced the way a FinOps review would price it.

![The provisioned Grafana dashboard: broker throughput, consumer lag, event-time freshness reading 1.02 years, end-to-end latency p50 33 minutes and p95 2.14 hours, 169,753 rows in gh.events, zero SCD2 keys with more than one current row, and data files per table flat at 15 after compaction](./docs/screenshots/grafana_dashboard.png)

*The dashboard a week after the run, with the stack restarted and no stream job running. Freshness reads 1.02 years because the replayed archive hour is from 2025-09-22, and consumer lag reads "No data" because no consumer group is live. That is the point of the freshness gauge: lag alone would say nothing is wrong.*

* **Ingest:** GH Archive hour files replayed onto **Kafka (Redpanda)** as
  **Avro** with a **Schema Registry** subject under BACKWARD compatibility,
  idempotent producer, event-time record timestamps, replay at any speed.
* **Process:** **Spark Structured Streaming** with RocksDB state, event-time
  watermarks, `dropDuplicatesWithinWatermark`, 5-minute tumbling windows.
* **Store:** **Apache Iceberg** (REST catalog locally, Glue on AWS), hidden
  partitioning by event hour and type, **exactly-once** commits, row-level
  deletes for replay, `rewrite_data_files` / `expire_snapshots` maintenance.
* **CDC:** **Debezium** on Postgres (pgoutput) -> Kafka -> Spark `MERGE INTO`
  producing an **SCD2** dimension, idempotent on replays.
* **Observe:** **Prometheus + Grafana**: broker throughput and consumer lag
  from Redpanda, and a custom exporter for the numbers that matter to the
  business: event-time freshness, producer-to-committed end-to-end latency,
  small-file count, SCD2 invariant.
* **FinOps:** a measured cost model (`docs/finops.md`) that says what this
  costs on AWS in three designs and which one you should actually pick.

## What is real here

Everything below ran on one laptop against the compose stack, on GH Archive hour `2025-09-22-15`.

| Item | Number | Where to see it |
|---|---|---|
| Events produced (60k slice, then the full hour replayed on top) | 229,770 messages, 169,770 distinct ids, 0 producer errors | Redpanda Console, `make query` |
| Producer throughput (idempotent, acks=all, zstd) | 169,770 events in 27 s (~6,300/s) on one laptop core | producer output |
| Events in Iceberg `gh.events` | 169,753 rows = 169,753 distinct ids; 17 dropped as late (beyond the 15-min watermark), 60,000 duplicates dropped by id | progress log, `make query` |
| Event time range | 15:00:00 to 15:59:59, 15 event types, hidden partitions `hours(created_at)/type` | `make query` |
| 5-minute windows `gh.repo_activity_5m` | 87,340 windows for 56,414 repos, finalised up to 15:40 (the rest wait for the watermark) | `make query` |
| CDC `gh.repo_watchlist_scd2` | 10 snapshot rows, then 6 updates, 2 inserts, 2 deletes -> 17 versions over 12 keys, 10 current, 0 keys with >1 current | `make cdc-mutate`, exporter |
| Compaction (`make maintain`) | `gh.events` 75 files (1.1 MB avg) -> 15 files (5.6 MB avg), 84.7 MB total; snapshots expired to the last 5 | maintenance output |
| Replay idempotency | the whole hour re-produced (169,770 events, 20 s): stream wrote 0 rows (events behind the watermark dropped as late, the rest dropped by id), table still 169,753 = 169,753 distinct | progress log, `make query` |
| Monitoring | Prometheus scraping Redpanda + exporter, both `up`; Grafana dashboard provisioned | http://localhost:9090/targets, http://localhost:3001 |
| Offline tests | 5 (Avro framing round-trip, schema defaults, event normalisation, hour/replay bounds) | `pytest -q` |

Three bugs the run surfaced and how they were caught are written up in `docs/architecture.md`:
a timestamp cast that put every event in 1970 (caught by the freshness gauge), a
`MERGE` reading the previous batch's staging rows through the Iceberg catalog cache
(caught by the SCD2 invariant), and Spark jobs that died with the shell that launched
them (caught by the consumer lag flat-lining).

## Run it

```bash
cp .env.example .env
docker compose up -d --wait          # ~5 GB RAM; `make up-core` for the minimum
python -m venv .venv && .venv/Scripts/pip install -e ".[dev]"   # Python 3.12
make produce HOUR=2025-09-22-15      # ~250k events -> Kafka as Avro (idempotent; rerun-safe)
make stream                          # Spark: Kafka -> Iceberg gh.events + gh.repo_activity_5m (leave running)
make cdc-register && make cdc        # Debezium connector + SCD2 merge job (second terminal)
make cdc-mutate                      # change rows in Postgres, watch versions appear
make exporter                        # :9108/metrics -> Prometheus -> Grafana http://localhost:3001
make query                           # DuckDB over the Iceberg tables from the host
make maintain                        # compaction + snapshot expiry, prints files before/after
make replay HOUR=2025-09-22-15       # delete the hour from gh.events and re-produce it: same row count after
```

Redpanda Console: http://localhost:8085 (topics, schema, consumer lag).
Spark UI: http://localhost:4040 while a job runs.

## Design decisions worth asking me about

**Exactly once, and what that actually means here.** Idempotent producer
(no broker duplicates on retry) + Spark checkpointed offsets + Iceberg atomic
commits with the epoch id in the snapshot summary (no double commit on
restart) + `dropDuplicatesWithinWatermark(id)` (no duplicates from a
re-played archive hour). Each mechanism covers one failure class; the doc
spells out which. The one thing it does *not* cover, recomputing finalised
windows for an old-hour backfill, is written down with the procedure.

**Why Avro and a registry when the payload is JSON text?** The envelope
(id, type, time, actor, repo) is what every consumer keys on, so it is typed
and compatibility-checked at produce time. The ~15 payload shapes evolve
independently and are read by a few consumers who `json_extract` what they
need; modelling them as Avro unions would make every producer change a
registry negotiation.

**Why does the exporter exist when Redpanda already exports lag?** Lag in
offsets does not tell you whether the table is fresh. Freshness (now minus
max event time in Iceberg) and end-to-end latency (commit time minus produce
time, p50/p95) are the two numbers an on-call engineer pages on.

**Why is compaction a first-class job?** A 30-second trigger writes ~2,900
files a day into `events`. That is the difference between a $7/month table
and one nobody can query. `make maintain` prints the file counts before and
after, and `docs/finops.md` turns them into dollars.

**Why keep Kafka if the volume does not need it?** It does not: 300k
events/hour could be a Spark file-source stream straight from the archive.
Kafka is here to demonstrate the bus (CDC leg, fan-out, replay semantics),
and the FinOps doc says so and prices the alternative.

## Cloud status (honest)

| Target | What happened |
|---|---|
| AWS (account 744359206351, us-east-2) | `deploy/aws` applied: S3 bucket `gh-lakehouse-bass990`, Glue database `gh`, $10 budget. **Pipeline verified:** the Spark job ran with `LAKEHOUSE_TARGET=aws` (Glue catalog + S3FileIO) over the full topic: 169,767 events = 169,767 distinct ids, 87,350 windows, three Glue tables, read back from the host through Glue. MSK Serverless stays behind `create_msk=true` (~$980/month idle). |
| Azure (Azure for Students, northcentralus) | `deploy/azure` applied: resource group `rg-gh-lakehouse`, ADLS Gen2 account `ghlakehousebass990`, filesystem, tiering, budget. **Pipeline verified:** Spark ran with `LAKEHOUSE_TARGET=azure` (JDBC catalog in the compose Postgres + ADLSFileIO) over the full topic: 169,767 events = 169,767 distinct ids, 87,350 windows, read back from the host through pyiceberg's SQL catalog. The subscription's region policy allows only canadacentral, westus, norwayeast, northcentralus, mexicocentral. |
| GCP (project stackoverflow-retention, us-central1) | `deploy/gcp` applied: GCS bucket `gh-lakehouse-bass990`, BigQuery dataset `gh_lakehouse`. **Pipeline verified:** Spark ran with `LAKEHOUSE_TARGET=gcp` (JDBC catalog + GCSFileIO, Application Default Credentials) over the full topic: 169,767 events = 169,767 distinct ids, 87,350 windows, read back from the host. A first attempt ran twice under two checkpoints and doubled the table (309,762 rows); it was wiped and rerun once, and the lesson is in `docs/architecture.md`. |

## Things that went wrong

Five failures from the actual runs, with how I noticed each one. The logs and the fixes are in this repo.

1. **Every event had a 1970 timestamp.** The Avro schema declares `created_at` as `timestamp-millis`, and Spark's `from_avro` already turns that into a timestamp. My job then divided it by 1,000 and cast it again, which moved 169,000 events to January 21, 1970. The streaming job was perfectly happy. What was not happy was the exporter's freshness gauge, which reported the table as 56 years stale. I dropped both tables, cleared the checkpoints and replayed the topic from offset 0. I had added that gauge because consumer lag says nothing about whether the data is right, and this was the first time it caught something.

2. **A `MERGE` that read the previous batch.** The CDC job writes each micro-batch to a small staging table and merges from it. After running `make cdc-mutate` I expected 17 rows and saw 14: an insert and a delete were missing. The SCD2 panel on the Grafana dashboard was still green, so at first I suspected the merge logic. I wrote a one-off Spark script that re-ran the same two `MERGE` statements against the staged rows, and it applied exactly the two missing changes. So the SQL was fine and the input had been stale. Spark's Iceberg catalog caches table metadata for 30 seconds by default, and two micro-batches one second apart had both read the first batch's staging table. `cache-enabled=false` on the catalog fixed it.

3. **Exactly-once, twice.** While verifying the GCS target I ran the job with a fresh checkpoint directory after an earlier run had already written the table. Each run was exactly-once with respect to its own checkpoint. The table had every event twice: 309,762 rows for 169,767 events. I wiped it and ran once. The checkpoint location is part of the deployment now, not something I regenerate when a run misbehaves.

4. **Azure and GCP sharing a catalog.** Both use a JDBC catalog in Postgres, and that catalog keys tables by name. The Azure run opened `gh.events` and got back metadata pointing at `gs://`. One catalog database per cloud target.

5. **Jobs that died with the shell that started them.** I launched the Spark jobs from a background subshell; they died the moment that shell returned, and later the CDC driver died by itself after about 15 idle hours. In both cases the first sign was consumer lag flat-lining. Finding which of three identical `spark-submit` processes to kill meant reading each one's environment from `/proc/<pid>/environ` for its `LAKEHOUSE_TARGET`. The jobs now print one JSON line per trigger (input rows, rows dropped as late, rows written, watermark), which is the log I should have written first.

6. **The stack only started on my machine.** The first CI run after pushing failed at `docker compose up`: MinIO's community images on quay.io and Docker Hub went behind a login during 2025, and anonymous pulls answer "unauthorized". Locally the image had been cached since the first run, so nothing ever complained. Anyone cloning the repo would have been stuck at step one. The compose file now defaults to RustFS, which speaks the same S3 API on the same ports, keeps the service name `minio` so every endpoint is unchanged, and creates the bucket with the AWS CLI image. The MinIO image is still selectable through two environment variables for machines that have it cached over an existing volume. The same push also showed the host-side exporter could not reach `localhost` ports on Docker Desktop (an IPv6 quirk), so every example endpoint now says 127.0.0.1.

## Layout

```
src/gh_stream/
  producer.py     GH Archive hour -> Kafka (Avro, idempotent, event-time timestamps, paced replay)
  avro.py         schema registry REST + Confluent framing without the Confluent client
  events.py       JSON -> record (pure, tested)
  cdc.py          Debezium connector registration + a mutation driver
  replay.py       row-level delete of an hour + re-produce
  exporter.py     Prometheus metrics computed from Iceberg via pyiceberg/DuckDB
  query.py        ad-hoc DuckDB over the tables
streaming/        Spark jobs (run in the spark container): stream_events.py, cdc_merge.py, maintenance.py, common.py
schemas/          gh_event.avsc
cdc/init.sql      source table with REPLICA IDENTITY FULL
monitoring/       prometheus.yml, Grafana provisioning + dashboard
deploy/aws/       Terraform
docs/             architecture.md, finops.md
tests/            producer/avro/replay unit tests (no broker)
```
