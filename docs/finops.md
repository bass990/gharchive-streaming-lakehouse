# FinOps: what this pipeline costs and where the money goes

Numbers measured locally on one GH Archive hour (2025-09-22-15) and scaled;
the measured inputs are in the README's "What is real here" table. Prices are
AWS us-east-2 list prices as of September 2026; treat them as order of magnitude.

## Workload shape

| Quantity | Measured / assumed |
|---|---|
| Events per hour (GH Archive) | ~200k to 300k, ~110 MB gzipped JSON |
| Avro on the wire | measured 4.5 KB/event uncompressed (payload_json dominates), ~770 MB/hour before zstd; the broker stores it compressed |
| Iceberg Parquet (zstd) | measured 84.7 MB for 169,753 events (~500 bytes/event), 1.25 MB for 87,340 five-minute windows |
| Spark trigger | 20 s in the run (30 s default); the hour landed as 75 data files (1.1 MB avg) before compaction, 15 (5.6 MB avg) after |

## The three cost drivers, in order

1. **Always-on compute.** A Kafka cluster and a streaming job run whether or
   not anything useful happens. MSK Serverless: $0.75/cluster-hour + $0.10/
   partition-hour (6 partitions) = **~$980/month before a single byte**.
   EMR Serverless for a 1-driver + 1-executor Spark stream (4 vCPU, 16 GB):
   ~$0.35/hour = **~$250/month**. Self-hosting Redpanda on one t4g.medium is
   ~$25/month plus the operational burden. For a portfolio, the local compose
   stack costs $0, which is why the cloud module keeps MSK behind a flag.

2. **S3 requests, not S3 storage.** Storage for a year of `events` is about
   35 MB x 24 x 365 = 300 GB = **$7/month**. Requests are where a naive
   streaming sink bleeds: 120 files/hour x 4 partitions-ish (types) x 24 h =
   ~11k PUTs/day plus manifest and metadata writes, ~$0.06/day, fine. The
   *reads* are the trap: a daily aggregate over 2,900 tiny files opens 2,900
   objects; at $0.0004/1k GETs it is cents, but the latency and the
   Spark/Athena task overhead are what make queries slow and compute expensive.
   `maintenance.py` compacts to 128 MB files (about 7 files/day instead of
   2,900) and expires snapshots older than 3 days so the old files are
   actually deleted. Measured on the local run: 75 files -> 15 for one hour of
   `events`. Storage for a year at the measured 85 MB/hour is ~740 GB
   (~$17/month in S3 Standard, ~$9 after the IA transition), which revises the
   $7 estimate above; `payload_json` is 80% of it and is the first thing to
   move to a colder table if the bill matters.

3. **Reprocessing.** Every replay re-reads the archive (free), re-produces to
   Kafka (broker bytes) and re-writes Iceberg. Because deletes are row-level
   and commits are atomic, a replay of one hour costs one hour of writes, not a
   table rewrite. The dedupe-within-watermark means a replay of a *recent*
   hour writes nothing at all.

## Cheapest correct architecture on AWS, for this volume

* Kafka: MSK Serverless only if you need <1 s latency and a shared bus; for a
  single pipeline at 300k events/hour, **skip Kafka entirely** and have the
  Spark job read the hourly archive files directly (Structured Streaming
  file source). Keep Kafka for the CDC leg and for fan-out to other consumers.
  This project keeps Kafka because the point is to demonstrate it; the honest
  cost note is that the volume does not require it.
* Spark: EMR Serverless with a 5-minute trigger instead of 30 s cuts
  micro-batches 10x and small files 10x at the cost of freshness.
* Storage: S3 Standard, lifecycle to IA after 30 days, compaction hourly,
  snapshot expiry at 3 days (keep time travel for replay debugging only).
* Catalog: Glue (free at this scale).
* Monitoring: CloudWatch for MSK and EMR, plus the exporter's freshness /
  end-to-end latency metrics: the two numbers an on-call engineer actually
  pages on.

## Monthly estimate

| Design | Monthly |
|---|---|
| This repo, local Docker | $0 |
| AWS, MSK Serverless + EMR Serverless (30 s trigger) | ~$1,250 |
| AWS, self-hosted Redpanda (t4g.medium) + EMR Serverless (5 min trigger) | ~$180 |
| AWS, no Kafka, EMR Serverless file-source + Glue + S3 | ~$120 |

The last row is the answer to "how would you make this cheaper"; the first
cloud row is what a team that adopts Kafka reflexively ends up paying.
