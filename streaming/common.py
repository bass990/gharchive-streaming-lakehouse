"""Shared Spark session + Iceberg catalog config for the streaming jobs.

Runs inside the `spark` container. `LAKEHOUSE_TARGET` selects where the
Iceberg tables live; the jobs themselves never change:

    local  REST catalog (compose) + MinIO             s3://<bucket>/warehouse
    aws    AWS Glue catalog + S3                      s3://<bucket>/warehouse
    azure  JDBC catalog (compose Postgres) + ADLS Gen2   abfss://<fs>@<account>.dfs.core.windows.net/warehouse
    gcp    JDBC catalog (compose Postgres) + GCS      gs://<bucket>/warehouse

The JDBC catalog writes the same `iceberg_tables` rows pyiceberg's SQL catalog
reads, so the host-side exporter/query tools see the cloud tables too.
Checkpoints are kept per target so switching targets never replays into the
wrong warehouse.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.6.1"
SPARK_VERSION = "3.5.3"
BASE_PACKAGES = [
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
    f"org.apache.spark:spark-avro_2.12:{SPARK_VERSION}",
]
TARGET_PACKAGES = {
    "local": [f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"],
    "aws": [f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"],
    "azure": [f"org.apache.iceberg:iceberg-azure-bundle:{ICEBERG_VERSION}", "org.postgresql:postgresql:42.7.4"],
    "gcp": [f"org.apache.iceberg:iceberg-gcp-bundle:{ICEBERG_VERSION}", "org.postgresql:postgresql:42.7.4"],
}

CATALOG = "lh"
DB = "gh"


def env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise SystemExit(f"missing env {name}")
    return v


def target() -> str:
    t = env("LAKEHOUSE_TARGET", "local")
    if t not in TARGET_PACKAGES:
        raise SystemExit(f"LAKEHOUSE_TARGET must be one of {sorted(TARGET_PACKAGES)}, got {t!r}")
    return t


def packages(t: str | None = None) -> str:
    return ",".join(BASE_PACKAGES + TARGET_PACKAGES[t or target()])


def checkpoint_root() -> str:
    return f"{env('CHECKPOINT_ROOT', '/checkpoints')}/{target()}"


def catalog_conf(t: str) -> dict[str, str]:
    bucket = env("LAKEHOUSE_BUCKET", "gh-lakehouse")
    c = f"spark.sql.catalog.{CATALOG}"
    if t == "local":
        return {
            f"{c}.type": "rest", f"{c}.uri": env("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
            f"{c}.warehouse": f"s3://{bucket}/warehouse", f"{c}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            f"{c}.s3.endpoint": env("S3_ENDPOINT", "http://minio:9000"), f"{c}.s3.path-style-access": "true",
        }
    if t == "aws":
        return {
            f"{c}.type": "glue", f"{c}.warehouse": f"s3://{bucket}/warehouse",
            f"{c}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO", f"{c}.glue.region": env("AWS_REGION", "us-east-2"),
        }
    # one catalog database per target: the JDBC catalog keys tables by name, so gh.events on Azure and
    # gh.events on GCP must not share a catalog store
    jdbc = {
        f"{c}.type": "jdbc", f"{c}.uri": env("CATALOG_JDBC_URI", f"jdbc:postgresql://postgres:5432/gh_{t}"),
        f"{c}.jdbc.user": env("PG_USER", "gh"), f"{c}.jdbc.password": env("PG_PASSWORD", "gh-local"),
    }
    if t == "azure":
        account = env("AZURE_STORAGE_ACCOUNT")
        jdbc.update({
            f"{c}.warehouse": f"abfss://{bucket}@{account}.dfs.core.windows.net/warehouse",
            f"{c}.io-impl": "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
            f"{c}.adls.auth.shared-key.account.name": account,
            f"{c}.adls.auth.shared-key.account.key": env("AZURE_STORAGE_KEY"),
        })
        return jdbc
    if t == "gcp":
        jdbc.update({
            f"{c}.warehouse": f"gs://{bucket}/warehouse", f"{c}.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
            f"{c}.gcs.project-id": env("GOOGLE_CLOUD_PROJECT"),
        })
        return jdbc
    raise SystemExit(t)


def spark_session(app: str) -> SparkSession:
    t = target()
    b = (SparkSession.builder.appName(f"{app}-{t}")
         .config("spark.jars.packages", packages(t))
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
         # the CDC job rewrites a staging table every micro-batch; the default 30 s metadata cache would let a
         # MERGE read the previous batch's rows (seen in testing), so table metadata is always re-read
         .config(f"spark.sql.catalog.{CATALOG}.cache-enabled", "false")
         .config("spark.sql.defaultCatalog", CATALOG)
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
         .config("spark.sql.session.timeZone", "UTC"))
    for k, v in catalog_conf(t).items():
        b = b.config(k, v)
    return b.getOrCreate()


def ensure_tables(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")
    # bronze-ish: every event once, partitioned by hour of EVENT time and type (small files are compacted by maintenance.py)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.events (
            id STRING, type STRING, created_at TIMESTAMP, actor_id BIGINT, actor_login STRING, repo_id BIGINT,
            repo_name STRING, org_login STRING, is_public BOOLEAN, payload_json STRING, source_file STRING,
            ingested_at TIMESTAMP, kafka_partition INT, kafka_offset BIGINT, committed_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (hours(created_at), type)
        TBLPROPERTIES ('write.target-file-size-bytes'='134217728', 'write.parquet.compression-codec'='zstd',
                       'write.distribution-mode'='hash', 'format-version'='2')
    """)
    # 5-minute activity per repo and type, finalised by watermark -> append-only, exactly once
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.repo_activity_5m (
            window_start TIMESTAMP, window_end TIMESTAMP, repo_name STRING, type STRING,
            n_events BIGINT, n_actors BIGINT, committed_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(window_start))
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
    """)
    # CDC target: SCD2 of the watchlist table in Postgres
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.repo_watchlist_scd2 (
            repo_name STRING, tier STRING, owner_team STRING, notes STRING,
            valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN, source_op STRING, source_lsn BIGINT
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
    """)
