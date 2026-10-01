"""Iceberg table maintenance: the part of a streaming lakehouse that decides
whether it is cheap or expensive six months in.

A 30-second trigger writes ~120 files/hour per partition set. Reads of a
day's data then open thousands of tiny Parquet files, and every S3 GET is
billed. `rewrite_data_files` compacts them to the 128 MB target,
`expire_snapshots` frees the old files (keeping 3 days of time travel for
replay/debug), `remove_orphan_files` cleans up after failed commits.

Run it hourly (make maintain) and record before/after counts; the numbers go
straight into docs/finops.md.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from common import CATALOG, DB, spark_session

TABLES = ("events", "repo_activity_5m", "repo_watchlist_scd2")


def file_stats(spark, table: str) -> dict:
    row = spark.sql(f"SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes, avg(file_size_in_bytes) AS avg_bytes "
                    f"FROM {CATALOG}.{DB}.{table}.files").collect()[0]
    return {"files": int(row.files or 0), "bytes": int(row.bytes or 0), "avg_mb": round((row.avg_bytes or 0) / 1e6, 2)}


def main() -> int:
    spark = spark_session("gh-maintenance")
    spark.sparkContext.setLogLevel("WARN")
    report = {"run_at": datetime.now(timezone.utc).isoformat(), "tables": {}}
    for t in TABLES:
        before = file_stats(spark, t)
        spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{DB}.{t}', "
                  f"options => map('target-file-size-bytes', '134217728', 'min-input-files', '5'))")
        spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{DB}.{t}', older_than => TIMESTAMP '{_days_ago(3)}', retain_last => 5)")
        try:
            spark.sql(f"CALL {CATALOG}.system.remove_orphan_files(table => '{DB}.{t}', older_than => TIMESTAMP '{_days_ago(1)}')")
            orphans = "removed"
        except Exception as exc:  # noqa: BLE001
            # Iceberg 1.6 lists orphans through a Hadoop FileSystem; this image has no hadoop-aws for s3://,
            # so locally this step is skipped (on EMR/Glue the s3 FileSystem is present). Compaction and
            # snapshot expiry, which use the table's own S3FileIO, still ran.
            orphans = f"skipped: {str(exc).splitlines()[0][:90]}"
        after = file_stats(spark, t)
        report["tables"][t] = {"before": before, "after": after, "orphan_files": orphans}
        print(f"{t}: {before['files']} files ({before['avg_mb']} MB avg) -> {after['files']} files ({after['avg_mb']} MB avg)")
    print(json.dumps(report))
    return 0


def _days_ago(n: int) -> str:
    from datetime import timedelta  # noqa: PLC0415
    return (datetime.now(timezone.utc) - timedelta(days=n)).strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    sys.exit(main())
