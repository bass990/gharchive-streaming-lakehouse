"""Prometheus exporter for the lakehouse side of the pipeline (:9108/metrics).

The broker tells you about lag in offsets. This tells you what the business
cares about: how fresh is the data in the table (now - max event time), how
long did an event take from producer to committed Iceberg snapshot, how many
files a day's partition has (the FinOps signal), and whether the SCD2 has
at most one current row per key (a deleted key has none).

Reads Iceberg through pyiceberg + DuckDB; no Spark needed on the host.
"""
from __future__ import annotations

import sys
import time

import duckdb
from prometheus_client import Gauge, start_http_server
from pyiceberg.catalog import load_catalog

from .config import Settings, settings

ROWS = Gauge("lakehouse_table_rows", "row count", ["table"])
FILES = Gauge("lakehouse_table_data_files", "number of live data files", ["table"])
AVG_FILE_MB = Gauge("lakehouse_table_avg_file_mb", "average data file size in MB", ["table"])
SNAPSHOTS = Gauge("lakehouse_table_snapshots", "snapshots retained", ["table"])
FRESHNESS = Gauge("lakehouse_events_freshness_seconds", "now - max(created_at) in gh.events")
E2E_P50 = Gauge("lakehouse_events_e2e_latency_p50_seconds", "committed_at - ingested_at, p50 over the last snapshot")
E2E_P95 = Gauge("lakehouse_events_e2e_latency_p95_seconds", "committed_at - ingested_at, p95 over the last snapshot")
SCD2_BAD = Gauge("lakehouse_scd2_keys_with_multiple_current", "watchlist keys with more than one current row (deleted keys legitimately have none)")
SCRAPE_SECONDS = Gauge("lakehouse_exporter_scrape_seconds", "time to compute all metrics")


def catalog(cfg: Settings):
    """Host-side pyiceberg catalog for the same tables the Spark jobs write, per LAKEHOUSE_TARGET.
    azure/gcp use pyiceberg's SQL catalog, which reads the rows Spark's JDBC catalog writes."""
    if cfg.target == "local":
        return load_catalog("lh", **{
            "type": "rest", "uri": cfg.iceberg_rest_uri, "warehouse": cfg.warehouse,
            "s3.endpoint": cfg.s3_endpoint, "s3.access-key-id": cfg.s3_access_key, "s3.secret-access-key": cfg.s3_secret_key,
            "s3.path-style-access": "true", "s3.region": "us-east-1",
        })
    if cfg.target == "aws":
        return load_catalog("lh", **{"type": "glue", "glue.region": cfg.aws_region, "warehouse": cfg.warehouse})
    # same per-target catalog database the Spark JDBC catalog writes (gh_azure / gh_gcp)
    sql_uri = cfg.pg_dsn.replace("postgresql://", "postgresql+psycopg://").rsplit("/", 1)[0] + f"/gh_{cfg.target}"
    if cfg.target == "azure":
        return load_catalog("lh", **{"type": "sql", "uri": sql_uri, "warehouse": cfg.warehouse,
                                     "adls.account-name": cfg.azure_storage_account, "adls.account-key": cfg.azure_storage_key})
    if cfg.target == "gcp":
        return load_catalog("lh", **{"type": "sql", "uri": sql_uri, "warehouse": cfg.warehouse, "gcs.project-id": cfg.gcp_project or ""})
    raise ValueError(f"unknown LAKEHOUSE_TARGET {cfg.target!r}")


def collect(cfg: Settings = settings) -> dict:
    t0 = time.perf_counter()
    cat = catalog(cfg)
    out: dict = {}
    for name in ("gh.events", "gh.repo_activity_5m", "gh.repo_watchlist_scd2"):
        try:
            t = cat.load_table(name)
        except Exception:  # noqa: BLE001 - table not created yet
            continue
        snap = t.current_snapshot()
        files = list(t.scan().plan_files())
        n_rows = sum(int(f.file.record_count) for f in files)
        n_bytes = sum(int(f.file.file_size_in_bytes) for f in files)
        ROWS.labels(name).set(n_rows)
        FILES.labels(name).set(len(files))
        AVG_FILE_MB.labels(name).set((n_bytes / len(files) / 1e6) if files else 0)
        SNAPSHOTS.labels(name).set(len(t.metadata.snapshots))
        out[name] = {"rows": n_rows, "files": len(files), "snapshot": snap.snapshot_id if snap else None}
    try:
        ev = cat.load_table("gh.events")
        recent = ev.scan(selected_fields=("created_at", "ingested_at", "committed_at")).to_arrow()
        if len(recent):
            con = duckdb.connect()
            con.register("ev", recent)
            # compute in SQL so no tz-aware Python objects cross the boundary (DuckDB would need pytz)
            fresh, p50, p95 = con.execute("""SELECT epoch(now() - max(created_at)),
                                                    quantile_cont(epoch(committed_at - ingested_at), 0.5),
                                                    quantile_cont(epoch(committed_at - ingested_at), 0.95) FROM ev""").fetchone()
            FRESHNESS.set(fresh or 0)
            E2E_P50.set(p50 or 0)
            E2E_P95.set(p95 or 0)
            out["freshness_s"] = round(fresh or 0, 1)
            out["e2e_p50_s"], out["e2e_p95_s"] = p50, p95
    except Exception as exc:  # noqa: BLE001
        out["events_error"] = str(exc)[:120]
    try:
        scd = cat.load_table("gh.repo_watchlist_scd2").scan(selected_fields=("repo_name", "is_current")).to_arrow()
        con = duckdb.connect()
        con.register("s", scd)
        bad = con.execute("SELECT count(*) FROM (SELECT repo_name FROM s GROUP BY 1 HAVING sum(is_current::int) > 1)").fetchone()[0]
        SCD2_BAD.set(bad)
        out["scd2_bad_keys"] = bad
    except Exception:  # noqa: BLE001
        pass
    SCRAPE_SECONDS.set(time.perf_counter() - t0)
    return out


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9108)
    ap.add_argument("--interval", type=int, default=30)
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args(argv)
    if a.once:
        print(collect())
        return 0
    start_http_server(a.port)
    print(f"exporter on :{a.port}/metrics")
    while True:
        try:
            collect()
        except Exception as exc:  # noqa: BLE001
            print("collect failed:", exc)
        time.sleep(a.interval)


if __name__ == "__main__":
    sys.exit(main())
