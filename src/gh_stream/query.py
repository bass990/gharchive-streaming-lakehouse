"""Ad-hoc analytics over the Iceberg tables from the host with DuckDB (no Spark).

    python -m gh_stream.query                   # the standard report
    python -m gh_stream.query "SELECT ..."      # any SQL; tables are exposed as events, repo_activity_5m, repo_watchlist_scd2
"""
from __future__ import annotations

import sys

import duckdb

from .config import Settings, settings
from .exporter import catalog

REPORT = """
SELECT 'events' AS table_name, count(*) AS rows, min(created_at) AS first_event, max(created_at) AS last_event,
       count(DISTINCT id) AS distinct_ids FROM events
UNION ALL
SELECT 'repo_activity_5m', count(*), min(window_start), max(window_end), NULL FROM repo_activity_5m
UNION ALL
SELECT 'repo_watchlist_scd2', count(*), min(valid_from), max(valid_from), count(DISTINCT repo_name) FROM repo_watchlist_scd2
"""

TOP = """
SELECT w.tier, a.repo_name, sum(a.n_events) AS events_5m_total
FROM repo_activity_5m a
JOIN repo_watchlist_scd2 w ON w.repo_name = a.repo_name AND w.is_current
GROUP BY 1, 2 ORDER BY 1, 3 DESC LIMIT 15
"""


def connection(cfg: Settings = settings) -> duckdb.DuckDBPyConnection:
    cat = catalog(cfg)
    con = duckdb.connect()
    for name in ("events", "repo_activity_5m", "repo_watchlist_scd2"):
        try:
            con.register(name, cat.load_table(f"gh.{name}").scan().to_arrow())
        except Exception as exc:  # noqa: BLE001
            print(f"({name} not available: {str(exc)[:80]})")
    return con


def show(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    widths = [max(len(str(c)), *(len(str(r[i])) for r in rows)) if rows else len(str(c)) for i, c in enumerate(cols)]
    print("  ".join(str(c).ljust(w) for c, w in zip(cols, widths, strict=True)))
    for r in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(r, widths, strict=True)))


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    con = connection()
    if argv:
        show(con, " ".join(argv))
        return 0
    show(con, REPORT)
    print()
    show(con, TOP)
    return 0


if __name__ == "__main__":
    sys.exit(main())
