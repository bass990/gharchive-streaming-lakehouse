"""Replay / backfill one GH Archive hour.

The streaming job dedupes on event id within its watermark, so re-producing an
hour that is *recent* is a no-op downstream. For an hour that is older than the
watermark (a true backfill, or a re-run after a bug in the producer), the rows
already in Iceberg have to be removed first, otherwise they would be duplicated:

  1. delete the hour's partition from gh.events (Iceberg row-level delete, atomic, one snapshot);
  2. re-produce the hour from the archive file;
  3. the running stream picks the events up and writes them once.

Aggregates for that hour are recomputed by the stream because the windows are
re-emitted when their watermark passes again ONLY if the job is restarted with a
fresh checkpoint for that query; the honest documentation of that limitation is
in docs/architecture.md ("replay semantics").
"""
from __future__ import annotations

import sys

from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

from .config import Settings, settings
from .exporter import catalog
from .producer import produce_hour


def hour_bounds(hour: str) -> tuple[str, str]:
    from datetime import datetime, timedelta  # noqa: PLC0415
    d, h = hour.rsplit("-", 1)
    start = datetime.strptime(d, "%Y-%m-%d").replace(hour=int(h))
    return start.strftime("%Y-%m-%dT%H:%M:%S"), (start + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")


def delete_hour(hour: str, cfg: Settings = settings) -> int:
    t = catalog(cfg).load_table("gh.events")
    lo, hi = hour_bounds(hour)
    before = len(t.scan(row_filter=And(GreaterThanOrEqual("created_at", lo), LessThan("created_at", hi)), selected_fields=("id",)).to_arrow())
    t.delete(And(EqualTo("source_file", hour), GreaterThanOrEqual("created_at", lo), LessThan("created_at", hi)))
    print(f"deleted {before:,} rows of {hour} from gh.events")
    return before


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415
    ap = argparse.ArgumentParser()
    ap.add_argument("--hour", required=True)
    ap.add_argument("--no-delete", action="store_true")
    a = ap.parse_args(argv)
    if not a.no_delete:
        delete_hour(a.hour)
    produce_hour(a.hour)
    return 0


if __name__ == "__main__":
    sys.exit(main())
