"""Replay GH Archive hour files onto Kafka as Avro, keyed by repo, with the
event's own timestamp as the Kafka record timestamp.

Why a replaying producer and not a live feed: GH Archive publishes each hour
about 10 minutes after it ends, and the files are the ground truth. Replaying
a named hour is also what makes backfills and incident replays deterministic:
`--hour 2025-09-22-15 --speed 60` re-plays that hour at 60x with the original
inter-arrival gaps, `--speed 0` fires as fast as the broker accepts.

Delivery: idempotent producer (enable.idempotence) + acks=all. Combined with
the consumer-side dedupe on event id in Spark, an interrupted replay that is
restarted produces no duplicates downstream.
"""
from __future__ import annotations

import gzip
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .avro import AvroCodec, load_schema, register_schema
from .config import ROOT, Settings, settings
from .events import hour_id, to_record

SCHEMA_PATH = ROOT / "schemas" / "gh_event.avsc"


def ensure_topic(cfg: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": cfg.kafka_bootstrap})
    if cfg.topic_events in admin.list_topics(timeout=10).topics:
        return
    fut = admin.create_topics([NewTopic(cfg.topic_events, num_partitions=cfg.topic_partitions, replication_factor=1,
                                        config={"retention.ms": str(7 * 24 * 3600 * 1000), "cleanup.policy": "delete"})])
    for f in fut.values():
        f.result()


def download_hour(hour: str, cfg: Settings) -> Path:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    dest = cfg.data_dir / f"{hour}.json.gz"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    url = f"{cfg.gharchive_base}/{hour}.json.gz"
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(".part")
        with tmp.open("wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
        tmp.replace(dest)
    return dest


def produce_hour(hour: str, cfg: Settings = settings, speed: float = 0.0, limit: int | None = None) -> dict:
    ensure_topic(cfg)
    schema = load_schema(SCHEMA_PATH)
    schema_id = register_schema(cfg.schema_registry, f"{cfg.topic_events}-value", schema)
    codec = AvroCodec(schema, schema_id)
    producer = Producer({
        "bootstrap.servers": cfg.kafka_bootstrap, "enable.idempotence": True, "acks": "all",
        "linger.ms": 20, "batch.num.messages": 5000, "compression.type": "zstd", "queue.buffering.max.messages": 500_000,
    })
    path = download_hour(hour, cfg)
    stats = {"hour": hour, "sent": 0, "bytes": 0, "errors": 0, "skipped": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["errors"] += 1

    t_start = time.perf_counter()
    first_event_ms: int | None = None
    with gzip.open(path, "rb") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                rec = to_record(line, hour)
            except (KeyError, ValueError):
                stats["skipped"] += 1
                continue
            if speed > 0:  # pace by event time
                if first_event_ms is None:
                    first_event_ms = rec["created_at"]
                target = (rec["created_at"] - first_event_ms) / 1000 / speed
                lag = target - (time.perf_counter() - t_start)
                if lag > 0:
                    time.sleep(min(lag, 5))
            payload = codec.encode(rec)
            while True:
                try:
                    producer.produce(cfg.topic_events, key=rec["repo_name"].encode(), value=payload,
                                     timestamp=rec["created_at"], on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.1)
            stats["sent"] += 1
            stats["bytes"] += len(payload)
            if stats["sent"] % 20_000 == 0:
                producer.poll(0)
                print(f"  {hour}: {stats['sent']:,} events")
            if limit and stats["sent"] >= limit:
                break
    producer.flush(60)
    stats["seconds"] = round(time.perf_counter() - t_start, 1)
    stats["schema_id"] = schema_id
    print(f"{hour}: {stats}")
    return stats


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415
    ap = argparse.ArgumentParser(description="replay GH Archive hours onto Kafka as Avro")
    ap.add_argument("--hour", action="append", help="e.g. 2025-09-22-15 (repeatable). Default: the hour 3h ago")
    ap.add_argument("--speed", type=float, default=0.0, help="0 = as fast as possible; 60 = 60x real time")
    ap.add_argument("--limit", type=int, default=None, help="stop after N events per hour (smoke tests)")
    args = ap.parse_args(argv)
    hours = args.hour or [hour_id(datetime.now(UTC) - timedelta(hours=3))]
    for h in hours:
        produce_hour(h, speed=args.speed, limit=args.limit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
