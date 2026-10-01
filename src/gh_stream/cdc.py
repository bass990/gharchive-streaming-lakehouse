"""Register the Debezium Postgres connector and drive changes into the source table.

    python -m gh_stream.cdc register          # create/update the connector (idempotent)
    python -m gh_stream.cdc status
    python -m gh_stream.cdc mutate --n 5      # random tier changes + one insert + one delete, to watch SCD2 happen
"""
from __future__ import annotations

import random
import sys
import time

import psycopg
import requests

from .config import Settings, settings

CONNECTOR = "gh-watchlist-cdc"


def connector_config(cfg: Settings) -> dict:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": "postgres", "database.port": "5432",
        "database.user": "gh", "database.password": "gh-local", "database.dbname": "gh",
        "topic.prefix": "cdc",
        "table.include.list": "public.repo_watchlist",
        "slot.name": "gh_watchlist_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "snapshot.mode": "initial",
        "decimal.handling.mode": "double",
        "time.precision.mode": "connect",
        "heartbeat.interval.ms": "10000",
    }


def register(cfg: Settings = settings) -> dict:
    r = requests.put(f"{cfg.connect_url}/connectors/{CONNECTOR}/config", json=connector_config(cfg), timeout=60)
    r.raise_for_status()
    time.sleep(3)
    return status(cfg)


def status(cfg: Settings = settings) -> dict:
    r = requests.get(f"{cfg.connect_url}/connectors/{CONNECTOR}/status", timeout=30)
    r.raise_for_status()
    s = r.json()
    print(f"{CONNECTOR}: connector={s['connector']['state']} tasks={[t['state'] for t in s['tasks']]}")
    return s


def mutate(cfg: Settings = settings, n: int = 5, seed: int | None = None) -> None:
    rng = random.Random(seed)
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        repos = [r[0] for r in conn.execute("SELECT repo_name FROM repo_watchlist").fetchall()]
        for _ in range(n):
            repo = rng.choice(repos)
            tier = rng.choice(["tier1", "tier2", "tier3"])
            conn.execute("UPDATE repo_watchlist SET tier = %s, updated_at = now() WHERE repo_name = %s", (tier, repo))
            print(f"update {repo} -> {tier}")
            time.sleep(0.5)
        new = f"example/repo-{rng.randint(1000, 9999)}"
        conn.execute("INSERT INTO repo_watchlist (repo_name, tier, owner_team) VALUES (%s, 'tier3', 'sandbox')", (new,))
        print(f"insert {new}")
        victim = rng.choice(repos)
        conn.execute("DELETE FROM repo_watchlist WHERE repo_name = %s", (victim,))
        print(f"delete {victim}")


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["register", "status", "mutate"])
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--seed", type=int, default=None)
    a = ap.parse_args(argv)
    {"register": lambda: register(), "status": lambda: status(), "mutate": lambda: mutate(n=a.n, seed=a.seed)}[a.cmd]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
