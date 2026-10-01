"""GH Archive JSON -> the Avro record. Pure functions, unit-tested."""
from __future__ import annotations

from datetime import UTC, datetime

import orjson

EVENT_TYPES = {
    "PushEvent", "PullRequestEvent", "IssuesEvent", "IssueCommentEvent", "WatchEvent", "ForkEvent", "CreateEvent",
    "DeleteEvent", "ReleaseEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent", "CommitCommentEvent",
    "GollumEvent", "MemberEvent", "PublicEvent",
}


def parse_created_at(s: str) -> int:
    """'2025-09-22T15:00:01Z' -> epoch millis (UTC)."""
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).timestamp() * 1000)


def to_record(raw: bytes | dict, source_file: str, ingested_at_ms: int | None = None) -> dict:
    ev = orjson.loads(raw) if isinstance(raw, bytes | bytearray) else raw
    return {
        "id": str(ev["id"]),
        "type": ev["type"],
        "created_at": parse_created_at(ev["created_at"]),
        "actor_id": int(ev["actor"]["id"]),
        "actor_login": ev["actor"]["login"],
        "repo_id": int(ev["repo"]["id"]),
        "repo_name": ev["repo"]["name"],
        "org_login": (ev.get("org") or {}).get("login"),
        "is_public": bool(ev.get("public", True)),
        "payload_json": orjson.dumps(ev.get("payload") or {}).decode(),
        "source_file": source_file,
        "ingested_at": ingested_at_ms if ingested_at_ms is not None else int(datetime.now(UTC).timestamp() * 1000),
    }


def hour_id(dt: datetime) -> str:
    """GH Archive file name for an hour: 2025-09-22-15 (hours are NOT zero-padded)."""
    return f"{dt:%Y-%m-%d}-{dt.hour}"
