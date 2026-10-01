"""Event normalisation, Avro framing round-trip, schema-evolution rules, replay bounds. No broker needed."""
from __future__ import annotations

import json
import struct
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastavro import parse_schema

from gh_stream.avro import MAGIC, AvroCodec, load_schema
from gh_stream.events import EVENT_TYPES, hour_id, parse_created_at, to_record
from gh_stream.replay import hour_bounds

SCHEMA = load_schema(Path(__file__).resolve().parents[1] / "schemas" / "gh_event.avsc")

RAW = {
    "id": "45123456789", "type": "PushEvent", "public": True, "created_at": "2025-09-22T15:00:01Z",
    "actor": {"id": 1234, "login": "octocat"}, "repo": {"id": 5678, "name": "apache/iceberg"},
    "org": {"id": 47359, "login": "apache"},
    "payload": {"ref": "refs/heads/main", "size": 2, "commits": [{"sha": "abc"}, {"sha": "def"}]},
}


def test_to_record_flattens_and_keeps_payload():
    rec = to_record(json.dumps(RAW).encode(), "2025-09-22-15", ingested_at_ms=1)
    assert rec["id"] == "45123456789" and rec["type"] in EVENT_TYPES
    assert rec["created_at"] == parse_created_at("2025-09-22T15:00:01Z") == 1758553201000
    assert rec["repo_name"] == "apache/iceberg" and rec["org_login"] == "apache"
    assert json.loads(rec["payload_json"])["size"] == 2
    assert rec["source_file"] == "2025-09-22-15" and rec["ingested_at"] == 1


def test_to_record_without_org():
    raw = {**RAW}
    raw.pop("org")
    assert to_record(raw, "x", 0)["org_login"] is None


def test_avro_roundtrip_with_confluent_framing():
    codec = AvroCodec(SCHEMA, schema_id=42)
    rec = to_record(RAW, "2025-09-22-15", ingested_at_ms=1758553300000)
    data = codec.encode(rec)
    assert data[:1] == MAGIC and struct.unpack(">I", data[1:5])[0] == 42
    sid, back = codec.decode(data)
    assert sid == 42
    # logical timestamp-millis comes back as a datetime
    assert back["created_at"] == datetime(2025, 9, 22, 15, 0, 1, tzinfo=UTC)
    assert back["repo_name"] == "apache/iceberg" and back["org_login"] == "apache"
    with pytest.raises(ValueError):
        codec.decode(b"\x01" + data[1:])


def test_schema_has_defaults_for_every_optional_field():
    """BACKWARD compatibility rule of thumb: a consumer on the old schema must read new data.
    Every nullable field must carry a default so the field can be added/removed safely."""
    parse_schema(SCHEMA)
    for f in SCHEMA["fields"]:
        if isinstance(f["type"], list) and "null" in f["type"]:
            assert "default" in f, f["name"]


def test_hour_helpers():
    assert hour_id(datetime(2025, 9, 22, 5, tzinfo=UTC)) == "2025-09-22-5"   # GH Archive does not zero-pad hours
    assert hour_bounds("2025-09-22-15") == ("2025-09-22T15:00:00", "2025-09-22T16:00:00")
    assert hour_bounds("2025-12-31-23") == ("2025-12-31T23:00:00", "2026-01-01T00:00:00")
