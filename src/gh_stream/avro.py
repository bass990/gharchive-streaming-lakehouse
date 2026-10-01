"""Confluent-framed Avro without the Confluent client: register the schema with
the (Redpanda) Schema Registry over REST, then serialise each record as

    0x00 | 4-byte big-endian schema id | Avro binary

which is exactly what every Kafka Avro consumer (Spark from_avro after
stripping 5 bytes, Kafka Connect, Flink, ksqlDB, the Console UI) expects.

Compatibility: the subject is set to BACKWARD, so a producer that tries to
register a schema which would break existing consumers is refused by the
registry before a single bad message lands on the topic. That is the contract.
"""
from __future__ import annotations

import io
import json
import struct
from pathlib import Path

import requests
from fastavro import parse_schema, schemaless_reader, schemaless_writer

MAGIC = b"\x00"


def load_schema(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def register_schema(registry_url: str, subject: str, schema: dict, compatibility: str = "BACKWARD") -> int:
    """Register (idempotent: same schema -> same id) and pin the subject's compatibility level."""
    r = requests.post(f"{registry_url}/subjects/{subject}/versions", json={"schema": json.dumps(schema), "schemaType": "AVRO"},
                      headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, timeout=30)
    r.raise_for_status()
    schema_id = r.json()["id"]
    requests.put(f"{registry_url}/config/{subject}", json={"compatibility": compatibility},
                 headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, timeout=30).raise_for_status()
    return schema_id


def check_compatible(registry_url: str, subject: str, schema: dict) -> bool:
    r = requests.post(f"{registry_url}/compatibility/subjects/{subject}/versions/latest", json={"schema": json.dumps(schema)},
                      headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, timeout=30)
    if r.status_code == 404:
        return True  # no versions yet
    r.raise_for_status()
    return bool(r.json().get("is_compatible"))


class AvroCodec:
    def __init__(self, schema: dict, schema_id: int):
        self.parsed = parse_schema(schema)
        self.schema_id = schema_id
        self._header = MAGIC + struct.pack(">I", schema_id)

    def encode(self, record: dict) -> bytes:
        buf = io.BytesIO()
        buf.write(self._header)
        schemaless_writer(buf, self.parsed, record)
        return buf.getvalue()

    def decode(self, data: bytes) -> tuple[int, dict]:
        if data[:1] != MAGIC:
            raise ValueError("not a Confluent-framed Avro message")
        schema_id = struct.unpack(">I", data[1:5])[0]
        return schema_id, schemaless_reader(io.BytesIO(data[5:]), self.parsed)
