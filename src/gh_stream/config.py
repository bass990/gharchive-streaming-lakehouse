"""Runtime settings for the host-side Python (producer, exporter, replay tooling).
The Spark jobs read the same names from their container environment."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    target: str = os.getenv("LAKEHOUSE_TARGET", "local").strip().lower()
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
    schema_registry: str = os.getenv("SCHEMA_REGISTRY", "http://localhost:18081")
    topic_events: str = os.getenv("TOPIC_EVENTS", "gh.events.v1")
    topic_partitions: int = int(os.getenv("TOPIC_PARTITIONS", "6"))
    lakehouse_bucket: str = os.getenv("LAKEHOUSE_BUCKET", "gh-lakehouse")
    iceberg_rest_uri: str = os.getenv("ICEBERG_REST_URI", "http://localhost:8182")
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://localhost:9010")
    s3_access_key: str = os.getenv("MINIO_ROOT_USER", "lakehouse")
    s3_secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "lakehouse-local")
    pg_dsn: str = os.getenv("PG_DSN", "postgresql://gh:gh-local@localhost:5434/gh")
    connect_url: str = os.getenv("CONNECT_URL", "http://localhost:8083")
    gharchive_base: str = os.getenv("GHARCHIVE_BASE", "https://data.gharchive.org")
    aws_region: str = os.getenv("AWS_REGION", "us-east-2")
    azure_storage_account: str | None = os.getenv("AZURE_STORAGE_ACCOUNT") or None
    azure_storage_key: str | None = os.getenv("AZURE_STORAGE_KEY") or None
    gcp_project: str | None = os.getenv("GOOGLE_CLOUD_PROJECT") or None
    data_dir: Path = ROOT / "data"

    @property
    def warehouse(self) -> str:
        if self.target == "azure":
            return f"abfss://{self.lakehouse_bucket}@{self.azure_storage_account}.dfs.core.windows.net/warehouse"
        if self.target == "gcp":
            return f"gs://{self.lakehouse_bucket}/warehouse"
        return f"s3://{self.lakehouse_bucket}/warehouse"


settings = Settings()
