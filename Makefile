.PHONY: help up up-core down produce stream cdc cdc-register cdc-mutate maintain exporter query test lint replay
PYTHON ?= .venv/Scripts/python.exe
HOUR ?=
SPEED ?= 0
export PYTHONUTF8 = 1

# TARGET selects where Iceberg tables live: local | aws | azure | gcp (see streaming/common.py and deploy/README.md).
# Cloud credentials are passed into the container from the host environment.
TARGET ?= local
BASE_PKGS = org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-avro_2.12:3.5.3
PKGS_local = $(BASE_PKGS),org.apache.iceberg:iceberg-aws-bundle:1.6.1
PKGS_aws   = $(PKGS_local)
PKGS_azure = $(BASE_PKGS),org.apache.iceberg:iceberg-azure-bundle:1.6.1,org.postgresql:postgresql:42.7.4
PKGS_gcp   = $(BASE_PKGS),org.apache.iceberg:iceberg-gcp-bundle:1.6.1,org.postgresql:postgresql:42.7.4
CLOUD_ENV = -e LAKEHOUSE_TARGET=$(TARGET) -e LAKEHOUSE_BUCKET -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
	-e AZURE_STORAGE_ACCOUNT -e AZURE_STORAGE_KEY -e GOOGLE_CLOUD_PROJECT -e GOOGLE_APPLICATION_CREDENTIALS=/opt/gcp-adc.json
# Git Bash on Windows rewrites /opt/... paths unless MSYS_NO_PATHCONV=1 is set
SPARK_SUBMIT = MSYS_NO_PATHCONV=1 docker compose exec -T $(CLOUD_ENV) spark /opt/spark/bin/spark-submit --master local[4] --driver-memory 2g \
	--packages $(PKGS_$(TARGET))

help:
	@echo "GitHub Archive streaming lakehouse"
	@echo "  up            start everything (redpanda, console, minio, iceberg-rest, postgres, debezium, spark, prometheus, grafana)"
	@echo "  up-core       only the broker + object store + catalog"
	@echo "  produce       replay a GH Archive hour onto Kafka as Avro     (HOUR=2025-09-22-15 SPEED=0)"
	@echo "  stream        Spark Structured Streaming: Kafka -> Iceberg events + 5-min windows (exactly once)"
	@echo "  cdc-register  create the Debezium Postgres connector"
	@echo "  cdc           Spark: CDC topic -> SCD2 dimension in Iceberg"
	@echo "  cdc-mutate    change rows in Postgres and watch SCD2 versions appear"
	@echo "  maintain      compact small files, expire snapshots, remove orphans"
	@echo "  exporter      Prometheus exporter for freshness / e2e latency / file counts (:9108)"
	@echo "  replay        delete an hour from gh.events and re-produce it (HOUR=...)"
	@echo "  query         ad-hoc DuckDB over the Iceberg tables"
	@echo "  test / lint"

up:
	docker compose up -d --wait

up-core:
	docker compose up -d --wait redpanda minio iceberg-rest

down:
	docker compose down -v

produce:
	$(PYTHON) -m gh_stream.producer $(if $(HOUR),--hour $(HOUR),) --speed $(SPEED)

stream:
	$(SPARK_SUBMIT) /app/streaming/stream_events.py

cdc-register:
	$(PYTHON) -m gh_stream.cdc register

cdc:
	$(SPARK_SUBMIT) /app/streaming/cdc_merge.py

cdc-mutate:
	$(PYTHON) -m gh_stream.cdc mutate --n 5

maintain:
	$(SPARK_SUBMIT) /app/streaming/maintenance.py

exporter:
	$(PYTHON) -m gh_stream.exporter

replay:
	$(PYTHON) -m gh_stream.replay --hour $(HOUR)

query:
	$(PYTHON) -m gh_stream.query

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check src tests streaming
