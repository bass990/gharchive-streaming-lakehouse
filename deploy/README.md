# Deploying the streaming lakehouse to a cloud

The Spark jobs, the exporter and the query tool are the same on every target.
`LAKEHOUSE_TARGET` selects the Iceberg catalog and object store; Kafka,
Debezium, Prometheus and Grafana stay in the compose stack (or become MSK /
managed Connect / managed Grafana on the cloud of your choice).

| Target  | Object store | Iceberg catalog (Spark)          | Host tools (pyiceberg)        | Status |
|---------|--------------|----------------------------------|-------------------------------|--------|
| `local` | MinIO        | REST catalog (compose)           | REST catalog                  | verified end to end (README) |
| `aws`   | S3           | AWS Glue Data Catalog            | Glue                          | applied + verified: full topic streamed into Glue-catalogued Iceberg on S3 (169,767 events = distinct ids, 87,350 windows), read back from the host through Glue |
| `azure` | ADLS Gen2    | JDBC catalog in compose Postgres | SQL catalog (same tables)     | applied (northcentralus) + verified: full topic streamed into Iceberg on ADLS Gen2 `ghlakehousebass990` via the JDBC catalog (169,767 events = distinct ids, 87,350 windows), read back from the host through pyiceberg's SQL catalog |
| `gcp`   | GCS          | JDBC catalog in compose Postgres | SQL catalog (same tables)     | applied + verified: full topic streamed into Iceberg on GCS `gh-lakehouse-bass990` via the JDBC catalog + GCSFileIO with ADC (169,767 events = distinct ids, 87,350 windows), read back from the host through pyiceberg's SQL catalog |

Spark's JDBC catalog and pyiceberg's SQL catalog share the `iceberg_tables`
schema, so a table Spark creates on Azure or GCP is immediately visible to the
exporter and `make query` on the host. Each target gets its own catalog
database in the compose Postgres (`gh_azure`, `gh_gcp`, created with
`CREATE DATABASE`), because the catalog keys tables by name and `gh.events`
on Azure must not collide with `gh.events` on GCS. Checkpoints are kept per target under
`/checkpoints/<target>`, so switching targets never replays into the wrong
warehouse.

## AWS

```bash
cd deploy/aws && terraform init && terraform apply -var account_alias=<suffix> -var alert_email=you@example.com
eval "$(aws configure export-credentials --format env)"       # AWS_ACCESS_KEY_ID / SECRET / SESSION_TOKEN
export LAKEHOUSE_BUCKET=$(terraform -chdir=deploy/aws output -raw bucket) AWS_REGION=us-east-2
make stream TARGET=aws              # Glue catalog `gh`, tables in s3://<bucket>/warehouse
LAKEHOUSE_TARGET=aws make query     # host-side read through Glue
```

MSK Serverless is defined behind `-var create_msk=true` and not created by
default (~$980/month idle). With it, point `KAFKA_BOOTSTRAP` at the cluster and
add the IAM auth jars to the Spark packages; nothing else changes.

## Azure

```bash
az login
cd deploy/azure && terraform init && terraform apply -var account_alias=<3-11 lowercase alnum>
export LAKEHOUSE_BUCKET=gh-lakehouse AZURE_STORAGE_ACCOUNT=$(terraform -chdir=deploy/azure output -raw storage_account)
export AZURE_STORAGE_KEY=$(az storage account keys list -g rg-gh-lakehouse -n $AZURE_STORAGE_ACCOUNT --query "[0].value" -o tsv)
make stream TARGET=azure            # ADLSFileIO with shared key; catalog rows in the compose Postgres
LAKEHOUSE_TARGET=azure make query
```

The Azure for Students subscription carries an "Allowed resource deployment
regions" policy (canadacentral, westus, norwayeast, northcentralus,
mexicocentral as of 2026-09); the module defaults to northcentralus.

## GCP

```bash
gcloud auth application-default login
cd deploy/gcp && terraform init && terraform apply -var project=<project-id> -var account_alias=<suffix>
docker compose cp "$APPDATA/gcloud/application_default_credentials.json" spark:/opt/gcp-adc.json   # ADC into the container
export LAKEHOUSE_BUCKET=$(terraform -chdir=deploy/gcp output -raw bucket) GOOGLE_CLOUD_PROJECT=<project-id>
make stream TARGET=gcp              # GCSFileIO with ADC; catalog rows in the compose Postgres
LAKEHOUSE_TARGET=gcp make query
```

## Bounded backfills

`STOP_WHEN_IDLE_BATCHES=3 make stream TARGET=aws` runs the stream until the
topic is caught up (three consecutive empty triggers) and exits. That is how
the cloud verifications below were produced: one pass over the full topic per
target, then a host-side read of the resulting tables.

## Tearing down

`terraform destroy` in each module directory; buckets are created with
`force_destroy`.
