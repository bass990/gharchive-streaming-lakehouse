# AWS target for the streaming lakehouse. Storage + catalog are cheap and are
# applied; the Kafka cluster (MSK Serverless) is defined behind a flag because
# it bills ~$0.75/hour whether or not messages flow, and a portfolio project
# should not leave that running. The Spark jobs run unchanged on EMR
# Serverless / Glue Streaming with the Glue catalog.
#
#   terraform apply -var account_alias=<suffix>                   # S3 + Glue + budget
#   terraform apply -var account_alias=<suffix> -var create_msk=true   # + MSK Serverless (costs money)

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.60" }
  }
}

provider "aws" {
  region = var.region
}

variable "region" { default = "us-east-2" }
variable "account_alias" { type = string }
variable "create_msk" {
  type    = bool
  default = false
}
variable "budget_usd" { default = 10 }
variable "alert_email" {
  type    = string
  default = ""
}

resource "aws_s3_bucket" "lakehouse" {
  bucket        = "gh-lakehouse-${var.account_alias}"
  force_destroy = true
  tags          = { project = "gharchive-streaming-lakehouse" }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

# Iceberg data files go to IA after 30 days; checkpoints are hot; multipart leftovers are aborted.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    id     = "events-to-ia"
    status = "Enabled"
    filter { prefix = "warehouse/gh/events/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
  rule {
    id     = "abort-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 2 }
  }
}

resource "aws_glue_catalog_database" "gh" {
  name         = "gh"
  location_uri = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse/gh"
}

# --- optional Kafka --------------------------------------------------------------
data "aws_vpc" "default" {
  count   = var.create_msk ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = var.create_msk ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default[0].id]
  }
}

resource "aws_security_group" "msk" {
  count  = var.create_msk ? 1 : 0
  name   = "gh-msk"
  vpc_id = data.aws_vpc.default[0].id
  ingress {
    from_port = 9098
    to_port   = 9098
    protocol  = "tcp"
    self      = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_serverless_cluster" "gh" {
  count        = var.create_msk ? 1 : 0
  cluster_name = "gh-events"
  vpc_config {
    subnet_ids         = slice(data.aws_subnets.default[0].ids, 0, 2)
    security_group_ids = [aws_security_group.msk[0].id]
  }
  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

resource "aws_budgets_budget" "monthly" {
  name         = "gh-lakehouse-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  dynamic "notification" {
    for_each = var.alert_email == "" ? [] : [1]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = 80
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.alert_email]
    }
  }
}

output "bucket" { value = aws_s3_bucket.lakehouse.bucket }
output "warehouse" { value = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse" }
output "glue_database" { value = aws_glue_catalog_database.gh.name }
output "msk_arn" { value = var.create_msk ? aws_msk_serverless_cluster.gh[0].arn : "not created (create_msk=false)" }
