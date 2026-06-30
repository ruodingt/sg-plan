variable "environment"        { type = string }
variable "aws_region"         { type = string; default = "ap-southeast-2" }
variable "name_prefix"        { type = string; default = "eg" }
variable "vpc_id"             { type = string }
variable "private_subnet_ids" { type = list(string) }

# ── Cross-component inputs (populated by CI/CD from upstream outputs) ─────────
variable "transactions_stream_name" { type = string }
variable "transactions_stream_arn"  { type = string }
variable "demographics_stream_name" { type = string }
variable "demographics_stream_arn"  { type = string }
variable "sagemaker_endpoint_name"  { type = string }
variable "fraud_alerts_queue_url"   { type = string }
variable "fraud_alerts_queue_arn"   { type = string }

# ── Flink artifact ────────────────────────────────────────────────────────────
variable "flink_artifacts_bucket"   { type = string }
variable "flink_jar_s3_key"         { type = string }
variable "flink_checkpoints_bucket" { type = string }
