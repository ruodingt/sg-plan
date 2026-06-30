variable "environment"   { type = string }
variable "aws_region"    { type = string; default = "ap-southeast-2" }
variable "aws_account_id" { type = string }

# Populated from inference component outputs
variable "sagemaker_endpoint_name" { type = string }

# Populated after ML training pipeline runs the baseline job
variable "baseline_statistics_s3_uri" {
  description = "s3:// path to statistics.json produced by the training pipeline baseline job"
  type        = string
}
variable "baseline_constraints_s3_uri" {
  description = "s3:// path to constraints.json produced by the training pipeline baseline job"
  type        = string
}
variable "model_baseline_constraints_s3_uri" {
  description = "s3:// path to model quality constraints.json (AUC / precision thresholds)"
  type        = string
}
variable "ground_truth_s3_uri" {
  description = "s3:// prefix where fraud ops pipeline merges confirmed labels for Model Quality Monitor"
  type        = string
}

variable "monitoring_bucket" {
  description = "S3 bucket for Model Monitor output reports"
  type        = string
}

# Slack — workspace must be authorized in AWS Console once before applying
variable "slack_workspace_id"  { type = string }
variable "slack_ml_channel_id" {
  description = "Channel ID for drift / model quality alerts (#ml-monitoring)"
  type        = string
}
variable "slack_tech_channel_id" {
  description = "Channel ID for infra / latency / error alerts (#fraud-infra-ops)"
  type        = string
}

variable "min_invocations_per_5min" {
  description = "Invocations below this threshold in a 5-min window triggers a volume alarm"
  type        = number
  default     = 10
}
