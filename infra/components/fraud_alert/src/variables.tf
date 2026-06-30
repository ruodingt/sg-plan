variable "environment"      { type = string }
variable "aws_region"       { type = string; default = "ap-southeast-2" }
variable "name_prefix"      { type = string; default = "eg" }
variable "slack_webhook_url" { type = string; sensitive = true }
variable "alert_threshold"  { type = string; default = "0.7" }
