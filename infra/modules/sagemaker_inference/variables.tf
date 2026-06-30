variable "name_prefix" {
  type = string
}

variable "environment" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  description = "Private subnets — endpoint stays inside the VPC, only reachable by Flink"
  type        = list(string)
}

variable "container_image" {
  description = "ECR image URI (registry/repo:tag)"
  type        = string
}

variable "model_s3_uri" {
  description = "S3 URI of fraud_model.tar.gz (SageMaker extracts to /opt/ml/model/)"
  type        = string
}

variable "model_version" {
  description = "Human-readable model version injected as env var, e.g. v1.2.0 or 2024-06-29"
  type        = string
}

variable "fraud_threshold" {
  description = "Probability threshold above which fraud_flag=true. Lower to increase recall during drift periods."
  type        = number
  default     = 0.5
}

variable "instance_type" {
  description = "SageMaker instance type"
  type        = string
  default     = "ml.c6i.xlarge"
}

variable "initial_instance_count" {
  type    = number
  default = 2
}

variable "min_capacity" {
  type    = number
  default = 2
}

variable "max_capacity" {
  type    = number
  default = 20
}

variable "data_capture_sampling_percentage" {
  description = "Percentage of requests captured to S3 (100 = all)"
  type        = number
  default     = 100
}
