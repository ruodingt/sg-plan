variable "environment"        { type = string }
variable "aws_region"         { type = string; default = "ap-southeast-2" }
variable "name_prefix"        { type = string; default = "eg" }
variable "vpc_id"             { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "container_image"    { type = string }
variable "model_s3_uri"       { type = string }
variable "model_version"      { type = string }
