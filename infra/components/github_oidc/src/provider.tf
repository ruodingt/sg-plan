terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  # Uses the S3 backend created by infra/bootstrap — run locally before GHA exists.
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region
}
