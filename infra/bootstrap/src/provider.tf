terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  # Local state only — bootstrap creates the S3 bucket, so it cannot use one.
  # Store the generated terraform.tfstate securely (e.g. a password manager).
  backend "local" {}
}

provider "aws" {
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]
}
