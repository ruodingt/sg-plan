terraform {
  required_version = ">= 1.5"
  required_providers {
    aws     = { source = "hashicorp/aws",        version = "~> 5.0" }
    archive = { source = "hashicorp/archive",    version = "~> 2.0" }
  }
  backend "s3" {}
}

provider "aws" {
  region              = var.aws_region
  allowed_account_ids = [local.account_ids[var.environment]]
  default_tags {
    tags = { Project = "fraud-inference", Environment = var.environment, ManagedBy = "terraform" }
  }
}

locals {
  prefix = "${var.name_prefix}-${var.environment}"
  account_ids = { dev = "111122223333", sit = "444455556666", prod = "777788889999" }
}
