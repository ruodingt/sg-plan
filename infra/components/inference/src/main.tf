module "sagemaker_inference" {
  source = "../../../modules/sagemaker_inference"

  name_prefix        = local.prefix
  environment        = var.environment
  aws_region         = var.aws_region
  vpc_id             = var.vpc_id
  private_subnet_ids = var.private_subnet_ids
  container_image    = var.container_image
  model_s3_uri       = var.model_s3_uri
  model_version      = var.model_version
  fraud_threshold    = var.fraud_threshold

  instance_type          = var.environment == "prod" ? "ml.c6i.xlarge" : "ml.c6i.large"
  initial_instance_count = var.environment == "prod" ? 3 : 1
  min_capacity           = var.environment == "prod" ? 2 : 1
  max_capacity           = var.environment == "prod" ? 20 : 2
}
