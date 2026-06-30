environment        = "prod"
vpc_id             = "vpc-prod-placeholder"
private_subnet_ids = ["subnet-prod-a", "subnet-prod-b", "subnet-prod-c"]
container_image    = "777788889999.dkr.ecr.ap-southeast-2.amazonaws.com/eg-fraud-inference:latest"
model_s3_uri       = "s3://eg-model-artifacts-777788889999/v1.0.0/model.tar.gz"
model_version      = "v1.0.0"
