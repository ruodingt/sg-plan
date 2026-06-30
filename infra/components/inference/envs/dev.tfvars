environment        = "dev"
vpc_id             = "vpc-dev-placeholder"
private_subnet_ids = ["subnet-dev-a", "subnet-dev-b"]
container_image    = "111122223333.dkr.ecr.ap-southeast-2.amazonaws.com/eg-fraud-inference:latest"
model_s3_uri       = "s3://eg-model-artifacts-111122223333/v1.0.0/model.tar.gz"
model_version      = "v1.0.0"
