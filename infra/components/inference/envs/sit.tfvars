environment        = "sit"
vpc_id             = "vpc-sit-placeholder"
private_subnet_ids = ["subnet-sit-a", "subnet-sit-b"]
container_image    = "444455556666.dkr.ecr.ap-southeast-2.amazonaws.com/eg-fraud-inference:latest"
model_s3_uri       = "s3://eg-model-artifacts-444455556666/v1.0.0/model.tar.gz"
model_version      = "v1.0.0"
