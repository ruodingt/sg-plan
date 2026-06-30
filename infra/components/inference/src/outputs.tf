output "endpoint_name"         { value = module.sagemaker_inference.endpoint_name }
output "ecr_repository_url"    { value = module.sagemaker_inference.ecr_repository_url }
output "data_capture_s3_bucket" { value = module.sagemaker_inference.data_capture_s3_bucket }
