output "endpoint_name" {
  description = "SageMaker endpoint name — Flink calls this via boto3 or the HTTPS invoke URL"
  value       = aws_sagemaker_endpoint.this.name
}

output "endpoint_arn" {
  value = aws_sagemaker_endpoint.this.arn
}

output "ecr_repository_url" {
  value = aws_ecr_repository.this.repository_url
}

output "data_capture_s3_bucket" {
  description = "S3 bucket where DataCaptureConfig writes inference inputs + outputs"
  value       = aws_s3_bucket.data_capture.bucket
}
