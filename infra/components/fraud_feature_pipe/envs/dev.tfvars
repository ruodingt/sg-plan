environment              = "dev"
vpc_id                   = "vpc-dev-placeholder"
private_subnet_ids       = ["subnet-dev-a", "subnet-dev-b"]
flink_artifacts_bucket   = "eg-flink-artifacts-111122223333"
flink_jar_s3_key         = "fraud-pipeline-latest.jar"
flink_checkpoints_bucket = "eg-flink-checkpoints-111122223333"

# Populated by CI/CD from upstream component outputs
transactions_stream_name = ""
transactions_stream_arn  = ""
demographics_stream_name = ""
demographics_stream_arn  = ""
sagemaker_endpoint_name  = ""
fraud_alerts_queue_url   = ""
fraud_alerts_queue_arn   = ""
