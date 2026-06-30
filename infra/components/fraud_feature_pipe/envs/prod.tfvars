environment              = "prod"
vpc_id                   = "vpc-prod-placeholder"
private_subnet_ids       = ["subnet-prod-a", "subnet-prod-b", "subnet-prod-c"]
flink_artifacts_bucket   = "eg-flink-artifacts-777788889999"
flink_jar_s3_key         = "fraud-pipeline-latest.jar"
flink_checkpoints_bucket = "eg-flink-checkpoints-777788889999"

# Populated by CI/CD from upstream component outputs
transactions_stream_name = ""
transactions_stream_arn  = ""
demographics_stream_name = ""
demographics_stream_arn  = ""
sagemaker_endpoint_name  = ""
fraud_alerts_queue_url   = ""
fraud_alerts_queue_arn   = ""
