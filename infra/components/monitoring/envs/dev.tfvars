environment    = "dev"
aws_account_id = "111122223333"

sagemaker_endpoint_name = ""   # populated from inference component output
monitoring_bucket       = "eg-monitoring-dev"

baseline_statistics_s3_uri        = ""  # populated after first training pipeline run
baseline_constraints_s3_uri       = ""
model_baseline_constraints_s3_uri = ""
ground_truth_s3_uri               = "s3://eg-monitoring-dev/ground-truth/"

slack_workspace_id    = "TXXXXXXXXX"
slack_ml_channel_id   = "CXXXXXXXXX"  # #ml-monitoring
slack_tech_channel_id = "CYYYYYYYYY"  # #fraud-infra-ops

min_invocations_per_5min = 5
