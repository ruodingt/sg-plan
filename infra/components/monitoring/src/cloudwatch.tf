# ── ML alerts → #ml-monitoring ────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "data_drift_violations" {
  alarm_name          = "fraud-data-drift-violations-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "feature_baseline_drift_violations"
  namespace           = "/aws/sagemaker/Endpoints/data-metrics"
  period              = 3600
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Feature drift vs training baseline detected"
  alarm_actions       = [aws_sns_topic.ml_alerts.arn]
  dimensions = {
    MonitoringSchedule = aws_sagemaker_monitoring_schedule.data_quality.name
    Endpoint           = var.sagemaker_endpoint_name
  }
}

resource "aws_cloudwatch_metric_alarm" "model_quality_violations" {
  alarm_name          = "fraud-model-quality-violations-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "metric_violations"
  namespace           = "/aws/sagemaker/Endpoints/model-metrics"
  period              = 86400
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Model quality (AUC/precision) dropped below baseline constraints"
  alarm_actions       = [aws_sns_topic.ml_alerts.arn]
  dimensions = {
    MonitoringSchedule = aws_sagemaker_monitoring_schedule.model_quality.name
    Endpoint           = var.sagemaker_endpoint_name
  }
}

# ── Tech alerts → #fraud-infra-ops ────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "endpoint_latency_p99" {
  alarm_name          = "fraud-endpoint-latency-p99-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ModelLatency"
  namespace           = "AWS/SageMaker"
  period              = 60
  extended_statistic  = "p99"
  threshold           = 500000 # 500 ms in microseconds
  alarm_description   = "SageMaker p99 latency > 500 ms for 3 consecutive minutes"
  alarm_actions       = [aws_sns_topic.tech_alerts.arn]
  dimensions = {
    EndpointName = var.sagemaker_endpoint_name
    VariantName  = "AllTraffic"
  }
}

resource "aws_cloudwatch_metric_alarm" "endpoint_5xx" {
  alarm_name          = "fraud-endpoint-5xx-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Invocation5XXErrors"
  namespace           = "AWS/SageMaker"
  period              = 60
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "SageMaker endpoint returning 5xx errors"
  alarm_actions       = [aws_sns_topic.tech_alerts.arn]
  dimensions = {
    EndpointName = var.sagemaker_endpoint_name
    VariantName  = "AllTraffic"
  }
}

resource "aws_cloudwatch_metric_alarm" "invocation_volume_low" {
  alarm_name          = "fraud-invocation-volume-low-${var.environment}"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 3
  metric_name         = "Invocations"
  namespace           = "AWS/SageMaker"
  period              = 300
  statistic           = "Sum"
  threshold           = var.min_invocations_per_5min
  alarm_description   = "Invocation volume below expected minimum — possible upstream Flink or Kinesis failure"
  alarm_actions       = [aws_sns_topic.tech_alerts.arn]
  dimensions = {
    EndpointName = var.sagemaker_endpoint_name
    VariantName  = "AllTraffic"
  }
}
