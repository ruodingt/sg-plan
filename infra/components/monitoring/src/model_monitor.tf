data "aws_sagemaker_prebuilt_ecr_image" "monitor" {
  repository_name = "sagemaker-model-monitor-analyzer"
}

# ── IAM ───────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "model_monitor" {
  name = "fraud-model-monitor-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sagemaker.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "model_monitor" {
  role = aws_iam_role.model_monitor.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = ["arn:aws:s3:::${var.monitoring_bucket}", "arn:aws:s3:::${var.monitoring_bucket}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["sagemaker:InvokeEndpoint", "sagemaker:DescribeEndpoint"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ── Data Quality Monitor (data drift) ─────────────────────────────────────────
# Baseline: statistics.json + constraints.json produced by ML training pipeline.
# Terraform references the S3 path — updating baseline = update the variable + re-apply.

resource "aws_sagemaker_data_quality_job_definition" "fraud" {
  name     = "fraud-data-quality-${var.environment}"
  role_arn = aws_iam_role.model_monitor.arn

  data_quality_app_specification {
    image_uri = data.aws_sagemaker_prebuilt_ecr_image.monitor.registry_path
  }

  data_quality_baseline_config {
    statistics_resource { s3_uri = var.baseline_statistics_s3_uri }
    constraints_resource { s3_uri = var.baseline_constraints_s3_uri }
  }

  data_quality_job_input {
    endpoint_input {
      endpoint_name             = var.sagemaker_endpoint_name
      local_path                = "/opt/ml/processing/input/endpoint"
      s3_data_distribution_type = "FullyReplicated"
      s3_input_mode             = "File"
    }
  }

  data_quality_job_output_config {
    monitoring_outputs {
      s3_output {
        local_path     = "/opt/ml/processing/output"
        s3_uri         = "s3://${var.monitoring_bucket}/data-quality/${var.environment}/"
        s3_upload_mode = "EndOfJob"
      }
    }
  }

  job_resources {
    cluster_config {
      instance_count    = 1
      instance_type     = "ml.m5.xlarge"
      volume_size_in_gb = 20
    }
  }
}

resource "aws_sagemaker_monitoring_schedule" "data_quality" {
  name = "fraud-data-quality-${var.environment}"

  monitoring_schedule_config {
    monitoring_job_definition_name = aws_sagemaker_data_quality_job_definition.fraud.name
    monitoring_type                = "DataQuality"
    schedule_config {
      schedule_expression = "cron(0 0 ? * * *)" # daily
    }
  }
}

# ── Model Quality Monitor (model drift) ───────────────────────────────────────
# Requires ground truth labels at var.ground_truth_s3_uri.
# Labels are merged by the fraud ops pipeline — typically with a 24-48h delay.

resource "aws_sagemaker_model_quality_job_definition" "fraud" {
  name     = "fraud-model-quality-${var.environment}"
  role_arn = aws_iam_role.model_monitor.arn

  model_quality_app_specification {
    image_uri    = data.aws_sagemaker_prebuilt_ecr_image.monitor.registry_path
    problem_type = "BinaryClassification"
  }

  model_quality_baseline_config {
    constraints_resource { s3_uri = var.model_baseline_constraints_s3_uri }
  }

  model_quality_job_input {
    endpoint_input {
      endpoint_name                   = var.sagemaker_endpoint_name
      local_path                      = "/opt/ml/processing/input/endpoint"
      inference_attribute             = "fraud_probability"
      probability_attribute           = "fraud_probability"
      probability_threshold_attribute = "0.5"
    }
    ground_truth_s3_input {
      s3_uri = var.ground_truth_s3_uri
    }
  }

  model_quality_job_output_config {
    monitoring_outputs {
      s3_output {
        local_path     = "/opt/ml/processing/output"
        s3_uri         = "s3://${var.monitoring_bucket}/model-quality/${var.environment}/"
        s3_upload_mode = "EndOfJob"
      }
    }
  }

  job_resources {
    cluster_config {
      instance_count    = 1
      instance_type     = "ml.m5.xlarge"
      volume_size_in_gb = 20
    }
  }
}

resource "aws_sagemaker_monitoring_schedule" "model_quality" {
  name = "fraud-model-quality-${var.environment}"

  monitoring_schedule_config {
    monitoring_job_definition_name = aws_sagemaker_model_quality_job_definition.fraud.name
    monitoring_type                = "ModelQuality"
    schedule_config {
      schedule_expression = "cron(0 0 ? * * *)" # daily — ground truth arrives with ~24h lag
    }
  }
}
