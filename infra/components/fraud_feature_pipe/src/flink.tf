data "aws_iam_policy_document" "kda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals { type = "Service"; identifiers = ["kinesisanalytics.amazonaws.com"] }
  }
}

resource "aws_iam_role" "kda" {
  name               = "${local.prefix}-kda-role"
  assume_role_policy = data.aws_iam_policy_document.kda_assume.json
}

resource "aws_iam_role_policy" "kda_kinesis" {
  name = "kinesis-read"
  role = aws_iam_role.kda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:ListShards"]
      Resource = [var.transactions_stream_arn, var.demographics_stream_arn]
    }]
  })
}

resource "aws_iam_role_policy" "kda_sagemaker" {
  name = "sagemaker-invoke"
  role = aws_iam_role.kda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sagemaker:InvokeEndpoint"]
      Resource = "arn:aws:sagemaker:${var.aws_region}:${local.account_ids[var.environment]}:endpoint/${var.sagemaker_endpoint_name}"
    }]
  })
}

resource "aws_iam_role_policy" "kda_sqs" {
  name = "sqs-send"
  role = aws_iam_role.kda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = var.fraud_alerts_queue_arn
    }]
  })
}

resource "aws_iam_role_policy" "kda_s3" {
  name = "s3-artifacts-and-checkpoints"
  role = aws_iam_role.kda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "arn:aws:s3:::${var.flink_artifacts_bucket}/${var.flink_jar_s3_key}"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "arn:aws:s3:::${var.flink_checkpoints_bucket}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = ["arn:aws:s3:::${var.flink_artifacts_bucket}", "arn:aws:s3:::${var.flink_checkpoints_bucket}"]
      },
    ]
  })
}

resource "aws_iam_role_policy" "kda_logs" {
  name = "cloudwatch-logs"
  role = aws_iam_role.kda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
      Resource = "${aws_cloudwatch_log_group.kda.arn}:*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "kda_vpc" {
  role       = aws_iam_role.kda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonKinesisAnalyticsFullAccess"
}

resource "aws_security_group" "kda" {
  name        = "${local.prefix}-kda-sg"
  description = "KDA Flink — outbound HTTPS only"
  vpc_id      = var.vpc_id
  egress {
    description = "HTTPS to AWS services"
    from_port   = 443; to_port = 443; protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_cloudwatch_log_group" "kda" {
  name              = "/aws/kda/${local.prefix}-fraud-pipeline"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_stream" "kda" {
  name           = "fraud-pipeline"
  log_group_name = aws_cloudwatch_log_group.kda.name
}

resource "aws_kinesisanalyticsv2_application" "fraud_pipeline" {
  name                   = "${local.prefix}-fraud-pipeline"
  runtime_environment    = "FLINK-1_18"
  service_execution_role = aws_iam_role.kda.arn

  application_configuration {
    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = "arn:aws:s3:::${var.flink_artifacts_bucket}"
          file_key   = var.flink_jar_s3_key
        }
      }
      code_content_type = "ZIPFILE"
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = 60000
        min_pause_between_checkpoints = 5000
      }
      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }
      parallelism_configuration {
        configuration_type   = "CUSTOM"
        auto_scaling_enabled = true
        parallelism          = var.environment == "prod" ? 30 : 2
        parallelism_per_kpu  = 1
      }
    }

    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = {
          "kinesis.transactions.stream" = var.transactions_stream_name
          "kinesis.demographics.stream" = var.demographics_stream_name
          "sagemaker.endpoint.name"     = var.sagemaker_endpoint_name
          "sqs.fraud.alerts.queue.url"  = var.fraud_alerts_queue_url
          "s3.checkpoints.bucket"       = var.flink_checkpoints_bucket
          "aws.region"                  = var.aws_region
        }
      }
    }

    vpc_configuration {
      security_group_ids = [aws_security_group.kda.id]
      subnet_ids         = var.private_subnet_ids
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.kda.arn
  }
}
