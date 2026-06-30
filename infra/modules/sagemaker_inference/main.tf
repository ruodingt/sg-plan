locals {
  tags = {
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ── ECR ───────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "this" {
  name                 = "${var.name_prefix}-fraud-inference"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.tags
}

resource "aws_ecr_lifecycle_policy" "this" {
  repository = aws_ecr_repository.this.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

# ── S3 for DataCaptureConfig ──────────────────────────────────────────────────
resource "aws_s3_bucket" "data_capture" {
  bucket        = "${var.name_prefix}-inference-capture-${var.environment}"
  force_destroy = var.environment != "prod"
  tags          = local.tags
}

resource "aws_s3_bucket_lifecycle_configuration" "data_capture" {
  bucket = aws_s3_bucket.data_capture.id

  rule {
    id     = "expire-captures"
    status = "Enabled"
    expiration {
      days = 90
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_capture" {
  bucket = aws_s3_bucket.data_capture.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ── IAM role for SageMaker ────────────────────────────────────────────────────
resource "aws_iam_role" "sagemaker" {
  name = "${var.name_prefix}-sagemaker-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "sagemaker.amazonaws.com" }
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "sagemaker_managed" {
  role       = aws_iam_role.sagemaker.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

resource "aws_iam_role_policy" "ecr_pull" {
  name = "ecr-pull"
  role = aws_iam_role.sagemaker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetAuthorizationToken",
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${replace(var.model_s3_uri, "s3://", "arn:aws:s3:::") }"
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.data_capture.arn}/captures/*"
      }
    ]
  })
}

# ── Security group ────────────────────────────────────────────────────────────
resource "aws_security_group" "sagemaker" {
  name        = "${var.name_prefix}-sagemaker-sg"
  description = "SageMaker endpoint — inbound from Flink only"
  vpc_id      = var.vpc_id

  ingress {
    description = "HTTPS from VPC (Flink)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

# ── SageMaker Model ───────────────────────────────────────────────────────────
resource "aws_sagemaker_model" "this" {
  name               = "${var.name_prefix}-fraud-model"
  execution_role_arn = aws_iam_role.sagemaker.arn

  primary_container {
    image          = var.container_image
    model_data_url = var.model_s3_uri

    environment = {
      MODEL_PATH       = "/opt/ml/model/fraud_model.pkl"
      MODEL_VERSION    = var.model_version
      FRAUD_THRESHOLD  = var.fraud_threshold
    }
  }

  vpc_config {
    security_group_ids = [aws_security_group.sagemaker.id]
    subnets            = var.private_subnet_ids
  }

  tags = local.tags
}

# ── Endpoint Configuration ────────────────────────────────────────────────────
resource "aws_sagemaker_endpoint_configuration" "this" {
  name = "${var.name_prefix}-fraud-endpoint-config"

  production_variants {
    variant_name           = "primary"
    model_name             = aws_sagemaker_model.this.name
    initial_instance_count = var.initial_instance_count
    instance_type          = var.instance_type

    # Keep instances warm — eliminates cold starts at 30k RPS
    initial_variant_weight = 1
  }

  data_capture_config {
    enable_capture              = true
    initial_sampling_percentage = var.data_capture_sampling_percentage
    destination_s3_uri          = "s3://${aws_s3_bucket.data_capture.bucket}/captures"

    capture_options {
      capture_mode = "Input"
    }
    capture_options {
      capture_mode = "Output"
    }

    capture_content_type_header {
      json_content_types = ["application/json"]
    }
  }

  tags = local.tags
}

# ── Endpoint ──────────────────────────────────────────────────────────────────
resource "aws_sagemaker_endpoint" "this" {
  name                 = "${var.name_prefix}-fraud-endpoint"
  endpoint_config_name = aws_sagemaker_endpoint_configuration.this.name
  tags                 = local.tags
}

# ── Auto-scaling ──────────────────────────────────────────────────────────────
resource "aws_appautoscaling_target" "this" {
  max_capacity       = var.max_capacity
  min_capacity       = var.min_capacity
  resource_id        = "endpoint/${aws_sagemaker_endpoint.this.name}/variant/primary"
  scalable_dimension = "sagemaker:variant:DesiredInstanceCount"
  service_namespace  = "sagemaker"
}

resource "aws_appautoscaling_policy" "invocations" {
  name               = "${var.name_prefix}-sagemaker-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.this.resource_id
  scalable_dimension = aws_appautoscaling_target.this.scalable_dimension
  service_namespace  = aws_appautoscaling_target.this.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "SageMakerVariantInvocationsPerInstance"
    }
    # ml.c6i.xlarge 跑 XGBoost 9 features，保守估算 3k invocations/min/instance
    target_value       = 3000
    scale_in_cooldown  = 300
    scale_out_cooldown = 60
  }
}
