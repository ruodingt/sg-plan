resource "aws_secretsmanager_secret" "slack_webhook" {
  name        = "${local.prefix}/slack-webhook-url"
  description = "Slack incoming webhook URL for fraud alerts"
}

resource "aws_secretsmanager_secret_version" "slack_webhook" {
  secret_id     = aws_secretsmanager_secret.slack_webhook.id
  secret_string = jsonencode({ webhook_url = var.slack_webhook_url })
}

resource "aws_sqs_queue" "fraud_alerts_dlq" {
  name                      = "${local.prefix}-fraud-alerts-dlq"
  message_retention_seconds = 1_209_600
}

resource "aws_sqs_queue" "fraud_alerts" {
  name                       = "${local.prefix}-fraud-alerts"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 86_400

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.fraud_alerts_dlq.arn
    maxReceiveCount     = 3
  })
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals { type = "Service"; identifiers = ["lambda.amazonaws.com"] }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${local.prefix}-slack-alerter-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_sqs" {
  name = "sqs-consume"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
      Resource = aws_sqs_queue.fraud_alerts.arn
    }]
  })
}

resource "aws_iam_role_policy" "lambda_secrets" {
  name = "read-slack-webhook"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = aws_secretsmanager_secret.slack_webhook.arn
    }]
  })
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.prefix}-slack-alerter"
  retention_in_days = 14
}

data "archive_file" "handler" {
  type        = "zip"
  source_file = "${path.module}/lambda_src/handler.py"
  output_path = "${path.module}/lambda_src/handler.zip"
}

resource "aws_lambda_function" "slack_alerter" {
  function_name    = "${local.prefix}-slack-alerter"
  role             = aws_iam_role.lambda.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  filename         = data.archive_file.handler.output_path
  source_code_hash = data.archive_file.handler.output_base64sha256
  timeout          = 15

  reserved_concurrent_executions = 10

  environment {
    variables = {
      SLACK_SECRET_ARN = aws_secretsmanager_secret.slack_webhook.arn
      ALERT_THRESHOLD  = var.alert_threshold
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
}

resource "aws_lambda_event_source_mapping" "sqs_to_lambda" {
  event_source_arn                   = aws_sqs_queue.fraud_alerts.arn
  function_name                      = aws_lambda_function.slack_alerter.arn
  batch_size                         = 10
  maximum_batching_window_in_seconds = 5
  function_response_types            = ["ReportBatchItemFailures"]
}

resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "${local.prefix}-fraud-alerts-dlq-depth"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Messages stuck in the fraud-alerts DLQ"
  dimensions          = { QueueName = aws_sqs_queue.fraud_alerts_dlq.name }
}
