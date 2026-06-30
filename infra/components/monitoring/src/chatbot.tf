resource "aws_iam_role" "chatbot" {
  name = "fraud-chatbot-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "chatbot.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "chatbot_cw" {
  role       = aws_iam_role.chatbot.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess"
}

# ML team — drift violations, model quality degradation
resource "aws_chatbot_slack_channel_configuration" "ml_monitoring" {
  configuration_name = "fraud-ml-monitoring-${var.environment}"
  iam_role_arn       = aws_iam_role.chatbot.arn
  slack_workspace_id = var.slack_workspace_id
  slack_channel_id   = var.slack_ml_channel_id
  sns_topic_arns     = [aws_sns_topic.ml_alerts.arn]
  guardrail_policy_arns = ["arn:aws:iam::aws:policy/ReadOnlyAccess"]
}

# Platform team — latency, errors, volume anomalies
resource "aws_chatbot_slack_channel_configuration" "tech_ops" {
  configuration_name = "fraud-tech-ops-${var.environment}"
  iam_role_arn       = aws_iam_role.chatbot.arn
  slack_workspace_id = var.slack_workspace_id
  slack_channel_id   = var.slack_tech_channel_id
  sns_topic_arns     = [aws_sns_topic.tech_alerts.arn]
  guardrail_policy_arns = ["arn:aws:iam::aws:policy/ReadOnlyAccess"]
}
